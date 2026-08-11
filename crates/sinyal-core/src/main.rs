//! `sinyald` — Sinyal çekirdeği.
//!
//! MT5 terminallerindeki EA'ların paylaşımlı belleğine bağlanır, tick ve
//! derinlik akışını WebSocket üzerinden dağıtır, karşılığında emir kabul eder.
//!
//! ```text
//! sinyald --instance mt5-1 --bind 127.0.0.1:8787
//! sinyald --instance icmarkets --instance pepperstone --enable-trading --token gizli
//! ```

mod candles;
mod hist_bridge;
mod history;
mod join;
mod server;
mod source;
mod state;
mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use server::{Ctx, OrderTracker};
use source::{spawn_reader, ReaderStats};
use state::Registry;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

struct Args {
    instances: Vec<String>,
    bind: String,
    token: Option<String>,
    trading: bool,
    allow_live: bool,
    deviation: u32,
    /// Yayın kanalı kapasitesi — yavaş istemci bu kadar mesaj geriye düşebilir.
    capacity: usize,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut a = Args {
            instances: Vec::new(),
            // Varsayılan localhost: bu uç EMİR YÜRÜTEBİLİYOR, kazara ağa
            // açılması kabul edilemez.
            bind: "127.0.0.1:8787".into(),
            token: None,
            trading: false,
            allow_live: false,
            deviation: 20,
            capacity: 8192,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            let k = argv[i].as_str();
            let val = || -> Result<String, String> {
                argv.get(i + 1).cloned().ok_or_else(|| format!("{k} bir değer bekliyor"))
            };
            match k {
                "--instance" => {
                    a.instances.push(val()?);
                    i += 2;
                }
                "--bind" => {
                    a.bind = val()?;
                    i += 2;
                }
                "--token" => {
                    a.token = Some(val()?);
                    i += 2;
                }
                "--deviation" => {
                    a.deviation = val()?.parse().map_err(|e| format!("--deviation: {e}"))?;
                    i += 2;
                }
                "--capacity" => {
                    a.capacity = val()?.parse().map_err(|e| format!("--capacity: {e}"))?;
                    i += 2;
                }
                "--enable-trading" => {
                    a.trading = true;
                    i += 1;
                }
                "--allow-live" => {
                    a.allow_live = true;
                    i += 1;
                }
                "--generate-token" => {
                    // Sistem CSPRNG'si; zayif bir degere DUSMEZ.
                    let mut buf = [0u8; 32];
                    match sinyal_shm::random_bytes(&mut buf) {
                        Some(()) => {
                            println!("{}", sinyal_shm::base64url(&buf));
                            std::process::exit(0);
                        }
                        None => {
                            eprintln!(
                                "hata: sistem rastgelelik uretemedi. Tahmin edilebilir bir \
                                 token uretmektense hic uretmemek dogru."
                            );
                            std::process::exit(1);
                        }
                    }
                }
                "--help" | "-h" => {
                    usage();
                    std::process::exit(0);
                }
                other => return Err(format!("bilinmeyen argüman: {other}")),
            }
        }
        if a.instances.is_empty() {
            a.instances.push("mt5-1".into());
        }
        Ok(a)
    }
}

fn usage() {
    eprintln!(
        "sinyald — Sinyal çekirdeği (MT5 -> WebSocket feed + emir yürütme)

KULLANIM:
  sinyald [SEÇENEKLER]

SEÇENEKLER:
  --instance ID       Bağlanılacak EA örneği. Birden çok kez verilebilir.
                      (varsayılan: mt5-1)
  --bind ADDR         Dinlenecek adres (varsayılan: 127.0.0.1:8787)
  --token GIZLI       Kimlik doğrulama token'ı. Verilmezse doğrulama YOK.
  --generate-token    Guclu rastgele token uretip basar ve cikar
  --enable-trading    Emir yürütmeyi aç (varsayılan KAPALI)
  --allow-live        DEMO OLMAYAN hesapta emir yürütmeye izin ver.
                      Varsayılan kapalı; hesap tipi okunamazsa da kapalı
                      kabul edilir (emniyetli taraf).
  --deviation N       Varsayılan kayma toleransı, point (varsayılan 20)
  --capacity N        Yayın kuyruğu boyutu (varsayılan 8192)

GÜVENLİK:
  Bu uç EMİR YÜRÜTEBİLİR. Varsayılan olarak yalnızca 127.0.0.1'i dinler ve
  emir yürütme kapalıdır. Ağa açacaksan --token KULLAN."
    );
}

#[tokio::main]
async fn main() {
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("hata: {e}\n");
            usage();
            std::process::exit(2);
        }
    };

    let registry = Arc::new(Registry::new());
    let candles = Arc::new(Mutex::new(candles::CandleStore::new()));
    let (tx, _) = broadcast::channel(args.capacity);

    let mut cmd_tx = HashMap::new();
    let mut hist_tx = HashMap::new();
    let mut stats_all = Vec::new();
    for inst in &args.instances {
        let (ctx_tx, ctx_rx) = std::sync::mpsc::channel();
        // Geçmiş isteği ayrı kanal: emir kuyruğuyla aynı yolu paylaşsaydı
        // binlerce barlık bir geçmiş turu emir gönderimini geciktirebilirdi.
        let (h_tx, h_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(Mutex::new(ReaderStats::default()));
        spawn_reader(
            inst.clone(),
            registry.clone(),
            tx.clone(),
            ctx_rx,
            h_rx,
            stats.clone(),
            candles.clone(),
        );
        cmd_tx.insert(inst.clone(), ctx_tx);
        hist_tx.insert(inst.clone(), h_tx);
        stats_all.push((inst.clone(), stats));
    }

    let ctx = Arc::new(Ctx {
        registry: registry.clone(),
        events: tx,
        cmd_tx,
        hist_tx,
        token: args.token.clone(),
        trading: args.trading,
        allow_live: args.allow_live,
        deviation: args.deviation,
        orders: Arc::new(OrderTracker::new()),
        candles: candles.clone(),
        // `candles` token istemez ve uç ağa açılabilir; geçmiş çekmek ticaret
        // terminalini meşgul eder. Tavan tüm bağlantılar için ORTAKTIR.
        hist_slots: Arc::new(tokio::sync::Semaphore::new(server::HIST_SLOTS)),
    });

    let listener = match TcpListener::bind(&args.bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hata: {} dinlenemedi: {e}", args.bind);
            std::process::exit(1);
        }
    };

    println!("sinyald dinliyor: ws://{}", args.bind);
    println!("  örnekler : {}", args.instances.join(", "));
    println!("  ticaret  : {}", if args.trading { "AÇIK" } else { "kapalı" });
    println!(
        "  kimlik   : {}",
        if args.token.is_some() { "token gerekli" } else { "YOK (açık uç)" }
    );
    let public_bind = !args.bind.starts_with("127.");
    if public_bind {
        println!();
        println!("  Piyasa verisi (tick/derinlik/mum/sembol) TOKEN'SIZ acik.");
        if args.token.is_some() {
            println!("  Hesap ve emir islemleri token istiyor.");
            println!("  NOT: ws:// duz metin — token ag uzerinde acik gider.");
        } else {
            eprintln!("  UYARI: token YOK ve localhost disina baglaniyorsun.");
            eprintln!("         Emir yurutme herkese acik olur. --token KULLAN.");
        }
    }

    // Periyodik durum raporu: akışın canlı olup olmadığını görmenin en hızlı
    // yolu. Sessiz bir daemon, "çalışıyor mu bilmiyorum" demektir.
    let telemetry_candles = candles.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        tick.tick().await;
        loop {
            tick.tick().await;
            let (tick_bars, mt5_bars) = {
                let cs = telemetry_candles.lock().unwrap_or_else(|e| e.into_inner());
                cs.bar_counts()
            };
            for (inst, st) in &stats_all {
                let s = *st.lock().unwrap_or_else(|e| e.into_inner());
                if !s.connected {
                    println!("[{inst}] EA bekleniyor...");
                    continue;
                }
                println!(
                    "[{inst}] tick={} dom={} emir={} bar={}(mt5={}) | EA-kayip={} birikmis={} \
                     | eslesme: bekleyen={} gec={} kimliksiz={} \
                     | gecmis: {} istek={} bekleyen={} eksik={} hata={} zamanasimi={}",
                    s.ticks,
                    s.books,
                    s.orders,
                    tick_bars,
                    mt5_bars,
                    s.ea_tick_loss,
                    s.backlog,
                    s.join_pending,
                    s.join_late,
                    s.join_unattributed,
                    if s.hist_ready { "acik" } else { "kapali" },
                    s.hist_reqs,
                    s.hist_pending,
                    s.hist_incomplete,
                    s.hist_failed,
                    s.hist_timeout,
                );
                // Eksik teslimat = bar halkası dolmuş demektir ve barlar
                // KALICI kaybolmuştur; sessiz kalması grafiği delikli
                // bırakırdı.
                if s.hist_incomplete > 0 {
                    println!(
                        "[{inst}] UYARI: {} gecmis istegi EKSIK teslim edildi \
                         — bar halkasi dolmus olabilir.",
                        s.hist_incomplete
                    );
                }
                if s.ea_tick_loss > 0 {
                    println!("[{inst}] UYARI: EA halkaya yazamadı — tick KAYBEDİLDİ.");
                }
                // Kimliksiz olay her zaman hata değil (terminalden elle yapılan
                // işlemler de böyle görünür), ama emir gönderiyorsak ve sayaç
                // artıyorsa korelasyon bozuk demektir — sessiz kalmasın.
                if s.join_unattributed > 0 && s.orders > 0 {
                    println!(
                        "[{inst}] NOT: {} emir olayi hicbir komuta baglanamadi \
                         (elle islem yaptiysan normal).",
                        s.join_unattributed
                    );
                }
            }
        }
    });

    let srv = server::serve(listener, ctx);
    tokio::select! {
        _ = srv => {}
        _ = tokio::signal::ctrl_c() => println!("\nkapatılıyor..."),
    }
}
