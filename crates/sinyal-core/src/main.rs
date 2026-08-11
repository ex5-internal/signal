//! `sinyald` — Sinyal çekirdeği.
//!
//! MT5 terminallerindeki EA'ların paylaşımlı belleğine bağlanır, tick ve
//! derinlik akışını WebSocket üzerinden dağıtır, karşılığında emir kabul eder.
//!
//! ```text
//! sinyald --instance mt5-1 --bind 127.0.0.1:8787
//! sinyald --instance icmarkets --instance pepperstone --enable-trading --token gizli
//! sinyald --instance mt5-1 --record ./veri
//! sinyald --replay ./veri --replay-date 20260811 --replay-speed 0
//! ```
//!
//! # İki kip, tek protokol
//!
//! **CANLI** (`--instance`): paylaşımlı bellekten okur, emirleri gerçekten
//! yürütür.
//!
//! **REPLAY** (`--replay`): diskteki kayıttan okur, emirleri **simüle** eder
//! ve paylaşımlı belleğe **hiç dokunmaz** — okuyucu thread'i bile
//! başlatılmaz, komut kanalı hiç kurulmaz.
//!
//! İkisi aynı anda kullanılamaz ve bu bir kolaylık kuralı değil: aynı
//! süreçte hem kayıttan oynatıp hem gerçek emir gönderebilmek, "test
//! ediyordum" diye başlayan bir kaza hikâyesinin ilk cümlesi olurdu.
//! `--replay` ile `--instance` birlikte verilirse çekirdek AÇIK bir hatayla
//! durur.
//!
//! Tel üzerindeki fark yalnızca `hello.mode` (`live` / `replay`), `hello`daki
//! kapsam alanları ve simüle emir olaylarındaki `sim: true`dur. Mesaj
//! biçimleri, alan adları ve kanal adları birebir aynıdır: amaç sinyal
//! sisteminin kod yolunun değişmemesi.

mod candles;
mod hist_bridge;
mod history;
mod join;
mod record;
mod replay;
mod server;
mod sim;
mod source;
mod state;
mod wire;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use server::{Ctx, OrderTracker, Replay};
use source::{spawn_reader, ReaderStats};
use state::Registry;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// Bir gün, ms — replay kapsamını gün içi saate çevirmek için.
const DAY_MS: i64 = 86_400_000;

/// `--replay-balance` verilmediğinde sentetik başlangıç bakiyesi.
const DEFAULT_REPLAY_BALANCE: f64 = 10_000.0;

/// Replay kipinin çözülmüş ayarları.
struct ReplayCfg {
    opts: replay::ReplayOpts,
    /// Sentetik başlangıç bakiyesi (`--replay-balance`).
    balance: f64,
}

/// Ham replay bayrakları; çapraz denetimler ayrıştırma bitince yapılır.
#[derive(Default)]
struct RawReplay {
    dir: Option<String>,
    date: Option<String>,
    from: Option<String>,
    to: Option<String>,
    speed: Option<f64>,
    balance: Option<f64>,
}

impl RawReplay {
    /// `--replay` dışındaki replay bayraklarından biri verildi mi.
    fn any_detail(&self) -> bool {
        self.date.is_some()
            || self.from.is_some()
            || self.to.is_some()
            || self.speed.is_some()
            || self.balance.is_some()
    }
}

struct Args {
    instances: Vec<String>,
    bind: String,
    token: Option<String>,
    trading: bool,
    allow_live: bool,
    deviation: u32,
    /// Yayın kanalı kapasitesi — yavaş istemci bu kadar mesaj geriye düşebilir.
    capacity: usize,
    /// Verilmişse tick akışı bu dizine kaydedilir (bkz. `record.rs`).
    record: Option<PathBuf>,
    /// `Some` ise REPLAY kipi (bkz. `replay.rs`); paylaşımlı belleğe hiç
    /// bağlanılmaz.
    replay: Option<ReplayCfg>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        Self::from_argv(std::env::args().skip(1).collect())
    }

    /// Ayrıştırma + kip çapraz denetimleri.
    ///
    /// `std::env::args()`'tan ayrı tutuldu: `--replay` ile `--instance`in
    /// birlikte reddedilmesi bir kolaylık değil güvenlik sınırı ve
    /// sınanabilir olmalı.
    fn from_argv(argv: Vec<String>) -> Result<Self, String> {
        let mut raw = RawReplay::default();
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
            record: None,
            replay: None,
        };
        let mut i = 0;
        while i < argv.len() {
            let k = argv[i].as_str();
            let val = || -> Result<String, String> {
                argv.get(i + 1).cloned().ok_or_else(|| format!("{k} bir değer bekliyor"))
            };
            let num = || -> Result<f64, String> {
                let s = argv.get(i + 1).ok_or_else(|| format!("{k} bir değer bekliyor"))?;
                s.parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite())
                    .ok_or_else(|| format!("{k}: '{s}' bir sayı değil"))
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
                "--record" => {
                    a.record = Some(PathBuf::from(val()?));
                    i += 2;
                }
                "--replay" => {
                    raw.dir = Some(val()?);
                    i += 2;
                }
                "--replay-date" => {
                    raw.date = Some(val()?);
                    i += 2;
                }
                "--replay-from" => {
                    raw.from = Some(val()?);
                    i += 2;
                }
                "--replay-to" => {
                    raw.to = Some(val()?);
                    i += 2;
                }
                "--replay-speed" => {
                    raw.speed = Some(num()?);
                    i += 2;
                }
                "--replay-balance" => {
                    raw.balance = Some(num()?);
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
        if let Some(dir) = raw.dir.take() {
            // --- REPLAY kipi ---
            //
            // Bu iki bayrağın birlikte bir anlamı yok ve sessizce birini
            // seçmek tehlikeli: operatör canlıya bağlandığını sanarken kayıt
            // oynatıyor (ya da tersi) olurdu.
            if !a.instances.is_empty() {
                return Err(
                    "--replay ile --instance BİRLİKTE kullanılamaz: replay kipinde \
                     paylaşımlı belleğe hiç bağlanılmaz ve örnek listesi kayıt dizininden \
                     okunur. Canlı için --instance, kayıttan oynatım için --replay kullan."
                        .into(),
                );
            }
            if a.record.is_some() {
                return Err(
                    "--replay ile --record birlikte kullanılamaz: bir kaydın oynatımını \
                     yeniden kaydetmek yeni veri değil, kaydın kopyasını üretirdi."
                        .into(),
                );
            }
            let Some(date) = raw.date else {
                return Err(
                    "--replay --replay-date YYYYMMDD ister (gün sınırı UTC). Hangi günün \
                     kaydının oynatılacağını tahmin etmiyoruz."
                        .into(),
                );
            };
            // Biçim hatasını diske dokunmadan yakala: "20260811" yerine
            // "2026-08-11" yazan biri hatayı hemen görmeli.
            replay::parse_yyyymmdd(&date).map_err(|e| format!("--replay-date: {e}"))?;
            for (flag, v) in [("--replay-from", &raw.from), ("--replay-to", &raw.to)] {
                if let Some(s) = v {
                    replay::parse_hhmm(s).map_err(|e| format!("{flag}: {e}"))?;
                }
            }
            let speed = raw.speed.unwrap_or(1.0);
            if speed < 0.0 {
                return Err(format!(
                    "--replay-speed negatif olamaz (0 = beklemeden en hızlı), verilen: {speed}"
                ));
            }
            let balance = raw.balance.unwrap_or(DEFAULT_REPLAY_BALANCE);
            if balance < 0.0 {
                return Err(format!("--replay-balance negatif olamaz, verilen: {balance}"));
            }
            a.replay = Some(ReplayCfg {
                opts: replay::ReplayOpts {
                    data_dir: PathBuf::from(dir),
                    date,
                    from: raw.from,
                    to: raw.to,
                    speed,
                },
                balance,
            });
            return Ok(a);
        }

        // --- CANLI kip ---
        if raw.any_detail() {
            // Sessizce yok saymak, "--replay yazmayı unuttum" hatasını canlı
            // kipte fark edilmez yapardı.
            return Err(
                "--replay-* bayrakları yalnızca --replay ile kullanılır; canlı kipte \
                 karşılıkları yok."
                    .into(),
            );
        }
        if a.instances.is_empty() {
            a.instances.push("mt5-1".into());
        }
        // Kayıt dizininin tek yazar kilidi DİZİN düzeyindedir
        // (`<data-dir>/.lock`): ikinci bir kaydedici aynı dizine giremez.
        // Bunu çalışma zamanında "kilit meşgul" hatasıyla keşfetmek yerine
        // burada söylüyoruz — o hata kendi ikinci örneğimizden geliyorsa
        // operatörü yanlış yere bakmaya yönlendirirdi.
        if a.record.is_some() && a.instances.len() > 1 {
            return Err(
                "--record tek bir --instance ile kullanılabilir: kayıt dizininin tek yazar \
                 kilidi dizin düzeyindedir. Birden fazla terminali kaydetmek için her biri \
                 için AYRI bir --record dizini ve ayrı bir sinyald çalıştır."
                    .into(),
            );
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
  --record DIZIN      Tick akışını DIZIN altına kaydet (append-only):
                        DIZIN/<instance>/ticks-YYYYMMDD.bin   (48 bayt/kayıt)
                        DIZIN/<instance>/symbols-YYYYMMDD.jsonl
                        DIZIN/.lock                           (tek yazar)
                      Gün sınırı UTC. Kayıt AYRI bir thread'de yazılır;
                      yazıcı yetişemezse tick DÜŞÜRÜLÜR ve sayılır — canlı
                      akış kayıttan önceliklidir. Tek --instance ile.

REPLAY (kayıttan oynatım — paylaşımlı belleğe HİÇ dokunmaz):
  --replay DIZIN      Kaydın kök dizini. --instance ile KULLANILAMAZ.
  --replay-date GUN   YYYYMMDD (ZORUNLU, gün sınırı UTC)
  --replay-from HH:MM Kapsam başlangıcı, UTC (istege bagli)
  --replay-to   HH:MM Kapsam sonu, UTC (istege bagli)
  --replay-speed N    1.0 = gerçek zamanlı (varsayılan), 0 = beklemeden en hızlı
  --replay-balance N  Sentetik başlangıç bakiyesi (varsayılan 10000)

  hello.mode replay'de \"replay\" olur ve hello kapsamı (replay_from_ms /
  replay_to_ms) ilan eder; başka hiçbir mesaj biçimi değişmez.

  Emirler REDDEDİLMEZ, SİMÜLE edilir: dolum o andaki KAYITLI bid/ask'ten
  yapılır (alış -> ask, satış -> bid) ve her emir olayı `sim: true` taşır.
  Kayma, komisyon ve swap MODELLENMEZ; bekleyen emirler kendiliğinden
  tetiklenmez. Simüle dolum GERÇEK DOLUM DEĞİLDİR.

GÜVENLİK:
  Bu uç EMİR YÜRÜTEBİLİR. Varsayılan olarak yalnızca 127.0.0.1'i dinler ve
  emir yürütme kapalıdır. Ağa açacaksan --token KULLAN."
    );
}

/// Epoch ms → gün içi `HH:MM:SS.mmm` (UTC).
///
/// Kapsam tek gün içinde olduğu için tarihi tekrar yazmıyoruz; tarih zaten
/// `--replay-date`.
fn hhmmss_utc(ms: i64) -> String {
    let t = ms.rem_euclid(DAY_MS);
    format!("{:02}:{:02}:{:02}.{:03}", t / 3_600_000, (t / 60_000) % 60, (t / 1000) % 60, t % 1000)
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

    // Kayıt AKIŞTAN ÖNCE kurulur ve başlatılamazsa çıkılır: "kaydediyorum"
    // sanıp kaydetmemek, günler sonra boş bir dizin bulmak demektir.
    // (Örnek sayısı `Args::parse` içinde 1'e sınırlandı; replay kipinde
    // `--record` zaten reddedilir, bu yüzden burada örnek listesi doludur.)
    let recorder = match &args.record {
        Some(dir) => match record::Recorder::start(dir, &args.instances[0]) {
            Ok(h) => {
                println!("kayit dizini: {}", h.dir().display());
                Some(Arc::new(h))
            }
            Err(e) => {
                eprintln!("hata: kayit baslatilamadi: {e}");
                std::process::exit(1);
            }
        },
        None => None,
    };

    let mut cmd_tx = HashMap::new();
    let mut hist_tx = HashMap::new();
    let mut stats_all = Vec::new();
    let mut replay_ctx: Option<Arc<Replay>> = None;
    // Oynatım tutamağı: düşürmek thread'i durdurmaz ama bitişi bekleyebilmek
    // ve kapanışta tanılama yazabilmek için elde tutuluyor.
    let mut replay_run = None;

    match &args.replay {
        // --- REPLAY: paylaşımlı belleğe HİÇ dokunulmaz ---
        //
        // Okuyucu thread'i başlatılmaz, komut kanalı hiç kurulmaz. Bu İLK
        // savunma hattı: gönderilebilecek bir kanal yok. İkincisi sunucu
        // tarafında (`Ctx::replay` → `dispatch` kilidi).
        Some(cfg) => {
            let rec = match replay::load(&cfg.opts) {
                Ok(r) => r,
                Err(e) => {
                    // Bozuk/eksik kaydın sessizce boş akışa dönüşmesi,
                    // "sistem çalışıyor ama piyasa hareketsiz" yanılsaması
                    // üretirdi.
                    eprintln!("hata: kayit yuklenemedi: {e}");
                    std::process::exit(1);
                }
            };
            let (from_ms, to_ms) = rec.span();
            let instances = rec.instances.clone();
            let items = rec.len();

            let (done_tx, done_rx) = tokio::sync::watch::channel(None);
            let run =
                replay::spawn(rec, registry.clone(), candles.clone(), tx.clone(), done_tx);
            // Oynatım kapısı sunucuya veriliyor: ilk istemci bağlanınca açılır.
            replay_ctx = Some(Arc::new(Replay::new(
                &instances,
                Some(from_ms),
                Some(to_ms),
                cfg.balance,
                done_rx,
                run.gate(),
            )));
            replay_run = Some(run);

            println!("sinyald REPLAY kipinde — diskteki kayittan oynatiliyor");
            println!("  kayit    : {} ({})", cfg.opts.data_dir.display(), cfg.opts.date);
            println!("  ornekler : {}", instances.join(", "));
            println!(
                "  kapsam   : {} .. {} UTC ({items} tick)",
                hhmmss_utc(from_ms),
                hhmmss_utc(to_ms)
            );
            println!(
                "  tempo    : {}",
                if cfg.opts.speed == 0.0 {
                    "beklemeden en hizli".to_string()
                } else {
                    format!("{}x", cfg.opts.speed)
                }
            );
            println!("  bakiye   : {} (SENTETIK)", cfg.balance);
        }

        // --- CANLI ---
        None => {
            for inst in &args.instances {
                let (ctx_tx, ctx_rx) = std::sync::mpsc::channel();
                // Geçmiş isteği ayrı kanal: emir kuyruğuyla aynı yolu
                // paylaşsaydı binlerce barlık bir geçmiş turu emir gönderimini
                // geciktirebilirdi.
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
                    recorder.clone(),
                );
                cmd_tx.insert(inst.clone(), ctx_tx);
                hist_tx.insert(inst.clone(), h_tx);
                stats_all.push((inst.clone(), stats));
            }
        }
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
        // `None` ise CANLI kip: akış paylaşımlı bellekten geliyor ve emirler
        // gerçekten yürütülür. `Some` ise sunucu hiçbir komut göndermez.
        replay: replay_ctx.clone(),
    });

    let listener = match TcpListener::bind(&args.bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hata: {} dinlenemedi: {e}", args.bind);
            std::process::exit(1);
        }
    };

    println!("sinyald dinliyor: ws://{}", args.bind);
    if replay_ctx.is_none() {
        println!("  örnekler : {}", args.instances.join(", "));
        println!("  ticaret  : {}", if args.trading { "AÇIK" } else { "kapalı" });
    }
    println!(
        "  kimlik   : {}",
        if args.token.is_some() { "token gerekli" } else { "YOK (açık uç)" }
    );
    if replay_ctx.is_some() {
        println!();
        println!("  hello.mode = \"replay\" — istemci canli sanmamali.");
        println!("  Emirler SIMULE edilir; her emir olayi `sim: true` tasir.");
        println!("  Paylasilan bellege HICBIR komut gitmez: gercek emir imkansiz.");
        println!("  UYARI: kayma, komisyon ve swap MODELLENMIYOR; bekleyen emirler");
        println!("         kendiliginden tetiklenmez. Simule dolum GERCEK DOLUM DEGILDIR.");
    }
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

    // Replay bitişini konsola da yaz: `replay_done` istemciye gider ama
    // daemon'u terminalden izleyen kişi de "bitti mi" sorusunun cevabını
    // görebilmeli.
    if let Some(r) = &replay_ctx {
        let mut rx = r.done.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let end = *rx.borrow();
                if let Some(end) = end {
                    println!(
                        "[replay] oynatim bitti: {} tick, son broker saati {} UTC",
                        end.ticks,
                        hhmmss_utc(end.last_ms)
                    );
                    break;
                }
            }
        });
    }

    // Periyodik durum raporu: akışın canlı olup olmadığını görmenin en hızlı
    // yolu. Sessiz bir daemon, "çalışıyor mu bilmiyorum" demektir. Replay'de
    // okuyucu da sayaç da yok; orada bu döngü hiç kurulmaz.
    let telemetry_candles = candles.clone();
    tokio::spawn(async move {
        if stats_all.is_empty() {
            return;
        }
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
                if s.rec_on {
                    println!(
                        "[{inst}] kayit: yazilan={} dusen={} hata={} sembol-satiri={}",
                        s.rec_written, s.rec_dropped, s.rec_errors, s.rec_symbol_lines
                    );
                    // Düşen kayıt = kayıt dosyasında DELİK. Canlı akışı
                    // korumak için bilerek düşürüyoruz, ama sessiz kalması
                    // aylar sonra "kayıt neden eksik" sorusuna dönüşürdü.
                    if s.rec_dropped > 0 {
                        println!(
                            "[{inst}] UYARI: {} tick kayda YAZILAMADI (yazici yetisemedi \
                             ya da disk hatasi) — kayitta bu kadar eksik var.",
                            s.rec_dropped
                        );
                    }
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

    // Tamponda ≤200 ms'lik kayıt bekliyor olabilir. Süreç bunu yazmadan
    // ölürse elle durdurulan her daemon kaydın son parçasını yer.
    if let Some(r) = &recorder {
        r.stop();
        let c = r.counts();
        println!("kayit kapatildi: yazilan={} dusen={} hata={}", c.written, c.dropped, c.errors);
    }

    // Oynatım hâlâ sürüyor olabilir; tutamağı burada bırakıyoruz. Thread
    // sürecin kapanmasıyla birlikte gider — yarıda kesilen bir replay'in
    // temizleyecek durumu yok (diske hiçbir şey yazmıyor).
    drop(replay_run.take());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn err(items: &[&str]) -> String {
        match Args::from_argv(argv(items)) {
            Ok(_) => panic!("kabul edilmemeliydi: {items:?}"),
            Err(e) => e,
        }
    }

    #[test]
    fn replay_and_instance_together_is_a_clear_error() {
        // Bu bir kolaylık denetimi DEĞİL: aynı süreçte hem kayıttan oynatıp
        // hem canlı emir gönderebilmek kabul edilemez bir kaza yüzeyi.
        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--instance", "mt5-1"]);
        assert!(e.contains("--replay"), "hata hangi bayrak oldugunu soylemeli: {e}");
        assert!(e.contains("--instance"), "hata --instance'i anmali: {e}");

        // Sıra fark etmemeli.
        let e = err(&["--instance", "mt5-1", "--replay", "veri", "--replay-date", "20260811"]);
        assert!(e.contains("--instance") && e.contains("--replay"), "{e}");
    }

    #[test]
    fn replay_requires_a_date() {
        let e = err(&["--replay", "veri"]);
        assert!(e.contains("--replay-date"), "eksik bayrak adiyla anilmali: {e}");
    }

    #[test]
    fn a_malformed_date_or_time_is_caught_before_touching_the_disk() {
        let e = err(&["--replay", "veri", "--replay-date", "2026-08-11"]);
        assert!(e.contains("--replay-date"), "hangi bayrak oldugu soylenmeli: {e}");

        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--replay-from", "9-00"]);
        assert!(e.contains("--replay-from"), "hangi bayrak oldugu soylenmeli: {e}");
    }

    #[test]
    fn replay_flags_without_replay_are_refused() {
        // Sessizce yok saymak, "--replay yazmayı unuttum" hatasını canlı
        // kipte fark edilmez yapardı.
        for flags in [
            vec!["--replay-date", "20260811"],
            vec!["--replay-speed", "0"],
            vec!["--replay-balance", "500"],
            vec!["--replay-from", "09:00"],
            vec!["--replay-to", "17:00"],
        ] {
            let e = err(&flags);
            assert!(e.contains("--replay"), "{flags:?} icin acik hata bekleniyordu: {e}");
        }
    }

    #[test]
    fn replay_and_record_together_is_refused() {
        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--record", "veri2"]);
        assert!(e.contains("--record") && e.contains("--replay"), "{e}");
    }

    #[test]
    fn replay_defaults_are_real_time_and_ten_thousand() {
        let a = Args::from_argv(argv(&["--replay", "veri", "--replay-date", "20260811"])).unwrap();
        let cfg = a.replay.expect("replay kipi kurulmali");
        assert!((cfg.opts.speed - 1.0).abs() < 1e-12, "varsayilan gercek zamanli");
        assert!((cfg.balance - DEFAULT_REPLAY_BALANCE).abs() < 1e-12);
        assert_eq!(cfg.opts.date, "20260811");
        assert!(cfg.opts.from.is_none() && cfg.opts.to.is_none(), "kapsam varsayilani tum gun");
        // Replay'de örnek listesi KAYITTAN gelir; canlı varsayılanı eklenmez.
        assert!(a.instances.is_empty(), "replay'de --instance varsayilani olmamali");
        assert!(a.record.is_none());
    }

    #[test]
    fn replay_speed_zero_is_allowed_but_negative_is_not() {
        let a = Args::from_argv(argv(&[
            "--replay",
            "veri",
            "--replay-date",
            "20260811",
            "--replay-speed",
            "0",
        ]))
        .unwrap();
        assert_eq!(a.replay.unwrap().opts.speed, 0.0, "0 = beklemeden en hizli");

        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--replay-speed", "-1"]);
        assert!(e.contains("--replay-speed"), "{e}");
        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--replay-balance", "-5"]);
        assert!(e.contains("--replay-balance"), "{e}");
    }

    #[test]
    fn live_mode_still_defaults_to_one_instance() {
        // Geriye dönük davranış: bayraksız çalıştırma CANLI kipte kalmalı.
        let a = Args::from_argv(Vec::new()).unwrap();
        assert!(a.replay.is_none(), "bayraksiz calistirma canli kip");
        assert_eq!(a.instances, vec!["mt5-1".to_string()]);
        assert!(!a.trading, "emir yurutme varsayilan KAPALI");
    }

    #[test]
    fn a_flag_without_its_value_is_reported_not_ignored() {
        assert!(err(&["--replay"]).contains("--replay"));
        assert!(err(&["--replay-speed"]).contains("--replay-speed"));
        assert!(err(&[
            "--replay",
            "veri",
            "--replay-date",
            "20260811",
            "--replay-speed",
            "hizli"
        ])
        .contains("--replay-speed"));
    }

    #[test]
    fn day_time_formatting_is_utc_and_millisecond_exact() {
        assert_eq!(hhmmss_utc(3_661_123), "01:01:01.123");
        assert_eq!(hhmmss_utc(0), "00:00:00.000");
        // Tarih düşer, gün içi saat kalır — kapsam zaten tek gün.
        assert_eq!(hhmmss_utc(DAY_MS + 1_000), "00:00:01.000");
    }
}
