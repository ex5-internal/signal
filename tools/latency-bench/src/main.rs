//! Faz 1 kapısı: paylaşımlı bellek turunun gecikmesini ve tick kaybını ölçer.
//!
//! ```text
//! cargo run --release -p latency-bench -- run --count 1000000 --rate 0
//! cargo run --release -p latency-bench -- run --count 500000 --rate 20000
//! ```
//!
//! # Ne ölçülüyor, ne ölçülmüyor
//!
//! Ölçülen: üretici `SinyalPushTick`'i çağırdığı andan (halkaya yazılan
//! `recv_qpc` damgası) tüketicinin o tick'i **ayrı bir süreçte** okuduğu ana
//! kadar geçen süre. Bu, mimarinin asıl iddiasıdır.
//!
//! Ölçülmeyen: MT5 terminalinin tick'i alıp EA'ya teslim etmesi. Bunun için
//! gerçek terminal ve broker bağlantısı gerekir. EA'nın eklediği tek ek maliyet
//! süreç içi bir DLL çağrısıdır (onlarca nanosaniye); asıl belirsizlik
//! terminalin kendi teslim gecikmesindedir ve o ancak canlıda ölçülür.
//!
//! # Üretici davranışı
//!
//! Üretici **yeniden denemez** — üretimdeki EA gibi davranır: halka doluysa
//! tick'i bırakır ve sayacı artırır. Böylece raporlanan kayıp, gerçek üretim
//! kaybıyla aynı anlama gelir.

mod stats;

use std::process::{Command, Stdio};

use sinyal_bridge::{Capacities, Session};
use sinyal_proto::{kind, tick_flag, Tick};
use sinyal_shm::{qpc, qpc_delta_nanos};

use stats::{fmt_ns, Percentiles};

/// Faz 1 çıkış kriteri (plandan): p99 < 100 µs ve kayıp = 0.
const GATE_P99_NS: u64 = 100_000;

struct Args {
    role: String,
    instance: String,
    count: u64,
    /// Hedef tick/saniye. 0 = olabildiğince hızlı (tavan bulma).
    rate: u64,
    symbols: u32,
    tick_cap: u64,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut a = Args {
            role: "run".into(),
            instance: format!("bench-{}", std::process::id()),
            count: 1_000_000,
            rate: 0,
            symbols: 64,
            tick_cap: sinyal_proto::capacity::TICKS,
        };
        let mut i = 0;
        while i < argv.len() {
            let arg = argv[i].as_str();
            if i == 0 && !arg.starts_with("--") {
                a.role = arg.to_string();
                i += 1;
                continue;
            }
            let val = || -> Result<String, String> {
                argv.get(i + 1)
                    .cloned()
                    .ok_or_else(|| format!("{arg} bir değer bekliyor"))
            };
            match arg {
                "--instance" => {
                    a.instance = val()?;
                    i += 2;
                }
                "--count" => {
                    a.count = val()?.parse().map_err(|e| format!("--count: {e}"))?;
                    i += 2;
                }
                "--rate" => {
                    a.rate = val()?.parse().map_err(|e| format!("--rate: {e}"))?;
                    i += 2;
                }
                "--symbols" => {
                    a.symbols = val()?.parse().map_err(|e| format!("--symbols: {e}"))?;
                    i += 2;
                }
                "--tick-cap" => {
                    let v: u64 = val()?.parse().map_err(|e| format!("--tick-cap: {e}"))?;
                    if !v.is_power_of_two() {
                        return Err(format!("--tick-cap 2'nin kuvveti olmalı, verilen {v}"));
                    }
                    a.tick_cap = v;
                    i += 2;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("bilinmeyen argüman: {other}")),
            }
        }
        if a.count == 0 {
            return Err("--count 0 olamaz".into());
        }
        if a.symbols == 0 {
            return Err("--symbols 0 olamaz".into());
        }
        Ok(a)
    }

    fn caps(&self) -> Capacities {
        Capacities { ticks: self.tick_cap, ..Default::default() }
    }
}

fn print_usage() {
    eprintln!(
        "latency-bench — Sinyal Faz 1 gecikme/kayıp ölçümü

KULLANIM:
  latency-bench [run|producer|consumer] [SEÇENEKLER]

ROLLER:
  run        üretici ve tüketiciyi ayrı SÜREÇLER olarak başlatır (varsayılan)
  producer   yalnızca üretici (EA rolü — segmentleri oluşturur)
  consumer   yalnızca tüketici (çekirdek rolü — bağlanır, sonuçları basar)

SEÇENEKLER:
  --count N      gönderilecek tick sayısı (varsayılan 1000000)
  --rate R       hedef tick/saniye; 0 = tavan bul (varsayılan 0)
  --symbols S    kaç farklı sembol simüle edilsin (varsayılan 64)
  --tick-cap C   halka slot sayısı, 2'nin kuvveti (varsayılan 1048576)
  --instance ID  paylaşımlı bellek örnek adı

FAZ 1 KAPISI: p99 < 100 µs ve kayıp = 0"
    );
}

fn main() {
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("hata: {e}\n");
            print_usage();
            std::process::exit(2);
        }
    };

    let code = match args.role.as_str() {
        "producer" => run_producer(&args),
        "consumer" => run_consumer(&args),
        "run" => run_both(&args),
        other => {
            eprintln!("hata: bilinmeyen rol '{other}'\n");
            print_usage();
            2
        }
    };
    std::process::exit(code);
}

/// Üretici ve tüketiciyi ayrı süreçler olarak başlat.
///
/// Aynı süreçte iki thread çalıştırmak daha kolay olurdu ama üretimdeki durum
/// SÜREÇLER ARASI: EA MT5'in içinde, çekirdek ayrı bir süreçte. Ölçümün bu
/// sınırı gerçekten geçmesi gerekiyor.
fn run_both(args: &Args) -> i32 {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("hata: kendi yolu bulunamadı: {e}");
            return 1;
        }
    };

    println!("== Sinyal Faz 1 gecikme ölçümü ==");
    println!(
        "tick={} rate={} sembol={} halka={} örnek={}",
        args.count,
        if args.rate == 0 { "tavan".to_string() } else { args.rate.to_string() },
        args.symbols,
        args.tick_cap,
        args.instance
    );
    println!();

    // Üretici önce başlar: segmentlerin sahibi odur (EA rolü).
    let mut producer = match Command::new(&exe)
        .args(["producer", "--instance", &args.instance])
        .args(["--count", &args.count.to_string()])
        .args(["--rate", &args.rate.to_string()])
        .args(["--symbols", &args.symbols.to_string()])
        .args(["--tick-cap", &args.tick_cap.to_string()])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("hata: üretici başlatılamadı: {e}");
            return 1;
        }
    };

    let mut consumer = match Command::new(&exe)
        .args(["consumer", "--instance", &args.instance])
        .args(["--count", &args.count.to_string()])
        .args(["--tick-cap", &args.tick_cap.to_string()])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("hata: tüketici başlatılamadı: {e}");
            let _ = producer.kill();
            return 1;
        }
    };

    let pc = producer.wait().map(|s| s.code().unwrap_or(1)).unwrap_or(1);
    let cc = consumer.wait().map(|s| s.code().unwrap_or(1)).unwrap_or(1);
    if pc != 0 {
        eprintln!("üretici hata koduyla çıktı: {pc}");
    }
    if pc == 0 {
        cc
    } else {
        pc
    }
}

fn run_producer(args: &Args) -> i32 {
    let session = match Session::create(&args.instance, args.caps()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("üretici: oturum oluşturulamadı: {e}");
            return 1;
        }
    };

    // Tüketicinin bağlanmasına fırsat ver. Bağlanmadan başlarsak ilk tick'ler
    // dolu olmayan halkaya yazılır ama tüketici onları da okuyacağı için
    // ölçüm bozulmaz; yine de gerçekçi olması için kısa bir pencere bırakıyoruz.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let freq = sinyal_shm::qpc_frequency();
    // rate=0 → aralık 0 (tavan). Aksi halde tick başına düşen QPC tick sayısı.
    let interval = if args.rate == 0 { 0u64 } else { freq / args.rate.max(1) };

    let started = qpc();
    let mut next = started;
    let mut sent = 0u64;
    let mut lost = 0u64;

    for i in 0..args.count {
        if interval > 0 {
            next = next.wrapping_add(interval);
            // Meşgul bekleme: sleep'in çözünürlüğü (~1-15 ms) hedeflediğimiz
            // aralıkların yanında çok kaba kalır ve ölçüme kendi gürültüsünü
            // katar.
            while qpc() < next {
                std::hint::spin_loop();
            }
        }
        let symbol_id = (i % args.symbols as u64) as u32;
        let bid = 1.10000 + (i % 1000) as f64 * 0.00001;
        let t = Tick {
            time_msc: 1_700_000_000_000 + i as i64,
            bid,
            ask: bid + 0.00002,
            last: 0.0,
            volume_real: 0.0,
            // Damga TAM burada basılır — ölçmek istediğimiz an bu.
            recv_qpc: qpc(),
            symbol_id,
            flags: tick_flag::BID | tick_flag::ASK,
            kind: kind::TICK,
            _pad: 0,
        };
        // Güvenlik: tek üretici thread'i (bu thread).
        if unsafe { session.push_tick(&t) } {
            sent += 1;
        } else {
            // Üretimdeki EA gibi: YENİDEN DENEME, kaybı say.
            lost += 1;
        }
    }

    let elapsed_ns = qpc_delta_nanos(started, qpc());
    let secs = elapsed_ns as f64 / 1e9;
    println!(
        "üretici : gönderilen={sent} kayıp={lost} süre={:.3}s hız={:.0} tick/sn",
        secs,
        sent as f64 / secs.max(1e-9)
    );
    if lost > 0 {
        println!(
            "üretici : UYARI — {lost} tick KAYBEDİLDİ (halka doldu, tüketici yetişemedi)"
        );
    }

    // Tüketicinin son tick'leri okumasını bekle; erken çıkarsak segment yok
    // olur ve tüketici okuyamadan biter.
    let deadline = qpc() + sinyal_shm::qpc_frequency() * 10;
    while session.tick_backlog() > 0 && qpc() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    if session.tick_backlog() > 0 {
        eprintln!(
            "üretici : tüketici {} tick'i okumadan zaman aşımı",
            session.tick_backlog()
        );
    }
    0
}

fn run_consumer(args: &Args) -> i32 {
    // Üretici segmentleri henüz kurmamış olabilir — bu beklenen bir durum.
    let session = match open_with_retry(&args.instance, 10_000) {
        Some(s) => s,
        None => {
            eprintln!("tüketici: üreticiye bağlanılamadı (zaman aşımı)");
            return 1;
        }
    };

    let mut samples: Vec<u64> = Vec::with_capacity(args.count as usize);
    let mut batch = vec![Tick::default(); 1024];
    let mut received = 0u64;
    // Boşta geçen tur sayısı — üretici bittiğinde çıkmak için.
    let mut idle_spins = 0u64;
    // Yaklaşık 5 saniye boşta kalınca üreticinin bittiğini varsay.
    const IDLE_LIMIT: u64 = 200_000_000;

    let first_seen = loop {
        // Güvenlik: tek tüketici thread'i (bu thread).
        let n = unsafe { session.pop_tick_batch(&mut batch) };
        if n == 0 {
            idle_spins += 1;
            if received > 0 && idle_spins > IDLE_LIMIT {
                break true;
            }
            if received == 0 && idle_spins > IDLE_LIMIT * 4 {
                eprintln!("tüketici: hiç tick gelmedi");
                return 1;
            }
            std::hint::spin_loop();
            continue;
        }
        idle_spins = 0;
        let now = qpc();
        for t in &batch[..n] {
            // Gecikme: üreticinin damgasından bu okumaya kadar.
            samples.push(qpc_delta_nanos(t.recv_qpc, now));
        }
        received += n as u64;
        if received >= args.count {
            break true;
        }
    };
    debug_assert!(first_seen);

    println!("tüketici: alınan={received}");

    let Some(p) = Percentiles::from_samples(&mut samples) else {
        eprintln!("tüketici: örnek yok, yüzdelik hesaplanamaz");
        return 1;
    };

    println!();
    println!("== gecikme (üretici push → tüketici pop, süreçler arası) ==");
    println!("  örnek : {}", p.count);
    println!("  min   : {}", fmt_ns(p.min_ns));
    println!("  p50   : {}", fmt_ns(p.p50_ns));
    println!("  p90   : {}", fmt_ns(p.p90_ns));
    println!("  p99   : {}", fmt_ns(p.p99_ns));
    println!("  p99.9 : {}", fmt_ns(p.p999_ns));
    println!("  max   : {}", fmt_ns(p.max_ns));
    println!("  ort   : {}", fmt_ns(p.mean_ns));
    println!();

    let missing = args.count.saturating_sub(received);
    let p99_ok = p.p99_ns < GATE_P99_NS;
    let loss_ok = missing == 0;

    println!("== Faz 1 kapısı ==");
    println!(
        "  p99 < {} : {}  ({})",
        fmt_ns(GATE_P99_NS),
        if p99_ok { "GEÇTİ" } else { "KALDI" },
        fmt_ns(p.p99_ns)
    );
    println!(
        "  kayıp = 0  : {}  (beklenen {}, alınan {}, eksik {})",
        if loss_ok { "GEÇTİ" } else { "KALDI" },
        args.count,
        received,
        missing
    );
    println!();
    println!(
        "NOT: Bu ölçüm paylaşımlı bellek turunu kapsar. MT5 terminalinin tick'i"
    );
    println!(
        "     EA'ya teslim etme gecikmesi DAHİL DEĞİLDİR — o ancak canlı terminalde"
    );
    println!("     ölçülebilir.");

    if p99_ok && loss_ok {
        0
    } else {
        1
    }
}

fn open_with_retry(instance: &str, timeout_ms: u64) -> Option<Session> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        match Session::open(instance) {
            Ok(s) => return Some(s),
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("tüketici: {e}");
                return None;
            }
        }
    }
}
