//! `sinyald` — Sinyal çekirdeği.
//!
//! MT5 terminallerindeki EA'ların paylaşımlı belleğine bağlanır, tick ve
//! derinlik akışını WebSocket üzerinden dağıtır, karşılığında emir kabul eder.
//!
//! ```text
//! sinyald --instance mt5-1 --bind 127.0.0.1:8787
//! sinyald --instance icmarkets --instance pepperstone --enable-trading --token gizli
//! sinyald --instance mt5-1 --record ./veri
//! sinyald --instance mt5-1 --paper-bind 127.0.0.1:8788 --sim-slippage 2
//! sinyald --replay ./veri --replay-date 20260811 --replay-speed 0
//! sinyald --replay ./veri --replay-date 20260513-20260812 --replay-from 09:00 --replay-to 17:00
//! sinyald --import-backfill ...\MQL5\Files\Sinyal\backfill --data-dir ./veri --instance mt5-1
//! ```
//!
//! # Üç kip, tek protokol
//!
//! **CANLI** (`--instance`): paylaşımlı bellekten okur, emirleri gerçekten
//! yürütür.
//!
//! **PAPER** (`--paper-bind`): canlı kipin YANINDA açılan ikinci dinleyici.
//! Veri CANLIDIR — aynı okuyucu, aynı mum deposu — ama yürütme **simüledir**
//! ve bu porttan paylaşımlı belleğe **hiçbir komut yazılmaz**. Ayrı bir
//! daemon olamazdı: paylaşımlı halkalar SPSC'dir, ikinci bir okur sözleşmeyi
//! kırar ve iki taraf da tick kaybeder.
//!
//! **REPLAY** (`--replay`): diskteki kayıttan okur, emirleri **simüle** eder
//! ve paylaşımlı belleğe **hiç dokunmaz** — okuyucu thread'i bile
//! başlatılmaz, komut kanalı hiç kurulmaz.
//!
//! `--replay` ile `--instance` birlikte kullanılamaz ve bu bir kolaylık
//! kuralı değil: aynı süreçte hem kayıttan oynatıp hem gerçek emir
//! gönderebilmek, "test ediyordum" diye başlayan bir kaza hikâyesinin ilk
//! cümlesi olurdu. `--replay` ile `--paper-bind` de birlikte kullanılamaz —
//! replay zaten baştan sona simüledir, paper'ın ekleyeceği bir şey yok.
//!
//! Tel üzerindeki fark yalnızca `hello.mode` (`live` / `paper` / `replay`),
//! `hello`daki kapsam ve simülasyon künyesi alanları, bir de simüle emir
//! olaylarındaki `sim: true`dur. Mesaj biçimleri, alan adları ve kanal
//! adları birebir aynıdır: amaç sinyal sisteminin kod yolunun değişmemesi.
//!
//! # Simülasyon dürüsttür
//!
//! Simülatör (`sim.rs`) spread'i, ALEYHTE kaymayı, bekleyen emir ve SL/TP
//! tetiklenmesini, `stops_level`i ve marjini modeller; komisyonu, swap'ı ve
//! requote'u MODELLEMEZ. İkisi de `hello.sim` ile ilan edilir ve açılışta
//! konsola yazılır — modellenmeyen bir maliyeti tahmin etmektense hiç
//! eklememek, sonucun ne olduğu konusunda dürüst kalmayı sağlar.

mod backfill;
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

use server::{Ctx, OrderTracker, Replay, SimExec};
use source::{spawn_reader, FeedEvent, ReaderStats};
use state::Registry;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// Bir gün, ms — replay kapsamını gün içi saate çevirmek için.
const DAY_MS: i64 = 86_400_000;

/// `--replay-balance` / `--paper-balance` verilmediğinde sentetik bakiye.
const DEFAULT_REPLAY_BALANCE: f64 = sim::DEFAULT_BALANCE;

/// Replay kipinin çözülmüş ayarları.
struct ReplayCfg {
    opts: replay::ReplayOpts,
    /// Sentetik başlangıç bakiyesi (`--replay-balance`).
    balance: f64,
}

/// PAPER kipinin çözülmüş ayarları — **ikinci dinleyici, aynı süreç**.
///
/// Ayrı bir daemon neden olmaz: paylaşımlı halkalar SPSC'dir (tek yazar, tek
/// okur). İkinci bir süreç aynı halkayı okumaya kalksa sözleşme kırılır ve
/// iki taraf da tick kaybeder. Bu yüzden paper, canlı okuyucunun ürettiği
/// akışa AYNI süreçte bağlanır.
struct PaperCfg {
    /// Dinlenecek adres (`--paper-bind`).
    bind: String,
    /// Sentetik başlangıç bakiyesi (`--paper-balance`).
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

/// Ham paper bayrakları.
#[derive(Default)]
struct RawPaper {
    bind: Option<String>,
    balance: Option<f64>,
    slippage: Option<f64>,
}

/// Ham geri-doldurma bayrakları (bkz. `backfill.rs`).
#[derive(Default)]
struct RawImport {
    src: Option<String>,
    data_dir: Option<String>,
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
    /// `Some` ise CANLI kipin yanında AYRICA bir paper dinleyicisi açılır.
    paper: Option<PaperCfg>,
    /// `Some` ise İÇE AKTARMA kipi: hiçbir şey dinlenmez, paylaşımlı belleğe
    /// dokunulmaz; geri-doldurulmuş dosyalar kanonik kayda taşınır ve çıkılır.
    import: Option<backfill::ImportOpts>,
    /// Simülasyonda daima ALEYHTE uygulanan kayma (point).
    ///
    /// Hem replay hem paper için geçerli: iki kipin dolum fiyatı ayrışırsa
    /// "kayıttan test ettim" ile "canlı veriyle test ettim" farklı sonuç
    /// verir ve hangisinin doğru olduğu bilinemezdi.
    sim_slippage: f64,
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
        let mut paper = RawPaper::default();
        let mut imp = RawImport::default();
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
            paper: None,
            import: None,
            sim_slippage: sim::DEFAULT_SLIPPAGE_POINTS,
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
                "--import-backfill" => {
                    imp.src = Some(val()?);
                    i += 2;
                }
                "--data-dir" => {
                    imp.data_dir = Some(val()?);
                    i += 2;
                }
                "--paper-bind" => {
                    paper.bind = Some(val()?);
                    i += 2;
                }
                "--paper-balance" => {
                    paper.balance = Some(num()?);
                    i += 2;
                }
                "--sim-slippage" => {
                    paper.slippage = Some(num()?);
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
        // Kayma iki kipte de aynı: burada bir kez çözülüyor.
        if let Some(s) = paper.slippage {
            if s < 0.0 {
                return Err(format!(
                    "--sim-slippage negatif olamaz (kayma DAIMA aleyhtedir), verilen: {s}"
                ));
            }
            a.sim_slippage = s;
        }

        if let Some(src) = imp.src.take() {
            // --- İÇE AKTARMA kipi (geri-doldurulmuş tick dosyaları) ---
            //
            // Bu kip bir SUNUCU DEĞİL: hiçbir port dinlenmez, paylaşımlı
            // belleğe bağlanılmaz, iş bitince süreç çıkar. Diğer kiplerle
            // birlikte verilen bayrakları sessizce yok saymak, operatörün
            // "canlıyı da başlattım" sanmasına yol açardı.
            if raw.dir.is_some() {
                return Err(
                    "--import-backfill ile --replay birlikte kullanılamaz: içe aktarma \
                     kayda YAZAR, replay kayıttan OKUR. İkisini aynı çalıştırmada \
                     yapmak, oynatılan günün altından dosyayı çekmek olurdu."
                        .into(),
                );
            }
            if a.record.is_some() {
                return Err(
                    "--import-backfill ile --record birlikte kullanılamaz: ikisi de aynı \
                     kayıt dizinine yazar ve dizinin tek yazar kilidi bunu zaten \
                     reddeder. Önce kaydı durdur, sonra içe aktar."
                        .into(),
                );
            }
            if paper.bind.is_some() || a.trading {
                return Err(
                    "--import-backfill hiçbir emir yürütmez ve hiçbir port dinlemez; \
                     --paper-bind / --enable-trading burada anlamsız."
                        .into(),
                );
            }
            let Some(data_dir) = imp.data_dir.take() else {
                return Err(
                    "--import-backfill --data-dir <dizin> ister: içe aktarma hangi kayıt \
                     kökünün altına yazacağını TAHMİN ETMEZ. Kaydedicinin --record \
                     dizinini ver."
                        .into(),
                );
            };
            // Örnek adı hedef dizinin ta kendisi (`<data-dir>/<instance>`):
            // varsayılana düşmek, kaydı yanlış örneğin altına yazmak olurdu.
            let instance = match a.instances.len() {
                1 => a.instances[0].clone(),
                0 => {
                    return Err(
                        "--import-backfill --instance <ad> ister: kayıt \
                         <data-dir>/<instance> altında tutulur ve hangi örneğin \
                         geçmişini doldurduğunu tahmin etmiyoruz."
                            .into(),
                    )
                }
                n => {
                    return Err(format!(
                        "--import-backfill tek bir --instance ile kullanılır ({n} verildi): \
                         geri-doldurulan dosyalar tek bir örneğin kaydına taşınır."
                    ))
                }
            };
            a.import = Some(backfill::ImportOpts {
                src_dir: PathBuf::from(src),
                data_dir: PathBuf::from(data_dir),
                instance,
            });
            return Ok(a);
        }
        if imp.data_dir.is_some() {
            // Sessizce yok saymak, "--import-backfill yazmayı unuttum" hatasını
            // fark edilmez yapardı: operatör hedefi verdiğini sanırken hiçbir
            // şey içe aktarılmamış olurdu.
            return Err(
                "--data-dir yalnızca --import-backfill ile kullanılır; canlı kayıt dizini \
                 --record, oynatım dizini --replay ile verilir."
                    .into(),
            );
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
            // `--paper-bind`in tek işi CANLI veriyi simüle yürütmeye
            // bağlamaktır; replay zaten baştan sona simüledir. İkisini
            // birlikte kabul etmek, operatöre canlı veriyle test ettiğini
            // sandırırken kayıt oynatmak olurdu — üstelik ikinci port
            // birincisiyle aynı simülasyonu gösterirdi.
            if paper.bind.is_some() {
                return Err(
                    "--replay ile --paper-bind BİRLİKTE kullanılamaz: replay kipinde \
                     yürütme ZATEN simüledir ve veri kayıttan gelir; paper kipi ise canlı \
                     veriyi simüle yürütmeye bağlar. Canlı veriyle kâğıt işlem için \
                     --instance + --paper-bind, kayıttan oynatım için yalnız --replay kullan."
                        .into(),
                );
            }
            if paper.balance.is_some() {
                return Err(
                    "--paper-balance yalnızca --paper-bind ile kullanılır; replay'de \
                     karşılığı --replay-balance."
                        .into(),
                );
            }
            let Some(date) = raw.date else {
                return Err(
                    "--replay --replay-date ister (gün sınırı UTC): 20260513 (tek gün), \
                     20260513-20260812 (aralık, ikisi de dahil) ya da 20260513- (o günden \
                     kayıttaki sona kadar). Hangi günün kaydının oynatılacağını tahmin \
                     etmiyoruz."
                        .into(),
                );
            };
            // Biçim hatasını diske dokunmadan yakala: "20260811" yerine
            // "2026-08-11" yazan biri hatayı hemen görmeli. Aralık biçimi de
            // burada çözülür; ters aralık (bitiş < başlangıç) da burada patlar.
            replay::parse_date_spec(&date).map_err(|e| format!("--replay-date: {e}"))?;
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

        // --- PAPER dinleyicisi (canlı kipin yanında) ---
        match paper.bind.take() {
            Some(bind) => {
                // Aynı adres iki kez dinlenemez; bunu çalışma zamanında
                // "adres kullanımda" hatasıyla keşfetmek, operatörü kendi
                // ikinci portunu bir başkasının işgal ettiğini sanmaya
                // yöneltirdi.
                if bind == a.bind {
                    return Err(format!(
                        "--paper-bind ile --bind aynı adres olamaz ({bind}): paper AYRI bir \
                         dinleyicidir. Aynı portta iki kip sunulsaydı gerçek emir ile simüle \
                         emir arasındaki fark bağlantı bazında kaybolurdu."
                    ));
                }
                let balance = paper.balance.unwrap_or(DEFAULT_REPLAY_BALANCE);
                if balance < 0.0 {
                    return Err(format!("--paper-balance negatif olamaz, verilen: {balance}"));
                }
                a.paper = Some(PaperCfg { bind, balance });
            }
            None => {
                if paper.balance.is_some() {
                    return Err(
                        "--paper-balance yalnızca --paper-bind ile kullanılır; tek başına \
                         hiçbir şeyi etkilemezdi."
                            .into(),
                    );
                }
                // Sessizce yok saymak, "--paper-bind yazmayı unuttum" hatasını
                // fark edilmez yapardı: operatör kaymayı ayarladığını sanırken
                // hiçbir simülasyon çalışmıyor olurdu.
                if paper.slippage.is_some() {
                    return Err(
                        "--sim-slippage yalnızca simülasyon çalışan kiplerde anlamlıdır: \
                         --replay veya --paper-bind ile kullan."
                            .into(),
                    );
                }
            }
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
  --replay-date GUN   ZORUNLU, gün sınırı UTC. Üç biçim:
                        20260513            tek gün
                        20260513-20260812   aralık, İKİSİ DE DAHİL
                        20260513-           o günden kayıttaki sona kadar
  --replay-from HH:MM Gün içi kapsam başlangıcı, UTC (istege bagli)
  --replay-to   HH:MM Gün içi kapsam sonu, UTC (istege bagli)
  --replay-speed N    1.0 = gerçek zamanlı (varsayılan), 0 = beklemeden en hızlı
  --replay-balance N  Sentetik başlangıç bakiyesi (varsayılan 10000)

  hello.mode replay'de \"replay\" olur ve hello kapsamı (replay_from_ms /
  replay_to_ms) ilan eder; başka hiçbir mesaj biçimi değişmez.

  ARALIK: günler kronolojik sırayla, TEK BAĞLANTIDA, kesintisiz akar ve
  replay_done yalnızca ARALIĞIN SONUNDA gelir — istemci gün değiştiğini fark
  etmemelidir. 66 ayrı süreç başlatmak sadece zahmet değil DOĞRULUK sorunuydu:
  gün sınırında bağlantı koptuğunda sinyal sisteminin durumu sıfırlanır ve
  gerçekte TAŞINACAK bir pozisyon taşınmamış olurdu.

  --replay-from / --replay-to aralıkla birlikte HER GÜNE ayrı ayrı uygulanır
  (ör. her günün 09:00-17:00'si).

  Kayıtta olmayan günler (hafta sonu, tatil) ve 0 baytlık gün dosyaları
  SESSİZCE atlanır; aralıkta HİÇ gün bulunamazsa hata AÇIKTIR.

  replay_done her zaman days / days_played taşır. Aralığın ortasındaki bir
  gün okunamazsa oynatım orada durur ve mesaj ayrıca truncated:true taşır —
  bu çalıştırmanın sonuçları EKSİK bir kaydın sonucudur. Kesintiyi sessiz
  bırakmak, 3 aylık sanılan bir backtest'i tek günün sonucuyla raporlamak
  olurdu.

  Günler arası bekleme en fazla 1 sn'dir (hız çarpanına da bölünür): Cuma
  23:58'den Pazartesi 01:00'e geçerken recv_ms farkı ~49 SAATtir ve
  kırpılmasaydı --replay-speed 1 ile oynatım 49 saat uyurdu. Gün İÇİNDEKİ
  boşluklar KIRPILMAZ — onlar gerçek piyasa sessizliğidir.

GERİ-DOLDURMA İÇE AKTARMA (sunucu değil: iş bitince çıkar):
  --import-backfill DIZIN  MQL5 Service'in yazdığı <Sembol>-YYYYMMDD.bin
                           dosyalarını KANONİK kayıt biçimine taşı.
  --data-dir DIZIN         Hedef kayıt kökü (ZORUNLU; kaydedicinin --record
                           dizini). Yazılan dosyalar:
                             DIZIN/<instance>/ticks-YYYYMMDD.bin
                             DIZIN/<instance>/symbols-YYYYMMDD.jsonl
  --instance AD            Hangi örneğin kaydına yazılacak (ZORUNLU, tek).

  Gün dosyası zaten varsa BİRLEŞTİRİLİR (üzerine yazılmaz): iki kaynak
  time_msc ile sıralanır ve aynı (symbol_id,time_msc,bid,ask) dörtlüsü bir
  kez yazılır. Aynı içe aktarmayı iki kez çalıştırmak çıktıyı DEĞİŞTİRMEZ.

  DİKKAT: geçmiş tick'lerde YEREL ALIM ZAMANI yoktur; Service `recv_ms`
  alanına BROKER saatini yazar. Replay temposu `recv_ms` farkından
  üretildiği için bu günlerde tempo broker saatinden gelir. Özet bunu ve
  ölçülen saat farkını AÇIKÇA yazar.

PAPER (canlı veri, simüle yürütme — AYNI süreçte İKİNCİ dinleyici):
  --paper-bind ADDR   Paper portu. --instance ile birlikte kullanılır,
                      --replay ile KULLANILAMAZ (replay zaten simüle).
  --paper-balance N   Sentetik başlangıç bakiyesi (varsayılan 10000)

  Bu porttan gelen bağlantı CANLI tick ve CANLI mumları görür ama emirleri
  SİMÜLE edilir; paylaşımlı belleğe HİÇBİR komut yazılmaz. hello.mode
  \"paper\" olur.

SİMÜLASYON (hem --replay hem --paper-bind için):
  --sim-slippage N    Aleyhte kayma, point (varsayılan 1 — SIFIR DEĞİL)

  MODELLENİR : spread, aleyhte kayma, bekleyen emir tetiklenmesi, SL/TP
               tetiklenmesi (aynı tick ikisini de vurursa SL kazanır),
               stops_level (10016), marjin (10019), hacim ızgarası (10014),
               emir son kullanma (süresi dolan emir DOLMAZ).
  MODELLENMEZ: komisyon, swap, requote/deviation penceresi, kur çevrimi,
               kısmi dolum, stop-out, freeze_level.
  Her emir olayı `sim: true` taşır ve hello.sim bu listeyi ilan eder.
  Simüle dolum GERÇEK DOLUM DEĞİLDİR.

GÜVENLİK:
  Bu uç EMİR YÜRÜTEBİLİR. Varsayılan olarak yalnızca 127.0.0.1'i dinler ve
  emir yürütme kapalıdır. Ağa açacaksan --token KULLAN."
    );
}

/// Simülasyonun neyi modelleyip neyi MODELLEMEDİĞİNİ açılışta yaz.
///
/// Aynı liste `hello.sim` ile tel üzerinden de gider; buradaki kopya
/// daemon'u terminalden izleyen kişi içindir. Eskiden burada "kayma
/// modellenmiyor" yazıyordu ve bu artık DOĞRU DEĞİL — yanlış kalmış bir
/// uyarı, hiç uyarı olmamasından daha kötüdür: okuyan kişi sonucu olduğundan
/// kötümser sanır ve düzeltmeye çalışır.
fn print_sim_honesty(slippage: f64) {
    println!("  MODELLENIR : spread; ALEYHTE kayma ({slippage} point); bekleyen emir ve");
    println!("               SL/TP tetiklenmesi (ayni tick ikisini de vurursa SL kazanir);");
    println!("               stops_level (10016); marjin (10019); hacim izgarasi (10014);");
    println!("               emir son kullanma (suresi dolan emir DOLMAZ).");
    println!("  MODELLENMEZ: komisyon, swap, requote/deviation penceresi, kur cevrimi,");
    println!("               kismi dolum, stop-out, freeze_level.");
    println!("  {}", server::SIM_WARNING);
    // `--sim-slippage 0` yukarıdaki "MODELLENIR: ALEYHTE kayma" satırını
    // YALANA çevirir: dolumlar yeniden mükemmelleşir ve strateji burada
    // kârlı görünüp canlıda kaymaya yenilir — bu işin kapatmaya çalıştığı
    // boşluğun ta kendisi. Varsayılan 1'dir; 0'a inmek bilinçli bir seçim
    // olmalı, sessiz bir yan etki değil.
    if slippage == 0.0 {
        println!();
        eprintln!("  UYARI: --sim-slippage 0 — KAYMA KAPALI.");
        eprintln!("         Her piyasa ve STOP dolumu gordugu fiyattan MUKEMMEL dolar.");
        eprintln!("         Bu kipte cikan kar egrisi CANLIYA GORE IYIMSERDIR.");
    }
}

/// Kayıttaki SON sembol tablosunda `contract_size` taşımayan semboller.
///
/// Alan kayıt biçimine sonradan eklendi; ondan önce alınmış kayıtlarda 0
/// gelir. Simülatör bu durumda emri açıkça reddediyor
/// (`sim::check_contract`) — ama bunu ancak ilk emirde söylüyor. Operatör
/// replay'i başlatırken öğrenmeli, bir saat sonra değil.
///
/// SON görüntüye bakılıyor: tablo kayıt boyunca güncellenebilir ve bizi
/// ilgilendiren, oynatımın büyük kısmında geçerli olan hâlidir.
/// `Recording` yerine yalnızca sembol görüntülerini alıyor: ihtiyacı olan
/// tek şey bu ve böylece test, üretim tipine sırf sınanabilsin diye
/// `Default` eklemek zorunda kalmıyor.
fn symbols_missing_contract_size(symbols: &[Vec<replay::SymbolSnapshot>]) -> Vec<String> {
    let mut out = Vec::new();
    for snaps in symbols {
        let Some(last) = snaps.last() else { continue };
        for it in &last.items {
            if !(it.contract_size > 0.0) && !out.contains(&it.s) {
                out.push(it.s.clone());
            }
        }
    }
    out.sort();
    out
}

fn warn_if_recording_lacks_contract_size(missing: &[String]) {
    if missing.is_empty() {
        return;
    }
    // `eprintln!`: bu bir bilgi satırı değil, oynatımın işe yaramayacağı
    // uyarısı. Sessiz kalsaydık simülatör her emri 10013 ile reddederdi ve
    // sebebi kayıt biçiminde aramak kimsenin aklına gelmezdi.
    eprintln!();
    eprintln!("  UYARI: kayit `contract_size` TASIMIYOR ({} sembol): {}", missing.len(), {
        let shown: Vec<&str> = missing.iter().take(8).map(String::as_str).collect();
        let mut s = shown.join(", ");
        if missing.len() > shown.len() {
            s.push_str(", ...");
        }
        s
    });
    eprintln!("         Bu alan olmadan marjin ve kar HESAPLANAMAZ; simulator bu");
    eprintln!("         sembollerdeki emirleri retcode 10013 ile REDDEDER.");
    eprintln!("         Alan kayit bicimine sonradan eklendi — YENIDEN KAYDET.");
}

/// Epoch ms → gün içi `HH:MM:SS.mmm` (UTC).
fn hhmmss_utc(ms: i64) -> String {
    let t = ms.rem_euclid(DAY_MS);
    format!("{:02}:{:02}:{:02}.{:03}", t / 3_600_000, (t / 60_000) % 60, (t / 1000) % 60, t % 1000)
}

/// Epoch ms → `YYYYMMDD HH:MM:SS.mmm` (UTC).
///
/// Replay kapsamı artık tek güne sığmak zorunda değil (`--replay-date`
/// aralık alabiliyor); yalnızca saati yazmak, hangi günün saati olduğunu
/// belirsiz bırakır ve 66 günlük bir aralıkta bu bilgi işe yaramaz olurdu.
fn stamp_hhmmss_utc(ms: i64) -> String {
    format!("{} {}", record::date_str(record::day_stamp(ms)), hhmmss_utc(ms))
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

    // İÇE AKTARMA: sunucu hiç kurulmaz. Bu kipte tek iş diski diske taşımak;
    // paylaşımlı bellek, yayın kanalı ve dinleyici HİÇ oluşturulmaz.
    if let Some(opts) = &args.import {
        match backfill::import(opts) {
            Ok(sum) => {
                print!("{sum}");
                return;
            }
            Err(e) => {
                eprintln!("hata: geri-doldurma ice aktarilamadi: {e}");
                std::process::exit(1);
            }
        }
    }

    let registry = Arc::new(Registry::new());
    let candles = Arc::new(Mutex::new(candles::CandleStore::new()));
    let (tx, _) = broadcast::channel(args.capacity);

    // Paper köprüsünün aboneliği OKUYUCULARDAN ÖNCE alınıyor: `broadcast`
    // yalnızca abone olduktan SONRA gönderilenleri taşır, yani aşağıda
    // abone olsaydık okuyucunun ilk tick'leri simülatöre hiç ulaşmazdı.
    // Pratikte EA bağlantısı yavaştır ve fark görünmezdi — görünmeyen bir
    // veri kaybı tam da en geç fark edileni olurdu.
    let paper_live_rx = args.paper.as_ref().map(|_| tx.subscribe());

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
    let mut sim_ctx: Option<Arc<SimExec>> = None;
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
            let days = rec.days.clone();
            // Emir gönderilmeden ÖNCE söylenmeli: `contract_size` taşımayan
            // bir kayıtla açılan replay, ilk emre kadar sorunsuz görünür ve
            // sorun ancak retcode 10013 ile ortaya çıkardı.
            //
            // Yalnızca SON günün tablosuna bakılıyor: 66 günün tablosunu
            // açılışta okumak, kaydın tamamına dokunmak olurdu.
            let missing_contract = match rec.symbols_of_last_day() {
                Ok(s) => symbols_missing_contract_size(&s),
                Err(e) => {
                    eprintln!("hata: kayit yuklenemedi: {e}");
                    std::process::exit(1);
                }
            };

            let (done_tx, done_rx) = tokio::sync::watch::channel(None);
            // Motor oynatımdan ÖNCE kurulur: oynatım thread'i her tick'te
            // onu besleyecek, yoksa bekleyen emirler ve SL/TP hiç
            // tetiklenmez.
            let sim = Arc::new(SimExec::new(
                server::MODE_REPLAY,
                &instances,
                sim::SimConfig {
                    balance: cfg.balance,
                    leverage: sim::DEFAULT_LEVERAGE,
                    slippage_points: args.sim_slippage,
                },
            ));
            sim_ctx = Some(sim.clone());
            let run = replay::spawn(
                rec,
                registry.clone(),
                candles.clone(),
                tx.clone(),
                done_tx,
                Some(sim),
            );
            // Oynatım kapısı sunucuya veriliyor: ilk istemci bağlanınca açılır.
            replay_ctx = Some(Arc::new(Replay::new(
                &instances,
                Some(from_ms),
                Some(to_ms),
                done_rx,
                run.gate(),
            )));
            replay_run = Some(run);

            println!("sinyald REPLAY kipinde — diskteki kayittan oynatiliyor");
            println!("  kayit    : {} ({})", cfg.opts.data_dir.display(), cfg.opts.date);
            println!("  ornekler : {}", instances.join(", "));
            // Gün sayısı ve gerçekten bulunan ilk/son gün YAZILIYOR: istenen
            // aralıkta hafta sonu ve tatiller yok ve operatör "3 ay istedim,
            // 66 gun bulundu" doğrulamasını burada yapabilmeli.
            match (days.first(), days.last()) {
                (Some(f), Some(l)) if days.len() > 1 => {
                    println!("  gunler   : {} gun ({f} .. {l}, eksik gunler atlandi)", days.len());
                }
                (Some(f), _) => println!("  gun      : {f}"),
                _ => {}
            }
            // Tarih de yazılıyor: aralık kipinde yalnız saat yazmak, hangi
            // günün saati olduğunu belirsiz bırakırdı.
            println!(
                "  kapsam   : {} .. {} UTC",
                stamp_hhmmss_utc(from_ms),
                stamp_hhmmss_utc(to_ms)
            );
            if cfg.opts.from.is_some() || cfg.opts.to.is_some() {
                println!(
                    "  gun ici  : {} .. {} — HER GUNE ayri ayri uygulanir",
                    cfg.opts.from.as_deref().unwrap_or("00:00"),
                    cfg.opts.to.as_deref().unwrap_or("24:00"),
                );
            }
            if days.len() > 1 {
                println!(
                    "  Gunler TEK BAGLANTIDA, kesintisiz akar; replay_done yalnizca"
                );
                println!(
                    "  ARALIGIN SONUNDA gelir. Gunler arasi bekleme en fazla {} ms.",
                    replay::REPLAY_GAP_MAX_MS
                );
            }
            println!(
                "  tempo    : {}",
                if cfg.opts.speed == 0.0 {
                    "beklemeden en hizli".to_string()
                } else {
                    format!("{}x", cfg.opts.speed)
                }
            );
            println!("  bakiye   : {} (SENTETIK)", cfg.balance);
            warn_if_recording_lacks_contract_size(&missing_contract);
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
        events: tx.clone(),
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
        // Bu bağlam CANLI ya da REPLAY'dir; paper AYRI bir bağlam kurar.
        sim: replay_ctx.as_ref().and(sim_ctx.clone()),
        // `None` ise CANLI kip: akış paylaşımlı bellekten geliyor ve emirler
        // gerçekten yürütülür. `Some` ise sunucu hiçbir komut göndermez.
        replay: replay_ctx.clone(),
        sltp: Arc::new(server::SltpGuard::default()),
    });

    // --- PAPER: aynı süreçte İKİNCİ dinleyici, canlı veri + simüle yürütme ---
    //
    // Ayrı bir daemon OLMAZ: paylaşımlı halkalar SPSC'dir, ikinci bir okur
    // sözleşmeyi kırar ve iki taraf da tick kaybeder. Bu yüzden paper aynı
    // okuyucunun ürettiği akışa bağlanır.
    //
    // Ayrı bir yayın kanalı kullanıyor ve bu bilinçli: paper istemcisi canlı
    // EMİR olaylarını GÖRMEMELİ (kendi işlemi sanardı), canlı istemci de
    // simüle olayları görmemeli. Piyasa verisi köprüden aynen geçer.
    let paper_srv = match &args.paper {
        Some(cfg) => {
            let (paper_tx, _) = broadcast::channel(args.capacity);
            let sim = Arc::new(SimExec::new(
                server::MODE_PAPER,
                &args.instances,
                sim::SimConfig {
                    balance: cfg.balance,
                    // Kaldıraç canlı hesaptan OKUNMUYOR: hesap görüntüsü
                    // açılışta henüz yok olabilir ve marjin modelini yarı
                    // yolda değiştirmek, aynı emrin bir dakika arayla farklı
                    // sonuç vermesi demekti. Değer `hello.sim`de ilan edilir.
                    leverage: sim::DEFAULT_LEVERAGE,
                    slippage_points: args.sim_slippage,
                },
            ));
            let listener = match TcpListener::bind(&cfg.bind).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("hata: paper portu {} dinlenemedi: {e}", cfg.bind);
                    std::process::exit(1);
                }
            };

            // Köprü: canlı akıştan oku, simülatörü besle, paper kanalına yaz.
            let mut live_rx = paper_live_rx.expect("paper aboneligi yukarida alinmis olmali");
            let bridge_tx = paper_tx.clone();
            let bridge_sim = sim.clone();
            tokio::spawn(async move {
                let mut dropped_total: u64 = 0;
                loop {
                    match live_rx.recv().await {
                        Ok(FeedEvent::Tick {
                            instance,
                            symbol,
                            bid,
                            ask,
                            last,
                            time_msc,
                            lat_us,
                        }) => {
                            // Önce fiyat, sonra o fiyatın doğurduğu dolum:
                            // istemci görmediği bir fiyattan dolum almamalı.
                            let _ = bridge_tx.send(FeedEvent::Tick {
                                instance: instance.clone(),
                                symbol: symbol.clone(),
                                bid,
                                ask,
                                last,
                                time_msc,
                                lat_us,
                            });
                            server::pump_tick(
                                &bridge_sim,
                                &bridge_tx,
                                &instance,
                                &symbol,
                                bid,
                                ask,
                                time_msc,
                            );
                        }
                        // Emir olayı GEÇMEZ: canlı terminalde gerçekleşen bir
                        // işlemi paper istemcisine göstermek, onu kendi simüle
                        // emrinin sonucu sanmasına yol açardı.
                        // Aynı gerekçe stop uyarısı için de geçerli, hatta
                        // daha güçlü: canlı bir pozisyonun korumasız kaldığını
                        // paper istemcisine söylemek, onu SİMÜLE pozisyonu
                        // sanıp yanlış yerde telafi işlemi yaptırırdı.
                        Ok(FeedEvent::Order { .. }) | Ok(FeedEvent::SltpUnverified { .. }) => {}
                        Ok(other) => {
                            let _ = bridge_tx.send(other);
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // Köprü geride kaldı: DÜŞEN TICK, TETİKLENMEYEN
                            // STOP demektir. Sessiz kalmak, simülasyonun neden
                            // canlıdan saptığını aylar sonra sorduracaktı.
                            dropped_total += n;
                            eprintln!(
                                "[paper] UYARI: kopru {n} tick geride kaldi (toplam \
                                 {dropped_total}). Bu tick'ler simulatore HIC ulasmadi; \
                                 bekleyen emir ve SL/TP tetiklenmemis olabilir."
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            });

            let paper_ctx = Arc::new(Ctx {
                registry: registry.clone(),
                events: paper_tx,
                // GÜVENLİK SINIRI — BOŞ BIRAKILDI.
                //
                // Paper bağlamının paylaşılan belleğe giden bir kanalı
                // YOKTUR: gönderilecek bir yer olmadığı için kazara gerçek
                // emir göndermek mümkün değil. `dispatch` içindeki kip
                // denetimi bunun ikinci hattı.
                cmd_tx: HashMap::new(),
                // Geçmiş isteği de paylaşılan belleğe YAZILAN bir komuttur;
                // sınır mutlak tutuluyor. Barlar canlı akışla dolan ORTAK
                // mum deposundan geliyor, yani istemci veri kaybetmiyor.
                hist_tx: HashMap::new(),
                token: args.token.clone(),
                // Simüle kipte `--enable-trading` bir riski kapatmıyor.
                trading: true,
                allow_live: false,
                deviation: args.deviation,
                // AYRI emir defteri: paper istemcisinin aldığı bir kimlik
                // canlı istemciyi engellememeli ve iki portun olayları
                // birbirinin kimliğine çözülmemeli.
                orders: Arc::new(OrderTracker::new()),
                candles: candles.clone(),
                hist_slots: Arc::new(tokio::sync::Semaphore::new(server::HIST_SLOTS)),
                sim: Some(sim),
                replay: None,
                // AYRI defter: paper'da doğrulanamayan bir stop, canlı
                // sayaçları kirletmemeli.
                sltp: Arc::new(server::SltpGuard::default()),
            });
            Some((listener, paper_ctx, cfg.bind.clone(), cfg.balance))
        }
        None => None,
    };

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
        print_sim_honesty(args.sim_slippage);
    }
    if let Some((_, _, bind, balance)) = &paper_srv {
        println!();
        println!("sinyald PAPER portu dinliyor: ws://{bind}");
        println!("  veri     : CANLI (ayni okuyucu, ayni mum deposu)");
        println!("  yurutme  : SIMULE — hello.mode = \"paper\"");
        println!("  bakiye   : {balance} (SENTETIK)");
        println!("  Bu porttan paylasilan bellege HICBIR komut yazilmaz:");
        println!("  komut kanali hic kurulmadi, gercek emir imkansiz.");
        print_sim_honesty(args.sim_slippage);
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
                        "[replay] oynatim bitti: {} tick, {}/{} gun, son broker saati {} UTC",
                        end.ticks,
                        end.days_played,
                        end.days,
                        stamp_hhmmss_utc(end.last_ms)
                    );
                    // Kesinti sessiz kalmamalı: eksik bir kaydın sonucunu tam
                    // sanmak, backtest'i sessizce yanlış yapar.
                    if end.days_played < end.days {
                        eprintln!(
                            "[replay] UYARI: ARALIK YARIDA KESILDI — {} gun planlandi, {} gun oynatildi. \
                             Bu calistirmanin sonuclari EKSIK bir kaydin sonucudur \
                             (istemciye de replay_done.truncated=true gitti).",
                            end.days, end.days_played
                        );
                    }
                    break;
                }
            }
        });
    }

    // Periyodik durum raporu: akışın canlı olup olmadığını görmenin en hızlı
    // yolu. Sessiz bir daemon, "çalışıyor mu bilmiyorum" demektir. Replay'de
    // okuyucu da sayaç da yok; orada bu döngü hiç kurulmaz.
    let telemetry_candles = candles.clone();
    // Stop doğrulama sayaçları — canlı/replay ve paper AYRI defterler.
    //
    // Okuyucu sayaçlarına (`stats_all`) BAĞLI DEĞİL: replay'de okuyucu hiç
    // çalışmaz ama `modify_sltp` yine gönderilebilir ve doğrulanamayan bir
    // stop orada da sessiz kalmamalı. Bu yüzden eski "okuyucu yoksa hiç
    // rapor verme" erken dönüşü kaldırıldı.
    let mut telemetry_sltp: Vec<(String, Arc<server::SltpGuard>)> =
        vec![(ctx.mode().to_string(), ctx.sltp.clone())];
    if let Some((_, paper_ctx, _, _)) = &paper_srv {
        telemetry_sltp.push((server::MODE_PAPER.to_string(), paper_ctx.sltp.clone()));
    }
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

            // Broker stop'u KURULDU MU. Hiç komut gönderilmediyse satır da
            // yok — her 30 saniyede bir sıfır basmak, gerçek sayının gözden
            // kaçmasına yol açardı.
            for (label, guard) in &telemetry_sltp {
                let c = guard.counts();
                if c.sent == 0 {
                    continue;
                }
                println!(
                    "[{label}] stop: modify_sltp gonderilen={} dogrulanan={} dogrulanamayan={} \
                     kapanmis={} bekleyen={}",
                    c.sent,
                    c.verified,
                    c.unverified,
                    c.closed,
                    c.sent
                        .saturating_sub(c.verified + c.unverified + c.closed),
                );
                // Doğrulanamayan stop = KORUMASIZ olabilecek pozisyon.
                // Sessiz kalması raporun asıl şikâyetiydi.
                if c.unverified > 0 {
                    println!(
                        "[{label}] UYARI: {} stop komutunun KURULDUGU dogrulanamadi \
                         — broker reddetmis ya da kopru bayat olabilir. Istemci kendi \
                         yedek stop'unu devrede tutmali.",
                        c.unverified
                    );
                }
            }
        }
    });

    // Paper dinleyicisi ayrı bir görevde: iki dinleyici birbirini
    // beklememeli, canlı portun kabul döngüsü paper yüzünden durmamalı.
    if let Some((paper_listener, paper_ctx, _, _)) = paper_srv {
        tokio::spawn(server::serve(paper_listener, paper_ctx));
    }

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
    fn an_old_recording_without_contract_size_is_named_at_startup() {
        // Simülatör bu sembollerdeki emirleri reddediyor; operatör bunu ilk
        // emirde DEĞİL, replay'i başlatırken öğrenmeli. Uyarının sessizce
        // boşa düşmesi (ör. `symbols` alanının şekli değişirse) hatayı geri
        // getirirdi, o yüzden fonksiyonun kendisi sınanıyor.
        let item = |name: &str, cs: f64| replay::SymbolItem {
            id: 0,
            s: name.into(),
            contract_size: cs,
            ..Default::default()
        };
        let snap = |items: Vec<replay::SymbolItem>| replay::SymbolSnapshot { at_ms: 1, items };

        let mixed = vec![snap(vec![item("EURUSD", 100_000.0), item("XAUUSD", 0.0)])];
        assert_eq!(
            symbols_missing_contract_size(&[mixed]),
            vec!["XAUUSD".to_string()],
            "yalnizca alani eksik olan sembol anilmali"
        );

        // Tam kayıtta uyarı HİÇ çıkmamalı: her açılışta bağıran bir uyarı,
        // gerçekten önemli olduğunda okunmaz.
        let complete = vec![snap(vec![item("EURUSD", 100_000.0)])];
        assert!(symbols_missing_contract_size(&[complete]).is_empty());

        // SON görüntü belirleyicidir: kayıt ortasında düzelen bir tablo
        // yüzünden uyarı takılı kalmamalı.
        let fixed_midway = vec![snap(vec![item("EURUSD", 0.0)]), snap(vec![item("EURUSD", 100_000.0)])];
        assert!(
            symbols_missing_contract_size(&[fixed_midway]).is_empty(),
            "son goruntu gecerli olmali"
        );
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

    // --- PAPER kipi bayrakları ---

    #[test]
    fn replay_and_paper_bind_together_is_a_clear_error() {
        // İkisi birlikte ANLAMSIZ: replay zaten baştan sona simüledir.
        // Sessizce birini seçmek, operatörün canlı veriyle test ettiğini
        // sanarken kayıt oynatması demekti.
        let e = err(&[
            "--replay",
            "veri",
            "--replay-date",
            "20260811",
            "--paper-bind",
            "127.0.0.1:8788",
        ]);
        assert!(e.contains("--paper-bind"), "hata hangi bayrak oldugunu soylemeli: {e}");
        assert!(e.contains("--replay"), "hata --replay'i anmali: {e}");
        // Sıra fark etmemeli.
        let e = err(&[
            "--paper-bind",
            "127.0.0.1:8788",
            "--replay",
            "veri",
            "--replay-date",
            "20260811",
        ]);
        assert!(e.contains("--paper-bind") && e.contains("--replay"), "{e}");
    }

    #[test]
    fn paper_listens_on_its_own_address_and_never_shares_the_live_one() {
        // Aynı portta iki kip sunulsaydı, gerçek emir ile simüle emir
        // arasındaki fark bağlantı bazında kaybolurdu.
        let e = err(&["--bind", "127.0.0.1:8787", "--paper-bind", "127.0.0.1:8787"]);
        assert!(e.contains("--paper-bind") && e.contains("--bind"), "{e}");

        // Varsayılan --bind ile de aynı çakışma yakalanmalı.
        assert!(err(&["--paper-bind", "127.0.0.1:8787"]).contains("--bind"));

        let a = Args::from_argv(argv(&["--paper-bind", "127.0.0.1:8788"])).unwrap();
        let p = a.paper.expect("paper kipi kurulmali");
        assert_eq!(p.bind, "127.0.0.1:8788");
        assert!((p.balance - DEFAULT_REPLAY_BALANCE).abs() < 1e-12, "varsayilan bakiye");
        // Paper CANLI veriyle çalışır: canlı örnek varsayılanı korunmalı.
        assert_eq!(a.instances, vec!["mt5-1".to_string()]);
        assert!(a.replay.is_none(), "paper bir replay degil");
    }

    #[test]
    fn paper_flags_without_paper_bind_are_refused() {
        // Sessizce yok saymak, "--paper-bind yazmayı unuttum" hatasını fark
        // edilmez yapardı: operatör bakiyeyi ayarladığını sanırken hiçbir
        // simülasyon çalışmıyor olurdu.
        assert!(err(&["--paper-balance", "500"]).contains("--paper-bind"));
        let e = err(&["--sim-slippage", "3"]);
        assert!(e.contains("--paper-bind") && e.contains("--replay"), "{e}");
        // Replay'de bakiye bayrağının adı farklı; karıştırmak sessiz kalmamalı.
        let e = err(&["--replay", "veri", "--replay-date", "20260811", "--paper-balance", "5"]);
        assert!(e.contains("--paper-balance"), "{e}");
    }

    #[test]
    fn slippage_defaults_to_one_point_and_is_shared_by_both_sim_modes() {
        // SIFIR varsayılan bir gerileme olurdu: motor "kaymayı modelliyorum"
        // diye ilan edip pratikte iyimser dolum üretirdi.
        assert!((sim::DEFAULT_SLIPPAGE_POINTS - 1.0).abs() < 1e-12);

        let a = Args::from_argv(argv(&["--paper-bind", "127.0.0.1:8788"])).unwrap();
        assert!((a.sim_slippage - sim::DEFAULT_SLIPPAGE_POINTS).abs() < 1e-12);

        // Aynı bayrak replay'de de geçerli: iki kipin dolum fiyatı ayrışırsa
        // hangisinin doğru olduğu bilinemezdi.
        let a = Args::from_argv(argv(&[
            "--replay",
            "veri",
            "--replay-date",
            "20260811",
            "--sim-slippage",
            "2.5",
        ]))
        .unwrap();
        assert!((a.sim_slippage - 2.5).abs() < 1e-12);

        let a = Args::from_argv(argv(&[
            "--paper-bind",
            "127.0.0.1:8788",
            "--sim-slippage",
            "0",
        ]))
        .unwrap();
        assert_eq!(a.sim_slippage, 0.0, "0 acikca istenebilir (ama varsayilan degil)");

        // Negatif kayma LEHTE kayma demekti — simülasyonu iyimser yapardı.
        assert!(err(&["--paper-bind", "127.0.0.1:8788", "--sim-slippage", "-1"])
            .contains("--sim-slippage"));
        assert!(err(&["--paper-bind", "127.0.0.1:8788", "--paper-balance", "-5"])
            .contains("--paper-balance"));
    }

    // --- İÇE AKTARMA kipi bayrakları ---

    #[test]
    fn import_needs_a_destination_and_an_instance_it_will_not_guess() {
        // Hedefi tahmin etmek, geçmişi YANLIŞ örneğin kaydına karıştırmak
        // olurdu; ve bu ancak aylar sonra "bu tick'ler nereden geldi" diye
        // sorulduğunda fark edilirdi.
        let e = err(&["--import-backfill", "gecmis"]);
        assert!(e.contains("--data-dir"), "eksik bayrak adiyla anilmali: {e}");

        let e = err(&["--import-backfill", "gecmis", "--data-dir", "veri"]);
        assert!(e.contains("--instance"), "{e}");

        let e = err(&[
            "--import-backfill",
            "gecmis",
            "--data-dir",
            "veri",
            "--instance",
            "a",
            "--instance",
            "b",
        ]);
        assert!(e.contains("--instance"), "tek ornek istendigi soylenmeli: {e}");

        // Sessizce yok saymak, "--import-backfill yazmayı unuttum" hatasını
        // fark edilmez yapardı.
        assert!(err(&["--data-dir", "veri"]).contains("--import-backfill"));

        let a = Args::from_argv(argv(&[
            "--import-backfill",
            "gecmis",
            "--data-dir",
            "veri",
            "--instance",
            "mt5-1",
        ]))
        .unwrap();
        let imp = a.import.expect("ice aktarma kipi kurulmali");
        assert_eq!(imp.src_dir, PathBuf::from("gecmis"));
        assert_eq!(imp.data_dir, PathBuf::from("veri"));
        assert_eq!(imp.instance, "mt5-1");
        // Bu kip bir sunucu DEĞİL: canlı varsayılanları kurulmamalı.
        assert!(a.replay.is_none() && a.paper.is_none() && a.record.is_none());
    }

    #[test]
    fn import_refuses_to_share_a_run_with_the_other_modes() {
        // İçe aktarma kayda YAZAR; replay okur, kaydedici de yazar. Aynı
        // çalıştırmada ikisi, oynatılan/kaydedilen günün altından dosyayı
        // çekmek olurdu — ve `--record` ile aynı dizin kilidi zaten çakışırdı.
        for extra in [
            vec!["--replay", "veri", "--replay-date", "20260811"],
            vec!["--record", "veri"],
            vec!["--paper-bind", "127.0.0.1:8788"],
            vec!["--enable-trading"],
        ] {
            let mut flags =
                vec!["--import-backfill", "gecmis", "--data-dir", "veri", "--instance", "mt5-1"];
            flags.extend(extra.iter().copied());
            let e = err(&flags);
            assert!(
                e.contains("--import-backfill"),
                "{extra:?} icin acik hata bekleniyordu: {e}"
            );
        }
    }

    #[test]
    fn day_time_formatting_is_utc_and_millisecond_exact() {
        assert_eq!(hhmmss_utc(3_661_123), "01:01:01.123");
        assert_eq!(hhmmss_utc(0), "00:00:00.000");
        assert_eq!(hhmmss_utc(DAY_MS + 1_000), "00:00:01.000");
    }

    #[test]
    fn the_replay_span_is_printed_with_its_date_because_a_range_spans_days() {
        // Yalnızca saati yazmak tek günde işe yarıyordu; 66 günlük bir
        // aralıkta "17:00:00" hangi günün 17:00'si olduğunu söylemez.
        assert_eq!(stamp_hhmmss_utc(0), "19700101 00:00:00.000");
        assert_eq!(stamp_hhmmss_utc(DAY_MS + 3_661_123), "19700102 01:01:01.123");
    }

    // --- --replay-date: gün aralığı ---

    #[test]
    fn replay_date_accepts_a_day_a_closed_range_and_an_open_range() {
        for spec in ["20260513", "20260513-20260812", "20260513-"] {
            let a = Args::from_argv(argv(&["--replay", "veri", "--replay-date", spec]))
                .unwrap_or_else(|e| panic!("{spec} kabul edilmeliydi: {e}"));
            // Ham değer olduğu gibi taşınmalı: çözümü `replay::load` yapar.
            assert_eq!(a.replay.expect("replay kipi").opts.date, spec);
        }
    }

    #[test]
    fn a_malformed_replay_date_range_is_caught_before_touching_the_disk() {
        // Hepsi AÇIK hata vermeli ve hangi bayrak olduğunu SÖYLEMELİ:
        // aralığı sessizce ilk parçaya indirmek, istenmeyen bir günü
        // oynatmak olurdu ve sonuç doğru görünürdü.
        for spec in [
            "2026-08-11",                 // ISO tarih
            "20260513-20260812-20260901", // fazladan ayirici
            "-20260812",                  // baslangic yok
            "2026051-20260812",           // kisa baslangic
            "20260513-2026081",           // kisa bitis
            "20260812-20260513",          // ters aralik
        ] {
            let e = err(&["--replay", "veri", "--replay-date", spec]);
            assert!(e.contains("--replay-date"), "{spec}: hangi bayrak oldugu soylenmeli: {e}");
        }

        // Ters aralık, biçim hatasından AYRI bir sebeple reddedilmeli.
        let e = err(&["--replay", "veri", "--replay-date", "20260812-20260513"]);
        assert!(e.contains("20260812-20260513"), "verilen deger yazilmali: {e}");

        // Eksik bayrak mesajı da üç biçimi anmalı.
        let e = err(&["--replay", "veri"]);
        assert!(e.contains("20260513-20260812"), "kabul edilen bicimler yazilmali: {e}");
    }

    #[test]
    fn the_day_window_flags_still_work_alongside_a_range() {
        // `--replay-from` / `--replay-to` aralıkla birlikte HER GÜNE
        // uygulanır; ayrıştırma bunları aralık kipinde de kabul etmeli.
        let a = Args::from_argv(argv(&[
            "--replay",
            "veri",
            "--replay-date",
            "20260513-20260812",
            "--replay-from",
            "09:00",
            "--replay-to",
            "17:00",
        ]))
        .unwrap();
        let cfg = a.replay.expect("replay kipi");
        assert_eq!(cfg.opts.from.as_deref(), Some("09:00"));
        assert_eq!(cfg.opts.to.as_deref(), Some("17:00"));
    }
}
