//! WebSocket sunucusu — feed dağıtımı ve emir kabulü.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use sinyal_proto::{
    action, filling, order_type, sym_flag, type_time, write_fixed_str, Cmd, COMMENT_LEN,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

use crate::history::{HistCmd, HistReply, HistStatus};
use crate::sim::{
    retcode as simret, PriceBook, SimCause, SimEvent, SimEventKind, SimOrderKind, SimOrderReq,
    SimOutcome, SimSymbol,
};
use crate::source::FeedEvent;
use crate::state::Registry;
use crate::wire::{
    AccountInfo, ClientMsg, OrderEvent, OrderInfo, OrderReq, PositionInfo, ServerMsg, SymbolInfo,
    TickSnap,
};

/// MT5 görüntüsü bu yaştan eskiyse `candles` isteği yeniden çekim tetikler.
///
/// Her istekte çekmek `CopyRates`'i sürekli meşgul ederdi; hiç çekmemek bayat
/// seri servis etmek olurdu. Oluşmakta olan barın da makul bir sıklıkta
/// tazelenmesi gerekiyor.
const HIST_REFRESH_MS: i64 = 20_000;

/// WebSocket tarafının geçmiş cevabı için beklediği azami süre.
///
/// Okuyucu tarafındaki zaman aşımından (`history::DEFAULT_TIMEOUT`) biraz
/// uzun: normalde cevabı oradan alırız, bu yalnızca okuyucu thread'i tamamen
/// tıkanırsa devreye giren son çare.
const HIST_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Aynı anda MT5'e gidebilecek azami geçmiş isteği (tüm bağlantılar toplamı).
///
/// `candles` işlemi **token istemez** ve uç ağa açılabilir. Sınırsız bıraksak
/// bir istemci `CopyRates`'i sürekli meşgul ederek ticaret terminalini
/// yavaşlatabilirdi — geçmiş çekmek ucuz değil. Sınır dolduğunda istek
/// reddedilmez, depodaki görüntüyle cevaplanır ve `hist: "refused"` denir.
///
/// NOT: bu bir eş zamanlılık tavanıdır, hız sınırı DEĞİL. Sürekli yeniden
/// istek gönderen bir istemci hâlâ terminali meşgul edebilir.
pub const HIST_SLOTS: usize = 4;

/// `modify_sltp` gönderildikten sonra stop'un durum yayınında görünmesi için
/// tanınan süre.
///
/// Durum yayını saniyede bir tazeleniyor; üç saniye, broker'ın emri işlemesi
/// artı iki yayın turu için makul bir pay bırakır. Daha kısası taze bir
/// görüntü gelmeden alarm çalardı (yanlış uyarı, en pahalı uyarı türüdür:
/// birkaç kez tekrarlanınca gerçek olan da yok sayılır). Daha uzunu ise
/// korumasız pozisyonun sessiz kaldığı pencereyi büyütürdü.
pub const SLTP_VERIFY_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);

/// Bekleyen doğrulamaların tarandığı sıklık.
///
/// Pencereden çok daha kısa: stop erkenden kurulduysa uyarı üretilmeden
/// kapatılabilsin ve süre dolduğunda uyarı geç kalmasın.
const SLTP_SWEEP: std::time::Duration = std::time::Duration::from_millis(250);

/// Durum görüntüsü bu yaştan sonra "BAKAMIYORUZ" sayılır.
///
/// EA durumu **saniyede bir** yayımlıyor (`OnTimer`, heartbeat + olay
/// güdümlü). Beş saniye, bir-iki turun kaçmasına pay bırakır ama zombi bir
/// köprüyü gizlemez.
///
/// Bu eşiğin tek işi "pozisyonu listede GÖREMEDİK" cevabını yorumlamak:
/// görüntü TAZE ise pozisyon gerçekten kapanmıştır (uyarı YOK), BAYAT ise
/// hiçbir şey bilmiyoruz demektir (uyarı VAR).
const SLTP_STATE_STALE: std::time::Duration = std::time::Duration::from_secs(5);

/// Sunucunun paylaşılan bağlamı.
pub struct Ctx {
    pub registry: Arc<Registry>,
    pub events: broadcast::Sender<FeedEvent>,
    /// Örnek adı → o örneğin okuyucu thread'ine giden komut kanalı.
    pub cmd_tx: HashMap<String, std::sync::mpsc::Sender<Cmd>>,
    /// Örnek adı → geçmiş bar isteği kanalı.
    ///
    /// Emir kanalından AYRI: binlerce barlık bir geçmiş turu emir gönderimini
    /// geciktirmemeli.
    pub hist_tx: HashMap<String, std::sync::mpsc::Sender<HistCmd>>,
    /// Boşsa kimlik doğrulama gerekmez.
    pub token: Option<String>,
    /// `false` ise hiçbir emir kabul edilmez (EA'daki bayrağa ek ikinci kapı).
    pub trading: bool,
    /// Demo OLMAYAN hesapta emir yürütmeye izin ver.
    ///
    /// Varsayılan `false`: hesap tipi okunamıyorsa (UNKNOWN) da gerçek sayılır
    /// ve emir reddedilir — emniyetli taraf.
    pub allow_live: bool,
    /// Varsayılan kayma toleransı (point).
    pub deviation: u32,
    pub orders: Arc<OrderTracker>,
    /// Mum deposu — okuyucu thread'i yazar, WS okur.
    pub candles: Arc<Mutex<crate::candles::CandleStore>>,
    /// Eş zamanlı geçmiş çekme tavanı (bkz. [`HIST_SLOTS`]).
    pub hist_slots: Arc<tokio::sync::Semaphore>,
    /// `Some` ise emirler **SİMÜLE** edilir (paper veya replay).
    ///
    /// Bu alan iki şeyi birden yönetir ve ikisi de güvenlik meselesidir:
    ///
    /// 1. `hello.mode` `"paper"`/`"replay"` olur ve emir olayları `sim: true`
    ///    taşır — istemci simülasyonu canlı SANMAMALIDIR.
    /// 2. `Some` iken paylaşılan belleğe **hiçbir komut yazılmaz**; emirler
    ///    [`SimExec`] içindeki motorda simüle edilir. Bu sınır [`dispatch`]
    ///    içinde ayrıca bir kez daha zorlanır: kazara gerçek emir gitmesi
    ///    kabul edilemez.
    ///
    /// **`replay` ile ilişkisi:** `replay.is_some()` ise `sim` de daima
    /// `Some`tur. Tersi doğru değil — paper kipinde `sim` doludur ama
    /// `replay` boştur, çünkü veri CANLIDIR.
    pub sim: Option<Arc<SimExec>>,
    /// `Some` ise **REPLAY** kipi: akış diskteki kayıttan geliyor.
    ///
    /// Yalnızca kayda özgü şeyleri tutar (kapsam, bitiş bildirimi, oynatım
    /// kapısı). Emir simülasyonu buraya DEĞİL [`Ctx::sim`]e bakar: paper
    /// kipinde de aynı motor çalışıyor ve iki kipin emir yolu ayrışırsa
    /// birinde düzeltilen hata diğerinde kalırdı.
    pub replay: Option<Arc<Replay>>,
    /// Broker'a yazılan stop'ların DOĞRULAMA defteri (bkz. [`SltpGuard`]).
    pub sltp: Arc<SltpGuard>,
}

impl Ctx {
    /// Simüle yürütme kipi mi (paper veya replay).
    pub fn sim(&self) -> Option<&SimExec> {
        self.sim.as_deref()
    }

    /// `hello.mode` — tek kaynak.
    pub fn mode(&self) -> &'static str {
        self.sim().map(|s| s.mode).unwrap_or(MODE_LIVE)
    }
}

// ---------------------------------------------------------------------------
// Simüle yürütme kipleri
// ---------------------------------------------------------------------------

/// Gerçek MT5 terminali, gerçek emirler.
pub const MODE_LIVE: &str = "live";
/// **Canlı veri, simüle yürütme.** Paylaşılan belleğe hiçbir komut gitmez.
pub const MODE_PAPER: &str = "paper";
/// Diskteki kayıttan oynatım, simüle yürütme.
pub const MODE_REPLAY: &str = "replay";

/// Simülatörün MODELLEDİĞİ şeyler — `hello` bunu ilan eder.
///
/// Liste tel üzerinde gider çünkü "neyin modellendiği" bir yayın notu değil,
/// sinyal sisteminin sonucu yorumlarken ihtiyaç duyduğu veridir: kaymayı
/// modellemeyen bir simülasyonun kâr eğrisi ile modelleyeninki farklı
/// şeylerdir ve ikisini aynı grafikte karşılaştırmak yanlış olurdu.
pub const SIM_MODELED: &[&str] = &[
    "spread (alis ASK, satis BID; LONG BID'den, SHORT ASK'ten kapanir)",
    "aleyhte kayma (piyasa ve STOP dolumlarinda, --sim-slippage)",
    "bekleyen emir tetiklenmesi (MT5 semantigi)",
    "SL/TP tetiklenmesi (ayni tick ikisini de vurursa SL kazanir)",
    "stops_level ihlali (retcode 10016)",
    "marjin yetersizligi (retcode 10019)",
    "hacim izgarasi (retcode 10014)",
    "emir son kullanma (expire_sn/expiration; suresi dolan emir DOLMAZ)",
];

/// Simülatörün MODELLEMEDİĞİ şeyler — `hello` bunu da ilan eder.
///
/// Tahmin EDİLMEZ: modellenmeyen bir maliyeti uydurmak, sonucun ne olduğu
/// konusunda yalan söylemek olurdu. Ama sessiz kalmak da yalan olurdu —
/// bu yüzden liste açıkça gider.
pub const SIM_NOT_MODELED: &[&str] = &[
    "komisyon",
    "swap (gecelik tasima maliyeti)",
    "requote / deviation penceresi",
    "kur cevrimi (kar sembolun kotasyon biriminde kalir)",
    "kismi dolum ve likidite derinligi",
    "teminat tamamlama (stop-out), freeze_level",
    "hafta sonu bosluklari ve seans kesintileri",
];

/// Simüle dolumun ne OLMADIĞINI söyleyen tek cümle.
pub const SIM_WARNING: &str =
    "Simule dolum GERCEK DOLUM DEGILDIR: komisyon, swap ve requote modellenmez.";

/// Simüle yürütme bağlamı — **paper ve replay kiplerinde dolu, canlıda boş**.
///
/// İki kip aynı motoru paylaşır. Ayrı birer simülatör yazmak, birinde
/// düzeltilen bir hatanın diğerinde yaşamaya devam etmesi demekti; oysa
/// projenin amacı tam tersi — test ile canlı arasındaki farkı kapatmak.
pub struct SimExec {
    /// [`MODE_PAPER`] veya [`MODE_REPLAY`]. `hello.mode` bunu yayar.
    pub mode: &'static str,
    /// İlan edilecek örnek adları (paper'da canlı örnekler, replay'de kayıt).
    pub instances: Vec<String>,
    /// Örneği çözülemeyen simüle olaylarda kullanılan varsayılan ad.
    pub primary: Arc<str>,
    /// Simülasyon motoru. Tek kilit: emir yolu da tick pompası da buradan
    /// geçer, böylece bir emir ile bir tetiklenme ARASINA giremez.
    pub engine: Mutex<crate::sim::SimEngine>,
    /// Motorun ayarları (`hello` ilan eder).
    pub cfg: crate::sim::SimConfig,
}

impl SimExec {
    pub fn new(mode: &'static str, instances: &[String], cfg: crate::sim::SimConfig) -> Self {
        Self {
            mode,
            instances: instances.to_vec(),
            // Ad uydurmaktansa kipin adını kullanıyoruz.
            primary: Arc::from(instances.first().map(String::as_str).unwrap_or(mode)),
            engine: Mutex::new(crate::sim::SimEngine::new(cfg)),
            cfg,
        }
    }

    /// Simüle olayın `src` alanı: sembolün çözüldüğü örnek, yoksa varsayılan.
    fn src_of(&self, instance: Option<&str>) -> Arc<str> {
        match instance {
            Some(i) if self.instances.iter().any(|k| k == i) => Arc::from(i),
            _ => self.primary.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, crate::sim::SimEngine> {
        self.engine.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Simüle motoru **HER TİCK'LE** besle ve doğan olayları yayınla.
///
/// Bu çağrı olmadan bekleyen emirler ve SL/TP hiç tetiklenmez — eski
/// simülatörün en büyük yalanı buydu: emir defterinde duran bir stop, fiyat
/// üstünden geçse bile sonsuza kadar bekliyordu.
///
/// Çağıran iki yerdedir ve ikisi de tick'in TEK sahibidir: replay oynatım
/// döngüsü (kayıt temposu) ve paper köprüsü (canlı yayın). Yayın kanalına
/// abone olan üçüncü bir pompa yazmadık; `broadcast` geride kalınca tick
/// DÜŞÜRÜR ve düşen tick, tetiklenmeyen bir stop demektir.
pub fn pump_tick(
    sim: &SimExec,
    events: &broadcast::Sender<FeedEvent>,
    instance: &str,
    symbol: &str,
    bid: f64,
    ask: f64,
    time_msc: i64,
) {
    let evs = sim.lock().on_tick(symbol, bid, ask, time_msc);
    if evs.is_empty() {
        return;
    }
    let src = sim.src_of(Some(instance));
    for ev in &evs {
        let _ = events.send(feed_order(&src, ev));
    }
}

// ---------------------------------------------------------------------------
// Replay kipi
// ---------------------------------------------------------------------------

/// Oynatımın bittiğini bildiren özet (motor doldurur).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ReplayEnd {
    pub ticks: u64,
    /// Oynatılan son tick'in broker saati; kapsam boşsa 0.
    pub last_ms: i64,
    /// Oynatılması planlanan gün sayısı (aralıktaki kayıtlı günler).
    pub days: u32,
    /// Gerçekten oynatılan gün sayısı.
    ///
    /// `days`ten küçükse aralık yarıda kesilmiştir. Bu iki sayı tel üzerine
    /// çıkıyor: bir gün okunamadığında oynatım duruyordu ama istemci normal
    /// bir `replay_done` gördüğü için 3 aylık sandığı bir backtest'i tek
    /// günün sonucuyla raporlayabilirdi.
    pub days_played: u32,
}

/// Replay kipinin bağlamı.
///
/// Kaydı gerçekten oynatan motor ayrı bir modüldedir; burası sunucunun o
/// kipte bilmesi gereken **kayda özgü** şeyleri tutar. Emir simülasyonu
/// burada DEĞİL [`SimExec`] içindedir: paper kipi de aynı motoru kullanıyor.
pub struct Replay {
    /// Kayıtta bulunan örnek adları (sıralı).
    ///
    /// `hello.instances` bunları ilan eder: oynatım motoru sembol tablosunu
    /// yazmadan bağlanan bir istemci de hangi kaydı dinlediğini görmeli.
    pub instances: Vec<String>,
    /// İstenen kapsam (epoch ms). `hello` bunları ilan eder.
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    /// Oynatım bitti bildirimi. Motor `Some(..)` yazdığında bağlı tüm
    /// istemcilere `replay_done` gider.
    pub done: tokio::sync::watch::Receiver<Option<ReplayEnd>>,
    /// Oynatımı başlatan kapı; **ilk bağlanan istemci** açar.
    ///
    /// Oynatım süreç başlarken akmaya başlasaydı, yayın kanalının o an hiç
    /// abonesi olmadığı için tick'ler boşluğa gider ve istemci yalnızca
    /// `replay_done` görürdü (bkz. `replay::StartGate`).
    pub start: Arc<crate::replay::StartGate>,
}

impl Replay {
    pub fn new(
        instances: &[String],
        from_ms: Option<i64>,
        to_ms: Option<i64>,
        done: tokio::sync::watch::Receiver<Option<ReplayEnd>>,
        start: Arc<crate::replay::StartGate>,
    ) -> Self {
        Self { instances: instances.to_vec(), from_ms, to_ms, done, start }
    }
}

/// İstemci kimliği (metin) ile wire kimliği (u64) arasındaki eşleme.
///
/// `Cmd.magic` 64 bit olduğu için wire kimliği doğrudan MT5 `magic`'ine
/// taşınır; broker'ın ezebildiği `comment`'e güvenmeye gerek kalmaz.
#[derive(Default)]
pub struct OrderTracker {
    next: AtomicU64,
    /// metin kimlik → wire kimlik (idempotency denetimi)
    by_text: Mutex<HashMap<String, u64>>,
    /// wire kimlik → metin kimlik (sonuçları geri eşlemek için)
    by_wire: Mutex<HashMap<u64, String>>,
}

impl OrderTracker {
    pub fn new() -> Self {
        Self { next: AtomicU64::new(1), ..Default::default() }
    }

    /// Yeni bir emir kaydet. Bu metin kimlik daha önce kullanıldıysa `Err`
    /// döner — aynı emri iki kez göndermek gerçek parayla çift pozisyon
    /// demektir, sessizce kabul edilemez.
    pub fn register(&self, text_id: &str) -> Result<u64, u64> {
        let mut by_text = self.by_text.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = by_text.get(text_id) {
            return Err(*existing);
        }
        let wire = self.next.fetch_add(1, Ordering::Relaxed);
        by_text.insert(text_id.to_owned(), wire);
        self.by_wire
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(wire, text_id.to_owned());
        Ok(wire)
    }

    pub fn text_of(&self, wire: u64) -> Option<String> {
        self.by_wire
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&wire)
            .cloned()
    }
}

/// Doğrulanmayı bekleyen tek bir stop.
#[derive(Debug, Clone)]
struct PendingSltp {
    /// `modify_sltp` isteğinin istemci kimliği — uyarı bununla geri bağlanır.
    id: String,
    ticket: u64,
    want_sl: f64,
    /// Bu ana kadar doğrulanmazsa uyarı üretilir.
    deadline: std::time::Instant,
}

/// **Broker'a yazılan stop gerçekten kuruldu mu?**
///
/// `modify_sltp`in kabul edilmesi, stop'un kurulduğu anlamına GELMEZ: broker
/// `stops_level`/`freeze_level` ihlalinde, requote'ta ya da "invalid stops"
/// (10016) ile emri düşürebilir ve pozisyon SESSİZCE korumasız kalır. Bu
/// defter komutu kaydeder, durum yayınındaki `sl`i izler ve istenen değere
/// ulaşmazsa [`FeedEvent::SltpUnverified`] üretir.
///
/// # Kapsam
///
/// Bu **köprünün kendi stop mantığının yerine geçmez**. Kullanıcının kararı:
/// broker stop'u ASIL koruma, köprü stop'u YEDEK — ikisi de durur. İkisi aynı
/// anda tetiklenirse ikinci kapatma "pozisyon yok" alır; zararsız ama
/// loglanır.
///
/// # Sayaçlar
///
/// Kaç komut gönderildi, kaçı doğrulandı, kaçı doğrulanamadı — telemetriye
/// çıkar. Doğrulanamayan bir stop sessiz kalırsa, korumasız bir pozisyonu
/// korunmuş sanmak demektir.
#[derive(Debug, Default)]
pub struct SltpGuard {
    pending: Mutex<Vec<PendingSltp>>,
    sent: AtomicU64,
    verified: AtomicU64,
    unverified: AtomicU64,
    closed: AtomicU64,
}

/// `(gonderilen, dogrulanan, dogrulanamayan, kapanmis)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SltpCounts {
    pub sent: u64,
    pub verified: u64,
    pub unverified: u64,
    /// Pozisyon doğrulanmadan KAPANDI — uyarı üretilmedi.
    ///
    /// Kapanmış pozisyonun stop'u da yoktur; bunu "doğrulanamadı" saymak
    /// her stop'a-değen pozisyonda yanlış alarm üretirdi. Ama sessizce
    /// yutmak da doğru değil: sayı beklenmedik biçimde büyükse, stop
    /// komutları pozisyonlar kapandıktan SONRA gidiyor demektir.
    pub closed: u64,
}

impl SltpGuard {
    /// Doğrulamayı KUR — komut kabul edildikten sonra çağrılır.
    fn arm(&self, id: &str, ticket: u64, want_sl: f64, now: std::time::Instant) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).push(PendingSltp {
            id: id.to_owned(),
            ticket,
            want_sl,
            deadline: now + SLTP_VERIFY_WINDOW,
        });
    }

    pub fn counts(&self) -> SltpCounts {
        SltpCounts {
            sent: self.sent.load(Ordering::Relaxed),
            verified: self.verified.load(Ordering::Relaxed),
            unverified: self.unverified.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
        }
    }
}

/// Bir bağlantının yetki seviyesi.
///
/// Bağlantı **Public** olarak başlar: piyasa verisi (tick, derinlik, mum,
/// sembol listesi) token istemez — grafik çizen bir istemcinin gizli bir şeye
/// ihtiyacı yok. `auth` başarılı olunca **Trader**'a yükselir ve hesap
/// bilgisi + emir yürütme açılır.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Public,
    Trader,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Public => "public",
            Level::Trader => "trader",
        }
    }
}

/// İki gizli değeri **sabit zamanda** karşılaştır.
///
/// Sıradan `==` ilk farklı baytta döner; saldırgan yanıt süresinden token'ı
/// bayt bayt tahmin edebilir. Bağımlılık eklemeye değmeyecek kadar basit.
fn secret_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    // Uzunluk farkı zaten sızar; onu gizlemeye çalışmıyoruz, önemli olan
    // eşit uzunlukta erken dönmemek.
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Bir istemcinin abonelikleri.
#[derive(Default)]
struct Subs {
    tick_all: bool,
    book_all: bool,
    ticks: HashSet<String>,
    books: HashSet<String>,
    /// "SEMBOL.TF" biçiminde; joker "*.TF" tüm sembolleri kapsar.
    candles: HashSet<String>,
    candle_all_tf: HashSet<String>,
    orders: bool,
}

impl Subs {
    fn add(&mut self, ch: &str) {
        match ch.split_once('.') {
            Some(("tick", "*")) => self.tick_all = true,
            Some(("tick", s)) => {
                self.ticks.insert(s.to_owned());
            }
            Some(("book", "*")) => self.book_all = true,
            Some(("book", s)) => {
                self.books.insert(s.to_owned());
            }
            // candle.EURUSD.M5  veya  candle.*.M5
            Some(("candle", rest)) => match rest.split_once('.') {
                Some(("*", tf)) => {
                    self.candle_all_tf.insert(tf.to_ascii_uppercase());
                }
                Some((sym, tf)) => {
                    self.candles.insert(format!("{sym}.{}", tf.to_ascii_uppercase()));
                }
                None => {}
            },
            _ if ch == "order" => self.orders = true,
            _ => {}
        }
    }

    fn remove(&mut self, ch: &str) {
        match ch.split_once('.') {
            Some(("tick", "*")) => self.tick_all = false,
            Some(("tick", s)) => {
                self.ticks.remove(s);
            }
            Some(("book", "*")) => self.book_all = false,
            Some(("book", s)) => {
                self.books.remove(s);
            }
            Some(("candle", rest)) => match rest.split_once('.') {
                Some(("*", tf)) => {
                    self.candle_all_tf.remove(&tf.to_ascii_uppercase());
                }
                Some((sym, tf)) => {
                    self.candles.remove(&format!("{sym}.{}", tf.to_ascii_uppercase()));
                }
                None => {}
            },
            _ if ch == "order" => self.orders = false,
            _ => {}
        }
    }

    fn wants(&self, ev: &FeedEvent) -> bool {
        match ev {
            FeedEvent::Tick { symbol, .. } => self.tick_all || self.ticks.contains(&**symbol),
            FeedEvent::Book { symbol, .. } => self.book_all || self.books.contains(&**symbol),
            // Stop doğrulama uyarısı `order` kanalından gider: emir gönderen
            // istemci zaten oraya abone ve korumasız pozisyonu ilgilendiren
            // taraf odur. Ayrı bir kanal, aboneliği unutan bir istemcinin
            // uyarıyı hiç görmemesi demek olurdu.
            FeedEvent::Order { .. } | FeedEvent::SltpUnverified { .. } => self.orders,
            FeedEvent::Candle { symbol, tf, .. } => {
                self.candle_all_tf.contains(*tf) || self.candles.contains(&format!("{symbol}.{tf}"))
            }
        }
    }
}

/// Sunucuyu çalıştır.
pub async fn serve(listener: TcpListener, ctx: Arc<Ctx>) {
    // Stop doğrulama tarayıcısı. Bağlantı başına DEĞİL, sunucu başına: bir
    // stop'un kurulup kurulmadığı, komutu gönderen bağlantının hâlâ açık
    // olmasına bağlı olmamalı — kopan bir istemcinin bıraktığı korumasız
    // pozisyon tam da endişe ettiğimiz durum.
    let guard = ctx.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SLTP_SWEEP);
        loop {
            tick.tick().await;
            for ev in sweep_sltp(&guard, std::time::Instant::now()) {
                if let FeedEvent::SltpUnverified { id, ticket, want_sl, actual_sl, why, .. } = &ev {
                    // Tel üzerinde uyarı gitse bile daemon günlüğünde de
                    // durmalı: istemci abone değilse ya da düşmüşse bunun
                    // hiçbir yerde iz bırakmaması kabul edilemez.
                    eprintln!(
                        "[stop] UYARI: sltp DOGRULANAMADI id={id} bilet={ticket} \
                         istenen_sl={want_sl} gercek_sl={} - {why}",
                        actual_sl.map(|v| v.to_string()).unwrap_or_else(|| "yok".into())
                    );
                }
                let _ = guard.events.send(ev);
            }
        }
    });

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, ctx).await {
                        eprintln!("[ws] {peer} bağlantısı sonlandı: {e}");
                    }
                });
            }
            Err(e) => eprintln!("[ws] accept hatası: {e}"),
        }
    }
}

async fn handle(stream: TcpStream, ctx: Arc<Ctx>) -> Result<(), String> {
    // Nagle kapalı: küçük fiyat mesajlarının birikmesini beklemek, tüm
    // gecikme çalışmasını boşa çıkarırdı.
    let _ = stream.set_nodelay(true);

    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| format!("el sıkışma: {e}"))?;
    let (mut out, mut inp) = ws.split();

    let mut rx = ctx.events.subscribe();
    let mut subs = Subs::default();
    let auth_required = ctx.token.is_some();

    // NOT: replay oynatım kapısı burada AÇILMAZ. Bağlantı kurulmuş olması,
    // istemcinin tick almaya hazır olduğu anlamına gelmiyor — kanal
    // abonelikleri henüz boş. Kapı `subscribe` işlendikten sonra açılıyor
    // (bkz. `handle_client_msg`).

    // Bu bağlantıya ait, GECİKMELİ üretilen cevapların kuyruğu.
    //
    // Geçmiş isteği MT5'ten saniyeler sürebilir. Cevabı burada `await`
    // etseydik o süre boyunca tick akışını pompalamayı bırakırdık; yayın
    // kanalı dolar ve istemci `lagged` yerdi — bir grafik isteği yüzünden
    // FİYAT KAYBI. Bu yüzden bekleme ayrı bir göreve alınıyor ve cevabı bu
    // kuyruktan geçiyor.
    let (local_tx, mut local_rx) = tokio::sync::mpsc::channel::<ServerMsg>(64);

    // Token TANIMLI DEĞİLSE bağlantı doğrudan Trader başlar.
    //
    // Aksi halde `--token` verilmeyen bir kurulumda hiçbir istemci Trader'a
    // yükselemez (`auth` "sunucuda token tanimli degil" der) ve işlem yüzeyi
    // KALICI olarak kilitli kalırdı — üstelik `hello` tam tersini,
    // `auth_required_for_trading: false` diyerek ilan ederdi. Bu tutarsızlık
    // canlı testte ortaya çıktı: emir "auth gerekli" ile reddedildi.
    //
    // `--token` verildiği anda başlangıç seviyesi tekrar Public olur.
    let mut level = if auth_required { Level::Public } else { Level::Trader };

    send(&mut out, &hello_msg(&ctx, level)).await?;

    // Replay bitiş bildirimi. Oynatım bu bağlantıdan ÖNCE bitmiş olabilir —
    // o durumda `replay_done` hemen gider, yoksa motor haber verdiğinde.
    let mut done_rx = ctx.replay.as_ref().map(|r| r.done.clone());
    let mut done_sent = false;
    if let Some(end) = done_rx.as_ref().and_then(|rx| *rx.borrow()) {
        send(&mut out, &replay_done(end)).await?;
        done_sent = true;
    }

    loop {
        tokio::select! {
            // --- replay bitti ---
            changed = async {
                match done_rx.as_mut() {
                    Some(rx) if !done_sent => rx.changed().await.is_ok(),
                    // Canlı kip (veya zaten gönderildi): bu kol hiç uyanmaz.
                    _ => std::future::pending().await,
                }
            } => {
                match (changed, done_rx.as_ref().and_then(|rx| *rx.borrow())) {
                    (true, Some(end)) => {
                        send(&mut out, &replay_done(end)).await?;
                        done_sent = true;
                    }
                    // Kanal kapandı: motor bildirmeden öldü. Bu kolu
                    // SUSTURUYORUZ — kapanmış bir `watch` alıcısı anında
                    // dönerdi ve bu select bir çekirdeği boş yere yakardı.
                    // Bildirim gelmemesi bir bilgi kaybı ama sessizce dönen
                    // bir döngü fiyat akışını da yavaşlatırdı.
                    (false, _) => done_sent = true,
                    // Değer hâlâ boş: gerçek bitişi beklemeye devam.
                    (true, None) => {}
                }
            }

            // --- gecikmeli üretilmiş cevaplar (geçmiş) ---
            Some(m) = local_rx.recv() => {
                send(&mut out, &m).await?;
            }

            // --- akıştan gelen olaylar ---
            ev = rx.recv() => {
                match ev {
                    Ok(ev) => {
                        if !subs.wants(&ev) {
                            continue;
                        }
                        let msg = match to_wire(&ev, &ctx) {
                            Some(m) => m,
                            None => continue,
                        };
                        send(&mut out, &msg).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Sessizce atlamıyoruz: fiyat akışındaki boşluk,
                        // istemcinin yanlış karar vermesi demektir.
                        send(&mut out, &ServerMsg::Lagged { dropped: n }).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }

            // --- istemciden gelen istekler ---
            msg = inp.next() => {
                let Some(msg) = msg else { return Ok(()) };
                let msg = msg.map_err(|e| format!("okuma: {e}"))?;
                match msg {
                    Message::Text(txt) => {
                        let parsed: Result<ClientMsg, _> = serde_json::from_str(&txt);
                        let Ok(cm) = parsed else {
                            send(&mut out, &ServerMsg::Error {
                                msg: format!("mesaj ayrıştırılamadı: {}", parsed.unwrap_err()),
                            }).await?;
                            continue;
                        };
                        // Geçmiş isteği MT5'e gidebilir ve saniyeler sürebilir;
                        // burada beklemek tick akışını durdururdu. Ayrı göreve
                        // alıp cevabı `local_tx` üzerinden geri veriyoruz.
                        if let ClientMsg::Candles { symbol, tf, count } = cm {
                            match ctx.hist_slots.clone().try_acquire_owned() {
                                Ok(permit) => {
                                    let ctx2 = ctx.clone();
                                    let tx2 = local_tx.clone();
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        for m in candles_op(&ctx2, symbol, tf, count).await {
                                            if tx2.send(m).await.is_err() {
                                                break; // bağlantı kapandı
                                            }
                                        }
                                    });
                                }
                                // Tavan dolu: MT5'i daha fazla meşgul etmiyoruz
                                // ama istemciyi de eli boş göndermiyoruz —
                                // depodaki görüntüyü, neden tazelenmediğini
                                // söyleyerek veriyoruz.
                                Err(_) => {
                                    let msgs = candles_from_store(
                                        &ctx,
                                        symbol,
                                        tf,
                                        count,
                                        "refused",
                                        Some("es zamanli gecmis cekme tavani dolu".into()),
                                    );
                                    for m in msgs {
                                        send(&mut out, &m).await?;
                                    }
                                }
                            }
                            continue;
                        }
                        if let Some(reply) = handle_client_msg(cm, &ctx, &mut subs, &mut level) {
                            for m in reply {
                                send(&mut out, &m).await?;
                            }
                        }
                    }
                    Message::Ping(p) => {
                        out.send(Message::Pong(p)).await.map_err(|e| format!("pong: {e}"))?;
                    }
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

async fn send<S>(out: &mut S, msg: &ServerMsg) -> Result<(), String>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let txt = serde_json::to_string(msg).map_err(|e| format!("serileştirme: {e}"))?;
    out.send(Message::Text(txt.into())).await.map_err(|e| format!("gönderme: {e}"))
}

/// Bağlantının ilk mesajı.
///
/// Canlı ile simüle kipler arasındaki fark burada, **tek yerde** doğar:
/// `mode`, kapsam alanları ve simülasyon notu. Kanal adları, mesaj biçimleri
/// ve alan adları aynı kalır — sinyal sisteminin kod yolu üç kipte de aynı
/// olmalı.
fn hello_msg(ctx: &Ctx, level: Level) -> ServerMsg {
    let replay = ctx.replay.as_deref();
    let sim = ctx.sim();
    let mut instances = ctx.registry.instances();
    if let Some(r) = replay {
        // Kayıttaki örnek adları, motor sembol tablosunu yazmadan önce de
        // bilinsin: istemci hangi kaydı dinlediğini ilk mesajda görmeli.
        for name in &r.instances {
            if !instances.contains(name) {
                instances.push(name.clone());
            }
        }
        instances.sort();
    }
    ServerMsg::Hello {
        proto: sinyal_proto::ring::RING_VERSION,
        mode: ctx.mode(),
        instances,
        // Simüle kipte emir yürütme daima "açık": emirler reddedilmez, simüle
        // edilir. `--enable-trading` orada bir kapı değil, çünkü kapatacağı
        // bir risk yok — gerçek emir zaten hiç gönderilmiyor.
        trading: sim.is_some() || ctx.trading,
        // Piyasa verisi daima token'sız; yalnızca hesap ve emir kilitli.
        public_feed: true,
        auth_required_for_trading: ctx.token.is_some(),
        level: level.name(),
        replay_from_ms: replay.and_then(|r| r.from_ms),
        replay_to_ms: replay.and_then(|r| r.to_ms),
        // Canlıda HİÇ gönderilmez; simüle kipte neyin modellendiğini ve
        // neyin MODELLENMEDİĞİNİ istemci ilk mesajda görür.
        sim: sim.map(sim_note),
    }
}

/// `hello.sim` — simülasyonun dürüst künyesi.
fn sim_note(s: &SimExec) -> Box<crate::wire::SimNote> {
    Box::new(crate::wire::SimNote {
        balance: s.cfg.balance,
        leverage: s.cfg.leverage,
        slippage_points: s.cfg.slippage_points,
        modeled: SIM_MODELED,
        not_modeled: SIM_NOT_MODELED,
        warning: SIM_WARNING,
    })
}

/// Simüle kipte `candles` cevabının açıklaması; canlıda `None`.
///
/// İki kipin sebebi FARKLI ve ikisini aynı cümleyle geçiştirmek istemciyi
/// yanlış yere baktırırdı: replay'de çekilecek bir terminal YOK; paper'da
/// terminal var ama ona istek göndermiyoruz — güvenlik sınırı.
fn sim_hist_note(ctx: &Ctx) -> Option<String> {
    match ctx.sim()?.mode {
        MODE_REPLAY => Some(crate::replay::HIST_NOTE.to_owned()),
        _ => Some(
            "paper kipinde MT5 gecmisi CEKILMEZ: paylasilan bellege hicbir istek yazilmaz. \
             Barlar canli akistan dolan ortak depodan geliyor."
                .to_owned(),
        ),
    }
}

fn replay_done(end: ReplayEnd) -> ServerMsg {
    ServerMsg::ReplayDone {
        ticks: end.ticks,
        // 0 "kayıt boştu" demek; sıfır zaman damgası göndermek uydurma olurdu.
        last_ms: (end.last_ms != 0).then_some(end.last_ms),
        days: end.days,
        days_played: end.days_played,
        // Planlanan günlerin hepsi oynatılmadıysa bu çalıştırmanın sonucu
        // EKSİK bir kaydın sonucudur ve istemci bunu bilmek zorundadır.
        truncated: end.days_played < end.days,
    }
}

fn to_wire(ev: &FeedEvent, ctx: &Ctx) -> Option<ServerMsg> {
    Some(match ev {
        FeedEvent::Tick { instance, symbol, bid, ask, last, time_msc, lat_us } => ServerMsg::Tick {
            s: symbol.to_string(),
            b: *bid,
            a: *ask,
            l: *last,
            ms: *time_msc,
            lat_us: *lat_us,
            src: instance.to_string(),
        },
        FeedEvent::Book { instance, symbol, time_msc, bids, asks } => ServerMsg::Book {
            s: symbol.to_string(),
            ms: *time_msc,
            bids: bids.clone(),
            asks: asks.clone(),
            src: instance.to_string(),
        },
        // Canlı kapanan bar DAİMA tick kaynaklıdır (MID). İstemci bunu bir
        // `mt5` dizisinin (BID) sonuna eklememeli; bunu ancak kaynağı
        // söylersek bilebilir.
        FeedEvent::Candle { symbol, tf, bar } => ServerMsg::Candle {
            s: symbol.to_string(),
            tf,
            src_kind: crate::candles::BarSource::Tick.name(),
            bar: *bar,
        },
        FeedEvent::Order {
            instance,
            client_id,
            kind,
            retcode,
            order,
            deal,
            position,
            volume,
            price,
            bid,
            ask,
            order_state,
            txn_type,
            source,
            profit,
            commission,
            swap,
            comment,
        } => {
            // Bizim göndermediğimiz bir işlem (ör. elle kapatma) olabilir;
            // metin kimliği yoksa boş bırakıyoruz, mesajı yutmuyoruz.
            let id = ctx.orders.text_of(*client_id).unwrap_or_default();
            ServerMsg::Order(OrderEvent {
                id,
                kind,
                retcode: Some(*retcode),
                order: (*order != 0).then_some(*order),
                deal: (*deal != 0).then_some(*deal),
                position: (*position != 0).then_some(*position),
                volume: (*volume != 0.0).then_some(*volume),
                price: (*price != 0.0).then_some(*price),
                // Sıfır = "EA ölçemedi" (sembol tabloda yok, ya da olay
                // canlı bir dolum değil). Sıfırı telden geçirmek istemciye
                // "spread sıfırdı" dedirtirdi; alan hiç görünmüyorsa
                // istemci ölçüm olmadığını bilir ve tick akışına düşer.
                bid: (*bid != 0.0).then_some(*bid),
                ask: (*ask != 0.0).then_some(*ask),
                order_state: (*order_state != 0).then_some(*order_state),
                txn_type: (*txn_type != 0).then_some(*txn_type),
                // Mutabakat bayrağı yalnız DOĞRUYKEN gider: `false` göndermek
                // her olaya bir alan ekler ve hiçbir şey söylemez.
                reconciled: (*source == sinyal_proto::res_source::RECONCILE).then_some(true),
                // Gerçekleşmiş sonuç YALNIZCA mutabakat olayında anlamlı.
                // Canlı olayda alanlar sıfırdır ve sıfırı telden geçirmek
                // "kâr sıfırdı" dedirtirdi — oysa ölçüm hiç yapılmadı.
                //
                // Mutabakat olayında sıfır GEÇERLİ bir değerdir (komisyonsuz
                // hesapta komisyon gerçekten 0'dır), o yüzden orada sıfır da
                // gönderilir. Ayrım `reconciled` bayrağıyla yapılır.
                profit: (*source == sinyal_proto::res_source::RECONCILE).then_some(*profit),
                commission: (*source == sinyal_proto::res_source::RECONCILE)
                    .then_some(*commission),
                swap: (*source == sinyal_proto::res_source::RECONCILE).then_some(*swap),
                // Emir yaşam döngüsü olayları stop doğrulama alanlarını
                // TAŞIMAZ. Açıkça yazılıyor (`..Default` ile geçiştirilmiyor)
                // ki ileride eklenen bir alan sessizce boş kalmasın.
                ticket: None,
                istenen_sl: None,
                gercek_sl: None,
                state_age_ms: None,
                comment: comment.clone(),
                src: instance.to_string(),
                // Simüle kipte emir olayının TEK kaynağı simülasyon motorudur
                // (paper portunun paylaşılan belleğe komut kanalı YOKTUR,
                // replay'de okuyucu hiç çalışmaz), canlıda ise simüle olay
                // hiç üretilmez. Bayrağı kipe bakarak basmak bu yüzden
                // doğru — ve tek bir yerde kalıyor.
                sim: ctx.sim.is_some(),
            })
        }
        FeedEvent::SltpUnverified {
            instance,
            id,
            ticket,
            want_sl,
            actual_sl,
            state_age_ms,
            why,
        } => ServerMsg::Order(OrderEvent {
            id: id.clone(),
            kind: "sltp_unverified",
            ticket: Some(*ticket),
            istenen_sl: Some(*want_sl),
            gercek_sl: *actual_sl,
            state_age_ms: *state_age_ms,
            comment: (*why).to_owned(),
            src: instance.to_string(),
            sim: ctx.sim.is_some(),
            // `retcode` BİLEREK yok: bu bir broker cevabı değil, bizim
            // gözlemimiz. Uydurma bir kod, istemcinin bunu emir sonucu
            // sanmasına yol açardı.
            ..Default::default()
        }),
    })
}

/// Trader seviyesi gerektiren işlemler için tek tip ret mesajı.
fn needs_auth(what: &str) -> ServerMsg {
    ServerMsg::Error {
        msg: format!("'{what}' icin auth gerekli — once {{\"op\":\"auth\",\"token\":\"...\"}} gonderin"),
    }
}

fn handle_client_msg(
    cm: ClientMsg,
    ctx: &Ctx,
    subs: &mut Subs,
    level: &mut Level,
) -> Option<Vec<ServerMsg>> {
    // Trader gerektiren işlemleri en başta ayıkla. Public yüzey (piyasa
    // verisi) hiçbir kontrolden geçmez — grafik çizen istemci token istemez.
    let trader_only = matches!(
        cm,
        ClientMsg::Account
            | ClientMsg::Positions
            | ClientMsg::Orders
            | ClientMsg::Order(_)
            | ClientMsg::Cancel { .. }
            | ClientMsg::Close { .. }
            | ClientMsg::ModifySltp { .. }
    );
    if trader_only && *level != Level::Trader {
        let what = match cm {
            ClientMsg::Account => "account",
            ClientMsg::Positions => "positions",
            ClientMsg::Orders => "orders",
            ClientMsg::Order(_) => "order",
            ClientMsg::Cancel { .. } => "cancel",
            ClientMsg::Close { .. } => "close",
            ClientMsg::ModifySltp { .. } => "modify_sltp",
            _ => "islem",
        };
        return Some(vec![needs_auth(what)]);
    }

    Some(match cm {
        ClientMsg::Auth { token } => {
            match ctx.token.as_deref() {
                // Sunucuda token yoksa bağlantı zaten Trader olarak başlamıştır
                // (bkz. `level` başlatması). Hata döndürmek yerine mevcut
                // seviyeyi bildiriyoruz: istemcinin `auth` göndermesi zararsız
                // olmalı, aksi halde aynı istemci kodu token'lı ve token'sız
                // kurulumlarda farklı davranmak zorunda kalırdı.
                None => vec![ServerMsg::Authed { level: level.name() }],
                Some(expected) if secret_eq(expected, &token) => {
                    *level = Level::Trader;
                    vec![ServerMsg::Authed { level: Level::Trader.name() }]
                }
                Some(_) => {
                    // Seviye DEĞİŞMEZ; başarısız denemeden sonra public kalır.
                    vec![ServerMsg::Error { msg: "gecersiz token".into() }]
                }
            }
        }
        ClientMsg::Ping => vec![ServerMsg::Pong],

        ClientMsg::Subscribe { channels } => {
            let mut out = Vec::new();
            for c in &channels {
                // `order` kanalı emir akışını taşır — hesap gizliliği kapsamında,
                // token ister. Piyasa kanalları istemez.
                if c == "order" && *level != Level::Trader {
                    out.push(needs_auth("subscribe order"));
                    continue;
                }
                subs.add(c);
            }
            // REPLAY: oynatımı BURADA başlat — abonelikler kurulduktan SONRA.
            //
            // Kapıyı bağlantı kurulurken açmak YETMİYOR ve bu pahalı bir
            // şekilde öğrenildi: yayın kanalına abone olmak, o istemcinin
            // hangi kanalları istediğini bilmekle aynı şey değil. Bağlantıda
            // açsaydık, `--replay-speed 0`da kayıt (binlerce tick, milisaniyeler
            // içinde) istemcinin `subscribe` mesajı daha gelmeden akıp biterdi;
            // pompa her tick'i "abone değil" diye ELERDİ ve istemci yine sıfır
            // tick görürdü. Ölçüldü: bağlantıda açınca 1689 tick'in 0'ı ulaştı.
            //
            // Abonelik kurulduktan sonra açmak, ilk tüketicinin kaydın
            // BAŞINDAN itibaren her tick'i görmesini garanti eder.
            if let Some(r) = &ctx.replay {
                r.start.open();
            }
            out
        }
        ClientMsg::Unsubscribe { channels } => {
            for c in &channels {
                subs.remove(c);
            }
            vec![]
        }

        ClientMsg::Symbols => {
            let items = ctx
                .registry
                .all_symbols()
                .into_iter()
                .map(|(src, e)| SymbolInfo {
                    s: e.name_str().to_owned(),
                    digits: e.digits,
                    point: e.point,
                    tick_size: e.tick_size,
                    volume_min: e.volume_min,
                    volume_max: e.volume_max,
                    volume_step: e.volume_step,
                    exec_mode: e.trade_exemode,
                    contract_size: e.contract_size,
                    filling_mask: e.filling_mode,
                    stops_level: e.stops_level,
                    book_depth: e.ticks_bookdepth,
                    polled_only: e.flags & sym_flag::POLLED_ONLY != 0,
                    chart: e.flags & sym_flag::CHART != 0,
                    ready: e.flags & sym_flag::READY != 0,
                    src,
                })
                .collect();
            vec![ServerMsg::Symbols { items }]
        }

        ClientMsg::Snapshot { symbols } => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let items = ctx
                .registry
                .snapshot(&symbols)
                .into_iter()
                .map(|(src, s, t)| TickSnap {
                    s,
                    b: t.bid,
                    a: t.ask,
                    l: t.last,
                    ms: t.time_msc,
                    age_ms: (now_ms - t.time_msc).max(0),
                    src,
                })
                .collect();
            vec![ServerMsg::Snapshot { items }]
        }

        // Normal yolda bu istek `candles_op` üzerinden gider (MT5'ten çekmeyi
        // de tetikler). Buradaki karşılık yalnız DEPODAN servis eder ve
        // yetki kuralının tek yerde kalmasını sağlar: `candles` PUBLIC'tir,
        // token istemez.
        ClientMsg::Candles { symbol, tf, count } => {
            candles_from_store(ctx, symbol, tf, count, "off", sim_hist_note(ctx))
        }

        // --- hesap yüzeyi: simüle kipte sentetik durum ---
        //
        // Alan adları ve biçimler canlıyla AYNI; yalnızca değerlerin kaynağı
        // farklı. İstemci kodu üç kipte de aynı yolu yürümeli.
        ClientMsg::Account => match ctx.sim() {
            Some(s) => vec![ServerMsg::Account { items: sim_accounts(ctx, s) }],
            None => vec![ServerMsg::Account { items: collect_accounts(ctx) }],
        },

        ClientMsg::Positions => {
            let (items, total, truncated) = match ctx.sim() {
                Some(s) => sim_positions(ctx, s),
                None => collect_positions(ctx),
            };
            vec![ServerMsg::Positions { items, total, truncated }]
        }

        ClientMsg::Orders => {
            let (items, total, truncated) = match ctx.sim() {
                Some(s) => sim_orders(ctx, s),
                None => collect_orders(ctx),
            };
            vec![ServerMsg::Orders { items, total, truncated }]
        }

        // --- emir yüzeyi ---
        //
        // Simüle kipte emirler REDDEDİLMEZ, simüle edilir: yoksa sinyal
        // sisteminin emir yolu hiç test edilemezdi. Bu dallanma aynı zamanda
        // GÜVENLİK SINIRIDIR — sim yolunda `Cmd` hiç üretilmez, dolayısıyla
        // paylaşılan belleğe yazılabilecek bir şey de doğmaz.
        ClientMsg::Order(req) => match ctx.sim() {
            Some(s) => sim_submit_order(req, ctx, s),
            None => vec![submit_order(req, ctx)],
        },

        ClientMsg::Cancel { id, ticket } => match ctx.sim() {
            Some(s) => sim_cancel(ctx, s, &id, ticket),
            None => vec![submit_simple(ctx, &id, action::REMOVE, ticket, 0.0, 0.0, 0.0)],
        },
        ClientMsg::Close { id, ticket, volume } => match ctx.sim() {
            Some(s) => sim_close(ctx, s, &id, ticket, volume),
            None => {
                vec![submit_simple(ctx, &id, action::CLOSE_POSITION, ticket, volume, 0.0, 0.0)]
            }
        },
        // Stop'un broker'a YAZILMASI ile KURULMASI ayrı şeyler. Komut kabul
        // edildiyse doğrulamayı kuruyoruz; kurulmadığı ortaya çıkarsa
        // `sltp_unverified` uyarısı gider (bkz. [`SltpGuard`]).
        ClientMsg::ModifySltp { id, ticket, sl, tp } => {
            let out = match ctx.sim() {
                Some(s) => sim_modify(ctx, s, &id, ticket, sl, tp),
                None => vec![submit_simple(ctx, &id, action::SLTP, ticket, 0.0, sl, tp)],
            };
            arm_sltp_verify(ctx, &id, ticket, sl, &out);
            out
        }
    })
}

/// Depodaki mumları, kaynağını ve geçmiş çekme durumunu bildirerek paketle.
///
/// **Tek kaynak döner**: MT5 barı varsa o, yoksa tick'ten üretilen. İkisi
/// birleştirilmez — birleşim noktası spread'in yarısı kadar sahte bir fiyat
/// boşluğu üretirdi (bkz. [`crate::candles`]).
fn candles_from_store(
    ctx: &Ctx,
    symbol: String,
    tf: String,
    count: usize,
    hist: &'static str,
    hist_note: Option<String>,
) -> Vec<ServerMsg> {
    let Some(canon) = crate::candles::canon_tf(&tf) else {
        let mut names: Vec<&str> = crate::candles::TIMEFRAMES.iter().map(|(n, _)| *n).collect();
        // D1 tick'ten üretilmiyor ama MT5'ten servis ediliyor; listede olmalı.
        names.push("D1");
        return vec![ServerMsg::Error {
            msg: format!("gecersiz tf '{tf}' — gecerli: {}", names.join(", ")),
        }];
    };
    // Üst sınır: bir istemci 10 milyon bar isteyip belleği şişirmesin.
    let n = count.min(crate::candles::MAX_REQUEST);
    let view = ctx
        .candles
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&symbol, canon, n);

    // Boş cevabın sebebi dilimin kendisi olabilir: D1 tick'ten ÜRETİLMİYOR
    // (gün sınırı broker sunucusunun günüdür, bizimkiyle tutmaz). Bunu
    // söylemezsek istemci "veri yok" ile "bu dilim buradan gelmez"i ayırt
    // edemezdi.
    let hist_note = if view.is_empty() && crate::candles::tf_millis(canon).is_none() {
        let why = format!("{canon} yalniz MT5 kaynagindan gelir, tick'ten uretilmez");
        Some(match hist_note {
            Some(n) => format!("{n}; {why}"),
            None => why,
        })
    } else {
        hist_note
    };

    vec![ServerMsg::Candles {
        s: symbol,
        tf: canon.to_string(),
        src_kind: view.src.name(),
        items: view.bars,
        age_ms: view.age_ms,
        hist,
        hist_note,
    }]
}

/// `candles` isteğinin tam yolu: gerekiyorsa MT5'ten çekmeyi tetikler.
///
/// **PUBLIC kalır** — token istemez. Grafik çizen bir istemcinin gizli bir
/// şeye ihtiyacı yok.
///
/// Sıra: (1) depodaki MT5 görüntüsü yeterince taze mi, (2) değilse Service'ten
/// çek ve bekle, (3) elde ne varsa onu — kaynağını söyleyerek — döndür.
/// Çekme başarısız olsa bile cevapsız kalmıyoruz; tick'ten üretilene düşüp
/// `hist` alanında neden düşüldüğünü söylüyoruz.
async fn candles_op(ctx: &Ctx, symbol: String, tf: String, count: usize) -> Vec<ServerMsg> {
    let Some(canon) = crate::candles::canon_tf(&tf) else {
        // Geçersiz dilim: aynı hatayı tek yerden üretelim.
        return candles_from_store(ctx, symbol, tf, count, "off", sim_hist_note(ctx));
    };
    // Simüle kiplerde MT5'ten ÇEKİM YAPILMAZ ve sebep kipe göre farklıdır
    // (bkz. `sim_hist_note`). Sebebi söylemeden `hist: "off"` demek,
    // istemcinin "Service'i açmayı unuttum" sanmasına yol açardı.
    //
    // Paper'da depo CANLI akışla dolduğu için istemci yine gerçek barları
    // görür; yalnızca tazeleme isteğini biz göndermiyoruz.
    if ctx.sim.is_some() {
        return candles_from_store(ctx, symbol, tf, count, "off", sim_hist_note(ctx));
    }
    let n = count.min(crate::candles::MAX_REQUEST);

    // 1) Depodaki görüntü yeterince taze ve yeterince uzunsa dokunma.
    let fresh = {
        let cs = ctx.candles.lock().unwrap_or_else(|e| e.into_inner());
        cs.mt5_status(&symbol, canon)
    };
    if let Some((have, age)) = fresh {
        if have >= n && age < HIST_REFRESH_MS {
            return candles_from_store(ctx, symbol, tf, count, "cached", None);
        }
    }

    // 2) Çekmeyi dene.
    let Some((instance, symbol_id)) = ctx.registry.resolve_any(&symbol) else {
        // Sembol hiçbir örnekte yok; depoda da olamaz ama boş cevabı yine de
        // kaynağıyla veriyoruz.
        return candles_from_store(ctx, symbol, tf, count, "off", Some("sembol bulunamadi".into()));
    };
    let Some(tx) = ctx.hist_tx.get(&instance) else {
        return candles_from_store(ctx, symbol, tf, count, "off", None);
    };

    let (rtx, rrx) = tokio::sync::oneshot::channel();
    if tx
        .send(HistCmd {
            symbol: symbol.clone(),
            symbol_id,
            tf: canon.to_string(),
            count: n as u32,
            // Sayfalama yok: daima en son barlardan geriye. `to_msc` ile
            // geriye sayfalamak deponun "son N bar" yüzeyiyle tutarsız
            // olurdu; ihtiyaç doğduğunda ayrı bir işlem olarak eklenmeli.
            to_msc: 0,
            reply: rtx,
        })
        .is_err()
    {
        return candles_from_store(ctx, symbol, tf, count, "off", Some("okuyucu kapali".into()));
    }

    let (hist, note) = match tokio::time::timeout(HIST_WAIT, rrx).await {
        Ok(Ok(HistReply::Done { status, .. })) => (status.name(), status.detail()),
        Ok(Ok(HistReply::Unavailable)) => ("off", Some("MQL5 Service calismiyor".into())),
        Ok(Ok(HistReply::Refused(why))) => ("refused", Some(why)),
        // Okuyucu thread'i cevap kanalını düşürdü.
        Ok(Err(_)) => ("refused", Some("okuyucu cevap vermedi".into())),
        Err(_) => (
            HistStatus::TimedOut { got: 0 }.name(),
            Some("cekirdek beklemesi doldu".into()),
        ),
    };

    // 3) Elde ne varsa onu, kaynağını söyleyerek ver.
    candles_from_store(ctx, symbol, tf, count, hist, note)
}

/// Emir öncesi ortak denetimler; hata varsa `Err(ServerMsg)`.
///
/// Üç ayrı kapı, her biri farklı bir hatayı yakalar:
/// 1. `--enable-trading` — operatörün açık izni
/// 2. MT5/hesap izinleri — hangi bayrağın düştüğü söylenir
/// 3. **Canlı para kilidi** — demo olmayan hesapta `--allow-live` şart
fn gate(ctx: &Ctx, id: &str) -> Result<u64, ServerMsg> {
    if !ctx.trading {
        return Err(rejected(ctx, id, "emir yurutme kapali (--enable-trading)"));
    }
    // Hangi örnek olduğunu bilmediğimiz komutlar için ilk örneği denetle;
    // sembollü emirler `submit_order` içinde kendi örneğiyle yeniden denetlenir.
    if let Some(inst) = ctx.cmd_tx.keys().next() {
        if let Err(why) = ctx.registry.trading_gate(inst, ctx.allow_live) {
            return Err(rejected(ctx, id, &why));
        }
    }
    ctx.orders.register(id).map_err(|_| duplicate(ctx, id))
}

/// Emir olayı iskeleti.
///
/// `sim` bayrağı **tek yerden** basılır: kip neyse o. Her çağrı yerinde elle
/// yazılsaydı, bir yerde unutulması simüle bir dolumun gerçek görünmesi
/// demek olurdu.
fn event(ctx: &Ctx, id: &str, kind: &'static str, src: &str) -> OrderEvent {
    OrderEvent {
        id: id.to_owned(),
        kind,
        src: src.to_owned(),
        sim: ctx.sim.is_some(),
        ..Default::default()
    }
}

/// Aynı kimlikle ikinci istek: sessizce kabul etmek çift pozisyon demek
/// olurdu. Kural replay'de de aynı — kod yolu değişmemeli.
fn duplicate(ctx: &Ctx, id: &str) -> ServerMsg {
    ServerMsg::Order(OrderEvent {
        comment: "bu id daha once kullanildi".into(),
        ..event(ctx, id, "duplicate", "")
    })
}

fn rejected(ctx: &Ctx, id: &str, why: &str) -> ServerMsg {
    ServerMsg::Order(OrderEvent { comment: why.to_owned(), ..event(ctx, id, "rejected", "") })
}

fn dispatch(ctx: &Ctx, instance: &str, cmd: Cmd, id: &str) -> ServerMsg {
    // GÜVENLİK SINIRI — simüle kipte paylaşılan belleğe HİÇBİR ŞEY yazılmaz.
    //
    // Bu noktaya gelinmesi bir programlama hatasıdır (`handle_client_msg`
    // simülasyon yoluna sapmalıydı). Panik atmıyoruz ama komutu da
    // göndermiyoruz.
    //
    // **PAPER kipinde bu, replay'dekinden daha kritik.** Replay'de komut
    // kanalı zaten kurulmaz — gönderilecek bir yer yoktur. Paper ise CANLI
    // bir terminalin yanında, aynı süreçte çalışır; "test ediyordum" diye
    // başlayan kaza hikâyesinin gerçekten mümkün olduğu tek yer burası.
    // Paper bağlamının `cmd_tx` haritası da bilerek BOŞ bırakılır, yani bu
    // kontrol ikinci savunma hattıdır.
    if let Some(s) = ctx.sim() {
        return rejected(ctx, id, &format!("{} kipinde gercek emir gonderilmez", s.mode));
    }
    match ctx.cmd_tx.get(instance) {
        Some(tx) => match tx.send(cmd) {
            Ok(()) => ServerMsg::Order(event(ctx, id, "queued", instance)),
            Err(_) => rejected(ctx, id, "okuyucu thread'i kapali"),
        },
        None => rejected(ctx, id, "bilinmeyen instance"),
    }
}

/// `action`/`side`/`type` üçlüsünü MT5 emir tipine çevir.
///
/// Canlı ve simüle yollar bunu PAYLAŞIR: istemci iki kipte de aynı doğrulama
/// hatasını görmeli.
fn resolve_order_type(req: &OrderReq) -> Result<(bool, u8), &'static str> {
    let is_pending = req.action == "pending";
    let otype = if is_pending {
        match req.order_type.as_str() {
            "buy_limit" => order_type::BUY_LIMIT,
            "sell_limit" => order_type::SELL_LIMIT,
            "buy_stop" => order_type::BUY_STOP,
            "sell_stop" => order_type::SELL_STOP,
            "buy_stop_limit" => order_type::BUY_STOP_LIMIT,
            "sell_stop_limit" => order_type::SELL_STOP_LIMIT,
            _ => return Err("gecersiz type (bekleyen emir)"),
        }
    } else {
        match req.side.as_str() {
            "buy" => order_type::BUY,
            "sell" => order_type::SELL,
            _ => return Err("gecersiz side (buy|sell)"),
        }
    };
    Ok((is_pending, otype))
}

fn submit_simple(
    ctx: &Ctx,
    id: &str,
    act: u8,
    ticket: u64,
    volume: f64,
    sl: f64,
    tp: f64,
) -> ServerMsg {
    let wire = match gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return m,
    };
    // Bilet hangi örneğe ait bilinmiyor; tek örnek varsa oraya, birden
    // fazlaysa istemci belirtmeli. Şimdilik ilk örnek kullanılıyor.
    let Some(instance) = ctx.cmd_tx.keys().next().cloned() else {
        return rejected(ctx, id, "bagli instance yok");
    };
    let mut cmd = Cmd {
        client_id: wire,
        magic: wire,
        ticket,
        volume,
        sl,
        tp,
        action: act,
        filling: filling::AUTO,
        type_time: type_time::GTC,
        ..Default::default()
    };
    write_fixed_str(&mut cmd.comment, &short(id));
    dispatch(ctx, &instance, cmd, id)
}

/// Son kullanma damgasını çöz: `expire_sn` (göreli) → mutlak BROKER damgası.
///
/// # Neden bu fonksiyon var
///
/// MT5 `MqlTradeRequest.expiration` alanını **sunucu saati** olarak yorumlar.
/// Gerçek UTC epoch göndermek ÖLÇÜLDÜ (2026-08-13, sunucu UTC+3):
///
/// ```text
/// UTC + 120 sn   -> retcode 10022 (INVALID_EXPIRATION), emir HİÇ kurulmaz
/// UTC + 1 gün    -> KABUL EDİLİR ama 3 saat ERKEN dolar, sessizce
/// ```
///
/// İkincisi tehlikelidir: hata vermez, telde de görünmez. Bu yüzden köprü
/// göreli saniyeyi kendisi çevirir ve broker saatini **son tick'ten** okur —
/// yerel saatten türetmek aynı 3 saatlik hatayı geri getirirdi.
///
/// # Sessiz yok saymanın sonu
///
/// Eskiden `expiration` verilip `time` verilmediğinde alan **sessizce
/// düşerdi**: emir GTC olarak sonsuza kadar bekler, istemci telden bunu
/// anlayamazdı. Artık açık hata döner.
fn resolve_expiry(
    ctx: &Ctx,
    req: &OrderReq,
    tt: u8,
    instance: &str,
    symbol_id: u32,
) -> Result<(u8, i64), String> {
    // Broker saati YALNIZCA tick'ten gelir. Tick yoksa uyduramayız: yerel
    // saatle hesaplamak ölçülmüş 3 saatlik hatayı geri getirirdi.
    let broker_simdi_ms = ctx.registry.last_of(instance, symbol_id).and_then(|t| {
        if t.time_msc <= 0 {
            return None;
        }
        // Tick BAYAT olabilir: sessiz bir sembolde son fiyat dakikalarca eski
        // kalır. Damgayı olduğu gibi "şu an" saymak, o bayatlık kadar kısa bir
        // ömür verirdi. ÖLÇÜLDÜ: 21 sn bayat tick, 120 sn istenen emri 99 sn'ye
        // düşürdü. Yerel saatte geçen süreyi ekleyerek telafi ediyoruz.
        let simdi = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let bayatlik = if t.recv_ms > 0 { (simdi - t.recv_ms).max(0) } else { 0 };
        Some(t.time_msc + bayatlik)
    });
    expiry_from(req, tt, broker_simdi_ms)
}

/// [`resolve_expiry`]'nin saatsiz çekirdeği — CANLI ve SİMÜLE yol ortak kullanır.
///
/// # Neden ayrıldı
///
/// Simüle yol bunu ÇAĞIRMIYORDU: `expire_sn` sessizce düşüyor, emir GTC olarak
/// sonsuza kadar bekliyordu. Tüketici sistem 2026-08-17'de bunu ölçtü
/// (`expiration: null`, 16 emrin 6'sı 120 sn'yi aştı). Sessiz yok saymayı
/// canlı yolda kapatıp simülede açık bırakmak, tam da bu köprünün önlemek
/// için var olduğu "test ile canlı ayrışır" hatasıydı.
///
/// # `broker_simdi_ms` nereden gelir
///
/// - **Canlı**: son tick + bayatlık telafisi (yukarıda).
/// - **Simüle**: akışın kendi damgası, telafi YOK. Replay'de yerel saate
///   bakmak Mayıs kaydını Ağustos saatiyle oynatmak olurdu; belirlenimcilik
///   sözü bunu yasaklıyor.
///
/// `None` = saat bilinmiyor; `expire_sn` istenmişse açık hata döner.
fn expiry_from(req: &OrderReq, tt: u8, broker_simdi_ms: Option<i64>) -> Result<(u8, i64), String> {
    let belirli = tt == type_time::SPECIFIED || tt == type_time::SPECIFIED_DAY;

    if req.expire_sn != 0 {
        if req.expiration != 0 {
            return Err(
                "expire_sn ile expiration BIRLIKTE gonderilemez: hangisinin \
                 kazandigi belirsiz kalirdi. Goreli sure icin yalniz expire_sn kullan."
                    .into(),
            );
        }
        if req.expire_sn < 0 {
            return Err("expire_sn negatif olamaz".into());
        }
        // `time` verilmemişse niyet açık: süreli emir. `gtc`/`day` ile
        // birlikte gelmesi ise çelişkidir — sessizce birini seçmek yerine
        // istemciye söylüyoruz.
        let tt = if req.time.is_empty() {
            type_time::SPECIFIED
        } else if belirli {
            tt
        } else {
            return Err(format!(
                "expire_sn ile time={:?} celisiyor: sureli emir icin time \
                 bos birakilmali ya da specified/specified_day olmali",
                req.time
            ));
        };

        let Some(ms) = broker_simdi_ms else {
            return Err(
                "expire_sn icin broker saati bilinmiyor: bu sembolde henuz \
                 tick gelmedi. Once tick akisini bekle ya da mutlak expiration ver."
                    .into(),
            );
        };
        let broker_simdi = ms / 1000;

        // MT5 son kullanmayı DAKİKAYA yuvarlar — ölçüldü: 120 sn istendi,
        // broker 1786655700 yazdı (tam dakika). Aşağı yuvarlamak istenenden
        // AZ süre vermek demek; sessizce eksik teslim etmektense yukarı
        // yuvarlayıp istenenden biraz FAZLA veriyoruz.
        let hedef = broker_simdi + req.expire_sn;
        let hedef = hedef.div_euclid(60) * 60 + if hedef.rem_euclid(60) == 0 { 0 } else { 60 };
        return Ok((tt, hedef));
    }

    if req.expiration != 0 && !belirli {
        return Err(format!(
            "expiration verildi ama time={:?}: MT5 bu alani yalnizca \
             time=specified/specified_day iken okur, aksi halde emir GTC olarak \
             SONSUZA KADAR bekler. time'i ayarla ya da expire_sn kullan.",
            req.time
        ));
    }

    Ok((tt, req.expiration))
}

fn submit_order(req: OrderReq, ctx: &Ctx) -> ServerMsg {
    let wire = match gate(ctx, &req.id) {
        Ok(w) => w,
        Err(m) => return m,
    };

    let Some((instance, symbol_id)) = ctx.registry.resolve_any(&req.symbol) else {
        return rejected(ctx, &req.id, "sembol bulunamadi");
    };
    let Some(entry) = ctx.registry.symbol(&instance, symbol_id) else {
        return rejected(ctx, &req.id, "sembol kaydi okunamadi");
    };

    let (is_pending, otype) = match resolve_order_type(&req) {
        Ok(v) => v,
        Err(why) => return rejected(ctx, &req.id, why),
    };

    // Hacim ve fiyatı sembolün ızgarasına oturt. Bu YAPILMAZSA broker
    // 10014/10015 ile reddeder ve sebebi loglardan anlaşılmaz.
    let volume = match sinyal_proto::normalize_volume(
        req.volume,
        entry.volume_min,
        entry.volume_max,
        entry.volume_step,
    ) {
        Ok(v) => v,
        Err(e) => return rejected(ctx, &req.id, &format!("hacim: {e}")),
    };

    let norm = |p: f64| {
        if p == 0.0 {
            0.0
        } else {
            sinyal_proto::normalize_price(p, entry.tick_size, entry.digits)
        }
    };

    let fill = match req.filling.as_str() {
        "" | "auto" => filling::AUTO,
        "fok" => filling::FOK,
        "ioc" => filling::IOC,
        "return" => filling::RETURN,
        "boc" => filling::BOC,
        _ => return rejected(ctx, &req.id, "gecersiz filling"),
    };
    let tt = match req.time.as_str() {
        "" | "gtc" => type_time::GTC,
        "day" => type_time::DAY,
        "specified" => type_time::SPECIFIED,
        "specified_day" => type_time::SPECIFIED_DAY,
        _ => return rejected(ctx, &req.id, "gecersiz time"),
    };

    let (tt, expiration) = match resolve_expiry(ctx, &req, tt, &instance, symbol_id) {
        Ok(v) => v,
        Err(e) => return rejected(ctx, &req.id, &e),
    };

    // AUTO'yu burada da çözebiliyor muyuz? Çözemiyorsak EA de çözemez —
    // emri göndermeden reddetmek daha dürüst.
    if fill == filling::AUTO {
        let act = if is_pending { action::PENDING } else { action::DEAL };
        if let Err(e) = sinyal_proto::resolve_filling(act, filling::AUTO, &entry) {
            return rejected(ctx, &req.id, &format!("doldurma modu: {e}"));
        }
    }

    let mut cmd = Cmd {
        client_id: wire,
        magic: wire,
        volume,
        price: norm(req.price),
        stoplimit: norm(req.stoplimit),
        sl: norm(req.sl),
        tp: norm(req.tp),
        expiration,
        symbol_id,
        deviation: if req.deviation > 0 { req.deviation } else { ctx.deviation },
        action: if is_pending { action::PENDING } else { action::DEAL },
        order_type: otype,
        filling: fill,
        type_time: tt,
        ..Default::default()
    };
    let c = if req.comment.is_empty() { short(&req.id) } else { req.comment.clone() };
    write_fixed_str(&mut cmd.comment, &c);

    dispatch(ctx, &instance, cmd, &req.id)
}

// ---------------------------------------------------------------------------
// Simüle emir yolu — PAPER ve REPLAY kiplerinde
// ---------------------------------------------------------------------------
//
// Neden simüle ediyoruz da reddetmiyoruz: emirleri reddeden bir kip, sinyal
// sisteminin emir yolunu HİÇ test edemez — en pahalı hataların çıktığı yer
// tam da orasıdır.
//
// Motorun kendisi `crate::sim`dedir ve saftır (I/O yok, saat yok, rastgelelik
// yok). Burası yalnızca TERCÜME katmanı: tel üzerindeki istek → motor isteği,
// motor olayı → tel üzerindeki olay. Bu ayrımın sebebi, "aynı kayıt aynı
// sonucu verir" sözünün sunucunun eş zamanlılığından etkilenmemesi.
//
// Hangi doğrulamalar canlıyla aynı, hangileri değil:
//
// - AYNI olanlar — İSTEMCİNİN İSTEĞİNE dair her şey: bilinmeyen sembol,
//   geçersiz `side`/`type`, hacim ızgarası, fiyat ızgarası, çift kimlik.
// - EKLENENLER — BROKER'IN reddedeceği şeyler: `stops_level` ihlali (10016),
//   yetersiz marjin (10019). Bunlar canlıda MT5'ten gelirdi; simülasyonda
//   üretilmezlerse strateji canlıda çalışmayan bir stop yönetimine alışır.
// - ATLANAN — BROKER BAĞLANTISINA dair olanlar: doldurma modu (`filling`),
//   hesap izinleri, canlı para kilidi. Simüle dolumun doldurma moduyla işi
//   yok; üstelik `filling_mask`/`exec_mode` kayıttan gelir ve eksik bir kayıt
//   yüzünden canlıda kabul edilen emri simülasyonda reddetmek, kod yolunu
//   test etmek yerine sahte bir hata üretirdi.

/// Sembolün simülasyon için gereken tüm görüntüsü.
struct SimLook {
    instance: String,
    sym: SimSymbol,
    entry: sinyal_proto::SymbolEntry,
    /// Son bilinen fiyat; yoksa 0/0 — motor bunu `PRICE_OFF` ile reddeder.
    /// **Uydurulmuş bir fiyattan dolum yapmak**, simülasyonu sessizce yalancı
    /// yapardı.
    bid: f64,
    ask: f64,
    time_msc: i64,
}

/// O ANKİ fiyat — **isteği çözen ÖRNEĞİN kendi fiyatı**.
///
/// Kaynak canlıdakiyle aynı: sembol kaydına işlenmiş son tick. Replay'de bu
/// kaydı oynatım motoru, paper'da canlı okuyucu besler — ikisi de aynı
/// `Registry` üzerinden.
///
/// `instance` parametresi ŞART. Önceki sürüm fiyatı adla, örnekten bağımsız
/// arıyordu; `snapshot` örnek adına göre SIRALI döndüğü için daima
/// alfabetik olarak ilk örneğin fiyatını alıyordu. Oysa sembol kaydı
/// (`contract_size`, `stops_level`, `digits`) `resolve_any`den geliyor ve o
/// bir `HashMap` üzerinde geziniyor, yani RASTGELE bir örneği seçiyor. İki
/// terminal aynı sembolü kote ettiğinde (iki broker, iki hesap) dolum bir
/// terminalin fiyatından, doğrulaması BAŞKA terminalin sözleşmesinden
/// yapılırdı — üstelik hangisinin seçileceği koşudan koşuya değişebilirdi,
/// yani replay tekrarlanabilirliği de kırılırdı.
fn sim_last(ctx: &Ctx, instance: &str, symbol: &str) -> Option<crate::state::LastTick> {
    let filter = [symbol.to_owned()];
    ctx.registry
        .snapshot(&filter)
        .into_iter()
        .find(|(i, _, _)| i == instance)
        .map(|(_, _, t)| t)
}

fn sim_lookup(ctx: &Ctx, symbol: &str) -> Result<SimLook, (u32, String)> {
    let Some((instance, symbol_id)) = ctx.registry.resolve_any(symbol) else {
        return Err((simret::INVALID, "sembol bulunamadi".into()));
    };
    let Some(entry) = ctx.registry.symbol(&instance, symbol_id) else {
        return Err((simret::INVALID, "sembol kaydi okunamadi".into()));
    };
    let last = sim_last(ctx, &instance, symbol);
    Ok(SimLook {
        instance,
        sym: SimSymbol::from_entry(&entry),
        entry,
        bid: last.map(|l| l.bid).unwrap_or(0.0),
        ask: last.map(|l| l.ask).unwrap_or(0.0),
        time_msc: last.map(|l| l.time_msc).unwrap_or(0),
    })
}

/// Tüm sembollerin son fiyatı — pozisyon değerlemesi için.
///
/// Motorun kendi defteri yerine `Registry` kullanılıyor: pompa bir tick'i
/// kaçırsa bile değerleme akışın SON halini göstermeli. Aynı ad birden çok
/// örnekte varsa sonuncusu kazanır; simüle cüzdan tektir ve sembolü örneğe
/// göre ayırmıyoruz.
fn sim_book(ctx: &Ctx) -> PriceBook {
    let mut b = PriceBook::new();
    for (_, name, t) in ctx.registry.snapshot(&[]) {
        b.set(&name, t.bid, t.ask, t.time_msc);
    }
    b
}

/// Simüle kipte emir kapısı.
///
/// Canlı kapılardan geçmez (`--enable-trading`, canlı para kilidi, hesap
/// izinleri): hiçbiri burada bir riski kapatmıyor, çünkü gerçek emir zaten
/// gönderilmiyor. **Idempotency denetimi aynen korunur** — çift kimlik
/// davranışı kod yolunun parçasıdır ve simülasyonda da aynı görünmelidir.
fn sim_gate(ctx: &Ctx, id: &str) -> Result<u64, ServerMsg> {
    ctx.orders.register(id).map_err(|_| duplicate(ctx, id))
}

/// Reddedilen simüle istek — **gerçek MT5 retcode'uyla**.
///
/// Canlı `rejected` retcode taşımaz (istek MT5'e hiç gitmemiştir); simülasyon
/// taşır, çünkü burada reddeden şey broker'ın ta kendisidir. İstemci aynı
/// hatayı iki kipte de aynı sayıyla görmeli, yoksa simülasyonda yazılan hata
/// işleme kodu canlıda çalışmaz.
fn sim_rejected(ctx: &Ctx, id: &str, retcode: u32, why: &str) -> ServerMsg {
    ServerMsg::Order(OrderEvent {
        comment: why.to_owned(),
        retcode: Some(retcode),
        ..event(ctx, id, "rejected", "")
    })
}

/// `queued` — isteğin kabul edildiği, canlıdaki ile aynı ilk cevap.
fn sim_queued(ctx: &Ctx, src: &Arc<str>, id: &str) -> ServerMsg {
    ServerMsg::Order(event(ctx, id, "queued", src))
}

/// Bir sembolün ait olduğu örnek — `src` alanı canlıdaki gibi buradan gelir.
fn sim_src(ctx: &Ctx, s: &SimExec, symbol: &str) -> Arc<str> {
    s.src_of(ctx.registry.resolve_any(symbol).map(|(i, _)| i).as_deref())
}

/// Tetiklenmenin sebebini tel üzerinde taşı.
///
/// İstemcinin isteğiyle olan olaylarda boş (canlıdaki gibi); kendiliğinden
/// olanlarda sebep yazılır. Bir dolumun SL mi TP mi bekleyen emir mi olduğunu
/// ayırt edememek, kayıttan strateji analizini imkânsız kılardı.
fn cause_comment(c: SimCause) -> String {
    if c == SimCause::Request {
        String::new()
    } else {
        c.as_str().to_owned()
    }
}

/// Motor olayını yayın olayına çevir.
fn feed_order(src: &Arc<str>, ev: &SimEvent) -> FeedEvent {
    FeedEvent::Order {
        instance: src.clone(),
        client_id: ev.client_id,
        kind: ev.kind.as_str(),
        retcode: ev.retcode,
        order: ev.order,
        deal: ev.deal,
        position: ev.position,
        volume: ev.volume,
        price: ev.price,
        // SİMÜLASYON BU ALANLARI ÖLÇEMEZ, dolayısıyla UYDURMAZ.
        //
        // Motor dolumu tek bir fiyattan modelliyor; bir "dolum anı bid/ask"
        // üretmek, gerçek bir ölçümmüş gibi görünen türetilmiş bir sayı
        // olurdu. Kayma analizi tam da bu alanlar üzerinde yapılacağı için,
        // simüle bir spread'in canlı ölçümle aynı yerde durması analizi
        // sessizce bozardı. Sıfır bırakılıyor → tel üzerinde alan HİÇ yok.
        bid: 0.0,
        ask: 0.0,
        order_state: 0,
        txn_type: 0,
        // Simülasyonda MUTABAKAT YOKTUR: kaçırılacak bir olay yok, çünkü
        // olayları motorun kendisi üretiyor. `LIVE` bırakmak "bu olay canlı
        // akıştan geldi" demek değil; "mutabakat telafisi DEĞİL" demek.
        source: sinyal_proto::res_source::LIVE,
        // Motor komisyon ve swap MODELLEMİYOR (bkz. `SIM_NOT_MODELED`).
        // Sıfır yazmak "komisyon sıfırdı" gibi okunurdu; `source` LIVE
        // olduğu için bu alanlar tel üzerinde HİÇ görünmez ve istemci
        // ölçüm olmadığını anlar.
        profit: 0.0,
        commission: 0.0,
        swap: 0.0,
        comment: cause_comment(ev.cause),
    }
}

/// Motor sonucunu tel cevabına çevir ve `ack`/`txn`'i yayınla.
///
/// Olay sırası canlıyla AYNI: `queued` → `ack`(10008) → `txn`(10009). Sıra
/// tesadüf değil — istemci `ack`i dolum sanmamalı (bkz. [`OrderEvent`]).
/// Simülasyon bu ayrımı yeniden üretmezse, ayrımı yanlış kuran bir istemci
/// hatası ancak canlıda ortaya çıkardı.
///
/// `client_id` BU isteğinkiyle ezilir: motor olayları pozisyonu AÇAN isteğin
/// kimliğiyle damgalar (kapanış kârını doğru emre bağlayabilmek için), ama
/// tel üzerinde bir `close` cevabının `close` isteğinin kimliğini taşıması
/// gerekir — canlıda da öyle.
fn sim_reply(
    ctx: &Ctx,
    s: &SimExec,
    instance: &str,
    id: &str,
    wire: u64,
    out: SimOutcome,
) -> Vec<ServerMsg> {
    let src = s.src_of(Some(instance));
    match out {
        SimOutcome::Rejected(r) => vec![sim_rejected(ctx, id, r.retcode, &r.reason)],
        SimOutcome::Accepted { events, .. } => {
            for ev in events.iter().filter(|e| e.kind != SimEventKind::Queued) {
                let mut ev = ev.clone();
                ev.client_id = wire;
                let _ = ctx.events.send(feed_order(&src, &ev));
            }
            vec![sim_queued(ctx, &src, id)]
        }
    }
}

fn sim_submit_order(req: OrderReq, ctx: &Ctx, s: &SimExec) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, &req.id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    // Doğrulama sırası canlıyla AYNI: istemci iki kipte de aynı hatayı görsün.
    let (_, otype) = match resolve_order_type(&req) {
        Ok(v) => v,
        Err(why) => return vec![sim_rejected(ctx, &req.id, simret::INVALID, why)],
    };
    // Tel adı üzerinden çeviriyoruz: iki tarafın adları ayrışırsa derleme
    // değil, sessiz bir yanlış emir tipi doğardı.
    let Some(kind) = SimOrderKind::parse(order_kind_name(otype)) else {
        return vec![sim_rejected(ctx, &req.id, simret::INVALID, "emir tipi cevrilemedi")];
    };
    let look = match sim_lookup(ctx, &req.symbol) {
        Ok(l) => l,
        Err((rc, why)) => return vec![sim_rejected(ctx, &req.id, rc, &why)],
    };

    // HACİM: canlı yolun TAM AYNISI.
    //
    // `normalize_volume` ızgaraya OTURTUR, reddetmez — canlıda 0.015 lot
    // 0.01'e yuvarlanıp gönderilir. Motorun kendi ızgara denetimi (10014)
    // burada bir kez daha çalışır ama oturtulmuş hacmi zaten kabul eder.
    // Simülasyonun burada reddetmesi, canlıda dolan bir emrin testte
    // reddedilmesi demek olurdu; bu işin amacı tam tersi.
    let volume = match sinyal_proto::normalize_volume(
        req.volume,
        look.entry.volume_min,
        look.entry.volume_max,
        look.entry.volume_step,
    ) {
        Ok(v) => v,
        Err(e) => {
            return vec![sim_rejected(
                ctx,
                &req.id,
                simret::INVALID_VOLUME,
                &format!("hacim: {e}"),
            )]
        }
    };
    let norm = |p: f64| {
        if p == 0.0 {
            0.0
        } else {
            sinyal_proto::normalize_price(p, look.entry.tick_size, look.entry.digits)
        }
    };

    // SON KULLANMA: canlı yolun TAM AYNI kuralları, canlı yolun TAM AYNI
    // hataları. Burası eskiden `req.expiration`ı olduğu gibi alıyordu; sonuç
    // `expire_sn`in sessizce düşmesi ve emrin sonsuza kadar beklemesiydi.
    // Saat akışın kendi damgasından gelir — yerel saat replay'i bozardı.
    let tt = match req.time.as_str() {
        "" | "gtc" => type_time::GTC,
        "day" => type_time::DAY,
        "specified" => type_time::SPECIFIED,
        "specified_day" => type_time::SPECIFIED_DAY,
        _ => return vec![sim_rejected(ctx, &req.id, simret::INVALID, "gecersiz time")],
    };
    let expiration = match expiry_from(&req, tt, Some(look.time_msc).filter(|&m| m > 0)) {
        Ok((_, e)) => e,
        Err(e) => return vec![sim_rejected(ctx, &req.id, simret::INVALID, &e)],
    };

    let sreq = SimOrderReq {
        id: req.id.clone(),
        client_id: wire,
        symbol: req.symbol.clone(),
        kind,
        volume,
        price: norm(req.price),
        stoplimit: norm(req.stoplimit),
        sl: norm(req.sl),
        tp: norm(req.tp),
        expiration,
        comment: if req.comment.is_empty() { short(&req.id) } else { req.comment.clone() },
        // Akışın saati; yoksa 0 — yerel saat yazmak veri akışının zamanıyla
        // çelişen bir damga üretirdi.
        time_msc: look.time_msc,
    };
    let out = s.lock().place(&sreq, &look.sym, look.bid, look.ask);
    sim_reply(ctx, s, &look.instance, &req.id, wire, out)
}

/// Biletin sembolünü çöz; bulunamazsa canlıdaki gibi açık ret.
fn sim_symbol_of(s: &SimExec, ticket: u64) -> Option<String> {
    s.lock().symbol_of(ticket).map(str::to_owned)
}

fn sim_close(ctx: &Ctx, s: &SimExec, id: &str, ticket: u64, volume: f64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    let Some(symbol) = sim_symbol_of(s, ticket) else {
        return vec![sim_rejected(ctx, id, simret::INVALID, "simule pozisyon bulunamadi")];
    };
    let look = match sim_lookup(ctx, &symbol) {
        Ok(l) => l,
        Err((rc, why)) => return vec![sim_rejected(ctx, id, rc, &why)],
    };
    // Kilit ARADA bırakıldı: bu sırada bir tick pozisyonu SL'den kapatmış
    // olabilir ve o zaman motor `INVALID` döner. Doğru davranış bu — kilidi
    // fiyat okuması boyunca tutmak, tick pompasını emir yolunun arkasında
    // bekletirdi.
    let out = s.lock().close(ticket, volume, look.bid, look.ask);
    sim_reply(ctx, s, &look.instance, id, wire, out)
}

fn sim_cancel(ctx: &Ctx, s: &SimExec, id: &str, ticket: u64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    // İptal fiyat istemez; örnek adını yine de sembolden çözüyoruz ki olayın
    // `src`si canlıdakiyle aynı anlamı taşısın.
    let instance = sim_symbol_of(s, ticket)
        .and_then(|sym| ctx.registry.resolve_any(&sym).map(|(i, _)| i))
        .unwrap_or_else(|| s.primary.to_string());
    let out = s.lock().cancel(ticket);
    sim_reply(ctx, s, &instance, id, wire, out)
}

fn sim_modify(ctx: &Ctx, s: &SimExec, id: &str, ticket: u64, sl: f64, tp: f64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    let Some(symbol) = sim_symbol_of(s, ticket) else {
        return vec![sim_rejected(ctx, id, simret::INVALID, "simule pozisyon/emir bulunamadi")];
    };
    let look = match sim_lookup(ctx, &symbol) {
        Ok(l) => l,
        Err((rc, why)) => return vec![sim_rejected(ctx, id, rc, &why)],
    };
    let norm = |p: f64| {
        if p == 0.0 {
            0.0
        } else {
            sinyal_proto::normalize_price(p, look.entry.tick_size, look.entry.digits)
        }
    };
    let out = s.lock().modify_sltp(ticket, norm(sl), norm(tp), &look.sym, look.bid, look.ask);
    sim_reply(ctx, s, &look.instance, id, wire, out)
}

/// Sentetik hesap durumu.
///
/// Alan adları canlıyla birebir aynı; değerlerin **anlamı** farklı ve bu
/// `hello.mode` ile ilan edilmiştir. `mode` daima `demo`: simüle bir hesabın
/// canlı para kilidinin yanlış tarafına düşmesi kabul edilemez.
///
/// Birden çok örnek olsa bile **tek** hesap dönülür: simüle bakiye tek bir
/// cüzdandır, onu örnek başına tekrarlamak aynı parayı iki kez saydırırdı.
fn sim_accounts(ctx: &Ctx, s: &SimExec) -> Vec<AccountInfo> {
    let a = s.lock().account(&sim_book(ctx));
    vec![AccountInfo {
        src: s.primary.to_string(),
        login: 0,
        server: s.mode.to_owned(),
        company: s.mode.to_owned(),
        // Para birimi bilinmiyor; uydurmak yerine boş bırakılıyor. Kâr
        // sembolün kotasyon biriminde kalır, hesap birimine ÇEVRİLMEZ.
        currency: String::new(),
        leverage: a.leverage,
        balance: a.balance,
        credit: a.credit,
        profit: a.profit,
        equity: a.equity,
        margin: a.margin,
        margin_free: a.margin_free,
        margin_level: a.margin_level,
        mode: "demo",
        // Aynı sembolde birden çok pozisyon tutulabiliyor — davranış hedging.
        margin_mode: "hedging",
        // Teminat tamamlama MODELLENMİYOR; bir eşik uydurmak yerine
        // "bilinmiyor" diyoruz.
        so_mode: "unknown",
        margin_so_call: 0.0,
        margin_so_so: 0.0,
        can_trade: true,
        blocked_by: None,
        age_ms: 0,
    }]
}

fn sim_positions(ctx: &Ctx, s: &SimExec) -> (Vec<PositionInfo>, u32, bool) {
    let items: Vec<PositionInfo> = s
        .lock()
        .positions(&sim_book(ctx))
        .into_iter()
        .map(|p| PositionInfo {
            // Canlıdaki gibi: pozisyonun `src`'si sembolün örneği.
            src: sim_src(ctx, s, &p.symbol).to_string(),
            ticket: p.ticket,
            identifier: p.ticket,
            client_id: p.client_id,
            symbol: p.symbol,
            side: p.side,
            volume: p.volume,
            price_open: p.price_open,
            price_current: p.price_current,
            sl: p.sl,
            tp: p.tp,
            profit: p.profit,
            // Swap modellenmiyor; 0 burada "hesaplanmıyor" demek.
            swap: p.swap,
            time_msc: p.time_msc,
            comment: p.comment,
        })
        .collect();
    let total = items.len() as u32;
    // Simüle liste hiç kesilmez: tavan yok, hepsi bellekte.
    (items, total, false)
}

fn sim_orders(ctx: &Ctx, s: &SimExec) -> (Vec<OrderInfo>, u32, bool) {
    let items: Vec<OrderInfo> = s
        .lock()
        .orders()
        .into_iter()
        .map(|o| OrderInfo {
            src: sim_src(ctx, s, &o.symbol).to_string(),
            ticket: o.ticket,
            client_id: o.client_id,
            symbol: o.symbol,
            kind: o.kind,
            volume_initial: o.volume_initial,
            volume_current: o.volume_current,
            price: o.price,
            stoplimit: o.stoplimit,
            sl: o.sl,
            tp: o.tp,
            time_setup_msc: o.time_setup_msc,
            expiration: o.expiration,
            comment: o.comment,
        })
        .collect();
    let total = items.len() as u32;
    (items, total, false)
}

fn mode_name(v: u8) -> &'static str {
    match v {
        sinyal_proto::trade_mode::DEMO => "demo",
        sinyal_proto::trade_mode::CONTEST => "contest",
        sinyal_proto::trade_mode::REAL => "real",
        _ => "unknown",
    }
}

fn margin_mode_name(v: u8) -> &'static str {
    match v {
        sinyal_proto::margin_mode::NETTING => "netting",
        sinyal_proto::margin_mode::EXCHANGE => "exchange",
        sinyal_proto::margin_mode::HEDGING => "hedging",
        _ => "unknown",
    }
}

fn so_mode_name(v: u8) -> &'static str {
    match v {
        sinyal_proto::so_mode::PERCENT => "percent",
        sinyal_proto::so_mode::MONEY => "money",
        _ => "unknown",
    }
}

fn order_kind_name(v: u8) -> &'static str {
    use sinyal_proto::order_type as t;
    match v {
        t::BUY => "buy",
        t::SELL => "sell",
        t::BUY_LIMIT => "buy_limit",
        t::SELL_LIMIT => "sell_limit",
        t::BUY_STOP => "buy_stop",
        t::SELL_STOP => "sell_stop",
        t::BUY_STOP_LIMIT => "buy_stop_limit",
        t::SELL_STOP_LIMIT => "sell_stop_limit",
        _ => "unknown",
    }
}

fn collect_accounts(ctx: &Ctx) -> Vec<AccountInfo> {
    ctx.registry
        .all_states()
        .into_iter()
        .map(|(src, s)| {
            let a = &s.snap.account;
            AccountInfo {
                src,
                login: a.login,
                server: a.server_str().to_owned(),
                company: a.company_str().to_owned(),
                currency: a.currency_str().to_owned(),
                leverage: a.leverage,
                balance: a.balance,
                credit: a.credit,
                profit: a.profit,
                equity: a.equity,
                margin: a.margin,
                margin_free: a.margin_free,
                margin_level: a.margin_level,
                mode: mode_name(a.trade_mode),
                margin_mode: margin_mode_name(a.margin_mode),
                so_mode: so_mode_name(a.so_mode),
                margin_so_call: a.margin_so_call,
                margin_so_so: a.margin_so_so,
                can_trade: a.can_trade(),
                blocked_by: a.permission_problem().map(|s| s.to_owned()),
                age_ms: s.at.elapsed().as_millis() as u64,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Broker stop'unun DOĞRULANMASI
// ---------------------------------------------------------------------------

/// Komut kabul edildiyse doğrulamayı kur.
///
/// Kabul ölçütü `kind == "queued"`: canlı ve simüle yollar kabul edilen
/// komuta AYNI etiketi basıyor (bkz. [`dispatch`] ve [`sim_reply`]). `rejected`
/// ya da `duplicate` için doğrulama kurulmaz — gönderilmemiş bir komutun
/// doğrulanmaması bir uyarı değil, beklenen durumdur.
fn arm_sltp_verify(ctx: &Ctx, id: &str, ticket: u64, sl: f64, out: &[ServerMsg]) {
    let accepted = out
        .iter()
        .any(|m| matches!(m, ServerMsg::Order(e) if e.kind == "queued"));
    if accepted {
        ctx.sltp.arm(id, ticket, sl, std::time::Instant::now());
    }
}

/// Şu anki pozisyon listesi ve **listeye güvenilip güvenilmeyeceği**.
///
/// İkinci alan bu doğrulamanın bel kemiği: "pozisyon listede YOK" cevabı
/// ancak liste GÜVENİLİRSE "pozisyon kapandı" demektir. Bayat ya da
/// kırpılmış bir görüntüde aynı cevap "hiçbir şey bilmiyoruz" demektir ve
/// ikisini karıştırmak ya her kapanışta yanlış alarm üretir ya da zombi bir
/// köprüyü sessizce gizler.
///
/// Simüle kipte motorun listesi ANLIKTIR ve daima güvenilir.
///
/// `age` hazır geçirilir: `all_states()` her çağrıda TÜM durum görüntüsünü
/// (pozisyon vektörleriyle birlikte) klonluyor ve bu tarama saniyede dört kez
/// çalışıyor — aynı bilgiyi iki kez toplamak boşuna kopya demek.
fn positions_now(ctx: &Ctx, age: Option<u64>) -> (Vec<PositionInfo>, bool) {
    match ctx.sim() {
        Some(s) => (sim_positions(ctx, s).0, true),
        None => {
            let (items, _total, truncated) = collect_positions(ctx);
            // Görüntü hiç yoksa (`None`) da güvenilmez: EA yayın yapmamış
            // demektir ve o hâlde "pozisyon yok" bilgi değil, körlüktür.
            let fresh = age.is_some_and(|ms| ms <= SLTP_STATE_STALE.as_millis() as u64);
            (items, fresh && !truncated)
        }
    }
}

/// Durum görüntülerinin EN ESKİSİNİN yaşı (ms).
///
/// En eskisi alınıyor: bir örnek taze diye zombi olan diğerini gizlemek,
/// bu uyarının var olma sebebini ortadan kaldırırdı. Simüle kiplerde
/// paylaşılan bellekten gelen bir görüntü YOKTUR — orada `None`.
fn state_age_ms(ctx: &Ctx, now: std::time::Instant) -> Option<u64> {
    ctx.registry
        .all_states()
        .iter()
        .map(|(_, s)| now.checked_duration_since(s.at).unwrap_or_default().as_millis() as u64)
        .max()
}

/// Sembolün fiyat ızgarası — bulunamazsa 0.
fn tick_size_of(ctx: &Ctx, symbol: &str) -> f64 {
    ctx.registry
        .resolve_any(symbol)
        .and_then(|(inst, id)| ctx.registry.symbol(&inst, id))
        .map(|e| e.tick_size)
        .unwrap_or(0.0)
}

/// İstenen stop kuruldu mu.
///
/// Birebir eşitlik ARANMAZ: broker fiyatı kendi ızgarasına (`tick_size`)
/// yuvarlar, bizim gönderdiğimiz ham `sl` ile geri okunan değer son basamakta
/// ayrışabilir. Katı karşılaştırma neredeyse her doğrulamayı "kurulmadı"
/// yapar; yanlış uyarı birkaç kez tekrarlanınca gerçek olanı da yok
/// ettirirdi. Tolerans bir ızgara adımı — broker'ın gerçekten reddettiği
/// durumda fark bundan kat kat büyük olur.
fn stop_reached(want: f64, actual: f64, tick: f64) -> bool {
    let tol = if tick > 0.0 { tick } else { want.abs().max(1.0) * 1e-9 };
    (want - actual).abs() <= tol
}

/// Bekleyen doğrulamaları tara; süresi dolmuş ve kurulmamış olanlar için
/// uyarı üret.
///
/// `now` DIŞARIDAN veriliyor: "pencere dolmadan uyarı üretilmez" kuralı
/// gerçek zaman beklemeden sınanabilmeli, yoksa test ya kırılgan olur ya da
/// saniyelerce uyur.
///
/// Dört sonuç var ve dördü de ayrı:
/// - stop istenen değerde → **doğrulandı**, sessizce düşer;
/// - pencere dolmadı → hiçbir şey (ERKEN ALARM YOK);
/// - pozisyon GÜVENİLİR bir listede yok → **kapanmış**, sessizce düşer;
/// - pencere doldu ve stop yerinde değil (ya da hiçbir şey göremiyoruz) →
///   uyarı.
///
/// # Kapanmış pozisyona alarm verilmez
///
/// Pozisyon yoksa stop'u da yoktur — bu tamamen normaldir ve stop'a değerek
/// kapanan HER pozisyonda tekrarlanır. Buna uyarı üretmek, uyarının kendisini
/// öldürürdü: birkaç gün sonra `sltp_unverified` satırlarının hepsi gürültü
/// sanılır ve gerçek olan da okunmaz. **Tek istisna**, listenin güvenilmez
/// olduğu durum (bayat/kırpılmış görüntü, ya da hiç görüntü yok): orada
/// "yok" bilgi değil körlüktür ve susmak zombi köprüyü gizlerdi.
fn sweep_sltp(ctx: &Ctx, now: std::time::Instant) -> Vec<FeedEvent> {
    let mut out = Vec::new();
    let mut pend = ctx.sltp.pending.lock().unwrap_or_else(|e| e.into_inner());
    if pend.is_empty() {
        // Bekleyen yoksa pozisyon listesi toplamıyoruz: bu tarama saniyede
        // birkaç kez çalışıyor ve normal hâl "bekleyen yok".
        return out;
    }
    let age = state_age_ms(ctx, now);
    let (items, trustworthy) = positions_now(ctx, age);
    let src: Arc<str> = Arc::from(
        ctx.sim()
            .map(|s| s.primary.to_string())
            .or_else(|| ctx.registry.all_states().first().map(|(i, _)| i.clone()))
            .unwrap_or_default(),
    );

    pend.retain(|p| {
        let found = items.iter().find(|q| q.ticket == p.ticket);
        match found {
            Some(q) if stop_reached(p.want_sl, q.sl, tick_size_of(ctx, &q.symbol)) => {
                ctx.sltp.verified.fetch_add(1, Ordering::Relaxed);
                false
            }
            // Pencere daha dolmadı: broker'a da, durum yayınına da zaman
            // tanıyoruz.
            _ if now < p.deadline => true,
            Some(q) => {
                ctx.sltp.unverified.fetch_add(1, Ordering::Relaxed);
                out.push(FeedEvent::SltpUnverified {
                    instance: src.clone(),
                    id: p.id.clone(),
                    ticket: p.ticket,
                    want_sl: p.want_sl,
                    actual_sl: Some(q.sl),
                    state_age_ms: age,
                    why: if trustworthy {
                        "durum yayinindaki sl istenen degere ulasmadi \
                         - broker stop'u uygulamamis olabilir"
                    } else {
                        // Bayat görüntüde eski `sl`i görüyor olabiliriz;
                        // broker'ı suçlamak yanlış teşhis olurdu.
                        "sl istenen degerde degil AMA durum goruntusu BAYAT \
                         - once kopruyu kontrol edin (bkz. state_age_ms)"
                    },
                });
                false
            }
            // Pozisyon GÜVENİLİR bir listede yok: kapanmış. Kapanmış
            // pozisyonun stop'u da yoktur — alarm YOK.
            None if trustworthy => {
                ctx.sltp.closed.fetch_add(1, Ordering::Relaxed);
                false
            }
            None => {
                ctx.sltp.unverified.fetch_add(1, Ordering::Relaxed);
                out.push(FeedEvent::SltpUnverified {
                    instance: src.clone(),
                    id: p.id.clone(),
                    ticket: p.ticket,
                    want_sl: p.want_sl,
                    // `0` DEĞİL: "stop yok" ile "bakamadık" aynı şey değil.
                    actual_sl: None,
                    state_age_ms: age,
                    why: "pozisyonu goremiyoruz: durum goruntusu BAYAT, kirpilmis \
                          ya da hic gelmemis - kopru zombi olabilir",
                });
                false
            }
        }
    });
    out
}

fn collect_positions(ctx: &Ctx) -> (Vec<PositionInfo>, u32, bool) {
    let mut items = Vec::new();
    let mut total = 0u32;
    let mut truncated = false;
    for (src, s) in ctx.registry.all_states() {
        total += s.snap.pos_total;
        truncated |= s.snap.truncated;
        for p in &s.snap.positions {
            items.push(PositionInfo {
                src: src.clone(),
                ticket: p.ticket,
                identifier: p.identifier,
                client_id: p.magic,
                symbol: p.symbol_str().to_owned(),
                side: if p.is_buy() { "buy" } else { "sell" },
                volume: p.volume,
                price_open: p.price_open,
                price_current: p.price_current,
                sl: p.sl,
                tp: p.tp,
                profit: p.profit,
                swap: p.swap,
                time_msc: p.time_msc,
                comment: p.comment_str().to_owned(),
            });
        }
    }
    (items, total, truncated)
}

fn collect_orders(ctx: &Ctx) -> (Vec<OrderInfo>, u32, bool) {
    let mut items = Vec::new();
    let mut total = 0u32;
    let mut truncated = false;
    for (src, s) in ctx.registry.all_states() {
        total += s.snap.ord_total;
        truncated |= s.snap.truncated;
        for o in &s.snap.orders {
            items.push(OrderInfo {
                src: src.clone(),
                ticket: o.ticket,
                client_id: o.magic,
                symbol: o.symbol_str().to_owned(),
                kind: order_kind_name(o.kind),
                volume_initial: o.volume_initial,
                volume_current: o.volume_current,
                price: o.price_open,
                stoplimit: o.price_stoplimit,
                sl: o.sl,
                tp: o.tp,
                time_setup_msc: o.time_setup_msc,
                expiration: o.time_expiration,
                comment: o.comment_str().to_owned(),
            });
        }
    }
    (items, total, truncated)
}

/// Yorum alanına sığacak şekilde kısalt (MT5 yorumu zaten kısaltır).
fn short(s: &str) -> String {
    s.chars().take(COMMENT_LEN - 1).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_comparison_is_length_and_content_correct() {
        assert!(secret_eq("abc", "abc"));
        assert!(!secret_eq("abc", "abd"));
        assert!(!secret_eq("abc", "ab"));
        assert!(!secret_eq("", "a"));
        assert!(secret_eq("", ""));
        // Uzun token'da tek bayt farkı yakalanmalı.
        let a = "A45zSMQPMrR7yL8qmu9sUHe1DBUcuvGMyc7fVF1sZC0";
        let mut b = a.to_string();
        b.replace_range(42..43, "1");
        assert!(!secret_eq(a, &b), "son bayttaki fark yakalanmalı");
    }

    #[test]
    fn candle_subscriptions_match_symbol_and_wildcard() {
        let mut s = Subs::default();
        let ev = |sym: &str, tf: &'static str| FeedEvent::Candle {
            symbol: Arc::from(sym),
            tf,
            bar: crate::candles::Bar { t: 0, o: 1.0, h: 1.0, l: 1.0, c: 1.0, ticks: 1, ..Default::default() },
        };

        assert!(!s.wants(&ev("EURUSD", "M1")), "abone olmadan gitmemeli");

        s.add("candle.EURUSD.M1");
        assert!(s.wants(&ev("EURUSD", "M1")));
        assert!(!s.wants(&ev("EURUSD", "M5")), "başka dilim kapsanmamalı");
        assert!(!s.wants(&ev("GOLD", "M1")), "başka sembol kapsanmamalı");

        s.add("candle.*.M5");
        assert!(s.wants(&ev("GOLD", "M5")), "joker tüm sembolleri kapsamalı");
        assert!(s.wants(&ev("EURUSD", "M1")), "tekil abonelik korunmalı");

        // Küçük harf de kabul edilmeli.
        let mut s2 = Subs::default();
        s2.add("candle.eurusd.m15");
        assert!(s2.wants(&ev("eurusd", "M15")));

        s.remove("candle.*.M5");
        assert!(!s.wants(&ev("GOLD", "M5")));
    }

    #[test]
    fn candle_subscription_does_not_leak_into_other_channels() {
        let mut s = Subs::default();
        s.add("candle.*.M1");
        let tick = FeedEvent::Tick {
            instance: Arc::from("i"),
            symbol: Arc::from("EURUSD"),
            bid: 1.0,
            ask: 1.0,
            last: 0.0,
            time_msc: 0,
            lat_us: 0,
        };
        assert!(!s.wants(&tick), "mum aboneliği tick getirmemeli");
    }

    #[test]
    fn duplicate_order_id_is_refused() {
        // Aynı kimlikle ikinci istek gerçek parayla ÇİFT POZİSYON demek.
        let t = OrderTracker::new();
        let a = t.register("order-1").unwrap();
        assert!(t.register("order-1").is_err(), "aynı kimlik ikinci kez kabul edilmemeli");
        let b = t.register("order-2").unwrap();
        assert_ne!(a, b);
        assert_eq!(t.text_of(a).as_deref(), Some("order-1"));
        assert_eq!(t.text_of(b).as_deref(), Some("order-2"));
    }

    #[test]
    fn wire_ids_start_at_one_so_zero_means_not_ours() {
        // client_id 0, "bizim göndermediğimiz işlem" anlamına gelir
        // (ör. elle kapatma). Sıfırdan başlamak bunu karıştırırdı.
        let t = OrderTracker::new();
        assert_eq!(t.register("x").unwrap(), 1);
        assert!(t.text_of(0).is_none());
    }

    #[test]
    fn subscription_matching() {
        let mut s = Subs::default();
        let tick = |sym: &str| FeedEvent::Tick {
            instance: Arc::from("i"),
            symbol: Arc::from(sym),
            bid: 1.0,
            ask: 1.0,
            last: 0.0,
            time_msc: 0,
            lat_us: 0,
        };

        assert!(!s.wants(&tick("EURUSD")), "abone olmadan mesaj gitmemeli");
        s.add("tick.EURUSD");
        assert!(s.wants(&tick("EURUSD")));
        assert!(!s.wants(&tick("GBPUSD")));

        s.add("tick.*");
        assert!(s.wants(&tick("GBPUSD")), "joker tüm sembolleri kapsamalı");

        s.remove("tick.*");
        assert!(!s.wants(&tick("GBPUSD")));
        assert!(s.wants(&tick("EURUSD")), "tekil abonelik joker kaldırılınca kalmalı");

        s.remove("tick.EURUSD");
        assert!(!s.wants(&tick("EURUSD")));
    }

    #[test]
    fn order_channel_is_separate_from_market_data() {
        let mut s = Subs::default();
        let ord = FeedEvent::Order {
            instance: Arc::from("i"),
            client_id: 1,
            kind: "txn",
            retcode: 10009,
            order: 5,
            deal: 6,
            position: 7,
            volume: 0.1,
            price: 1.0,
            bid: 0.0,
            ask: 0.0,
            order_state: 0,
            txn_type: 0,
            source: sinyal_proto::res_source::LIVE,
            profit: 0.0,
            commission: 0.0,
            swap: 0.0,
            comment: String::new(),
        };
        s.add("tick.*");
        assert!(!s.wants(&ord), "tick aboneliği emir olaylarını kapsamamalı");
        s.add("order");
        assert!(s.wants(&ord));
    }

    #[test]
    fn unknown_channels_are_ignored_not_crashing() {
        let mut s = Subs::default();
        s.add("saçmalık");
        s.add("tick");
        s.add("");
        s.remove("yok.böyle");
        // Hiçbir aboneliğe dönüşmemeli.
        assert!(!s.tick_all && !s.book_all && s.ticks.is_empty() && !s.orders);
    }

    #[test]
    fn comment_is_truncated_to_fit_the_wire_field() {
        let long = "x".repeat(200);
        assert!(short(&long).len() < COMMENT_LEN);
    }

    fn ctx_with(token: Option<&str>) -> Ctx {
        let (events, _rx) = broadcast::channel(16);
        Ctx {
            registry: Arc::new(Registry::default()),
            events,
            cmd_tx: HashMap::new(),
            hist_tx: HashMap::new(),
            token: token.map(str::to_owned),
            trading: true,
            allow_live: false,
            deviation: 20,
            orders: Arc::new(OrderTracker::new()),
            candles: Arc::new(Mutex::new(crate::candles::CandleStore::new())),
            hist_slots: Arc::new(tokio::sync::Semaphore::new(HIST_SLOTS)),
            sim: None,
            replay: None,
            sltp: Arc::new(SltpGuard::default()),
        }
    }

    /// Kaymasız simülasyon: dolum fiyatı beklentileri sade kalsın diye
    /// **açıkça** kapatılıyor. Kaymanın kendi testi ayrı (bkz.
    /// `slippage_is_applied_against_the_client_on_the_paper_port`).
    fn sim_cfg_no_slip(balance: f64) -> crate::sim::SimConfig {
        crate::sim::SimConfig { balance, leverage: 100, slippage_points: 0.0 }
    }

    /// Bağlantının başlangıç seviyesi — `serve_conn` ile aynı kural.
    fn start_level(ctx: &Ctx) -> Level {
        if ctx.token.is_some() { Level::Public } else { Level::Trader }
    }

    #[test]
    fn without_a_token_trading_is_open_as_hello_advertises() {
        // GERİLEME TESTİ — canlı testte yakalandı.
        //
        // `hello` token yokken `auth_required_for_trading: false` ilan ediyor,
        // ama bağlantı Public başlayıp `auth` da yükseltemediği için emir
        // "auth gerekli" ile reddediliyordu: yüzey kalıcı olarak kilitliydi ve
        // sunucu kendi ilanıyla çelişiyordu.
        let ctx = ctx_with(None);
        let mut level = start_level(&ctx);
        assert_eq!(level, Level::Trader, "token yoksa dogrudan trader");

        let mut subs = Subs::default();
        let out = handle_client_msg(ClientMsg::Positions, &ctx, &mut subs, &mut level).unwrap();
        assert!(
            !matches!(out.first(), Some(ServerMsg::Error { .. })),
            "token yokken hesap sorgusu reddedilmemeli: {out:?}"
        );

        // `auth` göndermek zararsız olmalı — istemci kodu iki kurulumda da aynı.
        let out = handle_client_msg(
            ClientMsg::Auth { token: "herhangi".into() },
            &ctx,
            &mut subs,
            &mut level,
        )
        .unwrap();
        assert!(matches!(out.first(), Some(ServerMsg::Authed { .. })));
        assert_eq!(level, Level::Trader);
    }

    #[test]
    fn with_a_token_trading_stays_locked_until_auth() {
        let ctx = ctx_with(Some("gizli"));
        let mut level = start_level(&ctx);
        assert_eq!(level, Level::Public);
        let mut subs = Subs::default();

        // Token'sız: reddedilmeli.
        let out = handle_client_msg(ClientMsg::Positions, &ctx, &mut subs, &mut level).unwrap();
        assert!(matches!(out.first(), Some(ServerMsg::Error { .. })));

        // Yanlış token: seviye DEĞİŞMEMELİ.
        let out =
            handle_client_msg(ClientMsg::Auth { token: "yanlis".into() }, &ctx, &mut subs, &mut level)
                .unwrap();
        assert!(matches!(out.first(), Some(ServerMsg::Error { .. })));
        assert_eq!(level, Level::Public, "basarisiz denemeden sonra public kalmali");

        // Doğru token: yükselir.
        let out =
            handle_client_msg(ClientMsg::Auth { token: "gizli".into() }, &ctx, &mut subs, &mut level)
                .unwrap();
        assert!(matches!(out.first(), Some(ServerMsg::Authed { .. })));
        assert_eq!(level, Level::Trader);

        // Artık geçmeli.
        let out = handle_client_msg(ClientMsg::Positions, &ctx, &mut subs, &mut level).unwrap();
        assert!(!matches!(out.first(), Some(ServerMsg::Error { .. })));
    }

    #[test]
    fn replay_playback_starts_on_subscribe_not_on_connect() {
        // Kapının NEREDE açıldığı, "açılıyor mu"dan daha önemli.
        //
        // Bağlantı kurulduğunda açmak yetmiyor: yayın kanalına abone olmak,
        // istemcinin hangi KANALLARI istediğini bilmek demek değil. Kapı
        // bağlantıda açılsaydı `--replay-speed 0`da kayıt, `subscribe` mesajı
        // gelmeden akıp biter ve pompa her tick'i "abone değil" diye elerdi.
        // Gerçek ikili ile ölçüldü: bağlantıda açınca 1689 tick'in 0'ı,
        // abonelikte açınca 1689'unun 1689'u ulaştı.
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        let gate = ctx.replay.as_ref().unwrap().start.clone();
        let mut level = start_level(&ctx);
        let mut subs = Subs::default();

        // Bağlantı kurulmuş ve hello gönderilmiş olsa bile kapı KAPALI.
        assert!(!gate.is_open(), "kapi baglantida acilmamali");
        handle_client_msg(ClientMsg::Ping, &ctx, &mut subs, &mut level).unwrap();
        assert!(!gate.is_open(), "ping oynatimi baslatmamali");

        handle_client_msg(
            ClientMsg::Subscribe { channels: vec!["tick.*".into()] },
            &ctx,
            &mut subs,
            &mut level,
        )
        .unwrap();
        assert!(subs.tick_all, "abonelik kurulmali");
        assert!(gate.is_open(), "abonelikten SONRA oynatim baslamali");
    }

    #[test]
    fn a_live_server_has_no_gate_to_open() {
        // Canlıda replay bağlamı yok; abonelik yolu buna rağmen çalışmalı.
        let ctx = ctx_with(None);
        let mut level = start_level(&ctx);
        let mut subs = Subs::default();
        assert!(ctx.replay.is_none());
        handle_client_msg(
            ClientMsg::Subscribe { channels: vec!["tick.*".into()] },
            &ctx,
            &mut subs,
            &mut level,
        )
        .unwrap();
        assert!(subs.tick_all);
    }

    #[test]
    fn market_data_never_requires_auth_even_with_a_token() {
        // Kullanıcının açık isteği: grafik/piyasa yüzeyi token'sız.
        let ctx = ctx_with(Some("gizli"));
        let mut level = start_level(&ctx);
        let mut subs = Subs::default();

        for msg in
            [ClientMsg::Symbols, ClientMsg::Snapshot { symbols: vec![] }, ClientMsg::Ping]
        {
            let out = handle_client_msg(msg, &ctx, &mut subs, &mut level).unwrap();
            assert!(
                !matches!(out.first(), Some(ServerMsg::Error { .. })),
                "piyasa yuzeyi token istememeli: {out:?}"
            );
        }

        // Ama `order` KANALI hesap gizliliği kapsamında — token ister.
        let out = handle_client_msg(
            ClientMsg::Subscribe { channels: vec!["tick.*".into(), "order".into()] },
            &ctx,
            &mut subs,
            &mut level,
        )
        .unwrap();
        assert!(subs.tick_all, "tick aboneligi kurulmali");
        assert!(!subs.orders, "emir kanali token olmadan acilmamali");
        assert!(matches!(out.first(), Some(ServerMsg::Error { .. })));
    }

    // --- geçmiş / mum kaynağı ---

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    fn mt5_bar(t: i64, c: f64) -> crate::candles::Bar {
        crate::candles::Bar {
            t,
            o: c,
            h: c,
            l: c,
            c,
            ticks: 10,
            spread: Some(9),
            ..Default::default()
        }
    }

    #[test]
    fn candles_is_public_even_with_a_token_configured() {
        // Grafik çizen istemci token istemez; bu kural yalnız `handle_client_msg`
        // içinde değil, işlemin kendisinde de geçerli.
        let ctx = ctx_with(Some("gizli"));
        let mut level = start_level(&ctx);
        let mut subs = Subs::default();
        let out = handle_client_msg(
            ClientMsg::Candles { symbol: "EURUSD".into(), tf: "M1".into(), count: 10 },
            &ctx,
            &mut subs,
            &mut level,
        )
        .unwrap();
        assert!(matches!(out.first(), Some(ServerMsg::Candles { .. })), "token istememeli: {out:?}");

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 10));
        assert!(matches!(out.first(), Some(ServerMsg::Candles { .. })));
    }

    #[test]
    fn answer_reports_the_source_and_never_mixes_the_two() {
        // MT5 serisi (BID) ile tick serisi (MID) aynı fiyat serisi değil.
        let ctx = ctx_with(None);
        {
            let mut cs = ctx.candles.lock().unwrap();
            let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);
            for i in 0..3 {
                cs.on_tick("EURUSD", 1.1000, 1.1002, base + i * 60_000 + 500);
            }
        }

        // Önce MT5 yok: tick kaynağı bildirilmeli.
        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 100));
        match &out[0] {
            ServerMsg::Candles { src_kind, items, age_ms, hist, .. } => {
                assert_eq!(*src_kind, "tick");
                assert_eq!(items.len(), 3);
                assert!(age_ms.is_none(), "tick serisi canli");
                // Kayıtlı örnek yok → geçmiş kanalı da yok.
                assert_eq!(*hist, "off");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // MT5 barı gelince O dönmeli ve tick barları KARIŞMAMALI.
        ctx.candles.lock().unwrap().ingest_mt5("EURUSD", "M1", &[mt5_bar(1_699_000_000_000, 1.0900)]);
        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 100));
        match &out[0] {
            ServerMsg::Candles { src_kind, items, age_ms, .. } => {
                assert_eq!(*src_kind, "mt5");
                assert_eq!(items.len(), 1, "tick barlari eklenmemeli");
                assert!(age_ms.is_some(), "goruntunun yasi bildirilmeli");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_client_request_triggers_the_mt5_fetch_and_the_answer_says_so() {
        // Sahte okuyucu thread'i: isteği alır, barları depoya işler, cevaplar.
        let mut ctx = ctx_with(None);
        let (h_tx, h_rx) = std::sync::mpsc::channel::<HistCmd>();
        ctx.hist_tx.insert("mt5-1".into(), h_tx);
        ctx.registry.set_symbols("mt5-1", vec![{
            let mut e = sinyal_proto::SymbolEntry { symbol_id: 3, ..Default::default() };
            sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
            e
        }]);

        let store = ctx.candles.clone();
        let reader = std::thread::spawn(move || {
            let cmd = h_rx.recv().expect("istek gelmeli");
            assert_eq!(cmd.symbol, "EURUSD");
            assert_eq!(cmd.symbol_id, 3, "sembol kaydindan cozulmeli");
            assert_eq!(cmd.tf, "H4", "kanonik dilim adi gitmeli");
            assert_eq!(cmd.count, 50);
            store.lock().unwrap().ingest_mt5(
                "EURUSD",
                "H4",
                &[mt5_bar(1_700_000_000_000, 1.2345)],
            );
            let _ = cmd.reply.send(HistReply::Done { status: HistStatus::Complete, bars: 1 });
        });

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "h4".into(), 50));
        reader.join().unwrap();
        match &out[0] {
            ServerMsg::Candles { tf, src_kind, items, hist, hist_note, .. } => {
                assert_eq!(tf, "H4");
                assert_eq!(*src_kind, "mt5");
                assert_eq!(*hist, "ok");
                assert!(hist_note.is_none());
                assert!((items[0].c - 1.2345).abs() < 1e-12);
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn incomplete_delivery_is_surfaced_to_the_client() {
        // Bar halkası dolarsa barlar KALICI kaybolur; istemci delikli seriyi
        // tam sanmamalı.
        let mut ctx = ctx_with(None);
        let (h_tx, h_rx) = std::sync::mpsc::channel::<HistCmd>();
        ctx.hist_tx.insert("mt5-1".into(), h_tx);
        ctx.registry.set_symbols("mt5-1", vec![{
            let mut e = sinyal_proto::SymbolEntry::default();
            sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
            e
        }]);

        let store = ctx.candles.clone();
        let reader = std::thread::spawn(move || {
            let cmd = h_rx.recv().unwrap();
            store.lock().unwrap().ingest_mt5("EURUSD", "M1", &[mt5_bar(1_700_000_000_000, 1.0)]);
            let _ = cmd.reply.send(HistReply::Done {
                status: HistStatus::Incomplete { expected: 500, got: 1 },
                bars: 1,
            });
        });

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 500));
        reader.join().unwrap();
        match &out[0] {
            ServerMsg::Candles { hist, hist_note, .. } => {
                assert_eq!(*hist, "incomplete");
                assert!(hist_note.as_ref().unwrap().contains("1/500"), "eksiklik yazilmali");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_dead_history_channel_still_produces_an_answer() {
        // Okuyucu cevap vermezse istemciyi asmıyoruz: elimizdeki tick
        // barlarını, neden MT5 gelmediğini söyleyerek veriyoruz.
        let mut ctx = ctx_with(None);
        let (h_tx, h_rx) = std::sync::mpsc::channel::<HistCmd>();
        ctx.hist_tx.insert("mt5-1".into(), h_tx);
        ctx.registry.set_symbols("mt5-1", vec![{
            let mut e = sinyal_proto::SymbolEntry::default();
            sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
            e
        }]);
        ctx.candles.lock().unwrap().on_tick("EURUSD", 1.1, 1.1, 1_700_000_000_000);

        // İsteği al ve cevap kanalını DÜŞÜR (okuyucu öldü).
        let reader = std::thread::spawn(move || {
            let cmd = h_rx.recv().unwrap();
            drop(cmd.reply);
        });

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 10));
        reader.join().unwrap();
        match &out[0] {
            ServerMsg::Candles { src_kind, items, hist, .. } => {
                assert_eq!(*src_kind, "tick", "MT5 gelmezse tick'e dusulmeli");
                assert_eq!(items.len(), 1);
                assert_eq!(*hist, "refused");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_fresh_snapshot_is_not_refetched() {
        // Her istekte çekmek `CopyRates`'i sürekli meşgul ederdi. Kanal
        // BAĞLI ama okuyucu yok: istek gitseydi test zaman aşımına düşerdi.
        let mut ctx = ctx_with(None);
        let (h_tx, h_rx) = std::sync::mpsc::channel::<HistCmd>();
        ctx.hist_tx.insert("mt5-1".into(), h_tx);
        ctx.candles.lock().unwrap().ingest_mt5("EURUSD", "M1", &[mt5_bar(1_700_000_000_000, 1.0)]);

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 1));
        match &out[0] {
            ServerMsg::Candles { hist, src_kind, .. } => {
                assert_eq!(*hist, "cached");
                assert_eq!(*src_kind, "mt5");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        assert!(h_rx.try_recv().is_err(), "taze goruntu icin yeniden cekilmemeli");
    }

    #[test]
    fn an_empty_d1_answer_explains_that_it_cannot_come_from_ticks() {
        // "veri yok" ile "bu dilim tick'ten uretilmiyor" ayni sey degil.
        let ctx = ctx_with(None);
        ctx.candles.lock().unwrap().on_tick("EURUSD", 1.1, 1.1, 1_700_000_000_000);

        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "D1".into(), 10));
        match &out[0] {
            ServerMsg::Candles { tf, items, hist_note, .. } => {
                assert_eq!(tf, "D1");
                assert!(items.is_empty());
                assert!(
                    hist_note.as_ref().unwrap().contains("tick'ten uretilmez"),
                    "sebep soylenmeli: {hist_note:?}"
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // M1 boş dönse bile böyle bir not OLMAMALI — o dilim tick'ten üretilir.
        let out = rt().block_on(candles_op(&ctx, "YOKSUN".into(), "M1".into(), 10));
        match &out[0] {
            ServerMsg::Candles { hist_note, .. } => {
                assert!(
                    hist_note.as_deref() != Some("M1 yalniz MT5 kaynagindan gelir, tick'ten uretilmez"),
                    "M1 tick'ten uretilebilir"
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn concurrent_history_fetches_are_capped_so_the_terminal_is_not_hammered() {
        // `candles` PUBLIC. Sinirsiz birakmak, token'siz bir istemcinin
        // `CopyRates` uzerinden ticaret terminalini yavaslatmasi demekti.
        let sem = Arc::new(tokio::sync::Semaphore::new(HIST_SLOTS));
        let mut held = Vec::new();
        for _ in 0..HIST_SLOTS {
            held.push(sem.clone().try_acquire_owned().expect("tavan kadar izin verilmeli"));
        }
        assert!(sem.clone().try_acquire_owned().is_err(), "tavan asilmamali");
        drop(held.pop());
        assert!(sem.clone().try_acquire_owned().is_ok(), "biten istek yerini birakmali");
    }

    // -----------------------------------------------------------------
    // Replay kipi + simüle emir motoru
    // -----------------------------------------------------------------

    /// `(bağlam, komut alıcısı, bitiş göndericisi)`.
    type ReplayFixture =
        (Ctx, std::sync::mpsc::Receiver<Cmd>, tokio::sync::watch::Sender<Option<ReplayEnd>>);

    /// Replay bağlamı kurulmuş bir `Ctx`.
    ///
    /// `cmd_tx` BİLEREK dolduruluyor: "replay'de paylaşılan belleğe hiçbir şey
    /// gitmiyor" iddiası, ancak gidebileceği bir kanal varken sınanabilir.
    /// Bitiş göndericisi de dönüyor — düşürülürse `done` kanalı kapanır ve
    /// gerçek kurulumdan sapardık.
    fn replay_ctx(balance: f64) -> ReplayFixture {
        let mut ctx = ctx_with(None);
        // Komut kanalı BİLEREK bağlı ve dinleniyor: "hiçbir komut gitmedi"
        // iddiası ancak gidebileceği bir yer varken anlamlıdır.
        let (c_tx, c_rx) = std::sync::mpsc::channel::<Cmd>();
        ctx.cmd_tx.insert("mt5-1".into(), c_tx);
        let (done_tx, done_rx) = tokio::sync::watch::channel(None);
        ctx.sim = Some(Arc::new(SimExec::new(
            MODE_REPLAY,
            &["mt5-1".to_string()],
            sim_cfg_no_slip(balance),
        )));
        ctx.replay = Some(Arc::new(Replay::new(
            &["mt5-1".to_string()],
            Some(1_700_000_000_000),
            Some(1_700_000_600_000),
            done_rx,
            Arc::new(crate::replay::StartGate::new()),
        )));
        (ctx, c_rx, done_tx)
    }

    /// PAPER bağlamı — canlı veri, simüle yürütme.
    ///
    /// `replay` alanı BOŞ: paper bir kaydın parçası değil. Komut kanalı yine
    /// bağlı bırakılıyor ki "paylaşılan belleğe hiçbir komut gitmedi" iddiası
    /// gerçekten sınanabilsin — gerçek daemon'da bu harita boştur ve bu
    /// fixture kasten ONDAN DAHA TEHLİKELİ bir kurulum kuruyor.
    fn paper_ctx(balance: f64) -> (Ctx, std::sync::mpsc::Receiver<Cmd>) {
        paper_ctx_cfg(sim_cfg_no_slip(balance))
    }

    fn paper_ctx_cfg(cfg: crate::sim::SimConfig) -> (Ctx, std::sync::mpsc::Receiver<Cmd>) {
        let mut ctx = ctx_with(None);
        let (c_tx, c_rx) = std::sync::mpsc::channel::<Cmd>();
        ctx.cmd_tx.insert("mt5-1".into(), c_tx);
        ctx.sim =
            Some(Arc::new(SimExec::new(MODE_PAPER, &["mt5-1".to_string()], cfg)));
        (ctx, c_rx)
    }

    /// EURUSD'yi tanıt ve bir fiyat yaz (kayıttan ya da canlıdan gelmiş gibi).
    fn seed_symbol(ctx: &Ctx, bid: f64, ask: f64) {
        seed_symbol_stops(ctx, bid, ask, 0);
    }

    fn seed_symbol_stops(ctx: &Ctx, bid: f64, ask: f64, stops_level: u32) {
        let mut e = sinyal_proto::SymbolEntry {
            symbol_id: 1,
            digits: 5,
            point: 0.00001,
            tick_size: 0.00001,
            volume_min: 0.01,
            volume_max: 100.0,
            volume_step: 0.01,
            contract_size: 100_000.0,
            stops_level,
            flags: sinyal_proto::sym_flag::READY,
            ..Default::default()
        };
        sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
        ctx.registry.set_symbols("mt5-1", vec![e]);
        ctx.registry.update_last(
            "mt5-1",
            1,
            crate::state::LastTick { bid, ask, last: 0.0, time_msc: 1_700_000_000_500 , recv_ms: 0},
        );
    }

    #[test]
    fn the_fill_price_comes_from_the_same_instance_that_resolved_the_symbol() {
        // İKİ TERMINAL AYNI SEMBOLU KOTE EDIYOR (iki broker / iki hesap —
        // paper birden çok --instance destekliyor).
        //
        // Eski `sim_last` fiyatı yalnızca ADLA arıyordu ve `snapshot` örnek
        // adına göre SIRALI döndüğü için daima alfabetik ilk örneğin fiyatını
        // veriyordu. Sembol KAYDI ise `resolve_any`den geliyor ve o bir
        // `HashMap` üzerinde geziniyor — yani rastgele bir örnek. Sonuç: bir
        // terminalin fiyatından, başka terminalin sözleşmesiyle dolum; üstelik
        // hangisinin seçileceği koşudan koşuya değişebildiği için replay
        // tekrarlanabilirliği de kırılırdı.
        //
        // Testin sınadığı şey: dolum fiyatı, isteği çözen örneğin fiyatıdır.
        let (ctx, _c_rx) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);

        // İkinci örnek AYNI sembolü ÇOK FARKLI bir fiyatla kote ediyor.
        let mut e = sinyal_proto::SymbolEntry {
            symbol_id: 1,
            digits: 5,
            point: 0.00001,
            tick_size: 0.00001,
            volume_min: 0.01,
            volume_max: 100.0,
            volume_step: 0.01,
            contract_size: 100_000.0,
            flags: sinyal_proto::sym_flag::READY,
            ..Default::default()
        };
        sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
        // Ad alfabetik olarak "mt5-1"den ÖNCE gelsin ki eski davranış
        // (sıralı listenin ilki) bu fiyatı seçsin ve test kırılabilsin.
        ctx.registry.set_symbols("aa-other", vec![e]);
        ctx.registry.update_last(
            "aa-other",
            1,
            crate::state::LastTick { bid: 2.0, ask: 2.00002, last: 0.0, time_msc: 1 , recv_ms: 0},
        );

        // ASIL İDDİA, `resolve_any`nin hangi örneği seçtiğinden BAĞIMSIZ
        // olmalı: `HashMap` sırasına bağlı bir test hatayı ancak yarı yarıya
        // yakalardı. Her iki örnek için de ayrı ayrı soruyoruz.
        let a = sim_last(&ctx, "aa-other", "EURUSD").expect("aa-other fiyati olmali");
        assert_eq!((a.bid, a.ask), (2.0, 2.00002), "aa-other kendi fiyatini vermeli");
        let b = sim_last(&ctx, "mt5-1", "EURUSD").expect("mt5-1 fiyati olmali");
        assert_eq!((b.bid, b.ask), (1.10000, 1.10002), "mt5-1 kendi fiyatini vermeli");

        // Ve `sim_lookup` ikisini TUTARLI eşleştirmeli: kayıt hangi
        // örnekten geldiyse fiyat da ondan.
        let look = sim_lookup(&ctx, "EURUSD").expect("sembol cozulmeli");
        let expected = sim_last(&ctx, &look.instance, "EURUSD").unwrap();
        assert_eq!(
            (look.bid, look.ask),
            (expected.bid, expected.ask),
            "fiyat, sembol kaydini veren ornekten gelmeli — ornek: {}",
            look.instance
        );
    }

    // -- son kullanma çözümü --------------------------------------------
    //
    // Bu mantığın sessizce bozulması pahalı: ölçüldü ki yanlış saat dilimi
    // ya emri 10022 ile reddettirir ya da 3 saat ERKEN düşürür (sessizce).

    fn expiry_req(time: &str, expiration: i64, expire_sn: i64) -> OrderReq {
        OrderReq {
            id: "x".into(),
            action: "pending".into(),
            symbol: "EURUSD".into(),
            side: String::new(),
            order_type: "buy_limit".into(),
            volume: 0.01,
            price: 1.0,
            stoplimit: 0.0,
            sl: 0.0,
            tp: 0.0,
            deviation: 0,
            time: time.into(),
            expiration,
            expire_sn,
            filling: String::new(),
            comment: String::new(),
        }
    }

    /// Broker saati 1 000 000 sn olan tek sembollü bir kayıt.
    ///
    /// `update_last` var olmayan bir örneği OLUŞTURMAZ (tick'ten örnek
    /// türetmek yanlış olurdu), o yüzden önce sembol tablosu yazılıyor.
    fn expiry_ctx() -> Ctx {
        let ctx = ctx_with(None);
        ctx.registry.set_symbols("mt5-1", Vec::new());
        ctx.registry.update_last(
            "mt5-1",
            0,
            crate::state::LastTick { bid: 1.0, ask: 1.1, last: 0.0, time_msc: 1_000_000_000 , recv_ms: 0},
        );
        ctx
    }

    #[test]
    fn expire_sn_broker_saatinden_hesaplanir() {
        let ctx = expiry_ctx();
        let (tt, exp) =
            resolve_expiry(&ctx, &expiry_req("", 0, 120), type_time::GTC, "mt5-1", 0).unwrap();
        // Yerel saat DEĞİL, tick damgası taban alınmalı. Sonuç dakikaya
        // YUKARI yuvarlanır (MT5 aşağı kırpıyor; eksik teslim etmemek için).
        assert!(exp % 60 == 0, "dakika sinirina oturmali: {exp}");
        assert!(
            exp >= 1_000_000 + 120 && exp < 1_000_000 + 120 + 60,
            "istenenden az olmamali, bir dakikadan fazla da tasmamali: {exp}"
        );
        assert_eq!(tt, type_time::SPECIFIED, "time bos birakilinca specified varsayilmali");
    }

    #[test]
    fn expire_sn_tick_yoksa_uydurmaz() {
        // Yerel saatle hesaplamak ÖLÇÜLMÜŞ 3 saatlik hatayı geri getirirdi;
        // bilmiyorsak söylemek zorundayız.
        let ctx = ctx_with(None);
        let e = resolve_expiry(&ctx, &expiry_req("", 0, 120), type_time::GTC, "mt5-1", 0)
            .expect_err("tick yokken basarili olmamali");
        assert!(e.contains("broker saati"), "sebep soylenmeli: {e}");
    }

    #[test]
    fn expire_sn_ile_expiration_birlikte_reddedilir() {
        let ctx = expiry_ctx();
        let e = resolve_expiry(&ctx, &expiry_req("specified", 123, 120), type_time::SPECIFIED, "mt5-1", 0)
            .expect_err("ikisi birden kabul edilmemeli");
        assert!(e.contains("BIRLIKTE"), "{e}");
    }

    #[test]
    fn expire_sn_gtc_ile_celisir() {
        let ctx = expiry_ctx();
        let e = resolve_expiry(&ctx, &expiry_req("gtc", 0, 120), type_time::GTC, "mt5-1", 0)
            .expect_err("gtc + expire_sn celiskisi sessiz gecmemeli");
        assert!(e.contains("celisiyor"), "{e}");
    }

    #[test]
    fn expiration_time_verilmeden_artik_sessizce_dusmez() {
        // ESKİ DAVRANIŞ: alan sessizce yok sayılır, emir sonsuza kadar bekler
        // ve istemci bunu telden anlayamazdı.
        let ctx = expiry_ctx();
        let e = resolve_expiry(&ctx, &expiry_req("", 1_000_500, 0), type_time::GTC, "mt5-1", 0)
            .expect_err("sessiz yok sayma geri gelmemeli");
        assert!(e.contains("SONSUZA KADAR"), "sonucu soylemeli: {e}");
    }

    #[test]
    fn mutlak_expiration_specified_ile_gecer() {
        let ctx = expiry_ctx();
        let (tt, exp) = resolve_expiry(
            &ctx,
            &expiry_req("specified", 1_000_500, 0),
            type_time::SPECIFIED,
            "mt5-1",
            0,
        )
        .unwrap();
        assert_eq!(exp, 1_000_500, "mutlak damga oldugu gibi gecmeli");
        assert_eq!(tt, type_time::SPECIFIED);
    }

    #[test]
    fn son_kullanma_yoksa_sifir_kalir() {
        let ctx = expiry_ctx();
        let (tt, exp) =
            resolve_expiry(&ctx, &expiry_req("", 0, 0), type_time::GTC, "mt5-1", 0).unwrap();
        assert_eq!((tt, exp), (type_time::GTC, 0));
    }

    fn market_order(id: &str, side: &str, volume: f64) -> ClientMsg {
        ClientMsg::Order(OrderReq {
            id: id.into(),
            action: "deal".into(),
            symbol: "EURUSD".into(),
            side: side.into(),
            order_type: String::new(),
            volume,
            price: 0.0,
            stoplimit: 0.0,
            sl: 0.0,
            tp: 0.0,
            deviation: 0,
            time: String::new(),
            expiration: 0,
            expire_sn: 0,
            filling: String::new(),
            comment: String::new(),
        })
    }

    fn ask(ctx: &Ctx, cm: ClientMsg) -> Vec<ServerMsg> {
        let mut subs = Subs::default();
        let mut level = start_level(ctx);
        handle_client_msg(cm, ctx, &mut subs, &mut level).unwrap()
    }

    #[test]
    fn hello_says_replay_and_declares_the_span() {
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        match hello_msg(&ctx, Level::Trader) {
            ServerMsg::Hello { mode, trading, replay_from_ms, replay_to_ms, instances, .. } => {
                assert_eq!(mode, "replay", "istemci replay'i canli sanmamali");
                assert!(trading, "replay'de emir yolu acik (simule)");
                assert_eq!(replay_from_ms, Some(1_700_000_000_000));
                assert_eq!(replay_to_ms, Some(1_700_000_600_000));
                assert_eq!(instances, vec!["mt5-1".to_string()], "kayittaki ornek ilan edilmeli");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Canlı kip: aynı fonksiyon "live" demeli ve kapsam alanı olmamalı.
        let live = ctx_with(None);
        match hello_msg(&live, Level::Trader) {
            ServerMsg::Hello { mode, replay_from_ms, replay_to_ms, .. } => {
                assert_eq!(mode, "live");
                assert!(replay_from_ms.is_none() && replay_to_ms.is_none());
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_replayed_order_is_simulated_and_marked_sim() {
        // Emirleri reddeden bir replay, sinyal sisteminin emir yolunu HİÇ
        // test edemezdi.
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        let mut events = ctx.events.subscribe();

        let out = ask(&ctx, market_order("o1", "buy", 0.10));
        // (1) İlk cevap canlıdaki ile aynı: `queued`.
        match &out[0] {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "queued", "canlidaki ilk cevapla ayni olmali: {e:?}");
                assert!(e.sim, "simule oldugu SOYLENMELI");
                assert_eq!(e.src, "mt5-1");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // (2) Sonra ack(10008) → txn(10009), canlıyla AYNI sırada.
        let mut kinds = Vec::new();
        while let Ok(ev) = events.try_recv() {
            let msg = to_wire(&ev, &ctx).expect("emir olayi tele donmeli");
            match msg {
                ServerMsg::Order(e) => {
                    assert!(e.sim, "simule olayin sim bayragi dusmemeli: {e:?}");
                    assert_eq!(e.id, "o1", "istemci kimligi geri eslenmeli");
                    kinds.push((e.kind, e.retcode, e.price));
                }
                other => panic!("beklenmeyen: {other:?}"),
            }
        }
        assert_eq!(kinds.len(), 2, "ack ve txn bekleniyordu: {kinds:?}");
        assert_eq!(kinds[0].0, "ack");
        assert_eq!(kinds[0].1, Some(10008));
        assert_eq!(kinds[1].0, "txn");
        assert_eq!(kinds[1].1, Some(10009));
        // (3) Dolum KAYITTAKİ ask'ten: alış ask'ten dolar.
        assert_eq!(kinds[1].2, Some(1.10002), "alis ask'ten dolmali");

        // (4) Pozisyon simüle durumda görünmeli.
        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, total, .. } => {
                assert_eq!(*total, 1);
                assert_eq!(items[0].symbol, "EURUSD");
                assert_eq!(items[0].side, "buy");
                assert!((items[0].price_open - 1.10002).abs() < 1e-12);
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_sell_fills_at_the_recorded_bid_and_closing_moves_the_synthetic_balance() {
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);

        let out = ask(&ctx, market_order("s1", "sell", 0.10));
        assert!(matches!(&out[0], ServerMsg::Order(e) if e.kind == "queued"));
        let ticket = match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => {
                assert!((items[0].price_open - 1.10000).abs() < 1e-12, "satis bid'den dolmali");
                items[0].ticket
            }
            other => panic!("beklenmeyen: {other:?}"),
        };

        // Fiyat düştü: satış pozisyonu kârda ve ASK'ten kapanır.
        seed_symbol(&ctx, 1.09900, 1.09902);
        let out = ask(&ctx, ClientMsg::Close { id: "c1".into(), ticket, volume: 0.0 });
        assert!(matches!(&out[0], ServerMsg::Order(e) if e.kind == "queued" && e.sim));

        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, total, .. } => {
                assert!(items.is_empty() && *total == 0, "tam kapanis pozisyonu silmeli");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        match &ask(&ctx, ClientMsg::Account)[0] {
            ServerMsg::Account { items } => {
                let a = &items[0];
                // (1.10000 − 1.09902) × 0.1 × 100000 = 9.8
                assert!((a.balance - 10_009.8).abs() < 1e-6, "sentetik bakiye: {}", a.balance);
                assert_eq!(a.mode, "demo", "simule hesap ASLA gercek sayilmamali");
                assert!(a.can_trade);
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn replay_never_writes_a_single_command_to_shared_memory() {
        // GÜVENLİK SINIRI. Kanal bağlı ve dinleniyor: bir komut üretilseydi
        // burada görünürdü.
        let (ctx, cmd_rx, _done) = replay_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);

        ask(&ctx, market_order("o1", "buy", 0.10));
        let ticket = match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => items[0].ticket,
            other => panic!("beklenmeyen: {other:?}"),
        };
        ask(&ctx, ClientMsg::ModifySltp { id: "m1".into(), ticket, sl: 1.09, tp: 1.11 });
        ask(&ctx, ClientMsg::Close { id: "c1".into(), ticket, volume: 0.0 });
        ask(&ctx, ClientMsg::Cancel { id: "x1".into(), ticket: 4242 });
        ask(
            &ctx,
            ClientMsg::Order(OrderReq {
                id: "p1".into(),
                action: "pending".into(),
                symbol: "EURUSD".into(),
                side: String::new(),
                order_type: "buy_limit".into(),
                volume: 0.05,
                price: 1.09000,
                stoplimit: 0.0,
                sl: 0.0,
                tp: 0.0,
                deviation: 0,
                time: String::new(),
                expiration: 0,
                expire_sn: 0,
                filling: String::new(),
                comment: String::new(),
            }),
        );

        assert!(
            cmd_rx.try_recv().is_err(),
            "replay kipinde paylasilan bellege HICBIR komut gitmemeli"
        );

        // Son savunma hattı: `dispatch` doğrudan çağrılsa bile göndermez.
        let out = dispatch(&ctx, "mt5-1", Cmd::default(), "zorla");
        match out {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "rejected");
                assert!(e.comment.contains("replay"), "sebep soylenmeli: {e:?}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        assert!(cmd_rx.try_recv().is_err(), "dispatch bile komut yazmamali");
    }

    #[test]
    fn live_order_events_never_carry_the_sim_flag() {
        // Canlı kipte `sim` alanı tel üzerinde HİÇ görünmemeli.
        let ctx = ctx_with(None);
        let ev = FeedEvent::Order {
            instance: Arc::from("mt5-1"),
            client_id: 0,
            kind: "txn",
            retcode: 10009,
            order: 5,
            deal: 6,
            position: 7,
            volume: 0.1,
            price: 1.2345,
            bid: 1.2343,
            ask: 1.2344,
            order_state: sinyal_proto::order_state::FILLED,
            txn_type: sinyal_proto::txn_type::DEAL_ADD,
            source: sinyal_proto::res_source::LIVE,
            profit: 0.0,
            commission: 0.0,
            swap: 0.0,
            comment: String::new(),
        };
        let msg = to_wire(&ev, &ctx).unwrap();
        match &msg {
            ServerMsg::Order(e) => {
                assert!(!e.sim, "canli olay simule isaretlenemez");
                // Canlı olayda gerçekleşmiş alanlar TELDE OLMAMALI: sıcak
                // yolda okunamıyorlar ve sıfırı göndermek "kâr sıfırdı"
                // dedirtirdi.
                assert!(e.reconciled.is_none(), "canli olay mutabakat isaretlenemez");
                assert!(e.profit.is_none() && e.commission.is_none() && e.swap.is_none());
                // Ölçüm alanları EA'dan tele KESİNTİSİZ geçmeli; bir yerde
                // düşürülürse kayma analizi sessizce imkânsız hale gelir.
                assert_eq!(e.bid, Some(1.2343));
                assert_eq!(e.ask, Some(1.2344));
                assert_eq!(e.order_state, Some(4));
                assert_eq!(e.txn_type, Some(6));
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        let j = serde_json::to_string(&msg).unwrap();
        assert!(!j.contains("sim"), "canli olayda sim alani HIC olmamali: {j}");

        // Reddedilen canlı emirler de temiz olmalı.
        let j = serde_json::to_string(&rejected(&ctx, "o1", "test")).unwrap();
        assert!(!j.contains("sim"), "{j}");
    }

    /// Mutabakat olayı gerçekleşmiş sonucu tele TAŞIMALI — sıfır olsa bile.
    ///
    /// İki ayrı şeyi birden sabitliyor:
    /// 1. `reconciled: true` gitmeli — istemci "geç gelen" olayı canlıdan
    ///    ayırt edebilmeli, yoksa aynı dolumu iki kez sayar.
    /// 2. Komisyon **sıfırken de** gönderilmeli. Ölçüldü: bu brokerda GOLD
    ///    komisyonsuz. "Sıfırı atla" optimizasyonu, komisyonsuz bir hesapta
    ///    alanı hiç göstermez ve istemci "ölçüm yok" sanardı — oysa ölçüm
    ///    yapıldı ve sonucu sıfır.
    #[test]
    fn reconcile_events_carry_realized_result_even_when_zero() {
        let (ctx, _rx) = live_ctx(7, 0.0);
        let ev = FeedEvent::Order {
            instance: Arc::from("mt5-1"),
            client_id: 0,
            kind: "txn",
            retcode: 10009,
            order: 5,
            deal: 6,
            position: 7,
            volume: 0.01,
            price: 4350.0,
            bid: 0.0,
            ask: 0.0,
            order_state: sinyal_proto::order_state::FILLED,
            txn_type: sinyal_proto::txn_type::DEAL_ADD,
            source: sinyal_proto::res_source::RECONCILE,
            profit: 12.34,
            commission: 0.0,
            swap: -0.89,
            comment: String::new(),
        };
        let msg = to_wire(&ev, &ctx).unwrap();
        match &msg {
            ServerMsg::Order(e) => {
                assert_eq!(e.reconciled, Some(true), "mutabakat isareti dusmus");
                assert_eq!(e.profit, Some(12.34));
                assert_eq!(e.commission, Some(0.0), "SIFIR komisyon da olculmus bir degerdir");
                assert_eq!(e.swap, Some(-0.89));
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        let j = serde_json::to_string(&msg).unwrap();
        assert!(j.contains(r#""reconciled":true"#), "{j}");
        assert!(j.contains(r#""commission":0.0"#), "sifir komisyon telde olmali: {j}");
    }

    #[test]
    fn a_simulated_order_without_a_recorded_price_is_rejected_not_invented() {
        // Uydurma fiyattan dolum, simülasyonu sessizce yalancı yapardı.
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        // Sembol var ama fiyatı yok (kayıtta o an tick yok).
        let mut e = sinyal_proto::SymbolEntry {
            symbol_id: 1,
            digits: 5,
            tick_size: 0.00001,
            volume_min: 0.01,
            volume_max: 100.0,
            volume_step: 0.01,
            flags: sinyal_proto::sym_flag::READY,
            ..Default::default()
        };
        sinyal_proto::write_fixed_str(&mut e.name, "EURUSD");
        ctx.registry.set_symbols("mt5-1", vec![e]);

        match &ask(&ctx, market_order("o1", "buy", 0.10))[0] {
            ServerMsg::Order(ev) => {
                assert_eq!(ev.kind, "rejected");
                assert!(ev.sim, "ret de simule yolun cevabi: {ev:?}");
                assert!(ev.comment.contains("fiyat yok"), "sebep soylenmeli: {ev:?}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn duplicate_ids_are_refused_in_replay_too() {
        // Idempotency kod yolunun parçası; replay'de de aynı görünmeli.
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        assert!(matches!(&ask(&ctx, market_order("o1", "buy", 0.1))[0],
            ServerMsg::Order(e) if e.kind == "queued"));
        match &ask(&ctx, market_order("o1", "buy", 0.1))[0] {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "duplicate");
                assert!(e.sim);
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn trading_flags_do_not_gate_the_simulator_but_the_token_still_does() {
        // `--enable-trading` replay'de bir riski kapatmıyor (gerçek emir
        // zaten gitmiyor); token ise istemci yüzeyinin kuralı ve DEĞİŞMEMELİ.
        let (mut ctx, _rx, _done) = replay_ctx(10_000.0);
        ctx.trading = false;
        seed_symbol(&ctx, 1.10000, 1.10002);
        assert!(matches!(&ask(&ctx, market_order("o1", "buy", 0.1))[0],
            ServerMsg::Order(e) if e.kind == "queued"));

        // Token tanımlıysa emir yüzeyi yine auth ister.
        let (mut ctx, _rx, _done) = replay_ctx(10_000.0);
        ctx.token = Some("gizli".into());
        seed_symbol(&ctx, 1.10000, 1.10002);
        let out = ask(&ctx, market_order("o2", "buy", 0.1));
        assert!(matches!(out.first(), Some(ServerMsg::Error { .. })), "token kurali degismemeli");
    }

    #[test]
    fn pending_orders_are_recorded_and_can_be_cancelled() {
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        let pending = |id: &str, price: f64| {
            ClientMsg::Order(OrderReq {
                id: id.into(),
                action: "pending".into(),
                symbol: "EURUSD".into(),
                side: String::new(),
                order_type: "buy_limit".into(),
                volume: 0.05,
                price,
                stoplimit: 0.0,
                sl: 0.0,
                tp: 0.0,
                deviation: 0,
                time: String::new(),
                expiration: 0,
                expire_sn: 0,
                filling: String::new(),
                comment: String::new(),
            })
        };
        assert!(matches!(&ask(&ctx, pending("p1", 1.09))[0],
            ServerMsg::Order(e) if e.kind == "queued"));

        let ticket = match &ask(&ctx, ClientMsg::Orders)[0] {
            ServerMsg::Orders { items, total, .. } => {
                assert_eq!(*total, 1);
                assert_eq!(items[0].kind, "buy_limit");
                assert!((items[0].price - 1.09).abs() < 1e-9);
                items[0].ticket
            }
            other => panic!("beklenmeyen: {other:?}"),
        };
        // Bekleyen emir POZİSYON değildir: tetiklenme modellenmiyor.
        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => assert!(items.is_empty()),
            other => panic!("beklenmeyen: {other:?}"),
        }

        assert!(matches!(&ask(&ctx, ClientMsg::Cancel { id: "x1".into(), ticket })[0],
            ServerMsg::Order(e) if e.kind == "queued"));
        match &ask(&ctx, ClientMsg::Orders)[0] {
            ServerMsg::Orders { items, .. } => assert!(items.is_empty(), "iptal edilen emir kalmamali"),
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Olmayan bilet: sessizce başarı DEĞİL, açık ret.
        match &ask(&ctx, ClientMsg::Cancel { id: "x2".into(), ticket: 9999 })[0] {
            ServerMsg::Order(e) => assert_eq!(e.kind, "rejected"),
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn simulation_is_deterministic_across_identical_runs() {
        // "Aynı kayıt + aynı bayraklar → aynı çıktı" sözü biletleri de kapsar.
        let run = || {
            let (ctx, _rx, _done) = replay_ctx(10_000.0);
            seed_symbol(&ctx, 1.10000, 1.10002);
            ask(&ctx, market_order("o1", "buy", 0.1));
            ask(&ctx, market_order("o2", "sell", 0.2));
            match &ask(&ctx, ClientMsg::Positions)[0] {
                ServerMsg::Positions { items, .. } => {
                    items.iter().map(|p| (p.ticket, p.volume)).collect::<Vec<_>>()
                }
                other => panic!("beklenmeyen: {other:?}"),
            }
        };
        assert_eq!(run(), run());
        // Biletler 1'den başlar ve 0 "bilet yok" demek olarak korunur.
        assert_eq!(run()[0].0, 1);
    }

    #[test]
    fn replay_candles_say_there_is_no_mt5_history() {
        // "veri yok" ile "bu kipte MT5 gecmisi hic yok" ayni sey degil.
        let (ctx, _rx, _done) = replay_ctx(10_000.0);
        ctx.candles.lock().unwrap().on_tick("EURUSD", 1.1, 1.1, 1_700_000_000_000);
        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M1".into(), 10));
        match &out[0] {
            ServerMsg::Candles { src_kind, hist, hist_note, .. } => {
                assert_eq!(*src_kind, "tick");
                assert_eq!(*hist, "off");
                assert!(
                    hist_note.as_deref().unwrap_or_default().contains("replay"),
                    "sebep soylenmeli: {hist_note:?}"
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // PAPER kipi — canlı veri, simüle yürütme
    // -----------------------------------------------------------------------

    /// Yayından çıkan emir olaylarını tel biçiminde topla.
    fn drain_orders(ctx: &Ctx, rx: &mut broadcast::Receiver<FeedEvent>) -> Vec<OrderEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            match to_wire(&ev, ctx) {
                Some(ServerMsg::Order(e)) => out.push(e),
                _ => continue,
            }
        }
        out
    }

    fn pending_order(id: &str, kind: &str, price: f64, sl: f64, tp: f64) -> ClientMsg {
        ClientMsg::Order(OrderReq {
            id: id.into(),
            action: "pending".into(),
            symbol: "EURUSD".into(),
            side: String::new(),
            order_type: kind.into(),
            volume: 0.10,
            price,
            stoplimit: 0.0,
            sl,
            tp,
            deviation: 0,
            time: String::new(),
            expiration: 0,
            expire_sn: 0,
            filling: String::new(),
            comment: String::new(),
        })
    }

    #[test]
    fn hello_mode_is_correct_in_all_three_modes() {
        // Kipi yanlış ilan etmek, bu projede yapılabilecek en pahalı yalan:
        // istemci paper'ı canlı sanarsa risk yönetimini gerçek para
        // varmış gibi kurar, canlıyı paper sanarsa hiç kurmaz.
        let live = ctx_with(None);
        match hello_msg(&live, Level::Trader) {
            ServerMsg::Hello { mode, sim, replay_from_ms, .. } => {
                assert_eq!(mode, "live");
                assert!(sim.is_none(), "canlida simulasyon kunyesi OLMAMALI");
                assert!(replay_from_ms.is_none());
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        let (paper, _cmd) = paper_ctx(10_000.0);
        match hello_msg(&paper, Level::Trader) {
            ServerMsg::Hello { mode, trading, sim, replay_from_ms, replay_to_ms, .. } => {
                assert_eq!(mode, "paper", "paper kipi ILAN EDILMELI");
                assert!(trading, "simule kipte emir yolu acik");
                let note = sim.expect("paper'da simulasyon kunyesi gonderilmeli");
                assert!(!note.not_modeled.is_empty(), "MODELLENMEYENLER ilan edilmeli");
                assert!(
                    note.not_modeled.iter().any(|s| s.contains("komisyon")),
                    "komisyon modellenmiyor, soylenmeli: {:?}",
                    note.not_modeled
                );
                assert!(note.not_modeled.iter().any(|s| s.contains("swap")));
                assert!(note.not_modeled.iter().any(|s| s.contains("requote")));
                // Paper CANLIDIR: bir kaydın kapsamı yoktur.
                assert!(
                    replay_from_ms.is_none() && replay_to_ms.is_none(),
                    "paper bir kayit degil, kapsam ilan etmemeli"
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        let (rep, _rx, _done) = replay_ctx(10_000.0);
        match hello_msg(&rep, Level::Trader) {
            ServerMsg::Hello { mode, sim, replay_from_ms, .. } => {
                assert_eq!(mode, "replay");
                assert!(sim.is_some(), "replay'de de kunye gonderilmeli");
                assert!(replay_from_ms.is_some(), "replay kapsam ilan eder");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_paper_order_is_simulated_and_marked_sim() {
        let (ctx, _cmd) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        let mut events = ctx.events.subscribe();

        let out = ask(&ctx, market_order("p1", "buy", 0.10));
        match &out[0] {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "queued", "canlidaki ilk cevapla ayni olmali: {e:?}");
                assert!(e.sim, "paper dolumu simule oldugunu SOYLEMELI");
                assert_eq!(e.src, "mt5-1");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        let evs = drain_orders(&ctx, &mut events);
        assert_eq!(evs.len(), 2, "ack ve txn bekleniyordu: {evs:?}");
        assert!(evs.iter().all(|e| e.sim), "her paper olayi sim tasimali: {evs:?}");
        assert!(evs.iter().all(|e| e.id == "p1"), "istemci kimligi geri eslenmeli: {evs:?}");
        assert_eq!((evs[0].kind, evs[0].retcode), ("ack", Some(10008)));
        assert_eq!((evs[1].kind, evs[1].retcode), ("txn", Some(10009)));
        // Alış ASK'ten dolar (fixture kaymasız).
        assert_eq!(evs[1].price, Some(1.10002));

        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, total, .. } => {
                assert_eq!(*total, 1);
                assert_eq!(items[0].side, "buy");
                assert!((items[0].price_open - 1.10002).abs() < 1e-12);
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn a_simulated_fill_never_fabricates_the_market_it_hit() {
        // Kayma analizi tam da bu alanların üzerinde yapılacak. Simülatör
        // dolumu tek fiyattan modelliyor; oradan türetilmiş bir "dolum anı
        // bid/ask" üretmek, gerçek bir ÖLÇÜM gibi görünen uydurma bir sayı
        // olurdu ve analizi sessizce bozardı.
        let (ctx, _cmd) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        let mut events = ctx.events.subscribe();

        ask(&ctx, market_order("p1", "buy", 0.10));
        let evs = drain_orders(&ctx, &mut events);
        assert!(!evs.is_empty(), "olay bekleniyordu");
        for e in &evs {
            assert_eq!(e.bid, None, "simule olay bid uydurmamali: {e:?}");
            assert_eq!(e.ask, None, "simule olay ask uydurmamali: {e:?}");
            assert_eq!(e.order_state, None, "{e:?}");
            assert_eq!(e.txn_type, None, "{e:?}");
        }
        // Dolum fiyatı yine de var — eksik olan ÖLÇÜM, dolum değil.
        assert!(evs.iter().any(|e| e.price.is_some()), "{evs:?}");
    }

    #[test]
    fn the_paper_port_never_writes_a_single_command_to_shared_memory() {
        // GÜVENLİK SINIRI — bu testin varlık sebebi tek cümle: yanlışlıkla
        // gerçek emir gitmesi kabul edilemez.
        //
        // Fixture, gerçek daemon'dan KASTEN daha tehlikeli: orada paper
        // bağlamının `cmd_tx` haritası boştur, burada kanal bağlı ve
        // dinleniyor. Yani "komut gitmedi" iddiası, gidebileceği bir yer
        // varken sınanıyor.
        let (ctx, cmd_rx) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);

        ask(&ctx, market_order("o1", "buy", 0.10));
        let ticket = match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => items[0].ticket,
            other => panic!("beklenmeyen: {other:?}"),
        };
        ask(&ctx, ClientMsg::ModifySltp { id: "m1".into(), ticket, sl: 1.09, tp: 1.11 });
        ask(&ctx, ClientMsg::Close { id: "c1".into(), ticket, volume: 0.0 });
        ask(&ctx, ClientMsg::Cancel { id: "x1".into(), ticket: 4242 });
        ask(&ctx, pending_order("p1", "buy_limit", 1.09000, 0.0, 0.0));
        // Tick akışı da komut doğurmamalı: tetiklenen bir emir simüle kalır.
        seed_symbol(&ctx, 1.08990, 1.08992);
        pump_tick(
            ctx.sim().unwrap(),
            &ctx.events,
            "mt5-1",
            "EURUSD",
            1.08990,
            1.08992,
            1_700_000_001_000,
        );

        assert!(
            cmd_rx.try_recv().is_err(),
            "paper portundan paylasilan bellege HICBIR komut gitmemeli"
        );

        // Son savunma hattı: `dispatch` doğrudan çağrılsa bile göndermez.
        match dispatch(&ctx, "mt5-1", Cmd::default(), "zorla") {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "rejected");
                assert!(e.comment.contains("paper"), "hangi kip oldugu soylenmeli: {e:?}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        assert!(cmd_rx.try_recv().is_err(), "dispatch bile komut yazmamali");
    }

    #[test]
    fn a_real_order_never_carries_the_sim_field_while_a_paper_one_always_does() {
        // "Yok" ile "false" arasındaki fark kasıtlı: istemci alanın VARLIĞINA
        // bakarak da karar verebilmeli. Bu yüzden iki kip aynı testte
        // karşılaştırılıyor — biri değişip diğeri değişmezse yakalanır.
        let order_ev = || FeedEvent::Order {
            instance: Arc::from("mt5-1"),
            client_id: 0,
            kind: "txn",
            retcode: 10009,
            order: 5,
            deal: 6,
            position: 7,
            volume: 0.1,
            price: 1.2345,
            bid: 0.0,
            ask: 0.0,
            order_state: 0,
            txn_type: 0,
            source: sinyal_proto::res_source::LIVE,
            profit: 0.0,
            commission: 0.0,
            swap: 0.0,
            comment: String::new(),
        };

        let live = ctx_with(None);
        let j = serde_json::to_string(&to_wire(&order_ev(), &live).unwrap()).unwrap();
        assert!(!j.contains("sim"), "GERCEK emir olayinda sim alani HIC olmamali: {j}");
        // Canlı ret de temiz kalmalı.
        let j = serde_json::to_string(&rejected(&live, "o1", "test")).unwrap();
        assert!(!j.contains("sim"), "{j}");

        let (paper, _cmd) = paper_ctx(10_000.0);
        let j = serde_json::to_string(&to_wire(&order_ev(), &paper).unwrap()).unwrap();
        assert!(j.contains(r#""sim":true"#), "paper olayi simule oldugunu soylemeli: {j}");
    }

    #[test]
    fn a_pending_order_is_triggered_by_the_tick_stream() {
        // EN BÜYÜK EKSİK BUYDU: eski simülatörde bekleyen emir fiyat
        // seviyesinden geçse bile sonsuza kadar bekliyordu. Uçtan uca test:
        // istemci emri koyar, akıştan tick gelir, emir POZİSYONA döner.
        let (ctx, cmd_rx) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        let mut events = ctx.events.subscribe();

        // ASK 1.10002 iken 1.09500'e buy_limit: henüz tetiklenmemeli.
        let out = ask(&ctx, pending_order("p1", "buy_limit", 1.09500, 1.09000, 1.10000));
        assert!(matches!(&out[0], ServerMsg::Order(e) if e.kind == "queued"), "{out:?}");
        drain_orders(&ctx, &mut events);

        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => {
                assert!(items.is_empty(), "seviyeye gelmeden pozisyon acilmamali");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Fiyat seviyenin ALTINA iner: BUY_LIMIT ask <= price ile tetiklenir.
        let (bid, ask_p) = (1.09490, 1.09492);
        ctx.registry.update_last(
            "mt5-1",
            1,
            crate::state::LastTick { bid, ask: ask_p, last: 0.0, time_msc: 1_700_000_002_000 , recv_ms: 0},
        );
        pump_tick(
            ctx.sim().unwrap(),
            &ctx.events,
            "mt5-1",
            "EURUSD",
            bid,
            ask_p,
            1_700_000_002_000,
        );

        let evs = drain_orders(&ctx, &mut events);
        assert_eq!(evs.len(), 1, "tetiklenen emir TAM BIR dolum olayi uretmeli: {evs:?}");
        let fill = &evs[0];
        assert_eq!(fill.kind, "txn");
        assert_eq!(fill.retcode, Some(10009));
        assert!(fill.sim, "tetiklenen dolum da simule: {fill:?}");
        assert_eq!(fill.comment, "pending_triggered", "sebep soylenmeli: {fill:?}");
        assert_eq!(fill.id, "p1", "dolum, emri KOYAN istege baglanmali: {fill:?}");
        // Limit emri fiyatından KÖTÜ dolamaz: kayma limit'e uygulanmaz.
        assert_eq!(fill.price, Some(1.09500));

        // Emir defterinden düştü, pozisyon açıldı.
        match &ask(&ctx, ClientMsg::Orders)[0] {
            ServerMsg::Orders { items, .. } => {
                assert!(items.is_empty(), "tetiklenen emir bekleyenlerde kalmamali");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        let pos_ticket = match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, total, .. } => {
                assert_eq!(*total, 1, "tetiklenen emir POZISYONA donmeli");
                assert_eq!(items[0].side, "buy");
                assert!((items[0].sl - 1.09000).abs() < 1e-12, "SL tasinmali: {:?}", items[0]);
                items[0].ticket
            }
            other => panic!("beklenmeyen: {other:?}"),
        };

        // --- ve SL de akıştan tetiklenir ---
        let (bid, ask_p) = (1.08900, 1.08902);
        ctx.registry.update_last(
            "mt5-1",
            1,
            crate::state::LastTick { bid, ask: ask_p, last: 0.0, time_msc: 1_700_000_003_000 , recv_ms: 0},
        );
        pump_tick(
            ctx.sim().unwrap(),
            &ctx.events,
            "mt5-1",
            "EURUSD",
            bid,
            ask_p,
            1_700_000_003_000,
        );
        let evs = drain_orders(&ctx, &mut events);
        assert_eq!(evs.len(), 1, "SL tek kapanis olayi uretmeli: {evs:?}");
        assert_eq!(evs[0].comment, "sl", "kapanis sebebi SL olmali: {:?}", evs[0]);
        assert_eq!(evs[0].position, Some(pos_ticket));
        // LONG BID'den kapanır (fixture kaymasız).
        assert_eq!(evs[0].price, Some(1.08900));

        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => {
                assert!(items.is_empty(), "SL pozisyonu kapatmali");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        // Zarar sentetik bakiyeye yansımalı: (1.08900 − 1.09500) × 0.1 × 100000 = −60
        match &ask(&ctx, ClientMsg::Account)[0] {
            ServerMsg::Account { items } => {
                assert!(
                    (items[0].balance - 9_940.0).abs() < 1e-6,
                    "SL zarari bakiyeye islemeli: {}",
                    items[0].balance
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Tüm bu tetiklenmeler boyunca paylaşılan belleğe hiçbir şey gitmedi.
        assert!(cmd_rx.try_recv().is_err(), "tetiklenme de komut dogurmamali");
    }

    #[test]
    fn slippage_is_applied_against_the_client_on_the_paper_port() {
        // Kayma DAİMA aleyhte: alışta yukarı, satışta aşağı. Lehte kayan bir
        // simülasyon, stratejiyi canlıda karşılığı olmayan bir kârla besler.
        let (ctx, _cmd) = paper_ctx_cfg(crate::sim::SimConfig {
            balance: 10_000.0,
            leverage: 100,
            slippage_points: 2.0, // point = 0.00001 → 0.00002
        });
        seed_symbol(&ctx, 1.10000, 1.10002);
        let mut events = ctx.events.subscribe();

        ask(&ctx, market_order("b1", "buy", 0.10));
        let evs = drain_orders(&ctx, &mut events);
        assert_eq!(evs.last().unwrap().price, Some(1.10004), "alis ASK+kayma: {evs:?}");

        ask(&ctx, market_order("s1", "sell", 0.10));
        let evs = drain_orders(&ctx, &mut events);
        assert_eq!(evs.last().unwrap().price, Some(1.09998), "satis BID−kayma: {evs:?}");
    }

    #[test]
    fn broker_side_rejections_carry_the_real_mt5_retcode() {
        // Simülatörün kendine ait hata kodu YOKTUR: istemci aynı hatayı iki
        // kipte de aynı sayıyla görmeli, yoksa simülasyonda yazılan hata
        // işleme kodu canlıda çalışmaz.
        let (ctx, _cmd) = paper_ctx(10_000.0);
        // stops_level = 100 point (0.001): SL fiyata bu kadar yakın olamaz.
        seed_symbol_stops(&ctx, 1.10000, 1.10002, 100);

        let mut req = match market_order("o1", "buy", 0.10) {
            ClientMsg::Order(r) => r,
            _ => unreachable!(),
        };
        req.sl = 1.09999; // BID'e 1 point uzaklıkta — ihlal.
        match &ask(&ctx, ClientMsg::Order(req))[0] {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "rejected");
                assert_eq!(e.retcode, Some(10016), "MT5'in gercek kodu: {e:?}");
                assert!(e.sim, "ret de simule yolun cevabi: {e:?}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Marjin: 10 lot × 100000 × ~1.1 / 100 = ~11000 > 10000 bakiye.
        match &ask(&ctx, market_order("o2", "buy", 10.0))[0] {
            ServerMsg::Order(e) => {
                assert_eq!(e.kind, "rejected");
                assert_eq!(e.retcode, Some(10019), "yetersiz marjin 10019 olmali: {e:?}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn paper_snaps_volume_exactly_like_the_live_path_instead_of_rejecting_it() {
        // AYRIŞMA TESTİ. Canlı yol (`normalize_volume`) ızgaraya OTURTUR,
        // reddetmez; motorun kendi denetimi ise ızgara dışını 10014 ile
        // reddeder. Bağlama katmanı bilerek canlı yolu kullanıyor: aksi
        // halde canlıda 0.01'e yuvarlanıp DOLAN bir emir, testte
        // REDDEDİLİRDİ — "test ile canlı arasında fark olmamalı" kuralının
        // tam ihlali.
        let (ctx, _cmd) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002); // volume_step = 0.01

        let out = ask(&ctx, market_order("v1", "buy", 0.015));
        assert!(
            matches!(&out[0], ServerMsg::Order(e) if e.kind == "queued"),
            "izgara disi hacim canlidaki gibi OTURTULMALI, reddedilmemeli: {out:?}"
        );
        match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => {
                assert!(
                    (items[0].volume - 0.02).abs() < 1e-9,
                    "canli yolun yuvarlamasiyla ayni olmali: {:?}",
                    items[0].volume
                );
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn paper_and_live_keep_separate_order_books() {
        // İki portun emir defteri ORTAK olsaydı, paper istemcisinin aldığı
        // bir kimlik canlı istemcinin aynı kimlikli emrini "duplicate" diye
        // reddederdi — bir test koşusu gerçek işlemi engellerdi.
        let (paper, _cmd) = paper_ctx(10_000.0);
        let live = ctx_with(None);
        assert!(paper.orders.register("ayni-id").is_ok());
        assert!(
            live.orders.register("ayni-id").is_ok(),
            "paper defteri canli defteri kirletmemeli"
        );
    }

    #[test]
    fn invalid_timeframe_is_refused_and_lists_the_mt5_only_one() {
        let ctx = ctx_with(None);
        let out = rt().block_on(candles_op(&ctx, "EURUSD".into(), "M2".into(), 10));
        match &out[0] {
            ServerMsg::Error { msg } => {
                assert!(msg.contains("M2"));
                assert!(msg.contains("D1"), "D1 mt5'ten servis ediliyor, listede olmali: {msg}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // BROKER STOP'U GERÇEKTEN KURULDU MU
    //
    // `modify_sltp`in kabul edilmesi stop'un kurulduğu anlamına GELMEZ.
    // Broker `stops_level`/`freeze_level` ihlalinde, requote'ta ya da
    // "invalid stops" (10016) ile emri düşürebilir ve pozisyon SESSİZCE
    // korumasız kalır. Aşağıdaki testler doğrulamanın dört kenarını da
    // sabitliyor: kurulduysa sus, kurulmadıysa bağır, değerleri taşı, ve
    // ERKEN bağırma.
    // -----------------------------------------------------------------

    /// Durum yayınını (EA'nın saniyede bir tazelediği görüntü) kur.
    ///
    /// `sl` = pozisyonun broker tarafındaki GERÇEK stop'u. Testler bunu
    /// oynatarak "broker uyguladı" ile "broker düşürdü"yü ayırıyor.
    fn publish_position(ctx: &Ctx, ticket: u64, sl: f64) {
        let account = sinyal_proto::AccountSnapshot {
            balance: 10_000.0,
            equity: 10_000.0,
            trade_mode: sinyal_proto::trade_mode::DEMO,
            margin_mode: sinyal_proto::margin_mode::HEDGING,
            trade_allowed: 1,
            trade_expert: 1,
            terminal_trade_allowed: 1,
            mql_trade_allowed: 1,
            connected: 1,
            ..Default::default()
        };
        let mut p = sinyal_proto::PositionRec {
            ticket,
            identifier: ticket,
            volume: 0.10,
            price_open: 1.10002,
            price_current: 1.10002,
            sl,
            ..Default::default()
        };
        sinyal_proto::write_fixed_str(&mut p.symbol, "EURUSD");
        ctx.registry.set_state(
            "mt5-1",
            sinyal_proto::StateSnapshot {
                account,
                positions: vec![p],
                orders: Vec::new(),
                built_at_msc: 1_700_000_000_000,
                built_at_qpc: 0,
                truncated: false,
                unstable: false,
                pos_total: 1,
                ord_total: 0,
            },
        );
    }

    /// CANLI bağlam: komut kanalı bağlı, simülasyon YOK.
    fn live_ctx(ticket: u64, sl_now: f64) -> (Ctx, std::sync::mpsc::Receiver<Cmd>) {
        let mut ctx = ctx_with(None);
        let (c_tx, c_rx) = std::sync::mpsc::channel::<Cmd>();
        ctx.cmd_tx.insert("mt5-1".into(), c_tx);
        seed_symbol(&ctx, 1.10000, 1.10002);
        publish_position(&ctx, ticket, sl_now);
        (ctx, c_rx)
    }

    fn modify(id: &str, ticket: u64, sl: f64) -> ClientMsg {
        ClientMsg::ModifySltp { id: id.into(), ticket, sl, tp: 0.0 }
    }

    /// Doğrulama penceresinin SONRASI.
    fn after_window() -> std::time::Instant {
        std::time::Instant::now() + SLTP_VERIFY_WINDOW + std::time::Duration::from_millis(1)
    }

    #[test]
    fn a_stop_the_broker_really_set_raises_no_alarm() {
        // Doğrulanmış stop SESSİZ olmalı. Her komuta uyarı üreten bir sistem,
        // birkaç gün sonra hiçbir uyarısı okunmayan bir sistemdir.
        let (ctx, c_rx) = live_ctx(942649399, 0.0);
        let out = ask(&ctx, modify("m1", 942649399, 1.09500));
        assert!(out.iter().any(|m| matches!(m, ServerMsg::Order(e) if e.kind == "queued")));
        assert!(c_rx.try_recv().is_ok(), "komut paylasilan bellege gitmeliydi");

        // Broker uyguladı: durum yayını istenen stop'u gösteriyor.
        publish_position(&ctx, 942649399, 1.09500);

        assert!(
            sweep_sltp(&ctx, after_window()).is_empty(),
            "kurulmus stop icin uyari URETILMEMELI"
        );
        let c = ctx.sltp.counts();
        assert_eq!((c.sent, c.verified, c.unverified), (1, 1, 0));
        // Defter boşaldı: aynı komut ikinci turda tekrar sayılmamalı.
        assert!(sweep_sltp(&ctx, after_window()).is_empty());
        assert_eq!(ctx.sltp.counts(), c, "ikinci tarama sayaci degistirmemeli");
    }

    #[test]
    fn a_stop_the_broker_silently_dropped_is_reported() {
        // RAPORUN ASIL ŞİKÂYETİ. Komut kabul edildi, `sl` hâlâ 0 — pozisyon
        // KORUMASIZ. Bunun sessiz kalması, korumasız bir pozisyonu korunmuş
        // sanmak demek.
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        ask(&ctx, modify("m1", 942649399, 1.09500));

        // Durum yayını GELDİ ama `sl` hâlâ 0: broker emri düşürmüş.
        let evs = sweep_sltp(&ctx, after_window());
        assert_eq!(evs.len(), 1, "tam bir uyari beklenirdi: {evs:?}");
        let c = ctx.sltp.counts();
        assert_eq!((c.sent, c.verified, c.unverified), (1, 0, 1));
    }

    #[test]
    fn the_warning_carries_both_the_asked_and_the_actual_stop() {
        // "Doğrulanamadı" tek başına eyleme dönüşmez: istemci NEYİ istediğini
        // ve broker'da NE olduğunu görmeden kendi yedek stop'unu doğru yere
        // koyamaz.
        let (ctx, _c_rx) = live_ctx(942649399, 1.08000);
        ask(&ctx, modify("m7", 942649399, 1.09500));

        let evs = sweep_sltp(&ctx, after_window());
        match &evs[..] {
            [FeedEvent::SltpUnverified { id, ticket, want_sl, actual_sl, .. }] => {
                assert_eq!(id, "m7");
                assert_eq!(*ticket, 942649399);
                assert!((want_sl - 1.09500).abs() < 1e-12, "istenen: {want_sl}");
                // Eski stop hâlâ yerinde: broker YENİSİNİ uygulamamış.
                assert_eq!(*actual_sl, Some(1.08000));
            }
            other => panic!("beklenmeyen: {other:?}"),
        }

        // Tel üzerinde de görünmeli — istemcinin gördüğü şey bu.
        let msg = to_wire(&evs[0], &ctx).expect("uyari tele cevrilmeli");
        let j = serde_json::to_string(&msg).unwrap();
        assert!(j.contains(r#""kind":"sltp_unverified""#), "{j}");
        assert!(j.contains(r#""ticket":942649399"#), "{j}");
        assert!(j.contains(r#""istenen_sl":1.095"#), "{j}");
        assert!(j.contains(r#""gercek_sl":1.08"#), "{j}");
        // `retcode` UYDURULMAZ: bu bizim gözlemimiz, broker cevabı değil.
        assert!(!j.contains("retcode"), "{j}");
    }

    #[test]
    fn no_alarm_before_the_verification_window_closes() {
        // ERKEN ALARM YOK. Durum yayını saniyede bir geliyor; komuttan hemen
        // sonra bakıp "kurulmamış" demek, her doğru stop'u da yanlış
        // damgalardı — ve birkaç kez tekrarlanan yanlış uyarı, gerçek olanı
        // da okunmaz hâle getirirdi.
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        let armed_at = std::time::Instant::now();
        ask(&ctx, modify("m1", 942649399, 1.09500));

        // Pencere içindeki her tarama SESSİZ: stop henüz kurulmamış olsa bile.
        for ms in [0u64, 500, 1_500, 2_500] {
            let now = armed_at + std::time::Duration::from_millis(ms);
            assert!(
                sweep_sltp(&ctx, now).is_empty(),
                "pencere dolmadan ({ms} ms) uyari uretilmemeli"
            );
        }
        let c = ctx.sltp.counts();
        assert_eq!(
            (c.sent, c.verified, c.unverified),
            (1, 0, 0),
            "pencere icinde karar VERILMEMELI"
        );

        // Pencere içinde stop kurulursa uyarı hiç doğmaz.
        publish_position(&ctx, 942649399, 1.09500);
        assert!(sweep_sltp(&ctx, armed_at + std::time::Duration::from_millis(2_500)).is_empty());
        assert_eq!(ctx.sltp.counts().verified, 1);
    }

    #[test]
    fn a_position_that_simply_closed_raises_no_alarm() {
        // KAPANMIŞ pozisyonun stop'u da yoktur — bu normaldir ve stop'a
        // değerek kapanan HER pozisyonda tekrarlanır. Buna uyarı üretmek
        // `sltp_unverified`in tamamını gürültüye çevirir ve gerçek olanı da
        // okunmaz hâle getirirdi.
        //
        // Durum görüntüsü TAZE (`publish_position` az önce çalıştı), yani
        // "listede yok" gerçekten "kapandı" demek.
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        ask(&ctx, modify("m1", 111, 1.09500)); // artık var olmayan bilet

        assert!(
            sweep_sltp(&ctx, after_window()).is_empty(),
            "kapanmis pozisyon icin uyari URETILMEMELI"
        );
        let c = ctx.sltp.counts();
        assert_eq!(
            (c.sent, c.verified, c.unverified, c.closed),
            (1, 0, 0, 1),
            "kapanmis ayri sayilmali: ne dogrulandi ne dogrulanamadi"
        );
    }

    #[test]
    fn a_position_we_cannot_see_says_so_instead_of_claiming_the_stop_is_zero() {
        // Pozisyonu göremediğimiz İKİ sebep var ve ayrımı hayati: TAZE bir
        // görüntüde "yok" = kapandı (sessiz), BAYAT bir görüntüde "yok" =
        // hiçbir şey bilmiyoruz (uyarı). Zombi köprü tam olarak ikincisi gibi
        // görünür ve orada susmak, güvenlik olayının kendisini tekrarlardı.
        //
        // `gercek_sl` YOK: `0` göndermek "stop yok" diye okunurdu, oysa doğru
        // cevap "bakamadık".
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        ask(&ctx, modify("m1", 111, 1.09500));

        // Görüntü bayatladı: köprü artık durum yayınlamıyor.
        let much_later = std::time::Instant::now() + SLTP_STATE_STALE + SLTP_VERIFY_WINDOW;
        let evs = sweep_sltp(&ctx, much_later);
        match &evs[..] {
            [FeedEvent::SltpUnverified { actual_sl, state_age_ms, why, .. }] => {
                assert_eq!(*actual_sl, None, "bulunamayan pozisyon icin 0 UYDURULMAMALI");
                assert!(state_age_ms.is_some_and(|ms| ms >= 5_000), "{state_age_ms:?}");
                assert!(why.contains("BAYAT"), "{why}");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
        assert_eq!(ctx.sltp.counts().unverified, 1);
        let j = serde_json::to_string(&to_wire(&evs[0], &ctx).unwrap()).unwrap();
        assert!(!j.contains("gercek_sl"), "olcum yoksa alan HIC gonderilmemeli: {j}");
    }

    #[test]
    fn a_stop_that_never_landed_is_reported_even_after_the_position_closes() {
        // TUZAK: "kapanmışa alarm verme" kuralı, gerçek bir kaçırılmış
        // stop'u da gizleyebilir. Pozisyon pencere İÇİNDE hâlâ açıkken
        // `sl` istenen değere ulaşmadıysa, sonra kapanması bunu affetmez —
        // çünkü o pencerede pozisyon KORUMASIZDI.
        //
        // Ayrım şu: karar penceresi dolduğu ANDA verilir. Pozisyon o an hâlâ
        // açık ve stop'suz ise UYARI; o an artık yoksa kapanmıştır.
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        ask(&ctx, modify("m1", 942649399, 1.09500));

        // Pencere dolduğunda pozisyon HÂLÂ açık ve `sl` hâlâ 0.
        let evs = sweep_sltp(&ctx, after_window());
        assert_eq!(evs.len(), 1, "acik ve korumasiz pozisyon bildirilmeli: {evs:?}");
        assert_eq!(ctx.sltp.counts().closed, 0, "bu kapanis DEGIL");
    }

    #[test]
    fn a_refused_command_arms_no_verification() {
        // Gönderilmemiş bir komutun doğrulanmaması uyarı değil, beklenen
        // durumdur. Aksi hâlde her ret bir "korumasız pozisyon" alarmına
        // dönüşür ve uyarı listesi okunmaz hâle gelirdi.
        let ctx = ctx_with(None); // komut kanalı YOK → "bagli instance yok"
        let out = ask(&ctx, modify("m1", 5, 1.09));
        assert!(out.iter().any(|m| matches!(m, ServerMsg::Order(e) if e.kind == "rejected")));
        assert_eq!(ctx.sltp.counts().sent, 0, "reddedilen komut icin dogrulama KURULMAMALI");
        assert!(sweep_sltp(&ctx, after_window()).is_empty());
    }

    #[test]
    fn the_broker_rounding_the_price_to_its_grid_is_not_a_failure() {
        // Broker fiyatı `tick_size` ızgarasına yuvarlar. Birebir eşitlik
        // aramak neredeyse her doğrulamayı "kurulmadi" yapardı — ve yanlış
        // uyarı, uyarının kendisini öldürür.
        let (ctx, _c_rx) = live_ctx(942649399, 0.0);
        ask(&ctx, modify("m1", 942649399, 1.095004));
        // tick_size = 0.00001; broker 1.09500'e yuvarladı.
        publish_position(&ctx, 942649399, 1.09500);
        assert!(sweep_sltp(&ctx, after_window()).is_empty(), "izgara yuvarlamasi hata degil");
        assert_eq!(ctx.sltp.counts().verified, 1);

        // Ama GERÇEK bir sapma (bir ızgara adımından büyük) yakalanmalı.
        let (ctx2, _c2) = live_ctx(942649399, 0.0);
        ask(&ctx2, modify("m2", 942649399, 1.09500));
        publish_position(&ctx2, 942649399, 1.09480);
        assert_eq!(sweep_sltp(&ctx2, after_window()).len(), 1, "20 point sapma yakalanmali");
    }

    #[test]
    fn the_stop_warning_rides_the_order_channel_not_the_price_feed() {
        // Emir gönderen istemci zaten `order` kanalında. Ayrı bir kanal,
        // aboneliği unutan bir istemcinin uyarıyı HİÇ görmemesi demek olurdu.
        let ev = FeedEvent::SltpUnverified {
            instance: Arc::from("mt5-1"),
            id: "m1".into(),
            ticket: 7,
            want_sl: 1.0,
            actual_sl: Some(0.0),
            state_age_ms: Some(12),
            why: "test",
        };
        let mut s = Subs::default();
        s.add("tick.*");
        assert!(!s.wants(&ev), "fiyat aboneligi stop uyarisi getirmemeli");
        s.add("order");
        assert!(s.wants(&ev));
    }

    #[test]
    fn a_simulated_stop_is_verified_on_the_same_path_as_a_live_one() {
        // Paper/replay'de de doğrulama ÇALIŞIR. İki kipin doğrulama yolu
        // ayrışsaydı, burada düzeltilen bir hata canlıda kalırdı — ve stop
        // doğrulamasını test edecek tek güvenli yer zaten simülasyon.
        let (ctx, _c_rx) = paper_ctx(10_000.0);
        seed_symbol(&ctx, 1.10000, 1.10002);
        ask(&ctx, market_order("o1", "buy", 0.10));
        let ticket = match &ask(&ctx, ClientMsg::Positions)[0] {
            ServerMsg::Positions { items, .. } => items[0].ticket,
            other => panic!("beklenmeyen: {other:?}"),
        };

        ask(&ctx, modify("m1", ticket, 1.09500));
        assert_eq!(ctx.sltp.counts().sent, 1);
        assert!(sweep_sltp(&ctx, after_window()).is_empty(), "motor stop'u kurdu, uyari olmamali");
        assert_eq!(ctx.sltp.counts().verified, 1);

        // Motorun REDDETTİĞİ bir stop (ters taraf) doğrulanamaz olarak
        // bildirilmeli — kabul edilmiş sanıp korumasız kalmak kabul edilemez.
        let out = ask(&ctx, modify("m2", ticket, 1.20000));
        assert!(
            out.iter().any(|m| matches!(m, ServerMsg::Order(e) if e.kind == "rejected")),
            "alis pozisyonunda fiyatin USTUNE sl reddedilmeli: {out:?}"
        );
        assert_eq!(ctx.sltp.counts().sent, 1, "reddedilen komut sayaca girmemeli");
    }
}
