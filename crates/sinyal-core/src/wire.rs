//! WebSocket tel protokolü.
//!
//! JSON seçildi çünkü bu ucun tüketicisi herhangi bir dilde yazılabilir ve
//! ilk hedef "bağlan, çalıştığını gör". Tick mesajlarında alan adları kısa
//! tutuldu (`b`/`a`/`ms`) — saniyede binlerce mesajda bu fark ediliyor.
//!
//! # Emir güvenliği
//!
//! Bu uç **emir yürütebilir**. Bu yüzden:
//! - Token yapılandırılmışsa `auth` gelmeden hiçbir işlem kabul edilmez.
//! - Sunucu varsayılan olarak `127.0.0.1`'e bağlanır.
//! - Emir yürütme `--enable-trading` olmadan kapalıdır (EA'daki bayrağa ek
//!   ikinci bir kapı; ikisi de açık olmalı).

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// İstemci → sunucu
// ---------------------------------------------------------------------------

/// İstemciden gelen istek.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Kimlik doğrula. Sunucuda token tanımlıysa ilk mesaj bu olmalıdır.
    Auth { token: String },

    /// Kanallara abone ol.
    ///
    /// Kanal biçimleri:
    /// - `tick.EURUSD` — tek sembol
    /// - `tick.*` — tüm semboller
    /// - `book.XAUUSD` / `book.*`
    /// - `order` — emir sonuçları
    Subscribe { channels: Vec<String> },

    Unsubscribe { channels: Vec<String> },

    /// Sembol tablosunu iste.
    Symbols,

    /// Mum (OHLC) geçmişi iste.
    ///
    /// İki kaynak vardır ve cevap **hangisinden geldiğini söyler**
    /// (`src_kind`):
    ///
    /// - `mt5` — broker'ın kendi serisi (forex/CFD'de genellikle BID).
    ///   Geriye dönük geçmiş vardır. MQL5 Service çalışıyorsa bu istek onu
    ///   tetikler.
    /// - `tick` — tick akışından burada üretilen seri (MID). Geriye dönük
    ///   geçmiş YOKTUR; depo daemon başladığı andan itibaren dolar ve ilk bar
    ///   `partial: true` ile işaretlenir.
    ///
    /// İki seri **birleştirilmez** — birleşim noktası sahte bir fiyat boşluğu
    /// üretirdi.
    Candles {
        symbol: String,
        /// `M1` | `M5` | `M15` | `M30` | `H1` | `H4` | `D1`
        ///
        /// `D1` yalnızca `mt5` kaynağından gelir: günlük barın sınırı broker
        /// sunucusunun gün başlangıcıdır, bizim UTC sınırımızla tutmaz.
        #[serde(default = "default_tf")]
        tf: String,
        #[serde(default = "default_candle_count")]
        count: usize,
    },

    /// Hesap durumunu iste (bakiye, equity, marjin, izinler, hesap tipi).
    Account,

    /// Açık pozisyonları iste.
    Positions,

    /// Bekleyen emirleri iste.
    Orders,

    /// Sembol başına son bilinen fiyatı iste (abone olmadan anlık görüntü).
    Snapshot {
        #[serde(default)]
        symbols: Vec<String>,
    },

    /// Emir gönder.
    Order(OrderReq),

    /// Bekleyen emri iptal et.
    Cancel { id: String, ticket: u64 },

    /// Pozisyonu kapat (tam veya kısmi).
    Close {
        id: String,
        ticket: u64,
        #[serde(default)]
        volume: f64,
    },

    /// Pozisyonun SL/TP'sini değiştir.
    ModifySltp {
        id: String,
        ticket: u64,
        #[serde(default)]
        sl: f64,
        #[serde(default)]
        tp: f64,
    },

    Ping,
}

/// Emir isteği.
///
/// `action` ve `type` ayrı: `deal` piyasa emri, `pending` bekleyen emir.
/// Bekleyen emirde `type` zorunlu (`buy_limit`, `sell_stop`, ...).
#[derive(Debug, Clone, Deserialize)]
pub struct OrderReq {
    /// İstemcinin verdiği kimlik. **Idempotency anahtarı** — aynı kimlikle
    /// gelen ikinci istek yeniden yürütülmez.
    pub id: String,

    /// `deal` | `pending`
    #[serde(default = "default_action")]
    pub action: String,

    pub symbol: String,

    /// Piyasa emri için `buy` | `sell`.
    #[serde(default)]
    pub side: String,

    /// Bekleyen emir için: `buy_limit` | `sell_limit` | `buy_stop` |
    /// `sell_stop` | `buy_stop_limit` | `sell_stop_limit`.
    #[serde(rename = "type", default)]
    pub order_type: String,

    pub volume: f64,

    /// Piyasa emrinde 0 → güncel bid/ask kullanılır.
    #[serde(default)]
    pub price: f64,

    /// Yalnızca `*_stop_limit` emirlerinde.
    #[serde(default)]
    pub stoplimit: f64,

    #[serde(default)]
    pub sl: f64,
    #[serde(default)]
    pub tp: f64,

    /// İzin verilen azami kayma (point). 0 → sunucu varsayılanı.
    #[serde(default)]
    pub deviation: u32,

    /// `gtc` | `day` | `specified` | `specified_day`
    #[serde(default)]
    pub time: String,

    /// `time` = `specified*` ise epoch saniye — **BROKER saatinde**.
    ///
    /// # Bu alan bir tuzaktır; `expire_sn` kullanın
    ///
    /// MT5 bu değeri **sunucu saati** olarak yorumlar. Gerçek UTC epoch
    /// göndermek ÖLÇÜLDÜ (2026-08-13, sunucu UTC+3):
    ///
    /// - `UTC + 120 sn` → `retcode 10022` ile **reddedilir**
    /// - `UTC + 1 gün` → **kabul edilir ama 3 saat ERKEN dolar**, sessizce
    ///
    /// İkinci hâli gürültü çıkarmadığı için tehlikelidir. Bu yüzden
    /// [`OrderReq::expire_sn`] eklendi: göreli saniye verirsiniz, dönüşümü
    /// köprü yapar, saat dilimi hiç gündeme gelmez.
    ///
    /// `time` verilmeden gönderilirse **hata döner** — eskiden sessizce yok
    /// sayılırdı ve emir sonsuza kadar bekleyen emir olarak kalırdı.
    #[serde(default)]
    pub expiration: i64,

    /// Emrin **kaç saniye sonra** düşeceği. `0` = kullanılmıyor.
    ///
    /// [`OrderReq::expiration`]'ın saat dilimi tuzağını ortadan kaldırır:
    /// köprü broker saatini son tick'ten okur ve mutlak damgayı kendisi
    /// hesaplar. `time` verilmemişse `specified` varsayılır.
    ///
    /// `expiration` ile **birlikte gönderilemez** — hangisinin kazandığı
    /// belirsiz kalırdı.
    #[serde(default)]
    pub expire_sn: i64,

    /// `auto` (varsayılan) | `fok` | `ioc` | `return` | `boc`
    ///
    /// `auto` bırakılması önerilir: doğru mod sembolün `trade_exemode` ve
    /// doldurma maskesinden hesaplanır. Elle seçim yanlışsa broker 10030 ile
    /// reddeder.
    #[serde(default)]
    pub filling: String,

    #[serde(default)]
    pub comment: String,
}

fn default_tf() -> String {
    "M1".into()
}

fn default_candle_count() -> usize {
    300
}

fn default_action() -> String {
    "deal".into()
}

// ---------------------------------------------------------------------------
// Sunucu → istemci
// ---------------------------------------------------------------------------

/// Sunucudan giden mesaj.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Bağlantı kurulduğunda ilk mesaj.
    Hello {
        proto: u32,
        /// `live` | `paper` | `replay`.
        ///
        /// - `live` — gerçek MT5, gerçek emirler.
        /// - `paper` — **CANLI veri, simüle yürütme**. Aynı okuyucu, aynı
        ///   mum deposu; ama paylaşılan belleğe hiçbir komut gitmez.
        /// - `replay` — diskteki kayıttan oynatım, simüle yürütme.
        ///
        /// **Canlı ile simüle kipler arasındaki mesaj farkı budur** (bir de
        /// `sim` bayrağı ve [`SimNote`]). Mesaj biçimleri, alan adları ve
        /// kanal adları birebir aynı kalır ki sinyal sisteminin kod yolu
        /// değişmesin. Amaç sistemi kandırmak DEĞİL: simülasyonu canlı sanıp
        /// gerçek emir göndermek kabul edilemez bir kaza olurdu, bu yüzden
        /// fark burada AÇIKÇA ilan edilir.
        mode: &'static str,
        instances: Vec<String>,
        /// Emir yürütme açık mı. Simüle kiplerde daima `true` — emirler
        /// reddedilmez, **simüle** edilir (bkz. `OrderEvent::sim`).
        trading: bool,
        /// Piyasa verisi (tick/derinlik/mum/sembol) token'sız erişilebilir mi.
        public_feed: bool,
        /// Hesap ve emir işlemleri için token gerekiyor mu.
        auth_required_for_trading: bool,
        /// Bu bağlantının şu anki seviyesi: `public` | `trader`.
        level: &'static str,
        /// Replay kapsamının başlangıcı (epoch ms). **Canlıda gönderilmez.**
        #[serde(skip_serializing_if = "Option::is_none")]
        replay_from_ms: Option<i64>,
        /// Replay kapsamının sonu (epoch ms). **Canlıda gönderilmez.**
        ///
        /// Bu, istenen kapsamdır; kaydın gerçekten nereye kadar veri
        /// içerdiği ayrı bir sorudur ve oynatım bitince `replay_done` ile
        /// bildirilir.
        #[serde(skip_serializing_if = "Option::is_none")]
        replay_to_ms: Option<i64>,
        /// Simülasyonun künyesi. **Canlıda HİÇ gönderilmez.**
        ///
        /// `Box`lu: `ServerMsg` her tick için üretiliyor ve künye yalnızca
        /// bağlantı başına BİR kez gönderiliyor. Satır içi tutmak, sıcak
        /// yoldaki her mesajı yüzlerce bayt büyütürdü.
        #[serde(skip_serializing_if = "Option::is_none")]
        sim: Option<Box<SimNote>>,
    },

    Authed { level: &'static str },

    /// Tek fiyat güncellemesi. Alan adları kısa — yüksek frekanslı.
    Tick {
        /// sembol
        s: String,
        /// bid
        b: f64,
        /// ask
        a: f64,
        /// last (CFD/forex'te 0 olabilir)
        #[serde(skip_serializing_if = "is_zero")]
        l: f64,
        /// broker sunucu saati, epoch ms
        ms: i64,
        /// EA'nın yakalamasından bu mesajın üretilmesine kadar geçen süre (µs)
        lat_us: u64,
        /// kaynak örnek (broker)
        src: String,
    },

    /// Derinlik anlık görüntüsü (artımlı değil, tam tablo).
    Book {
        s: String,
        ms: i64,
        /// [fiyat, hacim] — en iyiden kötüye
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
        src: String,
    },

    Symbols { items: Vec<SymbolInfo> },

    Snapshot { items: Vec<TickSnap> },

    /// İstenen mum geçmişi.
    Candles {
        s: String,
        tf: String,
        /// Bu barlar **nereden** geldi: `mt5` (broker'ın kendi serisi, genelde
        /// BID) veya `tick` (bizim ürettiğimiz, MID).
        ///
        /// Söylemek zorunlu: iki seri aynı fiyat serisi değildir ve istemci
        /// hangisine baktığını bilmeden gösterge hesaplayamaz. Bir cevap
        /// **asla** iki kaynağı karıştırmaz.
        src_kind: &'static str,
        items: Vec<crate::candles::Bar>,
        /// `mt5` görüntüsünün yaşı (ms). Bu seri bir **anlık görüntüdür**,
        /// kendiliğinden güncellenmez. `tick` kaynağında yoktur (canlıdır).
        #[serde(skip_serializing_if = "Option::is_none")]
        age_ms: Option<i64>,
        /// MT5'ten geçmiş çekme denemesinin sonucu:
        ///
        /// - `off` — geçmiş kanalı yok (Service çalışmıyor veya kapalı derlendi)
        /// - `cached` — depodaki görüntü yeterince taze, çekilmedi
        /// - `ok` — çekildi, tam geldi
        /// - `incomplete` — çekildi ama **eksik**; seri delikli olabilir
        /// - `failed` — Service hata bildirdi (kod `hist_note` içinde)
        /// - `timeout` — süre doldu; Service yanıt vermedi
        /// - `refused` — istek hiç gönderilemedi
        hist: &'static str,
        /// `hist` için insan okunur ayrıntı (hata kodu, kaç bar eksik).
        #[serde(skip_serializing_if = "Option::is_none")]
        hist_note: Option<String>,
    },

    /// Bir mum kapandı (canlı).
    ///
    /// **Daima `tick` kaynaklıdır** — tick akışından üretilir. `mt5` kaynaklı
    /// bir dizinin sonuna eklenmemelidir; iki seri farklı fiyat tabanındadır.
    Candle {
        s: String,
        tf: &'static str,
        src_kind: &'static str,
        bar: crate::candles::Bar,
    },

    Account { items: Vec<AccountInfo> },

    Positions {
        items: Vec<PositionInfo>,
        /// Terminaldeki gerçek sayı; `items.len()`'ten büyükse liste kesildi.
        total: u32,
        truncated: bool,
    },

    Orders {
        items: Vec<OrderInfo>,
        total: u32,
        truncated: bool,
    },

    /// Emir yaşam döngüsü olayı.
    Order(OrderEvent),

    /// Kaydın oynatımı bitti (**yalnızca replay kipinde**).
    ///
    /// Canlı akış hiç bitmez; bu mesajın gelmesi tek başına "artık tick
    /// gelmeyecek" demektir. Sessizce durmak, istemcinin hareketsiz bir
    /// piyasa ile biten bir kaydı ayırt edememesi demek olurdu.
    ReplayDone {
        /// Oynatılan tick sayısı.
        ticks: u64,
        /// Oynatılan son tick'in broker saati (epoch ms). Kayıt bu kapsamda
        /// boşsa gönderilmez.
        #[serde(skip_serializing_if = "Option::is_none")]
        last_ms: Option<i64>,
        /// Oynatılması PLANLANAN gün sayısı (aralıktaki kayıtlı gün sayısı).
        days: u32,
        /// Gerçekten oynatılan gün sayısı. `days` ile eşit değilse oynatım
        /// yarıda kesilmiştir.
        days_played: u32,
        /// **Aralık yarıda kesildi**: bir gün okunamadı ve sonrası
        /// oynatılmadı.
        ///
        /// Bu bayrak olmadan kesinti tel üzerinde GÖRÜNMEZDİ: istemci normal
        /// bir `replay_done` görür ve 3 aylık sandığı bir backtest'i tek
        /// günün sonucuyla raporlardı. Sessiz kesinti, yanlış sonucu doğru
        /// sanmak demektir; bu yüzden AÇIKÇA ilan ediliyor.
        ///
        /// Normal (kesintisiz) bitişte alan hiç gönderilmez.
        #[serde(skip_serializing_if = "is_false")]
        truncated: bool,
    },

    /// İstemci yavaş kaldı ve mesaj atlandı.
    ///
    /// Gizlemiyoruz: fiyat akışında sessiz boşluk, yanlış karar demektir.
    Lagged { dropped: u64 },

    Error { msg: String },

    Pong,
}

fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

/// Simülasyonun **dürüst künyesi** — `hello.sim`, yalnızca simüle kiplerde.
///
/// Neden tel üzerinde: "neyin modellendiği" bir yayın notu değil, sinyal
/// sisteminin sonucu yorumlarken ihtiyaç duyduğu VERİDİR. Kaymayı modelleyen
/// bir koşunun kâr eğrisi ile modellemeyeninki farklı şeylerdir; ikisini aynı
/// grafikte karşılaştıran bir istemci, aradaki farkı stratejinin başarısı
/// sanardı. Aynı sebeple `not_modeled` da gider: eksik olanı bilmeyen bir
/// tüketici, simüle bakiyeyi gerçek getiri sanar.
///
/// Listeler `&'static str` çünkü metinler kodda sabittir; kip başına
/// hesaplanan bir açıklama, iki kipin sessizce ayrışmasına kapı açardı.
#[derive(Debug, Clone, Serialize)]
pub struct SimNote {
    /// Sentetik başlangıç bakiyesi.
    pub balance: f64,
    /// Marjin hesabında kullanılan kaldıraç. **Canlı hesabın kaldıracı
    /// DEĞİLDİR** — simülasyon onu okumaz, bayraktan/varsayılandan alır.
    pub leverage: i64,
    /// Her dolumda ALEYHTE uygulanan kayma (point).
    pub slippage_points: f64,
    /// Modellenen etkiler.
    pub modeled: &'static [&'static str],
    /// **Modellenmeyen** etkiler — tahmin edilmez, gizlenmez.
    pub not_modeled: &'static [&'static str],
    /// Tek cümlelik uyarı.
    pub warning: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolInfo {
    /// Broker'daki gerçek ad.
    pub s: String,
    pub digits: u32,
    pub point: f64,
    pub tick_size: f64,
    pub volume_min: f64,
    pub volume_max: f64,
    pub volume_step: f64,
    /// Sözleşme büyüklüğü (`SYMBOL_TRADE_CONTRACT_SIZE`) — GOLD'da 100,
    /// forex'te tipik olarak 100000.
    ///
    /// **Pozisyon büyüklüğü ve kâr/zarar bunsuz hesaplanamaz:**
    /// `kar = fiyat_farki × hacim × contract_size` ve
    /// `marjin = hacim × contract_size × fiyat / kaldirac`.
    ///
    /// `0` ise broker'dan okunamamış demektir; hacim hesabı yapan bir istemci
    /// bunu "1" varsaymamalı — sessizce 100 kat yanlış pozisyon açardı.
    pub contract_size: f64,
    /// `SYMBOL_TRADE_EXEMODE` — doğru doldurma modunun belirleyicisi.
    pub exec_mode: u32,
    pub filling_mask: u32,
    pub stops_level: u32,
    /// Broker bu sembolde derinlik veriyor mu (0 = hayır).
    pub book_depth: u32,
    /// `true` ise bu sembol yalnızca timer taramasıyla toplanıyor: ~10-16 ms
    /// ek gecikme, ve iki tarama arasındaki ara tickler GÖRÜLMEZ.
    pub polled_only: bool,
    /// `true` ise bu sembol EA'nın bağlı olduğu grafiğin sembolüdür ve
    /// `OnTick` ile **olay güdümlü** akar — terminalin verdiği her tick olayı
    /// alınır.
    ///
    /// **En yüksek sadakat bu semboldedir.** Sinyal üretimi hangi sembolde
    /// yapılacaksa EA o sembolün grafiğine bağlanmalıdır; aksi halde strateji
    /// gördüğü fiyat serisinde ara tickleri kaçırır.
    pub chart: bool,
    /// Sembol canlı veri üretti mi. `false` ise fiyatına güvenme.
    pub ready: bool,
    pub src: String,
}

/// Hesap durumu.
#[derive(Debug, Clone, Serialize)]
pub struct AccountInfo {
    pub src: String,
    pub login: i64,
    pub server: String,
    pub company: String,
    pub currency: String,
    pub leverage: i64,
    pub balance: f64,
    pub credit: f64,
    pub profit: f64,
    pub equity: f64,
    pub margin: f64,
    pub margin_free: f64,
    pub margin_level: f64,
    /// `demo` | `contest` | `real` | `unknown`
    ///
    /// **`unknown` gerçek hesap gibi ele alınır.** Okunamayan bir hesabı demo
    /// saymak, canlı hesapta kazara emir göndermek demektir.
    pub mode: &'static str,
    /// `netting` | `exchange` | `hedging` | `unknown`
    ///
    /// `hedging` ise pozisyon kapatma komutu **ticket zorunlu** ister.
    pub margin_mode: &'static str,
    /// `margin_so_call`/`margin_so_so` birimi: `percent` | `money`.
    pub so_mode: &'static str,
    pub margin_so_call: f64,
    pub margin_so_so: f64,
    /// Emir gönderilebilir mi (tüm MT5 izinleri açık mı).
    pub can_trade: bool,
    /// Kapalıysa **hangi** iznin düştüğü. Tek bir "izin yok" hatası hata
    /// ayıklanamaz.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
    /// Bu görüntünün yaşı (ms).
    pub age_ms: u64,
}

/// Açık pozisyon.
#[derive(Debug, Clone, Serialize)]
pub struct PositionInfo {
    pub src: String,
    /// Kapatma komutunda **bu** kullanılır.
    pub ticket: u64,
    /// Geçmiş/deal eşleştirme anahtarı — kapatmada kullanılmaz.
    pub identifier: u64,
    /// Bizim gönderdiğimiz emirlerde istemci kimliğini taşır (0 = bize ait değil).
    pub client_id: u64,
    pub symbol: String,
    /// `buy` | `sell`
    pub side: &'static str,
    pub volume: f64,
    pub price_open: f64,
    pub price_current: f64,
    pub sl: f64,
    pub tp: f64,
    pub profit: f64,
    pub swap: f64,
    pub time_msc: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub comment: String,
}

/// Bekleyen emir.
#[derive(Debug, Clone, Serialize)]
pub struct OrderInfo {
    pub src: String,
    pub ticket: u64,
    pub client_id: u64,
    pub symbol: String,
    /// `buy_limit` | `sell_stop` | ...
    pub kind: &'static str,
    pub volume_initial: f64,
    /// **Kalan** hacim. Dolan = initial − current.
    pub volume_current: f64,
    pub price: f64,
    #[serde(skip_serializing_if = "is_zero")]
    pub stoplimit: f64,
    pub sl: f64,
    pub tp: f64,
    pub time_setup_msc: i64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub expiration: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub comment: String,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

#[derive(Debug, Clone, Serialize)]
pub struct TickSnap {
    pub s: String,
    pub b: f64,
    pub a: f64,
    /// last (CFD/forex'te 0 olabilir) — canlı `tick` ile aynı alan adı, ki
    /// istemci anlık görüntü ile akışı aynı şekilde işleyebilsin.
    #[serde(skip_serializing_if = "is_zero")]
    pub l: f64,
    pub ms: i64,
    /// Bu fiyatın yaşı (ms). Büyükse sembol hareketsiz veya akış durmuş.
    pub age_ms: i64,
    pub src: String,
}

/// `false` ise alanı tel üzerinde HİÇ gösterme (bkz. [`OrderEvent::sim`]).
fn is_false(v: &bool) -> bool {
    !*v
}

/// Emir olayı.
///
/// **`kind` = `ack` emrin DOLDUĞU anlamına GELMEZ.** `OrderSendAsync` iki
/// aşamalı geri bildirim üretir: `ack` isteğin sunucuya iletildiğini,
/// `txn` gerçek yürütmeyi bildirir. Emri yalnızca `txn` + `retcode` 10009
/// geldiğinde dolmuş sayın.
#[derive(Debug, Clone, Default, Serialize)]
pub struct OrderEvent {
    /// İstemcinin verdiği kimlik.
    pub id: String,
    /// `queued` | `ack` | `txn` | `expired` | `rejected` | `duplicate` |
    /// `sltp_unverified`
    ///
    /// `expired` bekleyen emrin SÜRESİNİN dolduğunu bildirir; `retcode`
    /// 10009 gelse bile **dolum değildir** (bkz. `source::order_kind`).
    ///
    /// `sltp_unverified` bir emir sonucu DEĞİL, bir KORUMA UYARISIDIR:
    /// `modify_sltp` kabul edildi ama broker'ın stop'u gerçekten kurduğu
    /// doğrulanamadı (bkz. [`OrderEvent::istenen_sl`]).
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retcode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deal: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    /// Olay ANINDAKİ piyasa fiyatı — giriş maliyetini AYIRMAK için.
    ///
    /// `price - ask` (alışta) kaymayı, `ask - bid` spread'i verir. Bu iki
    /// bileşen ayrılmadan "maliyet kazancın yarısını yiyor" gözlemi
    /// eyleme dönüştürülemez: motorun önerdiği fiyatın ulaşılamaz olması
    /// ile spread'in genişlemesi aynı sayıya karışır.
    ///
    /// **Türetilmiş bir `spread` alanı BİLEREK yok.** İstemci `ask - bid`i
    /// kendisi hesaplar; ikinci bir kaynak eklemek, biri güncellenmeyince
    /// sessizce tutarsızlaşan iki gerçek demek olurdu.
    ///
    /// Ölçüm yoksa alan HİÇ gönderilmez; `0` göndermek "spread sıfırdı"
    /// gibi okunurdu.
    ///
    /// Asıl kaynak `txn` olayıdır (EA'nın dolum ANINDAKİ tick önbelleği).
    /// `ack`te MT5'in `MqlTradeResult`'ı taşınır: `OrderSendAsync` normalde
    /// bunu boş döner, ama **requote'ta (10004) dolu gelir** — orada da
    /// gerçek bir ölçümdür, uydurma değil. `queued`/`rejected`/`duplicate`
    /// köprünün kendi ürettiği olaylardır ve alan asla bulunmaz.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ask: Option<f64>,
    /// MT5 `ENUM_ORDER_STATE` — ham sayı.
    ///
    /// `3` = `PARTIAL`: emir KISMEN doldu, kalan hacim hâlâ piyasada.
    /// `kind` bunu ayırt etmez (ikisi de `txn`), bu yüzden ham durum
    /// gerekiyor. `0` (`STARTED`) "ölçüm yok" demektir ve gönderilmez.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_state: Option<u8>,
    /// MT5 `ENUM_TRADE_TRANSACTION_TYPE` — ham sayı.
    ///
    /// `2` = `ORDER_DELETE`, `6` = `DEAL_ADD`. `0` (`ORDER_ADD`) "ölçüm
    /// yok" ile aynı sayı olduğu için gönderilmez — bu bilinçli bir kayıp:
    /// emrin sadece listeye eklendiği olay zaten tek başına eyleme
    /// dönüşmez, ama sıfır dolu bir alanı "veri var" sanmak pahalıdır.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_type: Option<u8>,

    /// Olay **mutabakattan** geldi mi (`true`) yoksa canlı akıştan mı.
    ///
    /// Terminalin işlem kuyruğu 1024 elemanlı ve taşarsa eski olaylar
    /// sessizce ezilir. EA periyodik olarak geçmişi tarayıp kaçırdığı
    /// olayları telafi eder; bu bayrak "geç geldi" demektir, "yeni oldu"
    /// değil. Yalnız `true` iken gönderilir.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciled: Option<bool>,
    /// **Gerçekleşmiş** kâr (`DEAL_PROFIT`), hesap para biriminde.
    ///
    /// `positions[].profit` YÜZEN kârdır; bu ise kapanmış deal'in kesin
    /// sonucudur. Yalnızca `reconciled: true` olaylarda bulunur — canlı
    /// olayda okunamaz, çünkü sıcak yolda `HistoryDealGetDouble`
    /// çağrılamaz (kuyruk taşması riski).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profit: Option<f64>,
    /// Komisyon (`DEAL_COMMISSION`).
    ///
    /// ⚠️ Genellikle **GİRİŞ** deal'inde görünür, çıkışta `0` olabilir.
    /// Yalnız çıkışa bakıp "komisyon yok" demek yaygın bir hatadır; bir
    /// pozisyonun toplam komisyonu için giriş ve çıkış deal'leri toplanır.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commission: Option<f64>,
    /// Gecelik taşıma (`DEAL_SWAP`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap: Option<f64>,

    /// `sltp_unverified` — stop'u doğrulanamayan pozisyonun bileti.
    ///
    /// Diğer `kind`lerde bulunmaz: emir yaşam döngüsü olayları pozisyonu
    /// `position` alanıyla söyler, bu uyarı ise komuttaki BİLETİ söyler ve
    /// ikisi hedging hesabında aynı sayı olmayabilir.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticket: Option<u64>,
    /// `sltp_unverified` — `modify_sltp` komutunda İSTENEN stop.
    ///
    /// # Neden bu uyarı var
    ///
    /// `modify_sltp`in kabul edilmesi, stop'un KURULDUĞU anlamına gelmez.
    /// Broker `stops_level`/`freeze_level` ihlalinde, requote'ta ya da
    /// "invalid stops" (10016) ile emri düşürebilir; pozisyon o an sessizce
    /// KORUMASIZ kalır. Bu yüzden komuttan sonra durum yayınındaki `sl`
    /// izlenir ve istenen değere ulaşmazsa bu olay üretilir.
    ///
    /// **Bu bir HATA DEĞİL, bir UYARIDIR.** Emir kabul edilmiş olabilir;
    /// söylenen tek şey, kurulduğunun DOĞRULANAMADIĞIdır. İstemci kendi
    /// yedek stop'unu devrede tutmalıdır (bkz. API.md, "stop broker
    /// tarafında, köprü yedek").
    ///
    /// Yalnızca **SL** doğrulanır, TP doğrulanmaz: doğrulanmayan bir TP kâr
    /// kaçırır, doğrulanmayan bir SL hesabı boşaltır. Bu bilinçli bir kapsam
    /// sınırı.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub istenen_sl: Option<f64>,
    /// `sltp_unverified` — durum yayınındaki GERÇEK stop.
    ///
    /// Pozisyon durum yayınında hiç bulunamadıysa alan GÖNDERİLMEZ: `0`
    /// göndermek "stop yok" diye okunurdu, oysa doğru cevap "bakamadık".
    /// Sebep `comment` alanında yazar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gercek_sl: Option<f64>,
    /// `sltp_unverified` — bakılan durum görüntüsünün yaşı (ms).
    ///
    /// Büyükse suç broker'da olmayabilir: **zombi bir köprü durumu
    /// dondurur** ve stop gerçekte kurulmuş olsa bile burada eski değer
    /// görünür. "Broker reddetti" ile "köprü ölmüş" arasındaki farkı
    /// ayırmanın tek yolu bu sayı.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_age_ms: Option<u64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub comment: String,
    pub src: String,
    /// **Bu dolum simüle edildi; gerçek bir emir yürütülmedi.**
    ///
    /// Yalnızca replay kipinde ve daima `true` olarak gönderilir. Canlı
    /// olaylarda alan HİÇ bulunmaz — "yok" ile "false" arasındaki farkı
    /// korumak kasıtlı: istemci `sim` alanının varlığına bakarak da karar
    /// verebilmeli, çünkü canlı bir olayı yanlışlıkla simüle sanmak da,
    /// simüle bir olayı gerçek sanmak da pahalıdır.
    ///
    /// Simüle dolum GERÇEK DOLUM DEĞİLDİR: kayma, komisyon ve swap
    /// modellenmez (bkz. `crate::server` simülasyon bölümü).
    #[serde(default, skip_serializing_if = "is_false")]
    pub sim: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_market_order() {
        let j = r#"{"op":"order","id":"a1","symbol":"EURUSD","side":"buy","volume":0.1}"#;
        let m: ClientMsg = serde_json::from_str(j).unwrap();
        match m {
            ClientMsg::Order(o) => {
                assert_eq!(o.id, "a1");
                assert_eq!(o.action, "deal", "action varsayılanı deal olmalı");
                assert_eq!(o.symbol, "EURUSD");
                assert_eq!(o.side, "buy");
                assert!((o.volume - 0.1).abs() < 1e-12);
                assert_eq!(o.filling, "", "boş = auto");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn parses_pending_limit_order() {
        // Eski adaptörde limit emir HİÇ yoktu; bu yolun çalışması hedefin
        // merkezinde.
        let j = r#"{"op":"order","id":"a2","action":"pending","symbol":"XAUUSD",
                    "type":"buy_limit","volume":0.05,"price":2300.5,"sl":2295.0}"#;
        let m: ClientMsg = serde_json::from_str(j).unwrap();
        match m {
            ClientMsg::Order(o) => {
                assert_eq!(o.action, "pending");
                assert_eq!(o.order_type, "buy_limit");
                assert!((o.price - 2300.5).abs() < 1e-12);
                assert!((o.sl - 2295.0).abs() < 1e-12);
                assert!((o.tp - 0.0).abs() < 1e-12, "verilmeyen alan 0 olmalı");
            }
            other => panic!("beklenmeyen: {other:?}"),
        }
    }

    #[test]
    fn parses_all_control_ops() {
        for (j, label) in [
            (r#"{"op":"auth","token":"x"}"#, "auth"),
            (r#"{"op":"subscribe","channels":["tick.*"]}"#, "subscribe"),
            (r#"{"op":"unsubscribe","channels":["tick.EURUSD"]}"#, "unsubscribe"),
            (r#"{"op":"symbols"}"#, "symbols"),
            (r#"{"op":"snapshot"}"#, "snapshot"),
            (r#"{"op":"cancel","id":"c1","ticket":42}"#, "cancel"),
            (r#"{"op":"close","id":"c2","ticket":42,"volume":0.05}"#, "close"),
            (r#"{"op":"modify_sltp","id":"c3","ticket":42,"sl":1.0}"#, "modify_sltp"),
            (r#"{"op":"ping"}"#, "ping"),
        ] {
            serde_json::from_str::<ClientMsg>(j)
                .unwrap_or_else(|e| panic!("{label} ayrıştırılamadı: {e}"));
        }
    }

    #[test]
    fn rejects_unknown_op_instead_of_ignoring() {
        // Bilinmeyen bir op'u sessizce yok saymak, istemcinin emrinin
        // gittiğini sanmasına yol açardı.
        assert!(serde_json::from_str::<ClientMsg>(r#"{"op":"yolla_gitsin"}"#).is_err());
    }

    #[test]
    fn tick_serialises_compactly_and_omits_zero_last() {
        let m = ServerMsg::Tick {
            s: "EURUSD".into(),
            b: 1.0850,
            a: 1.0852,
            l: 0.0,
            ms: 1_700_000_000_123,
            lat_us: 12,
            src: "mt5-1".into(),
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains(r#""t":"tick""#));
        assert!(j.contains(r#""b":1.085"#));
        assert!(!j.contains(r#""l":"#), "sıfır last atlanmalı: {j}");
    }

    #[test]
    fn order_event_omits_absent_fields() {
        let e = OrderEvent {
            id: "a1".into(),
            kind: "ack",
            retcode: Some(10008),
            order: None,
            deal: None,
            position: None,
            volume: None,
            price: None,
            comment: String::new(),
            src: "mt5-1".into(),
            sim: false,
            ..Default::default()
        };
        let j = serde_json::to_string(&ServerMsg::Order(e)).unwrap();
        assert!(j.contains(r#""kind":"ack""#));
        assert!(j.contains(r#""retcode":10008"#));
        // DİKKAT: `"order"` metni `"t":"order"` etiketinde de geçiyor;
        // alan adını ararken iki nokta ile birlikte aramak gerek.
        assert!(j.contains(r#""t":"order""#), "etiket bulunmalı: {j}");
        assert!(!j.contains(r#""order":"#), "boş alanlar gönderilmemeli: {j}");
        assert!(!j.contains(r#""deal":"#));
        assert!(!j.contains(r#""comment":"#));
    }

    #[test]
    fn live_order_events_never_carry_the_sim_field() {
        // Canlı bir olayda `sim` alanının hiç bulunmaması, istemcinin alanın
        // VARLIĞINA bakarak da karar verebilmesi demek. `"sim":false`
        // göndermek, alanı hiç göndermemekten farklı bir sözleşme olurdu.
        let e = OrderEvent {
            id: "a1".into(),
            kind: "txn",
            retcode: Some(10009),
            price: Some(1.2345),
            volume: Some(0.1),
            src: "mt5-1".into(),
            sim: false,
            ..Default::default()
        };
        let j = serde_json::to_string(&ServerMsg::Order(e)).unwrap();
        assert!(!j.contains("sim"), "canli olayda sim alani HIC olmamali: {j}");
    }

    #[test]
    fn simulated_order_events_say_so_out_loud() {
        let e = OrderEvent {
            id: "a1".into(),
            kind: "txn",
            retcode: Some(10009),
            src: "mt5-1".into(),
            sim: true,
            ..Default::default()
        };
        let j = serde_json::to_string(&ServerMsg::Order(e)).unwrap();
        assert!(j.contains(r#""sim":true"#), "simule dolum gizlenemez: {j}");
    }

    #[test]
    fn unmeasured_bid_ask_is_absent_not_zero() {
        // `"bid":0` "spread sıfırdı" diye okunur ve kayma hesabını sessizce
        // çöpe çevirir. Ölçüm yoksa alan HİÇ bulunmamalı — istemci o zaman
        // tick akışına düşmesi gerektiğini bilir.
        let e = OrderEvent {
            id: "a1".into(),
            kind: "ack",
            retcode: Some(10008),
            src: "mt5-1".into(),
            ..Default::default()
        };
        let j = serde_json::to_string(&ServerMsg::Order(e)).unwrap();
        assert!(!j.contains(r#""bid":"#), "olcum yokken bid gorunmemeli: {j}");
        assert!(!j.contains(r#""ask":"#), "olcum yokken ask gorunmemeli: {j}");
        assert!(!j.contains("order_state"), "sifir order_state gizlenmeli: {j}");
        assert!(!j.contains("txn_type"), "sifir txn_type gizlenmeli: {j}");
    }

    #[test]
    fn a_measured_fill_carries_the_market_it_hit() {
        let e = OrderEvent {
            id: "a1".into(),
            kind: "txn",
            retcode: Some(10009),
            price: Some(4367.95),
            bid: Some(4367.60),
            ask: Some(4367.90),
            order_state: Some(4),
            txn_type: Some(6),
            src: "mt5-1".into(),
            ..Default::default()
        };
        let j = serde_json::to_string(&ServerMsg::Order(e)).unwrap();
        assert!(j.contains(r#""bid":4367.6"#), "{j}");
        assert!(j.contains(r#""ask":4367.9"#), "{j}");
        assert!(j.contains(r#""order_state":4"#), "{j}");
        assert!(j.contains(r#""txn_type":6"#), "{j}");
        // TÜRETİLMİŞ ALAN YOK: istemci ask-bid'i kendisi hesaplar. İkinci bir
        // kaynak, biri güncellenmeyince sessizce tutarsızlaşırdı.
        assert!(!j.contains("spread"), "turetilmis spread alani olmamali: {j}");
    }

    #[test]
    fn hello_carries_replay_scope_only_in_replay_mode() {
        // Canlı hello, replay alanlarını HİÇ taşımamalı: bir istemcinin
        // "replay_from_ms yok, demek ki canlı" çıkarımı geçerli olmalı.
        let live = ServerMsg::Hello {
            proto: 3,
            mode: "live",
            instances: vec!["mt5-1".into()],
            trading: false,
            public_feed: true,
            auth_required_for_trading: true,
            level: "public",
            replay_from_ms: None,
            replay_to_ms: None,
            sim: None,
        };
        let j = serde_json::to_string(&live).unwrap();
        assert!(j.contains(r#""mode":"live""#));
        assert!(!j.contains("replay_from_ms"), "canlida kapsam alani olmamali: {j}");
        assert!(!j.contains("replay_to_ms"));
        assert!(!j.contains(r#""sim""#), "canlida simulasyon kunyesi olmamali: {j}");

        let replay = ServerMsg::Hello {
            proto: 3,
            mode: "replay",
            instances: vec!["mt5-1".into()],
            trading: true,
            public_feed: true,
            auth_required_for_trading: false,
            level: "trader",
            replay_from_ms: Some(1_700_000_000_000),
            replay_to_ms: Some(1_700_086_400_000),
            sim: None,
        };
        let j = serde_json::to_string(&replay).unwrap();
        assert!(j.contains(r#""mode":"replay""#), "replay ilan edilmeli: {j}");
        assert!(j.contains(r#""replay_from_ms":1700000000000"#));
        assert!(j.contains(r#""replay_to_ms":1700086400000"#));
    }

    #[test]
    fn the_sim_note_publishes_what_is_not_modeled_not_only_what_is() {
        // DÜRÜSTLÜK TESTİ. Yalnızca "neyi modelliyorum" demek, eksik olanı
        // sessizce gizlemek olurdu: simüle bakiyeyi gerçek getiri sanan bir
        // tüketici tam olarak buradan yanılır.
        let hello = ServerMsg::Hello {
            proto: 3,
            mode: "paper",
            instances: vec!["mt5-1".into()],
            trading: true,
            public_feed: true,
            auth_required_for_trading: false,
            level: "trader",
            replay_from_ms: None,
            replay_to_ms: None,
            sim: Some(Box::new(SimNote {
                balance: 10_000.0,
                leverage: 100,
                slippage_points: 1.0,
                modeled: &["kayma"],
                not_modeled: &["komisyon", "swap", "requote"],
                warning: "Simule dolum GERCEK DOLUM DEGILDIR.",
            })),
        };
        let j = serde_json::to_string(&hello).unwrap();
        assert!(j.contains(r#""mode":"paper""#), "{j}");
        assert!(j.contains(r#""slippage_points":1.0"#), "kayma ilan edilmeli: {j}");
        for eksik in ["komisyon", "swap", "requote"] {
            assert!(j.contains(eksik), "MODELLENMEYEN '{eksik}' ilan edilmeli: {j}");
        }
        assert!(j.contains("GERCEK DOLUM DEGILDIR"), "{j}");
        // Kapsam alanları paper'da YOK: paper canlıdır, bir kaydın parçası değil.
        assert!(!j.contains("replay_from_ms"), "paper'da kapsam alani olmamali: {j}");
    }

    #[test]
    fn replay_done_is_a_message_of_its_own() {
        // Sessizce durmak, hareketsiz piyasa ile biten kaydı ayırt edilemez
        // yapardı.
        let j = serde_json::to_string(&ServerMsg::ReplayDone {
            ticks: 1234,
            last_ms: Some(1_700_000_000_123),
            days: 66,
            days_played: 66,
            truncated: false,
        })
        .unwrap();
        assert!(j.contains(r#""t":"replay_done""#), "{j}");
        assert!(j.contains(r#""ticks":1234"#));
        assert!(j.contains(r#""last_ms":1700000000123"#));
        // Gün sayıları HER ZAMAN gider: istemci kaç günlük bir kapsam
        // oynatıldığını saymak zorunda kalmamalı.
        assert!(j.contains(r#""days":66"#), "{j}");
        assert!(j.contains(r#""days_played":66"#), "{j}");
        // Kesintisiz bitişte bayrak HİÇ görünmez; alanın varlığı kötü haberdir.
        assert!(!j.contains("truncated"), "saglam bitiste bayrak olmamali: {j}");

        let j = serde_json::to_string(&ServerMsg::ReplayDone {
            ticks: 0,
            last_ms: None,
            days: 0,
            days_played: 0,
            truncated: false,
        })
        .unwrap();
        assert!(!j.contains("last_ms"), "bos kapsamda son zaman uydurulmaz: {j}");
    }

    #[test]
    fn a_truncated_replay_says_so_on_the_wire() {
        // Bir gün okunamayıp oynatım yarıda kesildiğinde istemci NORMAL bir
        // `replay_done` görüyordu ve 3 aylık sandığı bir backtest'i tek günün
        // sonucuyla raporlayabilirdi. Sessiz kesinti = sessizce yanlış sonuç.
        let j = serde_json::to_string(&ServerMsg::ReplayDone {
            ticks: 163,
            last_ms: Some(1_786_363_257_689),
            days: 3,
            days_played: 1,
            truncated: true,
        })
        .unwrap();
        assert!(j.contains(r#""truncated":true"#), "kesinti ILAN EDILMELI: {j}");
        assert!(j.contains(r#""days":3"#), "{j}");
        assert!(j.contains(r#""days_played":1"#), "{j}");
    }

    #[test]
    fn candles_answer_always_says_which_source_it_came_from() {
        // İstemci hangi seriye baktığını bilmeden gösterge hesaplayamaz:
        // mt5 = BID tabanlı broker serisi, tick = bizim MID serimiz.
        let m = ServerMsg::Candles {
            s: "EURUSD".into(),
            tf: "M5".into(),
            src_kind: "mt5",
            items: vec![crate::candles::Bar {
                t: 1_700_000_000_000,
                o: 1.10,
                h: 1.11,
                l: 1.09,
                c: 1.105,
                ticks: 250,
                spread: Some(12),
                ..Default::default()
            }],
            age_ms: Some(1200),
            hist: "ok",
            hist_note: None,
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains(r#""src_kind":"mt5""#), "kaynak bildirilmeli: {j}");
        assert!(j.contains(r#""age_ms":1200"#), "goruntunun yasi bildirilmeli");
        assert!(j.contains(r#""hist":"ok""#));
        assert!(j.contains(r#""spread":12"#));
        // Broker gerçek hacim vermiyor: 0 GÖNDERİLMEZ, alan hiç olmaz.
        assert!(!j.contains(r#""real_volume""#), "veri yoksa hacim alani olmamali: {j}");
        assert!(!j.contains(r#""hist_note""#));
    }

    #[test]
    fn tick_sourced_answer_reports_no_age_because_it_is_live() {
        let m = ServerMsg::Candles {
            s: "EURUSD".into(),
            tf: "M1".into(),
            src_kind: "tick",
            items: vec![],
            age_ms: None,
            hist: "off",
            hist_note: Some("service yok".into()),
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains(r#""src_kind":"tick""#));
        assert!(!j.contains(r#""age_ms""#), "tick serisi canli, yas alani olmamali");
        assert!(j.contains(r#""hist":"off""#));
        assert!(j.contains(r#""hist_note":"service yok""#));
    }

    #[test]
    fn live_candle_event_is_marked_tick_sourced() {
        // Bu barı bir mt5 dizisinin sonuna eklemek, birleşim noktasında sahte
        // bir fiyat boşluğu üretirdi.
        let m = ServerMsg::Candle {
            s: "EURUSD".into(),
            tf: "M1",
            src_kind: "tick",
            bar: crate::candles::Bar { t: 1, o: 1.0, h: 1.0, l: 1.0, c: 1.0, ticks: 3, ..Default::default() },
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains(r#""t":"candle""#));
        assert!(j.contains(r#""src_kind":"tick""#), "canli bar kaynagi bildirilmeli: {j}");
    }

    #[test]
    fn real_volume_zero_is_sent_only_when_the_broker_actually_publishes_it() {
        // 0 "hacim sifir" degil "veri yok" demek — ayrimi tel uzerinde de
        // korumak zorundayiz.
        let with = crate::candles::Bar { real_volume: Some(0), ..Default::default() };
        let without = crate::candles::Bar { real_volume: None, ..Default::default() };
        assert!(serde_json::to_string(&with).unwrap().contains(r#""real_volume":0"#));
        assert!(!serde_json::to_string(&without).unwrap().contains("real_volume"));
    }

    #[test]
    fn lagged_is_reported_not_hidden() {
        // Fiyat akışında sessiz boşluk = yanlış karar.
        let j = serde_json::to_string(&ServerMsg::Lagged { dropped: 17 }).unwrap();
        assert!(j.contains(r#""t":"lagged""#));
        assert!(j.contains(r#""dropped":17"#));
    }
}
