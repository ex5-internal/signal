//! Saf simülasyon motoru — **I/O YOK, WebSocket YOK, kilit YOK**.
//!
//! Bu modül bir daemon parçası değil, bir hesap makinesidir: içeri istek ve
//! fiyat girer, dışarı olay ve durum çıkar. Ne dosya okur, ne saat sorar, ne
//! ağa dokunur. Bunun tek sebebi test edilebilirlik değil — **belirlenimcilik**:
//! aynı tick dizisi + aynı istekler her koşuda AYNI biletleri ve AYNI dolum
//! fiyatlarını üretmek zorunda. Duvar saatine veya tohumsuz rastgeleliğe
//! dokunan tek satır bunu bozar, bu yüzden burada ikisi de yok.
//!
//! # Neden yeniden yazıldı
//!
//! Önceki simülatör her emri gördüğü fiyattan mükemmel dolduruyordu: kayma
//! yok, `stops_level` yok, marjin yok, bekleyen emir kendiliğinden
//! tetiklenmiyordu. Böyle bir motorda strateji kârlı görünür, canlıda spread
//! ve kaymaya yenilir. Test ile canlı arasındaki farkı kapatmanın yolu iyimser
//! bir simülatör değil, **kötümser ve açıkça sınırlı** bir simülatördür.
//!
//! # Modellenen
//!
//! - Tetikleme semantiği MT5 ile birebir (bkz. [`SimOrderKind::triggers`]).
//! - Spread: alış ASK'ten, satış BID'den; pozisyon LONG ise BID'den, SHORT ise
//!   ASK'ten kapanır.
//! - Kayma: piyasa ve STOP dolumlarında **daima aleyhte**, sabit ve
//!   belirlenimci ([`SimConfig::slippage_points`], varsayılan 1 point).
//!   Dolum fiyatı kotasyon ızgarasına da ALEYHTE oturur — en yakına
//!   yuvarlamak kaymanın kendisini silebilirdi (bkz. [`round_px_adverse`]).
//! - `stops_level`: SL/TP ve bekleyen emir fiyatının güncel fiyata asgari
//!   uzaklığı; ihlal 10016 ile reddedilir.
//! - Marjin: `hacim × contract_size × fiyat / kaldıraç`; serbest marjin
//!   yetmezse 10019.
//! - Hacim ızgarası (`volume_step`/`min`/`max`); ihlal 10014.
//! - SL/TP tetiklenmesi; **aynı tick ikisini de vurursa SL kazanır**.
//!
//! # Eksik veriyle SİMÜLE EDİLMEZ
//!
//! `contract_size` veya `point` yoksa emir 10013 ile reddedilir
//! ([`check_contract`]). İlki olmadan marjin ve kâr, ikincisi olmadan kayma
//! ve `stops_level` hesaplanamaz; ikisinde de motor "modelliyorum" dediği
//! şeyi modellemeden İYİMSER dolum üretirdi. Eksik veriyle devam etmek,
//! bu modülün var oluş sebebini ortadan kaldırırdı.
//!
//! # Modellenmeyen — bunlar tahmin EDİLMEZ
//!
//! Komisyon, swap, kur çevrimi (kâr sembolün kotasyon biriminde kalır),
//! kısmi dolum/likidite, `deviation` penceresi ve requote, emir son kullanma
//! (`expiration` saklanır ama işletilmez), teminat tamamlama (stop-out),
//! hafta sonu boşlukları, freeze level. Modellenmeyen bir maliyeti tahmin
//! etmektense hiç eklememek, sonucun ne olduğu konusunda dürüst kalmayı
//! sağlar — ama **simüle dolum GERÇEK DOLUM DEĞİLDİR**.
//!
//! # Hesap modeli
//!
//! Hedging: aynı sembolde birden çok pozisyon yan yana durur, netleşme yok.
//! Her pozisyon marjinini açılışta kilitler ve kapanışta (kısmi kapanışta
//! oranında) serbest bırakır.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Retcode'lar — MT5'in GERÇEK kodları
// ---------------------------------------------------------------------------
//
// Simülatörün kendine ait hata kodu YOKTUR: istemci aynı hatayı iki kipte de
// aynı sayıyla görmeli, yoksa replay'de yazılan hata işleme kodu canlıda
// çalışmaz.

/// MT5 `ENUM_TRADE_RETCODE` — burada kullanılan altküme.
pub mod retcode {
    /// `TRADE_RETCODE_INVALID` — istek geçersiz (ör. bilinmeyen bilet).
    pub const INVALID: u32 = 10013;
    /// `TRADE_RETCODE_INVALID_VOLUME`.
    pub const INVALID_VOLUME: u32 = 10014;
    /// `TRADE_RETCODE_INVALID_PRICE`.
    pub const INVALID_PRICE: u32 = 10015;
    /// `TRADE_RETCODE_INVALID_STOPS` — `stops_level` ihlali.
    pub const INVALID_STOPS: u32 = 10016;
    /// `TRADE_RETCODE_NO_MONEY` — serbest marjin yetmiyor.
    pub const NO_MONEY: u32 = 10019;
    /// `TRADE_RETCODE_PRICE_OFF` — işlemek için kotasyon yok.
    pub const PRICE_OFF: u32 = 10021;
    /// `TRADE_RETCODE_PLACED` — istek sunucuya iletildi. **Dolum DEĞİL.**
    pub const PLACED: u32 = 10008;
    /// `TRADE_RETCODE_DONE` — istek tamamlandı.
    pub const DONE: u32 = 10009;
}

/// Kayan nokta ölçüm hatası payı (point cinsinden).
///
/// Mesafe iki çıkarma ve bir bölmeden türüyor: `(1.10010-1.10000)/0.00001`
/// matematiksel olarak 10, kayan noktada 9.999999999998899. Tolerans olmadan
/// tam sınırdaki GEÇERLİ bir emir reddedilirdi.
const POINT_TOLERANCE: f64 = 1e-6;

/// Hacim karşılaştırmalarında kullanılan pay.
const VOLUME_EPS: f64 = 1e-9;

// ---------------------------------------------------------------------------
// Yapılandırma ve sembol
// ---------------------------------------------------------------------------

/// `--replay-balance` verilmediğinde sentetik başlangıç bakiyesi.
pub const DEFAULT_BALANCE: f64 = 10_000.0;
/// Varsayılan kaldıraç.
pub const DEFAULT_LEVERAGE: i64 = 100;
/// **Varsayılan kayma — SIFIR DEĞİL.**
///
/// Sıfır varsayılan bir gerileme olurdu: motoru "kaymayı modelliyorum" diye
/// ilan edip pratikte iyimser dolum üretirdi. 1 point, hiçbir brokerda
/// kayma olmadığını iddia etmeyen en küçük dürüst değerdir.
pub const DEFAULT_SLIPPAGE_POINTS: f64 = 1.0;

/// Simülasyon ayarları.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimConfig {
    /// Sentetik başlangıç bakiyesi.
    pub balance: f64,
    /// Hesap kaldıracı. **0 veya negatif → marjin denetimi kapalı** (motor
    /// bunu sessizce yapmaz, `account().margin` 0 kalır ve öyle görünür).
    pub leverage: i64,
    /// Aleyhte kayma (point). Bkz. [`DEFAULT_SLIPPAGE_POINTS`].
    pub slippage_points: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            balance: DEFAULT_BALANCE,
            leverage: DEFAULT_LEVERAGE,
            slippage_points: DEFAULT_SLIPPAGE_POINTS,
        }
    }
}

/// Simülasyonun sembol hakkında bilmesi gereken her şey.
///
/// **Bilerek `Default` uygulanmıyor.** Varsayılan bir sembol, doldurmayı
/// unutan çağıranın emrini uydurulmuş broker özellikleriyle doldururdu;
/// bu modülün tamamı tam olarak bunun karşıtı için var. Değerler `SymbolEntry`
/// üzerinden gelir (bkz. [`SimSymbol::from_entry`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimSymbol {
    pub digits: u32,
    pub point: f64,
    /// `SYMBOL_TRADE_TICK_SIZE` — kotasyonun en küçük adımı.
    ///
    /// `point` ile AYNI DEĞİLDİR: bazı endeks ve CFD'lerde tick_size point'in
    /// katıdır (ör. point 0.01, tick_size 0.05). Canlı yol fiyatları bu
    /// ızgaraya oturtuyor (`normalize_price`); simülatör oturtmazsa broker'ın
    /// hiç veremeyeceği bir fiyattan dolum raporlar ve iki kip ayrışır.
    /// 0 = broker vermemiş, yalnızca `digits`e yuvarlanır.
    pub tick_size: f64,
    pub volume_step: f64,
    pub volume_min: f64,
    pub volume_max: f64,
    /// `SYMBOL_TRADE_STOPS_LEVEL` (point). 0 = broker kısıt koymuyor.
    pub stops_level: u32,
    /// `SYMBOL_TRADE_CONTRACT_SIZE`.
    ///
    /// **0 ise emir REDDEDİLİR** (bkz. [`check_contract`]). Bu alan hem
    /// marjinin hem kârın çarpanıdır; yokken 1 varsaymak forex'te 100 000 kat
    /// küçük bir kâr ve pratikte sonsuz kaldıraç üretirdi — yani marjin
    /// denetimi (10019) sessizce ölür, kâr eğrisi anlamsızlaşır ve strateji
    /// simülasyonda kârlı görünürdü. Sembol adına bakıp 100000 varsaymak da
    /// aynı yalanın başka bir biçimidir. Eksik veriyle simüle etmektense
    /// gürültüyle durmak doğrudur.
    pub contract_size: f64,
}

impl SimSymbol {
    /// Paylaşımlı bellekteki sembol kaydından üret.
    pub fn from_entry(e: &sinyal_proto::SymbolEntry) -> Self {
        Self {
            digits: e.digits,
            point: e.point,
            tick_size: e.tick_size,
            volume_step: e.volume_step,
            volume_min: e.volume_min,
            volume_max: e.volume_max,
            stops_level: e.stops_level,
            contract_size: e.contract_size,
        }
    }

    /// Kâr/marjin hesabında kullanılan sözleşme büyüklüğü.
    ///
    /// Buraya 0 ULAŞAMAZ: [`check_contract`] emri `place` içinde reddeder ve
    /// pozisyon/emir kaydı ancak o denetimden geçmiş bir sembolle doğar.
    /// Yine de savunmacı olarak 1 dönüyoruz — burada `unwrap` etmek, veri
    /// eksikliğini daemon çökmesine çevirirdi.
    fn cs(&self) -> f64 {
        if self.contract_size > 0.0 {
            self.contract_size
        } else {
            1.0
        }
    }

    /// Kaymanın fiyat karşılığı. `point` yoksa kayma uygulanamaz — uydurmak
    /// yerine 0 döner.
    fn slip(&self, points: f64) -> f64 {
        if self.point > 0.0 && points > 0.0 && points.is_finite() {
            self.point * points
        } else {
            0.0
        }
    }
}

// ---------------------------------------------------------------------------
// Emir tipleri
// ---------------------------------------------------------------------------

/// Emir tipi — tel üzerindeki adlarla birebir.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimOrderKind {
    #[default]
    Buy,
    Sell,
    BuyLimit,
    SellLimit,
    BuyStop,
    SellStop,
    BuyStopLimit,
    SellStopLimit,
}

impl SimOrderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::BuyLimit => "buy_limit",
            Self::SellLimit => "sell_limit",
            Self::BuyStop => "buy_stop",
            Self::SellStop => "sell_stop",
            Self::BuyStopLimit => "buy_stop_limit",
            Self::SellStopLimit => "sell_stop_limit",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "buy" => Self::Buy,
            "sell" => Self::Sell,
            "buy_limit" => Self::BuyLimit,
            "sell_limit" => Self::SellLimit,
            "buy_stop" => Self::BuyStop,
            "sell_stop" => Self::SellStop,
            "buy_stop_limit" => Self::BuyStopLimit,
            "sell_stop_limit" => Self::SellStopLimit,
            _ => return None,
        })
    }

    /// Alış tarafı mı — dolumda ASK, kapanışta BID kullanılır.
    pub fn is_buy(self) -> bool {
        matches!(self, Self::Buy | Self::BuyLimit | Self::BuyStop | Self::BuyStopLimit)
    }

    /// Piyasa emri mi.
    pub fn is_market(self) -> bool {
        matches!(self, Self::Buy | Self::Sell)
    }

    /// LIMIT tarafı mı — limit dolumu fiyatından KÖTÜ olamaz, o yüzden kayma
    /// almaz (bkz. [`SimEngine::fill_price`]).
    fn is_limit(self) -> bool {
        matches!(self, Self::BuyLimit | Self::SellLimit)
    }

    /// **Tetikleme semantiği — MT5 ile aynı.**
    ///
    /// ```text
    /// BUY_LIMIT   ask <= price      SELL_LIMIT  bid >= price
    /// BUY_STOP    ask >= price      SELL_STOP   bid <= price
    /// ```
    ///
    /// `*_STOP_LIMIT` STOP tarafından tetiklenir; tetiklendiğinde dolmaz,
    /// `stoplimit` fiyatındaki LIMIT emrine dönüşür.
    ///
    /// Karşılaştırmalar **tam**, tolerans yok: tolerans emri bir tık erken
    /// tetikler, yani gerçekte olmayan bir dolum uydurur. Kötümser taraf
    /// tetiklememektir.
    pub fn triggers(self, price: f64, bid: f64, ask: f64) -> bool {
        match self {
            Self::Buy | Self::Sell => false,
            Self::BuyLimit => ask <= price,
            Self::SellLimit => bid >= price,
            Self::BuyStop | Self::BuyStopLimit => ask >= price,
            Self::SellStop | Self::SellStopLimit => bid <= price,
        }
    }
}

/// Simülatöre verilen emir isteği.
///
/// Alan adları tel üzerindeki `OrderReq` ile aynı; motor istemcinin isteğini
/// yeniden yorumlamaz.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SimOrderReq {
    /// İstemcinin verdiği kimlik. Idempotency **burada denetlenmez** —
    /// o kapı canlıda da simülasyonda da motorun dışındadır (`OrderTracker`).
    pub id: String,
    /// Emri gönderen istemcinin wire kimliği (canlıdaki `magic` karşılığı).
    pub client_id: u64,
    pub symbol: String,
    pub kind: SimOrderKind,
    pub volume: f64,
    /// Bekleyen emirde ZORUNLU tetik fiyatı; piyasa emrinde YOK SAYILIR
    /// (piyasa emri gördüğü fiyattan dolar, istediğinden değil).
    pub price: f64,
    /// Yalnızca `*_stop_limit`: tetiklendikten sonra kurulacak limit fiyatı.
    pub stoplimit: f64,
    pub sl: f64,
    pub tp: f64,
    /// Saklanır ama İŞLETİLMEZ (bkz. modül başlığı).
    pub expiration: i64,
    pub comment: String,
    /// Kaydın saati. Motorun saat kaynağı yoktur; damgayı çağıran verir.
    pub time_msc: i64,
}

// ---------------------------------------------------------------------------
// Olaylar
// ---------------------------------------------------------------------------

/// Olay türü — tel üzerindeki `OrderEvent.kind` ile birebir.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimEventKind {
    #[default]
    Queued,
    Ack,
    Txn,
    Rejected,
}

impl SimEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Ack => "ack",
            Self::Txn => "txn",
            Self::Rejected => "rejected",
        }
    }
}

/// Olayın sebebi — tel üzerinde `comment` olarak taşınabilir.
///
/// Tetiklenen bir dolumun SL mi TP mi olduğunu ayırt edememek, kayıttan
/// strateji analizi yapmayı imkânsız kılardı.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimCause {
    /// İstemcinin doğrudan isteği.
    #[default]
    Request,
    /// Bekleyen emir fiyat seviyesine gelip doldu.
    PendingTriggered,
    /// `*_stop_limit` tetiklendi, limit emrine dönüştü (dolum DEĞİL).
    StopLimitArmed,
    StopLoss,
    TakeProfit,
    Close,
    Cancel,
    Modify,
}

impl SimCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::PendingTriggered => "pending_triggered",
            Self::StopLimitArmed => "stop_limit_armed",
            Self::StopLoss => "sl",
            Self::TakeProfit => "tp",
            Self::Close => "close",
            Self::Cancel => "cancel",
            Self::Modify => "modify",
        }
    }
}

/// Simüle emir olayı.
///
/// Alan adları canlı `OrderEvent` ile birebir aynı; `sim` **daima true**.
/// Canlı olayda bu alan hiç bulunmaz — "yok" ile "false" arasındaki farkı
/// korumak kasıtlıdır.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SimEvent {
    pub kind: SimEventKind,
    /// 0 = retcode yok (`queued`); çağıran bunu `None`a çevirir.
    pub retcode: u32,
    /// Olayı DOĞURAN isteğin kimliği — yani pozisyonu/emri açan `place`
    /// isteğinin `id`si. Tetiklenen dolumda, SL/TP kapanışında ve
    /// `close`/`cancel`/`modify_sltp` sonucunda da bu taşınır, çünkü o üç
    /// imza kendi istek kimliğini almıyor. Çağıran tel üzerinde kendi istek
    /// kimliğini göstermek istiyorsa üzerine yazar; motorun bildiği tek
    /// bağlantı budur ve boş bırakmak bilgiyi yok etmek olurdu.
    pub id: String,
    pub client_id: u64,
    pub symbol: String,
    pub order: u64,
    pub deal: u64,
    pub position: u64,
    pub volume: f64,
    pub price: f64,
    /// Kaydın saati.
    pub time_msc: i64,
    pub cause: SimCause,
    /// **Bu dolum simüle edildi.** Daima `true`.
    pub sim: bool,
    /// Kapanış olaylarında gerçekleşen kâr/zarar; diğerlerinde 0.
    pub profit: f64,
}

impl SimEvent {
    fn new(kind: SimEventKind, retcode: u32) -> Self {
        Self { kind, retcode, sim: true, ..Default::default() }
    }
}

/// Reddedilen isteğin sebebi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReject {
    /// **Gerçek MT5 retcode'u** (10014/10016/10019/...).
    pub retcode: u32,
    pub reason: String,
}

/// Bir isteğin sonucu.
#[derive(Debug, Clone, PartialEq)]
pub enum SimOutcome {
    /// Kabul edildi. Olaylar **canlıyla aynı sırada**:
    /// `queued` → `ack`(10008) → `txn`(10009).
    Accepted { ticket: u64, events: Vec<SimEvent> },
    Rejected(SimReject),
}

// Bu sorgulayıcılar motorun SINAMA yüzeyidir: bağlama katmanı (`server.rs`)
// sonucu doğrudan desen eşlemesiyle çözüyor ve olayların SAHİPLİĞİNİ alıyor,
// bu yüzden ödünç veren bu yolları kullanmıyor. Yine de silmiyoruz — motorun
// sözleşmesini test eden kod tam olarak buradan okuyor.
#[allow(dead_code)]
impl SimOutcome {
    fn reject(retcode: u32, reason: impl Into<String>) -> Self {
        Self::Rejected(SimReject { retcode, reason: reason.into() })
    }

    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// Reddedildiyse retcode, kabul edildiyse [`retcode::DONE`].
    pub fn retcode(&self) -> u32 {
        match self {
            Self::Accepted { .. } => retcode::DONE,
            Self::Rejected(r) => r.retcode,
        }
    }

    pub fn ticket(&self) -> Option<u64> {
        match self {
            Self::Accepted { ticket, .. } => Some(*ticket),
            Self::Rejected(_) => None,
        }
    }

    pub fn events(&self) -> &[SimEvent] {
        match self {
            Self::Accepted { events, .. } => events,
            Self::Rejected(_) => &[],
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Accepted { .. } => None,
            Self::Rejected(r) => Some(&r.reason),
        }
    }
}

// ---------------------------------------------------------------------------
// Fiyat defteri
// ---------------------------------------------------------------------------

/// Sembolün son bilinen kotasyonu.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SimQuote {
    pub bid: f64,
    pub ask: f64,
    pub time_msc: i64,
}

/// Sembol → son kotasyon.
///
/// Pozisyonların değerlenmesi için gerekli. Motor kendi defterini `on_tick`
/// ile günceller; dışarıdan verilen defter (canlı kayıt) **üstün sayılır**,
/// içerideki yalnızca eksik sembollerde yedektir.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PriceBook {
    map: HashMap<String, SimQuote>,
}

impl PriceBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, symbol: &str, bid: f64, ask: f64, time_msc: i64) {
        self.map.insert(symbol.to_owned(), SimQuote { bid, ask, time_msc });
    }

    pub fn get(&self, symbol: &str) -> Option<SimQuote> {
        self.map.get(symbol).copied()
    }
}

// ---------------------------------------------------------------------------
// Dışa verilen durum
// ---------------------------------------------------------------------------

/// Açık pozisyon görüntüsü — alan adları canlı `PositionInfo` ile aynı.
#[derive(Debug, Clone, PartialEq)]
pub struct SimPosition {
    pub ticket: u64,
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
    /// Swap modellenmiyor; 0 burada "hesaplanmıyor" demek.
    pub swap: f64,
    /// Açılışta kilitlenen marjin.
    pub margin: f64,
    pub time_msc: i64,
    pub comment: String,
}

/// Bekleyen emir görüntüsü — alan adları canlı `OrderInfo` ile aynı.
#[derive(Debug, Clone, PartialEq)]
pub struct SimOrder {
    pub ticket: u64,
    pub client_id: u64,
    pub symbol: String,
    pub kind: &'static str,
    pub volume_initial: f64,
    pub volume_current: f64,
    pub price: f64,
    pub stoplimit: f64,
    pub sl: f64,
    pub tp: f64,
    pub time_setup_msc: i64,
    pub expiration: i64,
    pub comment: String,
}

/// Sentetik hesap durumu — alan adları canlı `AccountInfo` ile aynı.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimAccount {
    pub balance: f64,
    pub credit: f64,
    pub profit: f64,
    pub equity: f64,
    pub margin: f64,
    pub margin_free: f64,
    pub margin_level: f64,
    pub leverage: i64,
}

// ---------------------------------------------------------------------------
// İç kayıtlar
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PosRec {
    ticket: u64,
    client_id: u64,
    id: String,
    symbol: String,
    buy: bool,
    volume: f64,
    price_open: f64,
    sl: f64,
    tp: f64,
    time_msc: i64,
    comment: String,
    /// Açılış anındaki sembol özellikleri — `on_tick`/`close` imzaları sembol
    /// almadığı için kayma ve yuvarlama buradan çözülür.
    sym: SimSymbol,
    margin: f64,
}

#[derive(Debug, Clone)]
struct OrdRec {
    ticket: u64,
    client_id: u64,
    id: String,
    symbol: String,
    kind: SimOrderKind,
    volume: f64,
    price: f64,
    stoplimit: f64,
    sl: f64,
    tp: f64,
    time_setup_msc: i64,
    expiration: i64,
    comment: String,
    sym: SimSymbol,
}

// ---------------------------------------------------------------------------
// Motor
// ---------------------------------------------------------------------------

/// Simülasyon motoru.
///
/// Tek iş parçacığında çalışır ve kendi kilidini tutmaz; paylaşılacaksa
/// çağıran `Mutex` ile sarar (canlı kodda olduğu gibi).
#[derive(Debug)]
pub struct SimEngine {
    cfg: SimConfig,
    balance: f64,
    /// Bilet sayacı. 1'den başlar: 0 "bilet yok" demektir. **Artan sayaç,
    /// rastgele değil** — belirlenimcilik sözünün yarısı burada.
    next_ticket: u64,
    positions: Vec<PosRec>,
    pending: Vec<OrdRec>,
    prices: PriceBook,
}

impl SimEngine {
    pub fn new(cfg: SimConfig) -> Self {
        Self {
            cfg,
            balance: cfg.balance,
            next_ticket: 1,
            positions: Vec::new(),
            pending: Vec::new(),
            prices: PriceBook::new(),
        }
    }

    /// Ham bakiye — gerçekleşmiş kâr/zarar dâhil, açık pozisyonlar HARİÇ.
    ///
    /// Bağlama katmanı bunu değil [`account`](Self::account) kullanıyor
    /// (özkaynak ve marjin de gerekiyor); burası motorun kendi testlerinin
    /// okuma noktası.
    #[allow(dead_code)]
    pub fn balance(&self) -> f64 {
        self.balance
    }

    fn ticket(&mut self) -> u64 {
        let t = self.next_ticket;
        self.next_ticket += 1;
        t
    }

    // --- istekler ----------------------------------------------------------

    /// Emir gönder.
    ///
    /// Doğrulama sırası canlıyla aynı mantıkta: önce istemcinin isteğine dair
    /// olanlar (hacim, fiyat, stops), sonra hesaba dair olan (marjin).
    pub fn place(&mut self, req: &SimOrderReq, sym: &SimSymbol, bid: f64, ask: f64) -> SimOutcome {
        if !quotes_ok(bid, ask) {
            return SimOutcome::reject(
                retcode::PRICE_OFF,
                "kayitta bu an icin fiyat yok — simule dolum yapilamaz",
            );
        }
        self.prices.set(&req.symbol, bid, ask, req.time_msc);

        // Sembol tablosu eksikse SİMÜLE ETME. Bu denetim hacimden ÖNCE:
        // eksik veriyle üretilen bir dolum, reddedilen bir emirden çok daha
        // pahalıdır (bkz. [`check_contract`]).
        if let Err(r) = check_contract(sym) {
            return SimOutcome::Rejected(r);
        }

        let volume = match check_volume(req.volume, sym) {
            Ok(v) => v,
            Err(r) => return SimOutcome::Rejected(r),
        };
        let sl = round_px(req.sl, sym);
        let tp = round_px(req.tp, sym);
        let buy = req.kind.is_buy();

        if req.kind.is_market() {
            // Piyasa emri: alış ASK'ten, satış BID'den + ALEYHTE kayma.
            let fill = self.fill_price(req.kind, 0.0, sym, bid, ask);
            // SL/TP kapanış tarafındaki fiyata göre denetlenir: LONG BID'den,
            // SHORT ASK'ten kapanır, MT5 mesafeyi o fiyattan ölçer.
            let refp = if buy { bid } else { ask };
            if let Err(r) = check_sltp(buy, refp, sl, tp, sym) {
                return SimOutcome::Rejected(r);
            }
            let margin = margin_for(volume, sym, fill, self.cfg.leverage);
            if let Err(r) = self.check_margin(margin) {
                return SimOutcome::Rejected(r);
            }

            let ticket = self.ticket();
            let deal = self.ticket();
            self.positions.push(PosRec {
                ticket,
                client_id: req.client_id,
                id: req.id.clone(),
                symbol: req.symbol.clone(),
                buy,
                volume,
                price_open: fill,
                sl,
                tp,
                time_msc: req.time_msc,
                comment: req.comment.clone(),
                sym: *sym,
                margin,
            });

            // MT5'te yeni pozisyonun kimliği, onu açan emrin biletidir.
            let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
            txn.order = ticket;
            txn.deal = deal;
            txn.position = ticket;
            txn.volume = volume;
            txn.price = fill;
            return SimOutcome::Accepted {
                ticket,
                events: self.request_events(req, ticket, txn),
            };
        }

        // --- bekleyen emir ---
        let price = round_px(req.price, sym);
        if !(price > 0.0) {
            return SimOutcome::reject(retcode::INVALID_PRICE, "bekleyen emir fiyat ister");
        }
        // Tetik tarafı: alış emirleri ASK'i, satış emirleri BID'i izler.
        // İşaretli mesafe hem YÖNÜ hem `stops_level` uzaklığını tek denetimde
        // kapsar: negatifse emir piyasanın yanlış tarafındadır.
        let distance = match req.kind {
            SimOrderKind::BuyLimit => ask - price,
            SimOrderKind::SellLimit => price - bid,
            SimOrderKind::BuyStop | SimOrderKind::BuyStopLimit => price - ask,
            SimOrderKind::SellStop | SimOrderKind::SellStopLimit => bid - price,
            SimOrderKind::Buy | SimOrderKind::Sell => unreachable!("piyasa emri yukarıda döndü"),
        };
        if !respects_gap(distance, sym) {
            return SimOutcome::Rejected(stops_reject(sym, "bekleyen emir fiyati"));
        }

        let stoplimit = round_px(req.stoplimit, sym);
        if matches!(req.kind, SimOrderKind::BuyStopLimit | SimOrderKind::SellStopLimit) {
            if !(stoplimit > 0.0) {
                return SimOutcome::reject(
                    retcode::INVALID_PRICE,
                    "stop limit emri stoplimit fiyati ister",
                );
            }
            if !respects_gap((price - stoplimit).abs(), sym) {
                return SimOutcome::Rejected(stops_reject(sym, "stoplimit fiyati"));
            }
        }

        // Bekleyen emrin SL/TP'si emrin KENDİ fiyatına göre ölçülür.
        if let Err(r) = check_sltp(buy, price, sl, tp, sym) {
            return SimOutcome::Rejected(r);
        }
        // Marjin bekleyen emirde KİLİTLENMEZ ama karşılanabilirliği burada da
        // denetlenir; tetiklendiğinde ikinci kez bakılır.
        let margin = margin_for(volume, sym, price, self.cfg.leverage);
        if let Err(r) = self.check_margin(margin) {
            return SimOutcome::Rejected(r);
        }

        let ticket = self.ticket();
        self.pending.push(OrdRec {
            ticket,
            client_id: req.client_id,
            id: req.id.clone(),
            symbol: req.symbol.clone(),
            kind: req.kind,
            volume,
            price,
            stoplimit,
            sl,
            tp,
            time_setup_msc: req.time_msc,
            expiration: req.expiration,
            comment: req.comment.clone(),
            sym: *sym,
        });

        // `txn`(10009) burada "emir KURULDU" demek, "doldu" demek değil —
        // canlıda da bekleyen emir kabulü böyle görünür.
        let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
        txn.order = ticket;
        txn.volume = volume;
        txn.price = price;
        SimOutcome::Accepted { ticket, events: self.request_events(req, ticket, txn) }
    }

    /// **HER TİCK'TE** çağrılır: bekleyen emirleri ve SL/TP'yi tetikler.
    ///
    /// Sıra: önce bekleyen emirler (yeni pozisyon doğabilir), sonra TÜM
    /// pozisyonların SL/TP'si — aynı tick'te açılan pozisyon dâhil. Bir
    /// boşlukta emir tetiklenip aynı anda stop seviyesinin ötesine düşebilir;
    /// bunu bir sonraki tick'e ertelemek, gerçekte olmuş bir zararı
    /// gizlemek olurdu.
    pub fn on_tick(&mut self, symbol: &str, bid: f64, ask: f64, time_msc: i64) -> Vec<SimEvent> {
        let mut out = Vec::new();
        if !quotes_ok(bid, ask) {
            // Bozuk tick hiçbir şeyi tetiklemez; fiyat defterine de yazılmaz.
            return out;
        }
        self.prices.set(symbol, bid, ask, time_msc);
        self.fire_pending(symbol, bid, ask, time_msc, &mut out);
        self.fire_stops(symbol, bid, ask, time_msc, &mut out);
        out
    }

    /// Bekleyen emirleri tetikle. Bilet sırası korunur: `pending` daima artan
    /// bilet sırasındadır, aynı tick birden çok emri vurursa sıra bellidir.
    fn fire_pending(
        &mut self,
        symbol: &str,
        bid: f64,
        ask: f64,
        time_msc: i64,
        out: &mut Vec<SimEvent>,
    ) {
        let mut kept: Vec<OrdRec> = Vec::with_capacity(self.pending.len());
        let mut fired: Vec<OrdRec> = Vec::new();
        for mut o in std::mem::take(&mut self.pending) {
            if o.symbol != symbol || !o.kind.triggers(o.price, bid, ask) {
                kept.push(o);
                continue;
            }
            match o.kind {
                // STOP LIMIT tetiklenince DOLMAZ: aynı biletle limit emrine
                // dönüşür ve beklemeye devam eder.
                SimOrderKind::BuyStopLimit | SimOrderKind::SellStopLimit => {
                    let mut ev = SimEvent::new(SimEventKind::Txn, retcode::DONE);
                    ev.id = o.id.clone();
                    ev.client_id = o.client_id;
                    ev.symbol = o.symbol.clone();
                    ev.order = o.ticket;
                    ev.volume = o.volume;
                    ev.price = o.stoplimit;
                    ev.time_msc = time_msc;
                    ev.cause = SimCause::StopLimitArmed;
                    out.push(ev);
                    o.kind = if o.kind == SimOrderKind::BuyStopLimit {
                        SimOrderKind::BuyLimit
                    } else {
                        SimOrderKind::SellLimit
                    };
                    o.price = o.stoplimit;
                    o.stoplimit = 0.0;
                    kept.push(o);
                }
                _ => fired.push(o),
            }
        }
        self.pending = kept;

        for o in fired {
            let fill = self.fill_price(o.kind, o.price, &o.sym, bid, ask);
            let margin = margin_for(o.volume, &o.sym, fill, self.cfg.leverage);
            if let Err(r) = self.check_margin(margin) {
                // Emir kurulduğunda para yetiyordu, tetiklendiğinde yetmiyor:
                // MT5 de emri iptal eder. Sessizce düşürmek, kaybolan bir
                // emrin sebebini aramakla geçen bir gün demekti.
                let mut ev = SimEvent::new(SimEventKind::Rejected, r.retcode);
                ev.id = o.id.clone();
                ev.client_id = o.client_id;
                ev.symbol = o.symbol.clone();
                ev.order = o.ticket;
                ev.volume = o.volume;
                ev.price = fill;
                ev.time_msc = time_msc;
                ev.cause = SimCause::PendingTriggered;
                out.push(ev);
                continue;
            }
            let deal = self.ticket();
            let position = o.ticket; // MT5: pozisyon kimliği = emrin bileti.
            self.positions.push(PosRec {
                ticket: position,
                client_id: o.client_id,
                id: o.id.clone(),
                symbol: o.symbol.clone(),
                buy: o.kind.is_buy(),
                volume: o.volume,
                price_open: fill,
                sl: o.sl,
                tp: o.tp,
                time_msc,
                comment: o.comment.clone(),
                sym: o.sym,
                margin,
            });
            let mut ev = SimEvent::new(SimEventKind::Txn, retcode::DONE);
            ev.id = o.id.clone();
            ev.client_id = o.client_id;
            ev.symbol = o.symbol.clone();
            ev.order = o.ticket;
            ev.deal = deal;
            ev.position = position;
            ev.volume = o.volume;
            ev.price = fill;
            ev.time_msc = time_msc;
            ev.cause = SimCause::PendingTriggered;
            out.push(ev);
        }
    }

    /// SL/TP tetikle.
    ///
    /// **Aynı tick ikisini de vuruyorsa SL kazanır.** Bunun sebebi tick içi
    /// fiyat yolunun bilinmemesi: elimizde yalnızca bid/ask var, hangisine
    /// önce değdiği kayıtta YOK. İki ihtimalden kötü olanı seçmek, olmamış
    /// bir kârı rapor etmekten iyidir.
    fn fire_stops(
        &mut self,
        symbol: &str,
        bid: f64,
        ask: f64,
        time_msc: i64,
        out: &mut Vec<SimEvent>,
    ) {
        let mut kept: Vec<PosRec> = Vec::with_capacity(self.positions.len());
        for p in std::mem::take(&mut self.positions) {
            if p.symbol != symbol {
                kept.push(p);
                continue;
            }
            // LONG BID'den, SHORT ASK'ten kapanır.
            let hit = if p.buy {
                if p.sl > 0.0 && bid <= p.sl {
                    Some(SimCause::StopLoss)
                } else if p.tp > 0.0 && bid >= p.tp {
                    Some(SimCause::TakeProfit)
                } else {
                    None
                }
            } else if p.sl > 0.0 && ask >= p.sl {
                Some(SimCause::StopLoss)
            } else if p.tp > 0.0 && ask <= p.tp {
                Some(SimCause::TakeProfit)
            } else {
                None
            };
            let Some(cause) = hit else {
                kept.push(p);
                continue;
            };

            // SL bir STOP'tur: tetiklenince piyasadan dolar, kayma yer ve
            // boşlukta SL fiyatından KÖTÜ dolabilir.
            // TP bir LIMIT'tir: fiyatından kötü dolamaz, kayma yemez.
            let exit = if cause == SimCause::StopLoss {
                self.market_exit(p.buy, &p.sym, bid, ask)
            } else {
                p.tp
            };
            let profit = profit_of(p.buy, p.price_open, exit, p.volume, &p.sym);
            self.balance += profit;
            let deal = self.ticket();
            let order = self.ticket();
            let mut ev = SimEvent::new(SimEventKind::Txn, retcode::DONE);
            ev.id = p.id.clone();
            ev.client_id = p.client_id;
            ev.symbol = p.symbol.clone();
            ev.order = order;
            ev.deal = deal;
            ev.position = p.ticket;
            ev.volume = p.volume;
            ev.price = exit;
            ev.time_msc = time_msc;
            ev.cause = cause;
            ev.profit = profit;
            out.push(ev);
        }
        self.positions = kept;
    }

    /// Pozisyonu kapat. `volume` 0 veya pozisyondan büyükse tamamı kapanır
    /// (canlıdaki ile aynı gevşeklik).
    pub fn close(&mut self, ticket: u64, volume: f64, bid: f64, ask: f64) -> SimOutcome {
        let Some(ix) = self.positions.iter().position(|p| p.ticket == ticket) else {
            return SimOutcome::reject(retcode::INVALID, "simule pozisyon bulunamadi");
        };
        if !quotes_ok(bid, ask) {
            return SimOutcome::reject(
                retcode::PRICE_OFF,
                "kayitta bu an icin fiyat yok — simule kapanis yapilamaz",
            );
        }
        // Pozisyonun kimlik bilgileri DEĞİŞİKLİKTEN ÖNCE alınır: tam kapanışta
        // `remove(ix)` sonrası o indekste BAŞKA bir pozisyon durur ve olayı
        // yanlış istemciye damgalamak, `client_id`nin var oluş sebebini
        // (kimin emri olduğunu bilmek) çöpe atardı.
        let (buy, sym, symbol, client_id, origin_id) = {
            let p = &self.positions[ix];
            (p.buy, p.sym, p.symbol.clone(), p.client_id, p.id.clone())
        };
        // Fiyat defterine yeni kotasyon yazılır ama ZAMAN DAMGASI KORUNUR:
        // `close` imzası saat almıyor, pozisyonun açılış saatini "şimdi" diye
        // yazmak kaydın zamanıyla çelişen bir damga üretirdi.
        let ts = self.prices.get(&symbol).map(|q| q.time_msc).unwrap_or(0);
        self.prices.set(&symbol, bid, ask, ts);

        let exit = self.market_exit(buy, &sym, bid, ask);
        let pos_vol = self.positions[ix].volume;
        let vol = if volume <= 0.0 || volume >= pos_vol - VOLUME_EPS { pos_vol } else { volume };
        let entry = self.positions[ix].price_open;
        let profit = profit_of(buy, entry, exit, vol, &sym);
        self.balance += profit;

        if vol >= pos_vol - VOLUME_EPS {
            self.positions.remove(ix);
        } else {
            // Marjin oranında serbest bırakılır.
            let p = &mut self.positions[ix];
            p.margin -= p.margin * (vol / pos_vol);
            p.volume -= vol;
        }

        let order = self.ticket();
        let deal = self.ticket();
        let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
        txn.id = origin_id;
        txn.client_id = client_id;
        txn.symbol = symbol;
        txn.order = order;
        txn.deal = deal;
        txn.position = ticket;
        txn.volume = vol;
        txn.price = exit;
        txn.cause = SimCause::Close;
        txn.profit = profit;
        SimOutcome::Accepted { ticket: order, events: seq(txn) }
    }

    /// Bekleyen emri iptal et.
    pub fn cancel(&mut self, ticket: u64) -> SimOutcome {
        let Some(ix) = self.pending.iter().position(|o| o.ticket == ticket) else {
            return SimOutcome::reject(retcode::INVALID, "simule bekleyen emir bulunamadi");
        };
        let o = self.pending.remove(ix);
        let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
        txn.id = o.id.clone();
        txn.client_id = o.client_id;
        txn.symbol = o.symbol.clone();
        txn.order = o.ticket;
        txn.volume = o.volume;
        txn.price = o.price;
        txn.cause = SimCause::Cancel;
        SimOutcome::Accepted { ticket: o.ticket, events: seq(txn) }
    }

    /// Pozisyonun veya bekleyen emrin SL/TP'sini değiştir. 0 = temizle.
    ///
    /// `stops_level` denetimi `place` ile AYNI: canlıda değiştirilemeyen bir
    /// seviyeyi simülasyonda kabul etmek, stratejiyi canlıda çalışmayacak bir
    /// stop yönetimine alıştırırdı.
    pub fn modify_sltp(
        &mut self,
        ticket: u64,
        sl: f64,
        tp: f64,
        sym: &SimSymbol,
        bid: f64,
        ask: f64,
    ) -> SimOutcome {
        if !quotes_ok(bid, ask) {
            return SimOutcome::reject(
                retcode::PRICE_OFF,
                "kayitta bu an icin fiyat yok — simule degisiklik yapilamaz",
            );
        }
        let nsl = round_px(sl, sym);
        let ntp = round_px(tp, sym);

        if let Some(ix) = self.positions.iter().position(|p| p.ticket == ticket) {
            let buy = self.positions[ix].buy;
            let refp = if buy { bid } else { ask };
            if let Err(r) = check_sltp(buy, refp, nsl, ntp, sym) {
                return SimOutcome::Rejected(r);
            }
            let p = &mut self.positions[ix];
            p.sl = nsl;
            p.tp = ntp;
            let (client_id, symbol) = (p.client_id, p.symbol.clone());
            let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
            txn.client_id = client_id;
            txn.symbol = symbol;
            txn.position = ticket;
            txn.cause = SimCause::Modify;
            return SimOutcome::Accepted { ticket, events: seq(txn) };
        }
        if let Some(ix) = self.pending.iter().position(|o| o.ticket == ticket) {
            let (buy, price) = (self.pending[ix].kind.is_buy(), self.pending[ix].price);
            if let Err(r) = check_sltp(buy, price, nsl, ntp, sym) {
                return SimOutcome::Rejected(r);
            }
            let o = &mut self.pending[ix];
            o.sl = nsl;
            o.tp = ntp;
            let (client_id, symbol) = (o.client_id, o.symbol.clone());
            let mut txn = SimEvent::new(SimEventKind::Txn, retcode::DONE);
            txn.client_id = client_id;
            txn.symbol = symbol;
            txn.order = ticket;
            txn.cause = SimCause::Modify;
            return SimOutcome::Accepted { ticket, events: seq(txn) };
        }
        SimOutcome::reject(retcode::INVALID, "simule pozisyon/emir bulunamadi")
    }

    // --- durum -------------------------------------------------------------

    /// Açık pozisyonlar, verilen fiyatlarla değerlenmiş.
    pub fn positions(&self, prices: &PriceBook) -> Vec<SimPosition> {
        self.positions
            .iter()
            .map(|p| {
                let cur = self.mark(p, prices);
                SimPosition {
                    ticket: p.ticket,
                    client_id: p.client_id,
                    symbol: p.symbol.clone(),
                    side: if p.buy { "buy" } else { "sell" },
                    volume: p.volume,
                    price_open: p.price_open,
                    price_current: cur,
                    sl: p.sl,
                    tp: p.tp,
                    profit: profit_of(p.buy, p.price_open, cur, p.volume, &p.sym),
                    swap: 0.0,
                    margin: p.margin,
                    time_msc: p.time_msc,
                    comment: p.comment.clone(),
                }
            })
            .collect()
    }

    /// Bekleyen emirler (artan bilet sırasında).
    pub fn orders(&self) -> Vec<SimOrder> {
        self.pending
            .iter()
            .map(|o| SimOrder {
                ticket: o.ticket,
                client_id: o.client_id,
                symbol: o.symbol.clone(),
                kind: o.kind.as_str(),
                volume_initial: o.volume,
                // Kısmi dolum modellenmiyor: bekleyen emir ya tam dolar ya
                // bekler, o yüzden kalan = başlangıç.
                volume_current: o.volume,
                price: o.price,
                stoplimit: o.stoplimit,
                sl: o.sl,
                tp: o.tp,
                time_setup_msc: o.time_setup_msc,
                expiration: o.expiration,
                comment: o.comment.clone(),
            })
            .collect()
    }

    /// Sentetik hesap durumu.
    pub fn account(&self, prices: &PriceBook) -> SimAccount {
        // `+ 0.0`: boş bir f64 toplamı `-0.0` verir ve tel üzerinde
        // `"profit":-0.0` görünürdü — olmayan bir zararı varmış gibi gösteren
        // bir arayüz, olmayan bir hatayı kovalatır.
        let profit: f64 = self
            .positions
            .iter()
            .map(|p| profit_of(p.buy, p.price_open, self.mark(p, prices), p.volume, &p.sym))
            .sum::<f64>()
            + 0.0;
        let margin: f64 = self.positions.iter().map(|p| p.margin).sum::<f64>() + 0.0;
        let equity = self.balance + profit;
        SimAccount {
            balance: self.balance,
            credit: 0.0,
            profit,
            equity,
            margin,
            margin_free: equity - margin,
            margin_level: if margin > 0.0 { equity / margin * 100.0 } else { 0.0 },
            leverage: self.cfg.leverage,
        }
    }

    /// Motorun kendi fiyat defteri (çağıranın defteri yoksa bunu verebilir).
    ///
    /// Bağlama katmanı bunun yerine `Registry`den defter kuruyor: pompa bir
    /// tick'i kaçırsa bile değerleme akışın SON halini göstermeli.
    #[allow(dead_code)]
    pub fn prices(&self) -> &PriceBook {
        &self.prices
    }

    /// Biletin sembolü — pozisyonlarda ve bekleyen emirlerde arar.
    ///
    /// [`close`](Self::close) ve [`modify_sltp`](Self::modify_sltp) fiyat
    /// parametresi alıyor ama bileti sembole ÇEVİRMİYOR; çağıranın o fiyatı
    /// nereden okuyacağını bilmesi için bu eşlemeye ihtiyacı var. Tüm listeyi
    /// dışarıya döküp (`positions()`) içinden aramak, her emir isteğinde
    /// bütün pozisyonları klonlamak demekti.
    pub fn symbol_of(&self, ticket: u64) -> Option<&str> {
        self.positions
            .iter()
            .find(|p| p.ticket == ticket)
            .map(|p| p.symbol.as_str())
            .or_else(|| {
                self.pending
                    .iter()
                    .find(|o| o.ticket == ticket)
                    .map(|o| o.symbol.as_str())
            })
    }

    // --- iç yardımcılar ----------------------------------------------------

    /// Pozisyonun o anki değerleme fiyatı: kapanış tarafından. Dışarıdan
    /// verilen defter üstün, sonra motorun defteri, ikisi de yoksa açılış
    /// fiyatı (kâr 0 görünür — uydurulmuş bir kârdan iyidir).
    fn mark(&self, p: &PosRec, prices: &PriceBook) -> f64 {
        prices
            .get(&p.symbol)
            .or_else(|| self.prices.get(&p.symbol))
            .map(|q| if p.buy { q.bid } else { q.ask })
            .filter(|v| *v > 0.0)
            .unwrap_or(p.price_open)
    }

    /// Açılış dolum fiyatı.
    ///
    /// - Piyasa ve STOP: alışta `ask + kayma`, satışta `bid − kayma`.
    ///   **Kayma DAİMA aleyhte** ve spread'in ÜSTÜNE eklenir.
    /// - LIMIT: emrin fiyatı. Bir limit emri fiyatından kötü dolamaz; ona
    ///   kayma eklemek gerçekte imkânsız bir dolum uydurmak olurdu. Piyasa
    ///   limitin ötesine geçtiğinde iyi tarafı da yazmıyoruz — kötümser sınır
    ///   tam olarak limit fiyatıdır.
    fn fill_price(&self, kind: SimOrderKind, price: f64, sym: &SimSymbol, bid: f64, ask: f64) -> f64 {
        if kind.is_limit() {
            return price;
        }
        let slip = sym.slip(self.cfg.slippage_points);
        let buy = kind.is_buy();
        let raw = if buy { ask + slip } else { bid - slip };
        // Izgaraya ALEYHTE oturt: alışta yukarı, satışta aşağı. En yakına
        // yuvarlamak kaymanın kendisini silebilirdi (bkz. `round_px_adverse`).
        round_px_adverse(raw, sym, buy)
    }

    /// Kapanış dolum fiyatı: LONG BID'den, SHORT ASK'ten — aleyhte kaymayla.
    ///
    /// Kapanışta aleyhte yön TERSTİR: LONG satarak çıkar, yani düşük fiyat
    /// kötüdür; SHORT alarak çıkar, yani yüksek fiyat kötüdür.
    fn market_exit(&self, buy: bool, sym: &SimSymbol, bid: f64, ask: f64) -> f64 {
        let slip = sym.slip(self.cfg.slippage_points);
        let raw = if buy { bid - slip } else { ask + slip };
        round_px_adverse(raw, sym, !buy)
    }

    /// Serbest marjin denetimi. `leverage <= 0` ise marjin modellenmiyordur.
    fn check_margin(&self, need: f64) -> Result<(), SimReject> {
        if need <= 0.0 {
            return Ok(());
        }
        let free = self.account(&self.prices).margin_free;
        if free + VOLUME_EPS < need {
            return Err(SimReject {
                retcode: retcode::NO_MONEY,
                reason: format!(
                    "yetersiz serbest marjin: gereken {need:.2}, serbest {free:.2}"
                ),
            });
        }
        Ok(())
    }

    /// `queued` → `ack`(10008) → `txn`(10009). **Sıra değişmez.**
    ///
    /// İstemci `ack`i dolum sanmamalı; replay bu ayrımı yeniden üretmezse,
    /// ayrımı yanlış kuran bir istemci hatası ancak canlıda ortaya çıkardı.
    fn request_events(&self, req: &SimOrderReq, order: u64, txn: SimEvent) -> Vec<SimEvent> {
        let mut queued = SimEvent::new(SimEventKind::Queued, 0);
        queued.id = req.id.clone();
        queued.client_id = req.client_id;
        queued.symbol = req.symbol.clone();
        queued.time_msc = req.time_msc;

        let mut ack = SimEvent::new(SimEventKind::Ack, retcode::PLACED);
        ack.id = req.id.clone();
        ack.client_id = req.client_id;
        ack.symbol = req.symbol.clone();
        ack.order = order;
        ack.time_msc = req.time_msc;

        let mut txn = txn;
        txn.id = req.id.clone();
        txn.client_id = req.client_id;
        txn.symbol = req.symbol.clone();
        txn.time_msc = req.time_msc;
        vec![queued, ack, txn]
    }
}

// ---------------------------------------------------------------------------
// Saf yardımcılar
// ---------------------------------------------------------------------------

/// `close`/`cancel`/`modify_sltp` için olay dizisi — sıra `place` ile aynı:
/// `queued` → `ack`(10008) → `txn`(10009).
fn seq(txn: SimEvent) -> Vec<SimEvent> {
    let mut queued = SimEvent::new(SimEventKind::Queued, 0);
    queued.id = txn.id.clone();
    queued.client_id = txn.client_id;
    queued.symbol = txn.symbol.clone();
    let mut ack = SimEvent::new(SimEventKind::Ack, retcode::PLACED);
    ack.id = txn.id.clone();
    ack.client_id = txn.client_id;
    ack.symbol = txn.symbol.clone();
    ack.order = txn.order;
    vec![queued, ack, txn]
}

fn quotes_ok(bid: f64, ask: f64) -> bool {
    bid > 0.0 && ask > 0.0 && bid.is_finite() && ask.is_finite() && ask >= bid
}

/// Fiyatı sembolün kotasyon ızgarasına oturt.
///
/// **Canlı yolun tam aynısı** (`server::submit_order` içindeki `norm`):
/// `tick_size` varsa ona, yoksa `digits`e. Yalnızca `digits`e yuvarlamak,
/// tick_size'ı point'in katı olan sembollerde (endeks/CFD) broker'ın hiç
/// veremeyeceği bir fiyattan dolum raporlardı — iki kip ayrışırdı.
fn round_px(p: f64, sym: &SimSymbol) -> f64 {
    if p == 0.0 || !p.is_finite() {
        return p;
    }
    sinyal_proto::normalize_price(p, sym.tick_size, sym.digits)
}

/// Kotasyon ızgarasının adımı: `tick_size`, yoksa `digits`ten türetilen adım.
fn grid_step(sym: &SimSymbol) -> f64 {
    if sym.tick_size > 0.0 && sym.tick_size.is_finite() {
        sym.tick_size
    } else {
        10f64.powi(-(sym.digits.min(15) as i32))
    }
}

/// Izgara toleransı: tam ızgarada duran bir fiyatı kayan nokta tozu yüzünden
/// bir tam adım öteye itmemek için.
const GRID_TOLERANCE: f64 = 1e-6;

/// Dolum fiyatını ızgaraya **ALEYHTE** oturt (`up` ise yukarı, değilse aşağı).
///
/// **En yakına yuvarlamak kaymayı YİYEBİLİR.** Somut örnek: `tick_size` 0.05,
/// `--sim-slippage 2` (= 0.02) ve ask 4000.20 iken ham dolum 4000.22'dir; en
/// yakın ızgara noktası 4000.20, yani tam olarak kaymasız fiyat. Kayma
/// sessizce silinir ve motor "aleyhte kayma modelliyorum" derken mükemmel
/// dolum üretir — düzeltmeye çalıştığımız yanılgının ta kendisi.
///
/// Doğrusu yönlü yuvarlamadır ve gerçekliğe de bu uyar: broker yalnızca
/// ızgara üzerindeki fiyatları kote edebilir, kayma da sizi bir sonraki
/// ızgara noktasına İTER, geri çekmez. Zaten ızgarada duran bir fiyat
/// (kayma 0, ya da kayma ızgaranın tam katı) yerinde kalır.
fn round_px_adverse(p: f64, sym: &SimSymbol, up: bool) -> f64 {
    if p == 0.0 || !p.is_finite() {
        return p;
    }
    let g = grid_step(sym);
    if !(g > 0.0) || !g.is_finite() {
        return p;
    }
    let k = p / g;
    let k = if up { (k - GRID_TOLERANCE).ceil() } else { (k + GRID_TOLERANCE).floor() };
    round_dec(k * g, sym.digits.min(15) as i32)
}

fn round_dec(v: f64, decimals: i32) -> f64 {
    let f = 10f64.powi(decimals);
    (v * f).round() / f
}

/// İşaretli fiyat mesafesi `stops_level`i sağlıyor mu.
///
/// Negatif mesafe = emir piyasanın YANLIŞ tarafında; `stops_level` 0 olsa bile
/// reddedilir (MT5 de reddeder). `point` okunamıyorsa yalnızca yön denetlenir.
fn respects_gap(distance: f64, sym: &SimSymbol) -> bool {
    if !(sym.point > 0.0) {
        return distance >= 0.0;
    }
    distance / sym.point + POINT_TOLERANCE >= f64::from(sym.stops_level)
}

fn stops_reject(sym: &SimSymbol, what: &str) -> SimReject {
    SimReject {
        retcode: retcode::INVALID_STOPS,
        reason: format!(
            "{what} guncel fiyata cok yakin veya yanlis tarafta (stops_level={} point)",
            sym.stops_level
        ),
    }
}

/// Simülasyonun yürüyebilmesi için sembol tablosundan ZORUNLU olan alanlar.
///
/// İkisi de eksikken motor çalışmaya devam edebilirdi — ve tam olarak bu
/// yüzden edememeli:
///
/// - `contract_size` yoksa marjin `hacim × fiyat / kaldıraç` kadar, yani
///   forex'te olması gerekenin 100 000'de biri çıkar: serbest marjin denetimi
///   (10019) hiçbir zaman tetiklenmez ve kâr para biriminde DEĞİLDİR.
/// - `point` yoksa kayma fiyata çevrilemez ([`SimSymbol::slip`] 0 döner), yani
///   `--sim-slippage` sessizce devre dışı kalır ve `stops_level` mesafesi
///   ölçülemez.
///
/// Her iki durumda da simülatör "modelliyorum" dediği şeyi modellemeden
/// iyimser dolum üretirdi. Emri reddetmek gürültülüdür ama yalan değildir;
/// sebep metni ne yapılacağını da söyler.
fn check_contract(sym: &SimSymbol) -> Result<(), SimReject> {
    if !(sym.contract_size > 0.0) {
        return Err(SimReject {
            retcode: retcode::INVALID,
            reason: "sembol tablosunda contract_size YOK: marjin ve kar hesaplanamaz. \
                     Kayittan oynatiyorsan kayit bu alani tasimayan eski bir surumdendir \
                     (yeniden kaydet); canli/paper kipinde EA'nin sembol tablosunu \
                     dolduramadigi anlamina gelir."
                .into(),
        });
    }
    if !(sym.point > 0.0) {
        return Err(SimReject {
            retcode: retcode::INVALID,
            reason: "sembol tablosunda point YOK: kayma fiyata cevrilemez ve stops_level \
                     olculemez — simule dolum iyimser olurdu."
                .into(),
        });
    }
    Ok(())
}

/// Hacmi doğrula. **Adıma oturmayan hacim REDDEDİLİR, sessizce
/// yuvarlanmaz**: canlıda 0.15'i 0.1'e çevirip dolduran bir broker yok;
/// istediğinden farklı hacimle dolduğunu fark etmeyen strateji, riskini
/// yanlış hesaplar.
fn check_volume(v: f64, sym: &SimSymbol) -> Result<f64, SimReject> {
    let bad = |why: String| SimReject { retcode: retcode::INVALID_VOLUME, reason: why };
    if !v.is_finite() || v <= 0.0 {
        return Err(bad(format!("hacim pozitif olmali: {v}")));
    }
    if !(sym.volume_step > 0.0) {
        return Err(bad(format!(
            "hacim adimi gecersiz ({}) — sembol tablosu bozuk",
            sym.volume_step
        )));
    }
    if sym.volume_min > 0.0 && v + VOLUME_EPS < sym.volume_min {
        return Err(bad(format!("hacim asgarinin altinda: {v} < {}", sym.volume_min)));
    }
    if sym.volume_max > 0.0 && v > sym.volume_max + VOLUME_EPS {
        return Err(bad(format!("hacim azaminin ustunde: {v} > {}", sym.volume_max)));
    }
    let k = v / sym.volume_step;
    if (k - k.round()).abs() > 1e-6 {
        return Err(bad(format!("hacim adima uymuyor: {v} % {}", sym.volume_step)));
    }
    // Kayan nokta tozunu temizle: 0.1+0.2 tarzı bir hacim 0.30000000000000004
    // olarak saklanırsa kısmi kapanış karşılaştırmaları bozulur.
    Ok(round_dec(k.round() * sym.volume_step, 8))
}

/// SL/TP `stops_level` denetimi. `reference` ölçümün yapıldığı fiyat:
/// pozisyonda kapanış tarafı, bekleyen emirde emrin kendi fiyatı.
fn check_sltp(buy: bool, reference: f64, sl: f64, tp: f64, sym: &SimSymbol) -> Result<(), SimReject> {
    // 0 = "kurulmadı"; MT5 de öyle yorumlar.
    if sl > 0.0 {
        let d = if buy { reference - sl } else { sl - reference };
        if !respects_gap(d, sym) {
            return Err(stops_reject(sym, "SL"));
        }
    }
    if tp > 0.0 {
        let d = if buy { tp - reference } else { reference - tp };
        if !respects_gap(d, sym) {
            return Err(stops_reject(sym, "TP"));
        }
    }
    Ok(())
}

/// Gereken marjin: `hacim × contract_size × fiyat / kaldıraç`.
/// `leverage <= 0` → 0 (marjin modellenmiyor).
fn margin_for(volume: f64, sym: &SimSymbol, price: f64, leverage: i64) -> f64 {
    if leverage <= 0 || !(price > 0.0) {
        return 0.0;
    }
    volume * sym.cs() * price / leverage as f64
}

/// Kâr/zarar: `(çıkış − giriş) × hacim × contract_size`, yöne göre işaretli.
///
/// Komisyon, swap ve kur çevrimi YOK (bkz. modül başlığı). `contract_size` 0
/// ise sonuç para değil, "fiyat farkı × hacim" birimindedir.
fn profit_of(buy: bool, entry: f64, exit: f64, volume: f64, sym: &SimSymbol) -> f64 {
    let dir = if buy { 1.0 } else { -1.0 };
    (exit - entry) * dir * volume * sym.cs()
}

// ---------------------------------------------------------------------------
// Testler
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 5 haneli forex sembolü — `stops_level` yok.
    fn eurusd() -> SimSymbol {
        SimSymbol {
            digits: 5,
            point: 0.00001,
            tick_size: 0.00001,
            volume_step: 0.01,
            volume_min: 0.01,
            volume_max: 100.0,
            stops_level: 0,
            contract_size: 100_000.0,
        }
    }

    /// `stops_level` = 100 point (10 pip) olan sembol.
    fn strict() -> SimSymbol {
        SimSymbol { stops_level: 100, ..eurusd() }
    }

    // --- eksik sembol verisi: simüle etmektense reddet ----------------------

    #[test]
    fn a_symbol_without_contract_size_is_rejected_instead_of_silently_simulated() {
        // BU TESTİN SEBEBİ: alan 0 iken motor çalışmaya DEVAM edebiliyordu ve
        // ettiği sürece iki şeyi birden bozuyordu — marjin (hacim × 0 ×
        // fiyat / kaldıraç ≈ 0, yani sonsuz kaldıraç) ve kâr (fiyat farkı ×
        // hacim, yani forex'te 100 000 kat küçük). İkisi de simülasyonu
        // OLDUĞUNDAN KÂRLI gösteriyordu; tam olarak kaçınılmak istenen şey.
        // Kayıttan oynatımda alan gerçekten 0 geliyordu (kaydedici yazmıyordu).
        let mut e = engine();
        let sym = SimSymbol { contract_size: 0.0, ..eurusd() };
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &sym, 1.10000, 1.10020);

        assert_eq!(out.retcode(), retcode::INVALID, "eksik sozlesme verisi reddedilmeli");
        assert!(
            out.reason().unwrap_or_default().contains("contract_size"),
            "sebep hangi alanin eksik oldugunu SOYLEMELI: {:?}",
            out.reason()
        );
        // Sessiz kabul edilseydi burada bir pozisyon dururdu.
        assert!(e.positions(&PriceBook::new()).is_empty(), "pozisyon acilmamali");
    }

    #[test]
    fn a_symbol_without_point_is_rejected_because_slippage_could_not_be_applied() {
        // `point` yoksa `slip()` 0 döner: motor "aleyhte kayma modelliyorum"
        // der ama uygulamaz. Sessizce iyimser dolum üretmektense reddediyoruz.
        let mut e = engine();
        let sym = SimSymbol { point: 0.0, ..eurusd() };
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &sym, 1.10000, 1.10020);

        assert_eq!(out.retcode(), retcode::INVALID);
        assert!(
            out.reason().unwrap_or_default().contains("point"),
            "sebep `point`i adiyla anmali: {:?}",
            out.reason()
        );
    }

    #[test]
    fn fills_land_on_the_tick_grid_exactly_like_the_live_path() {
        // point ile tick_size AYNI DEĞİL (endeks/CFD'de tick_size point'in
        // katıdır). Canlı yol fiyatı `normalize_price(p, tick_size, digits)`
        // ile ızgaraya oturtuyor; simülatör yalnızca `digits`e yuvarlarsa
        // broker'ın hiç veremeyeceği bir fiyattan dolum raporlar.
        let sym = SimSymbol {
            digits: 2,
            point: 0.01,
            tick_size: 0.05,
            volume_step: 0.01,
            volume_min: 0.01,
            volume_max: 100.0,
            stops_level: 0,
            contract_size: 1.0,
        };
        // ASIL TUZAK BURADA: 2 point kayma = 0.02, ham dolum 4000.22. En
        // YAKIN ızgara noktası 4000.20 — yani tam olarak kaymasız fiyat.
        // "En yakına yuvarla" deseydik kayma sessizce SİLİNİR ve motor
        // "aleyhte kayma modelliyorum" derken mükemmel dolum üretirdi.
        // Doğrusu bir sonraki ızgara noktası: 4000.25.
        let mut e = SimEngine::new(SimConfig { slippage_points: 2.0, ..SimConfig::default() });
        let out = e.place(&req(SimOrderKind::Buy, 1.0), &sym, 4000.15, 4000.20);
        assert!(out.is_accepted(), "{:?}", out.reason());

        let fill = e.positions(&PriceBook::new())[0].price_open;
        let k = fill / sym.tick_size;
        assert!(
            (k - k.round()).abs() < 1e-6,
            "dolum fiyati tick izgarasinda olmali: {fill} (tick_size {})",
            sym.tick_size
        );
        assert!(near(fill, 4000.25), "kayma izgaraya ALEYHTE oturmali, verilen: {fill}");

        // SATIŞ tarafı simetrik: bid 4000.15 − 0.02 = 4000.13, en yakın
        // 4000.15 (yine kaymasız), aleyhte olan 4000.10'dur.
        let out = e.place(&req(SimOrderKind::Sell, 1.0), &sym, 4000.15, 4000.20);
        assert!(out.is_accepted(), "{:?}", out.reason());
        let sell_fill = e.positions(&PriceBook::new())[1].price_open;
        assert!(near(sell_fill, 4000.10), "satista kayma asagi olmali, verilen: {sell_fill}");

        // Kayma ızgaranın tam katıysa fiyat YERİNDE kalmalı: yönlü yuvarlama
        // bedava bir tik daha eklememeli.
        let mut e = SimEngine::new(SimConfig { slippage_points: 5.0, ..SimConfig::default() });
        let out = e.place(&req(SimOrderKind::Buy, 1.0), &sym, 4000.15, 4000.20);
        assert!(out.is_accepted(), "{:?}", out.reason());
        let exact = e.positions(&PriceBook::new())[0].price_open;
        assert!(near(exact, 4000.25), "tam katta fazladan tik eklenmemeli: {exact}");
    }

    fn engine() -> SimEngine {
        SimEngine::new(SimConfig::default())
    }

    fn req(kind: SimOrderKind, volume: f64) -> SimOrderReq {
        SimOrderReq {
            id: "r1".into(),
            client_id: 7,
            symbol: "EURUSD".into(),
            kind,
            volume,
            time_msc: 1_000,
            ..Default::default()
        }
    }

    fn near(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    // --- tetikleme: bekleyen emirler ---------------------------------------

    #[test]
    fn buy_limit_only_triggers_when_ask_at_or_below_price() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.09900, ..req(SimOrderKind::BuyLimit, 0.10) };
        let out = e.place(&r, &eurusd(), 1.10000, 1.10010);
        assert!(out.is_accepted(), "{:?}", out.reason());

        // ask hâlâ fiyatın ÜSTÜNDE → tetiklenmez.
        assert!(e.on_tick("EURUSD", 1.09895, 1.09905, 2_000).is_empty());
        assert_eq!(e.orders().len(), 1, "emir hâlâ beklemeli");
        assert!(e.positions(&PriceBook::new()).is_empty());

        // ask == fiyat → tetiklenir.
        let evs = e.on_tick("EURUSD", 1.09890, 1.09900, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, SimEventKind::Txn);
        assert_eq!(evs[0].cause, SimCause::PendingTriggered);
        assert!(e.orders().is_empty());
        assert_eq!(e.positions(&PriceBook::new()).len(), 1);
        // LIMIT emri fiyatından dolar, kayma yemez.
        assert!(near(evs[0].price, 1.09900), "limit dolumu: {}", evs[0].price);
    }

    #[test]
    fn sell_stop_only_triggers_when_bid_at_or_below_price() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.09900, ..req(SimOrderKind::SellStop, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());

        // bid fiyatın ÜSTÜNDE → tetiklenmez.
        assert!(e.on_tick("EURUSD", 1.09901, 1.09911, 2_000).is_empty());
        assert_eq!(e.orders().len(), 1);

        // bid == fiyat → tetiklenir.
        let evs = e.on_tick("EURUSD", 1.09900, 1.09910, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::PendingTriggered);
        // STOP piyasadan dolar: satış BID'den, kayma AŞAĞI.
        assert!(evs[0].price < 1.09900, "stop dolumu kaymali: {}", evs[0].price);
        assert!(near(evs[0].price, 1.09899));
    }

    #[test]
    fn buy_stop_triggers_when_ask_at_or_above_price() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.10100, ..req(SimOrderKind::BuyStop, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.on_tick("EURUSD", 1.10085, 1.10095, 2_000).is_empty());
        let evs = e.on_tick("EURUSD", 1.10090, 1.10100, 3_000);
        assert_eq!(evs.len(), 1);
        // Alış STOP'u ASK'ten + YUKARI kayma.
        assert!(near(evs[0].price, 1.10101), "{}", evs[0].price);
    }

    #[test]
    fn sell_limit_only_triggers_when_bid_at_or_above_price() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.10100, ..req(SimOrderKind::SellLimit, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.on_tick("EURUSD", 1.10099, 1.10109, 2_000).is_empty());
        let evs = e.on_tick("EURUSD", 1.10100, 1.10110, 3_000);
        assert_eq!(evs.len(), 1);
        assert!(near(evs[0].price, 1.10100), "limit fiyatindan dolar: {}", evs[0].price);
    }

    // --- tetikleme: SL/TP ---------------------------------------------------

    #[test]
    fn long_sl_triggers_on_bid_and_tp_on_bid() {
        // LONG SL: bid <= sl
        let mut e = engine();
        let r = SimOrderReq { sl: 1.09900, ..req(SimOrderKind::Buy, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        // ASK sl'in altına inse bile BID inmediyse tetiklenmez.
        assert!(e.on_tick("EURUSD", 1.09901, 1.09891, 2_000).is_empty());
        assert!(e.on_tick("EURUSD", 1.09900, 1.09910, 3_000).len() == 1);
        assert!(e.positions(&PriceBook::new()).is_empty());

        // LONG TP: bid >= tp
        let mut e = engine();
        let r = SimOrderReq { tp: 1.10200, ..req(SimOrderKind::Buy, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.on_tick("EURUSD", 1.10199, 1.10209, 2_000).is_empty());
        let evs = e.on_tick("EURUSD", 1.10200, 1.10210, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::TakeProfit);
        // TP bir LIMIT: fiyatından dolar, kayma yemez.
        assert!(near(evs[0].price, 1.10200), "{}", evs[0].price);
    }

    #[test]
    fn short_sl_triggers_on_ask() {
        let mut e = engine();
        let r = SimOrderReq { sl: 1.10100, ..req(SimOrderKind::Sell, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        // BID sl'in üstüne çıksa bile ASK çıkmadıysa tetiklenmez.
        assert!(e.on_tick("EURUSD", 1.10099, 1.10099, 2_000).is_empty());
        let evs = e.on_tick("EURUSD", 1.10090, 1.10100, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::StopLoss);
        // SL bir STOP: ASK'ten + YUKARI kayma (short için aleyhte).
        assert!(evs[0].price > 1.10100, "{}", evs[0].price);
    }

    #[test]
    fn short_tp_triggers_on_ask() {
        let mut e = engine();
        let r = SimOrderReq { tp: 1.09900, ..req(SimOrderKind::Sell, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.on_tick("EURUSD", 1.09891, 1.09901, 2_000).is_empty());
        let evs = e.on_tick("EURUSD", 1.09890, 1.09900, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::TakeProfit);
    }

    #[test]
    fn sl_wins_when_same_tick_hits_both() {
        // LONG'da iki koşul da BID'e bakar: `bid <= sl` ve `bid >= tp`. İkisi
        // birden ancak `tp <= sl` iken doğru olabilir, yani düzgün kurulmuş
        // bir pozisyonda beraberliğin TEK yolu SL ile TP'nin aynı fiyatta
        // olmasıdır. `stops_level` 0 iken broker (ve MT5) bunu kabul eder:
        // SL de TP de güncel fiyatın tam üstüne kurulabilir.
        let mut e = engine();
        let r = SimOrderReq { sl: 1.10000, tp: 1.10000, ..req(SimOrderKind::Buy, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());

        // bid == sl == tp → hem `bid <= sl` hem `bid >= tp` DOĞRU.
        let evs = e.on_tick("EURUSD", 1.10000, 1.10010, 2_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::StopLoss, "ayni tick ikisini de vurursa SL kazanir");
        // SL kazandığı için dolum kaymayı da yer: TP kazansaydı fiyat tam
        // 1.10000 olurdu.
        assert!(near(evs[0].price, 1.09999), "{}", evs[0].price);
        assert!(e.balance() < SimConfig::default().balance, "kotumser taraf zarar yazar");
    }

    #[test]
    fn sl_wins_when_same_tick_hits_both_on_short() {
        // SHORT aynası: iki koşul da ASK'e bakar (`ask >= sl`, `ask <= tp`).
        let mut e = engine();
        let r = SimOrderReq { sl: 1.10010, tp: 1.10010, ..req(SimOrderKind::Sell, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        let evs = e.on_tick("EURUSD", 1.10000, 1.10010, 2_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::StopLoss);
    }

    #[test]
    fn sim_inverted_sltp_cannot_be_placed() {
        // Beraberliğin başka türlü kurulamamasının sebebi bu: LONG'da SL
        // piyasanın ÜSTÜNE, TP ALTINA konamaz. Kural olmasaydı "önce hangisi
        // doldu" sorusu her tick'te sorulurdu.
        let mut e = engine();
        let r = SimOrderReq { sl: 1.10100, ..req(SimOrderKind::Buy, 0.10) };
        assert_eq!(e.place(&r, &eurusd(), 1.10000, 1.10010).retcode(), retcode::INVALID_STOPS);
        let r = SimOrderReq { tp: 1.09900, ..req(SimOrderKind::Buy, 0.10) };
        assert_eq!(e.place(&r, &eurusd(), 1.10000, 1.10010).retcode(), retcode::INVALID_STOPS);
    }

    #[test]
    fn sl_wins_over_tp_on_a_gap_tick() {
        // Boşluk: LONG sl=1.09900 tp=1.10200 iken fiyat bir tick'te
        // 1.09800'e düşerse yalnızca SL geçerlidir; ama önce TP'yi geçmiş
        // bir yol da hayal edilebilir. Motor tick içi yolu bilmediği için
        // daima kötümser tarafı seçer.
        let mut e = engine();
        let r = SimOrderReq { sl: 1.09900, tp: 1.10200, ..req(SimOrderKind::Buy, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        let evs = e.on_tick("EURUSD", 1.09800, 1.09810, 2_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::StopLoss);
        // Boşlukta SL fiyatından KÖTÜ dolar — asıl mesele bu.
        assert!(evs[0].price < 1.09900, "bosluk dolumu SL'den kotu olmali: {}", evs[0].price);
    }

    // --- kayma --------------------------------------------------------------

    #[test]
    fn sim_slippage_is_always_adverse() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let buy_fill = out.events()[2].price;
        assert!(buy_fill > 1.10010, "alis dolumu ASK'ten yuksek olmali: {buy_fill}");
        assert!(near(buy_fill, 1.10011));

        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Sell, 0.10), &eurusd(), 1.10000, 1.10010);
        let sell_fill = out.events()[2].price;
        assert!(sell_fill < 1.10000, "satis dolumu BID'den dusuk olmali: {sell_fill}");
        assert!(near(sell_fill, 1.09999));
    }

    #[test]
    fn sim_slippage_default_is_not_zero() {
        // Sıfır varsayılan bir GERİLEME olur: motor kaymayı modellediğini
        // söyleyip iyimser dolum üretirdi.
        assert!(SimConfig::default().slippage_points > 0.0);
        assert!(near(SimConfig::default().slippage_points, DEFAULT_SLIPPAGE_POINTS));

        // Varsayılan yapılandırmayla açılan pozisyon spread'in ÖTESİNDE dolar.
        let mut e = SimEngine::new(SimConfig::default());
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        assert!(out.events()[2].price > 1.10010);
    }

    #[test]
    fn sim_slippage_scales_with_configured_points() {
        let mut e = SimEngine::new(SimConfig { slippage_points: 20.0, ..SimConfig::default() });
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        assert!(near(out.events()[2].price, 1.10030), "{}", out.events()[2].price);
    }

    #[test]
    fn sim_slippage_applies_to_close_too() {
        let mut e = SimEngine::new(SimConfig { slippage_points: 10.0, ..SimConfig::default() });
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let ticket = out.events()[2].position;
        let closed = e.close(ticket, 0.0, 1.10000, 1.10010);
        // LONG kapanışı BID'den, kayma AŞAĞI.
        assert!(near(closed.events()[2].price, 1.09990), "{}", closed.events()[2].price);
    }

    // --- stops_level --------------------------------------------------------

    #[test]
    fn sim_stops_level_violation_is_rejected_with_10016() {
        let mut e = engine();
        // stops_level 100 point: SL bid'den en az 0.00100 uzakta olmalı.
        let r = SimOrderReq { sl: 1.09950, ..req(SimOrderKind::Buy, 0.10) };
        let out = e.place(&r, &strict(), 1.10000, 1.10010);
        assert_eq!(out.retcode(), retcode::INVALID_STOPS, "{:?}", out.reason());
        assert!(!out.is_accepted());

        // Tam sınırda (100 point) kabul edilmeli — kayan nokta toleransı.
        let r = SimOrderReq { sl: 1.09900, ..req(SimOrderKind::Buy, 0.10) };
        assert!(e.place(&r, &strict(), 1.10000, 1.10010).is_accepted());
    }

    #[test]
    fn sim_stops_level_applies_to_pending_price() {
        let mut e = engine();
        // BUY_LIMIT ask'in 50 point altında — 100 point isteniyordu.
        let r = SimOrderReq { price: 1.09960, ..req(SimOrderKind::BuyLimit, 0.10) };
        let out = e.place(&r, &strict(), 1.10000, 1.10010);
        assert_eq!(out.retcode(), retcode::INVALID_STOPS);

        let r = SimOrderReq { price: 1.09910, ..req(SimOrderKind::BuyLimit, 0.10) };
        assert!(e.place(&r, &strict(), 1.10000, 1.10010).is_accepted());
    }

    #[test]
    fn sim_pending_on_wrong_side_is_rejected_even_without_stops_level() {
        let mut e = engine();
        // BUY_LIMIT piyasanın ÜSTÜNDE — MT5 de 10016 verir.
        let r = SimOrderReq { price: 1.10100, ..req(SimOrderKind::BuyLimit, 0.10) };
        assert_eq!(
            e.place(&r, &eurusd(), 1.10000, 1.10010).retcode(),
            retcode::INVALID_STOPS
        );
    }

    #[test]
    fn sim_modify_sltp_respects_stops_level() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &strict(), 1.10000, 1.10010);
        let ticket = out.ticket().unwrap();
        let bad = e.modify_sltp(ticket, 1.09990, 0.0, &strict(), 1.10000, 1.10010);
        assert_eq!(bad.retcode(), retcode::INVALID_STOPS);
        let ok = e.modify_sltp(ticket, 1.09800, 0.0, &strict(), 1.10000, 1.10010);
        assert!(ok.is_accepted());
        assert!(near(e.positions(&PriceBook::new())[0].sl, 1.09800));
    }

    // --- marjin -------------------------------------------------------------

    #[test]
    fn sim_insufficient_margin_is_rejected_with_10019() {
        // Bakiye 100, kaldıraç 100: 1 lot EURUSD ≈ 1100 marjin ister.
        let mut e = SimEngine::new(SimConfig { balance: 100.0, ..SimConfig::default() });
        let out = e.place(&req(SimOrderKind::Buy, 1.00), &eurusd(), 1.10000, 1.10010);
        assert_eq!(out.retcode(), retcode::NO_MONEY, "{:?}", out.reason());

        // 0.01 lot ≈ 11 marjin — bu geçer.
        let out = e.place(&req(SimOrderKind::Buy, 0.01), &eurusd(), 1.10000, 1.10010);
        assert!(out.is_accepted(), "{:?}", out.reason());
    }

    #[test]
    fn sim_margin_is_locked_and_released() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let ticket = out.ticket().unwrap();
        let acc = e.account(&PriceBook::new());
        // 0.1 × 100000 × 1.10011 / 100
        assert!((acc.margin - 110.011).abs() < 0.01, "{acc:?}");
        assert!(acc.margin_free < acc.equity);
        e.close(ticket, 0.0, 1.10000, 1.10010);
        assert!(near(e.account(&PriceBook::new()).margin, 0.0));
    }

    #[test]
    fn sim_pending_rejected_at_trigger_when_margin_gone() {
        let mut e = SimEngine::new(SimConfig { balance: 250.0, ..SimConfig::default() });
        // Bekleyen emir kurulurken para yetiyor (≈110).
        let r = SimOrderReq { price: 1.09900, id: "p1".into(), ..req(SimOrderKind::BuyLimit, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        // Araya iki piyasa emri girip serbest marjini tüketiyor.
        assert!(e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010).is_accepted());
        let evs = e.on_tick("EURUSD", 1.09890, 1.09900, 2_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, SimEventKind::Rejected);
        assert_eq!(evs[0].retcode, retcode::NO_MONEY);
        assert!(e.orders().is_empty(), "reddedilen bekleyen emir iptal edilir");
    }

    #[test]
    fn sim_zero_leverage_disables_margin_model() {
        let mut e = SimEngine::new(SimConfig { balance: 1.0, leverage: 0, ..SimConfig::default() });
        assert!(e.place(&req(SimOrderKind::Buy, 10.0), &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(near(e.account(&PriceBook::new()).margin, 0.0));
    }

    // --- hacim --------------------------------------------------------------

    #[test]
    fn sim_volume_off_step_is_rejected() {
        let mut e = engine();
        // 0.015, adım 0.01'e oturmuyor.
        let out = e.place(&req(SimOrderKind::Buy, 0.015), &eurusd(), 1.10000, 1.10010);
        assert_eq!(out.retcode(), retcode::INVALID_VOLUME, "{:?}", out.reason());
        assert!(e.positions(&PriceBook::new()).is_empty(), "reddedilen emir pozisyon acmaz");
    }

    #[test]
    fn sim_volume_bounds_are_rejected() {
        let mut e = engine();
        assert_eq!(
            e.place(&req(SimOrderKind::Buy, 0.001), &eurusd(), 1.10000, 1.10010).retcode(),
            retcode::INVALID_VOLUME
        );
        assert_eq!(
            e.place(&req(SimOrderKind::Buy, 200.0), &eurusd(), 1.10000, 1.10010).retcode(),
            retcode::INVALID_VOLUME
        );
        assert_eq!(
            e.place(&req(SimOrderKind::Buy, 0.0), &eurusd(), 1.10000, 1.10010).retcode(),
            retcode::INVALID_VOLUME
        );
    }

    #[test]
    fn sim_volume_on_step_survives_float_dust() {
        // 0.07 / 0.01 = 6.999999999999999 — naif bir denetim bunu reddederdi.
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.07), &eurusd(), 1.10000, 1.10010);
        assert!(out.is_accepted(), "{:?}", out.reason());
        assert!(near(e.positions(&PriceBook::new())[0].volume, 0.07));
    }

    // --- belirlenimcilik ----------------------------------------------------

    /// Aynı senaryoyu sıfırdan bir motorda oynatır.
    fn scripted() -> (Vec<SimEvent>, SimAccount, Vec<SimPosition>) {
        let mut e = engine();
        let mut evs = Vec::new();
        let ticks: [(f64, f64, i64); 5] = [
            (1.10000, 1.10010, 1_000),
            (1.10050, 1.10060, 2_000),
            (1.09950, 1.09960, 3_000),
            (1.09880, 1.09890, 4_000),
            (1.10300, 1.10310, 5_000),
        ];
        for (i, (bid, ask, ms)) in ticks.iter().enumerate() {
            evs.extend(e.on_tick("EURUSD", *bid, *ask, *ms));
            if i == 0 {
                let r = SimOrderReq {
                    sl: 1.09900,
                    tp: 1.10500,
                    time_msc: *ms,
                    ..req(SimOrderKind::Buy, 0.10)
                };
                evs.extend(e.place(&r, &eurusd(), *bid, *ask).events().to_vec());
                let p = SimOrderReq {
                    id: "p".into(),
                    price: 1.10200,
                    time_msc: *ms,
                    ..req(SimOrderKind::BuyStop, 0.20)
                };
                evs.extend(e.place(&p, &eurusd(), *bid, *ask).events().to_vec());
            }
        }
        let book = e.prices().clone();
        (evs, e.account(&book), e.positions(&book))
    }

    #[test]
    fn sim_is_deterministic_across_runs() {
        let (a_ev, a_acc, a_pos) = scripted();
        let (b_ev, b_acc, b_pos) = scripted();
        assert_eq!(a_ev, b_ev, "ayni tick dizisi ayni olaylari uretmeli");
        assert_eq!(a_acc, b_acc, "ayni tick dizisi ayni hesabi uretmeli");
        assert_eq!(a_pos, b_pos, "ayni tick dizisi ayni pozisyonlari uretmeli");
        // Biletler artan sayaçtan gelir; rastgele DEĞİL. Senaryonun bilet
        // dağıtımı: 1 = piyasa emri (= pozisyon), 2 = onun deal'i,
        // 3 = bekleyen emir, 4/5 = SL kapanışının deal'i ve emri,
        // 6 = tetiklenen bekleyen emrin deal'i.
        let orders: Vec<u64> = a_ev.iter().filter(|e| e.order > 0).map(|e| e.order).collect();
        assert_eq!(orders, vec![1, 1, 3, 3, 5, 3], "{a_ev:#?}");
        let deals: Vec<u64> = a_ev.iter().filter(|e| e.deal > 0).map(|e| e.deal).collect();
        assert_eq!(deals, vec![2, 4, 6], "{a_ev:#?}");
        // Tetiklenen dolumun fiyatı da sabit: ask + 1 point kayma.
        let fill = a_ev.last().expect("son olay tetiklenen dolum");
        assert_eq!(fill.cause, SimCause::PendingTriggered);
        assert!(near(fill.price, 1.10311), "{}", fill.price);
    }

    #[test]
    fn sim_tickets_are_sequential_from_one() {
        let mut e = engine();
        let a = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        // 1 = pozisyon/emir bileti, 2 = deal bileti.
        assert_eq!(a.ticket(), Some(1));
        let b = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        assert_eq!(b.ticket(), Some(3));
    }

    // --- bakiye / kâr -------------------------------------------------------

    #[test]
    fn sim_balance_moves_by_realized_profit_on_close() {
        let mut e = engine();
        let start = e.balance();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let entry = out.events()[2].price; // 1.10011
        let ticket = out.events()[2].position;
        assert!(near(e.balance(), start), "acilis bakiyeyi degistirmez");

        // 100 point yukarıda kapat.
        let closed = e.close(ticket, 0.0, 1.10100, 1.10110);
        let exit = closed.events()[2].price; // 1.10099
        let expect = (exit - entry) * 0.10 * 100_000.0;
        assert!(near(e.balance(), start + expect), "{} vs {}", e.balance(), start + expect);
        assert!(near(closed.events()[2].profit, expect));
        // Spread + iki kayma yüzünden 100 point'lik hareketin tamamı cebe
        // girmez — simülatörün varlık sebebi tam olarak bu fark.
        assert!(expect < 100.0, "spread ve kayma dusulmeli: {expect}");
    }

    #[test]
    fn sim_balance_moves_when_sl_closes_position() {
        let mut e = engine();
        let start = e.balance();
        let r = SimOrderReq { sl: 1.09900, ..req(SimOrderKind::Buy, 0.10) };
        let out = e.place(&r, &eurusd(), 1.10000, 1.10010);
        let entry = out.events()[2].price;
        let evs = e.on_tick("EURUSD", 1.09900, 1.09910, 2_000);
        assert_eq!(evs.len(), 1);
        let expect = (evs[0].price - entry) * 0.10 * 100_000.0;
        assert!(expect < 0.0, "SL zarar yazmali: {expect}");
        assert!(near(e.balance(), start + expect));
        assert!(near(e.account(&PriceBook::new()).equity, e.balance()));
    }

    #[test]
    fn sim_partial_close_keeps_rest_open() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let ticket = out.ticket().unwrap();
        let closed = e.close(ticket, 0.04, 1.10100, 1.10110);
        assert!(closed.is_accepted());
        let pos = e.positions(&PriceBook::new());
        assert_eq!(pos.len(), 1);
        assert!(near(pos[0].volume, 0.06), "{}", pos[0].volume);
        // Marjin oranında serbest bırakıldı.
        assert!((pos[0].margin - 66.0066).abs() < 0.01, "{}", pos[0].margin);
    }

    #[test]
    fn sim_short_profit_has_the_right_sign() {
        let mut e = engine();
        let start = e.balance();
        let out = e.place(&req(SimOrderKind::Sell, 0.10), &eurusd(), 1.10000, 1.10010);
        let entry = out.events()[2].price;
        let closed = e.close(out.ticket().unwrap(), 0.0, 1.09900, 1.09910);
        let exit = closed.events()[2].price;
        assert!(exit > 1.09910, "SHORT kapanisi ASK'ten + kayma: {exit}");
        let expect = (entry - exit) * 0.10 * 100_000.0;
        assert!(expect > 0.0, "dusen fiyatta SHORT kar yazar: {expect}");
        assert!(near(e.balance(), start + expect));
    }

    // --- olay dizisi --------------------------------------------------------

    #[test]
    fn sim_event_sequence_is_queued_ack_txn_and_all_marked_sim() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let evs = out.events();
        assert_eq!(evs.len(), 3);
        assert_eq!(evs[0].kind, SimEventKind::Queued);
        assert_eq!(evs[0].retcode, 0, "queued retcode tasimaz");
        assert_eq!(evs[1].kind, SimEventKind::Ack);
        assert_eq!(evs[1].retcode, retcode::PLACED);
        assert_eq!(evs[2].kind, SimEventKind::Txn);
        assert_eq!(evs[2].retcode, retcode::DONE);
        assert!(evs.iter().all(|e| e.sim), "her olay sim: true tasimali");
        assert!(evs.iter().all(|e| e.id == "r1" && e.client_id == 7));
        // `ack` dolum DEĞİL: fiyat/hacim taşımaz.
        assert_eq!(evs[1].price, 0.0);
        assert_eq!(evs[1].volume, 0.0);
    }

    #[test]
    fn sim_triggered_events_carry_origin_id_and_client() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.09900, id: "pend-9".into(), ..req(SimOrderKind::BuyLimit, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        let evs = e.on_tick("EURUSD", 1.09890, 1.09900, 2_000);
        assert_eq!(evs[0].id, "pend-9", "tetiklenen dolum emri acan istegin kimligini tasir");
        assert_eq!(evs[0].client_id, 7);
        assert!(evs[0].sim);
        assert_eq!(evs[0].time_msc, 2_000);
    }

    #[test]
    fn sim_rejects_when_there_is_no_quote() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 0.0, 0.0);
        assert_eq!(out.retcode(), retcode::PRICE_OFF);
        // Bozuk tick hiçbir şeyi tetiklemez.
        assert!(e.on_tick("EURUSD", 0.0, 0.0, 1).is_empty());
    }

    // --- iptal / diğer semboller -------------------------------------------

    #[test]
    fn sim_cancel_removes_pending_order() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.09900, ..req(SimOrderKind::BuyLimit, 0.10) };
        let ticket = e.place(&r, &eurusd(), 1.10000, 1.10010).ticket().unwrap();
        let out = e.cancel(ticket);
        assert!(out.is_accepted());
        assert_eq!(out.events()[2].cause, SimCause::Cancel);
        assert!(e.orders().is_empty());
        // İkinci iptal: bilet yok.
        assert_eq!(e.cancel(ticket).retcode(), retcode::INVALID);
    }

    #[test]
    fn sim_tick_of_another_symbol_does_not_trigger() {
        let mut e = engine();
        let r = SimOrderReq { price: 1.09900, ..req(SimOrderKind::BuyLimit, 0.10) };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.on_tick("GBPUSD", 1.00000, 1.00010, 2_000).is_empty());
        assert_eq!(e.orders().len(), 1);
    }

    #[test]
    fn sim_stop_limit_arms_a_limit_order_instead_of_filling() {
        let mut e = engine();
        let r = SimOrderReq {
            price: 1.10100,
            stoplimit: 1.10050,
            ..req(SimOrderKind::BuyStopLimit, 0.10)
        };
        assert!(e.place(&r, &eurusd(), 1.10000, 1.10010).is_accepted());
        // STOP tarafı tetiklendi → dolum YOK, limit emri kuruldu.
        let evs = e.on_tick("EURUSD", 1.10090, 1.10100, 2_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::StopLimitArmed);
        assert!(e.positions(&PriceBook::new()).is_empty());
        assert_eq!(e.orders()[0].kind, "buy_limit");
        // Limit fiyatına inince dolar.
        let evs = e.on_tick("EURUSD", 1.10040, 1.10050, 3_000);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].cause, SimCause::PendingTriggered);
        assert_eq!(e.positions(&PriceBook::new()).len(), 1);
    }

    #[test]
    fn sim_positions_are_marked_from_the_closing_side() {
        let mut e = engine();
        let out = e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010);
        let entry = out.events()[2].price;
        let mut book = PriceBook::new();
        book.set("EURUSD", 1.10100, 1.10110, 2_000);
        let pos = e.positions(&book);
        assert!(near(pos[0].price_current, 1.10100), "LONG BID'den degerlenir");
        assert!(near(pos[0].profit, (1.10100 - entry) * 0.10 * 100_000.0));
        let acc = e.account(&book);
        assert!(near(acc.equity, acc.balance + acc.profit));
        assert!(acc.margin_level > 0.0);
    }

    #[test]
    fn sim_close_event_belongs_to_the_closed_positions_client() {
        // Gerileme testi: tam kapanışta pozisyon listeden silinince o
        // indekste BAŞKA pozisyon kalır. Kimlik silmeden önce alınmazsa
        // kapanış olayı yanlış istemciye damgalanır — çok istemcili
        // daemon'da bir müşterinin işlemi başkasının hesabında görünürdü.
        let mut e = engine();
        let first = SimOrderReq { id: "a".into(), client_id: 11, ..req(SimOrderKind::Buy, 0.10) };
        let second = SimOrderReq { id: "b".into(), client_id: 22, ..req(SimOrderKind::Buy, 0.10) };
        let t1 = e.place(&first, &eurusd(), 1.10000, 1.10010).ticket().unwrap();
        assert!(e.place(&second, &eurusd(), 1.10000, 1.10010).is_accepted());

        let out = e.close(t1, 0.0, 1.10000, 1.10010);
        let txn = &out.events()[2];
        assert_eq!(txn.client_id, 11, "kapanan pozisyonun istemcisi");
        assert_eq!(txn.id, "a", "kapanan pozisyonu acan istegin kimligi");
        assert_eq!(txn.position, t1);
        // Hayatta kalan pozisyon dokunulmadan duruyor.
        let rest = e.positions(&PriceBook::new());
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].client_id, 22);
    }

    #[test]
    fn sim_hedging_keeps_both_directions_open() {
        let mut e = engine();
        assert!(e.place(&req(SimOrderKind::Buy, 0.10), &eurusd(), 1.10000, 1.10010).is_accepted());
        assert!(e.place(&req(SimOrderKind::Sell, 0.10), &eurusd(), 1.10000, 1.10010).is_accepted());
        assert_eq!(e.positions(&PriceBook::new()).len(), 2, "netlesme yok, hedging");
    }
}
