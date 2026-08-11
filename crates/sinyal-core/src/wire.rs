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
    /// DİKKAT: geçmiş, daemon başladığı andan itibarendir — geriye dönük
    /// broker geçmişi YOKTUR. İlk bar `partial: true` ile işaretlenir.
    Candles {
        symbol: String,
        /// `M1` | `M5` | `M15` | `M30` | `H1` | `H4`
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

    /// `time` = `specified*` ise epoch saniye.
    #[serde(default)]
    pub expiration: i64,

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
        /// `live` (gerçek MT5) veya `mock` (simülatör).
        mode: &'static str,
        instances: Vec<String>,
        /// Emir yürütme açık mı.
        trading: bool,
        /// Piyasa verisi (tick/derinlik/mum/sembol) token'sız erişilebilir mi.
        public_feed: bool,
        /// Hesap ve emir işlemleri için token gerekiyor mu.
        auth_required_for_trading: bool,
        /// Bu bağlantının şu anki seviyesi: `public` | `trader`.
        level: &'static str,
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
        items: Vec<crate::candles::Bar>,
    },

    /// Bir mum kapandı (canlı).
    Candle {
        s: String,
        tf: &'static str,
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
    /// `SYMBOL_TRADE_EXEMODE` — doğru doldurma modunun belirleyicisi.
    pub exec_mode: u32,
    pub filling_mask: u32,
    pub stops_level: u32,
    /// Broker bu sembolde derinlik veriyor mu (0 = hayır).
    pub book_depth: u32,
    /// `true` ise bu sembol yalnızca timer taramasıyla toplanıyor ve
    /// ~10-16 ms'lik ek gecikme taşıyor.
    pub polled_only: bool,
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

/// Emir olayı.
///
/// **`kind` = `ack` emrin DOLDUĞU anlamına GELMEZ.** `OrderSendAsync` iki
/// aşamalı geri bildirim üretir: `ack` isteğin sunucuya iletildiğini,
/// `txn` gerçek yürütmeyi bildirir. Emri yalnızca `txn` + `retcode` 10009
/// geldiğinde dolmuş sayın.
#[derive(Debug, Clone, Serialize)]
pub struct OrderEvent {
    /// İstemcinin verdiği kimlik.
    pub id: String,
    /// `ack` | `txn` | `rejected` | `duplicate`
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
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub comment: String,
    pub src: String,
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
    fn lagged_is_reported_not_hidden() {
        // Fiyat akışında sessiz boşluk = yanlış karar.
        let j = serde_json::to_string(&ServerMsg::Lagged { dropped: 17 }).unwrap();
        assert!(j.contains(r#""t":"lagged""#));
        assert!(j.contains(r#""dropped":17"#));
    }
}
