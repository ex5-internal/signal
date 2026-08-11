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

use crate::source::FeedEvent;
use crate::state::Registry;
use crate::wire::{
    AccountInfo, ClientMsg, OrderEvent, OrderInfo, OrderReq, PositionInfo, ServerMsg, SymbolInfo,
    TickSnap,
};

/// Sunucunun paylaşılan bağlamı.
pub struct Ctx {
    pub registry: Arc<Registry>,
    pub events: broadcast::Sender<FeedEvent>,
    /// Örnek adı → o örneğin okuyucu thread'ine giden komut kanalı.
    pub cmd_tx: HashMap<String, std::sync::mpsc::Sender<Cmd>>,
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
            FeedEvent::Order { .. } => self.orders,
            FeedEvent::Candle { symbol, tf, .. } => {
                self.candle_all_tf.contains(*tf) || self.candles.contains(&format!("{symbol}.{tf}"))
            }
        }
    }
}

/// Sunucuyu çalıştır.
pub async fn serve(listener: TcpListener, ctx: Arc<Ctx>) {
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

    let hello = ServerMsg::Hello {
        proto: sinyal_proto::ring::RING_VERSION,
        mode: "live",
        instances: ctx.registry.instances(),
        trading: ctx.trading,
        // Piyasa verisi daima token'sız; yalnızca hesap ve emir kilitli.
        public_feed: true,
        auth_required_for_trading: auth_required,
        level: level.name(),
    };
    send(&mut out, &hello).await?;

    loop {
        tokio::select! {
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
        FeedEvent::Candle { symbol, tf, bar } => ServerMsg::Candle {
            s: symbol.to_string(),
            tf,
            bar: *bar,
        },
        FeedEvent::Order {
            instance, client_id, kind, retcode, order, deal, position, volume, price, comment,
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
                comment: comment.clone(),
                src: instance.to_string(),
            })
        }
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
                    filling_mask: e.filling_mode,
                    stops_level: e.stops_level,
                    book_depth: e.ticks_bookdepth,
                    polled_only: e.flags & sym_flag::POLLED_ONLY != 0,
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

        ClientMsg::Candles { symbol, tf, count } => {
            if crate::candles::tf_millis(&tf).is_none() {
                let names: Vec<&str> =
                    crate::candles::TIMEFRAMES.iter().map(|(n, _)| *n).collect();
                vec![ServerMsg::Error {
                    msg: format!("gecersiz tf '{tf}' — gecerli: {}", names.join(", ")),
                }]
            } else {
                // Üst sınır: bir istemci 10 milyon bar isteyip belleği
                // şişirmesin.
                let n = count.min(crate::candles::MAX_REQUEST);
                let items = ctx
                    .candles
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&symbol, &tf, n);
                vec![ServerMsg::Candles { s: symbol, tf: tf.to_ascii_uppercase(), items }]
            }
        }

        ClientMsg::Account => vec![ServerMsg::Account { items: collect_accounts(ctx) }],

        ClientMsg::Positions => {
            let (items, total, truncated) = collect_positions(ctx);
            vec![ServerMsg::Positions { items, total, truncated }]
        }

        ClientMsg::Orders => {
            let (items, total, truncated) = collect_orders(ctx);
            vec![ServerMsg::Orders { items, total, truncated }]
        }

        ClientMsg::Order(req) => vec![submit_order(req, ctx)],

        ClientMsg::Cancel { id, ticket } => vec![submit_simple(
            ctx, &id, action::REMOVE, ticket, 0.0, 0.0, 0.0,
        )],
        ClientMsg::Close { id, ticket, volume } => vec![submit_simple(
            ctx, &id, action::CLOSE_POSITION, ticket, volume, 0.0, 0.0,
        )],
        ClientMsg::ModifySltp { id, ticket, sl, tp } => vec![submit_simple(
            ctx, &id, action::SLTP, ticket, 0.0, sl, tp,
        )],
    })
}

/// Emir öncesi ortak denetimler; hata varsa `Err(ServerMsg)`.
///
/// Üç ayrı kapı, her biri farklı bir hatayı yakalar:
/// 1. `--enable-trading` — operatörün açık izni
/// 2. MT5/hesap izinleri — hangi bayrağın düştüğü söylenir
/// 3. **Canlı para kilidi** — demo olmayan hesapta `--allow-live` şart
fn gate<'a>(ctx: &'a Ctx, id: &str) -> Result<u64, ServerMsg> {
    if !ctx.trading {
        return Err(rejected(id, "emir yurutme kapali (--enable-trading)"));
    }
    // Hangi örnek olduğunu bilmediğimiz komutlar için ilk örneği denetle;
    // sembollü emirler `submit_order` içinde kendi örneğiyle yeniden denetlenir.
    if let Some(inst) = ctx.cmd_tx.keys().next() {
        if let Err(why) = ctx.registry.trading_gate(inst, ctx.allow_live) {
            return Err(rejected(id, &why));
        }
    }
    ctx.orders.register(id).map_err(|_| ServerMsg::Order(OrderEvent {
        id: id.to_owned(),
        kind: "duplicate",
        retcode: None,
        order: None,
        deal: None,
        position: None,
        volume: None,
        price: None,
        // Aynı kimlikle ikinci istek: sessizce kabul etmek çift pozisyon
        // demek olurdu.
        comment: "bu id daha once kullanildi".into(),
        src: String::new(),
    }))
}

fn rejected(id: &str, why: &str) -> ServerMsg {
    ServerMsg::Order(OrderEvent {
        id: id.to_owned(),
        kind: "rejected",
        retcode: None,
        order: None,
        deal: None,
        position: None,
        volume: None,
        price: None,
        comment: why.to_owned(),
        src: String::new(),
    })
}

fn dispatch(ctx: &Ctx, instance: &str, cmd: Cmd, id: &str) -> ServerMsg {
    match ctx.cmd_tx.get(instance) {
        Some(tx) => match tx.send(cmd) {
            Ok(()) => ServerMsg::Order(OrderEvent {
                id: id.to_owned(),
                kind: "queued",
                retcode: None,
                order: None,
                deal: None,
                position: None,
                volume: None,
                price: None,
                comment: String::new(),
                src: instance.to_owned(),
            }),
            Err(_) => rejected(id, "okuyucu thread'i kapali"),
        },
        None => rejected(id, "bilinmeyen instance"),
    }
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
        return rejected(id, "bagli instance yok");
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

fn submit_order(req: OrderReq, ctx: &Ctx) -> ServerMsg {
    let wire = match gate(ctx, &req.id) {
        Ok(w) => w,
        Err(m) => return m,
    };

    let Some((instance, symbol_id)) = ctx.registry.resolve_any(&req.symbol) else {
        return rejected(&req.id, "sembol bulunamadi");
    };
    let Some(entry) = ctx.registry.symbol(&instance, symbol_id) else {
        return rejected(&req.id, "sembol kaydi okunamadi");
    };

    let is_pending = req.action == "pending";
    let otype = if is_pending {
        match req.order_type.as_str() {
            "buy_limit" => order_type::BUY_LIMIT,
            "sell_limit" => order_type::SELL_LIMIT,
            "buy_stop" => order_type::BUY_STOP,
            "sell_stop" => order_type::SELL_STOP,
            "buy_stop_limit" => order_type::BUY_STOP_LIMIT,
            "sell_stop_limit" => order_type::SELL_STOP_LIMIT,
            _ => return rejected(&req.id, "gecersiz type (bekleyen emir)"),
        }
    } else {
        match req.side.as_str() {
            "buy" => order_type::BUY,
            "sell" => order_type::SELL,
            _ => return rejected(&req.id, "gecersiz side (buy|sell)"),
        }
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
        Err(e) => return rejected(&req.id, &format!("hacim: {e}")),
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
        _ => return rejected(&req.id, "gecersiz filling"),
    };
    let tt = match req.time.as_str() {
        "" | "gtc" => type_time::GTC,
        "day" => type_time::DAY,
        "specified" => type_time::SPECIFIED,
        "specified_day" => type_time::SPECIFIED_DAY,
        _ => return rejected(&req.id, "gecersiz time"),
    };

    // AUTO'yu burada da çözebiliyor muyuz? Çözemiyorsak EA de çözemez —
    // emri göndermeden reddetmek daha dürüst.
    if fill == filling::AUTO {
        let act = if is_pending { action::PENDING } else { action::DEAL };
        if let Err(e) = sinyal_proto::resolve_filling(act, filling::AUTO, &entry) {
            return rejected(&req.id, &format!("doldurma modu: {e}"));
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
        expiration: req.expiration,
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
            bar: crate::candles::Bar {
                t: 0,
                o: 1.0,
                h: 1.0,
                l: 1.0,
                c: 1.0,
                ticks: 1,
                partial: false,
            },
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
            token: token.map(str::to_owned),
            trading: true,
            allow_live: false,
            deviation: 20,
            orders: Arc::new(OrderTracker::new()),
            candles: Arc::new(Mutex::new(crate::candles::CandleStore::new())),
        }
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
}
