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
    /// `Some` ise **REPLAY** kipi: akış diskteki kayıttan geliyor.
    ///
    /// Bu alan iki şeyi birden yönetir ve ikisi de güvenlik meselesidir:
    ///
    /// 1. `hello.mode` `"replay"` olur ve emir olayları `sim: true` taşır —
    ///    istemci replay'i canlı SANMAMALIDIR.
    /// 2. `Some` iken paylaşılan belleğe **hiçbir komut yazılmaz**; emirler
    ///    [`Sim`] içinde simüle edilir. Bu sınır [`dispatch`] içinde ayrıca
    ///    bir kez daha zorlanır: kazara gerçek emir gitmesi kabul edilemez.
    pub replay: Option<Arc<Replay>>,
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
}

/// Replay kipinin bağlamı.
///
/// Kaydı gerçekten oynatan motor ayrı bir modüldedir; burası sunucunun o
/// kipte bilmesi gereken her şeyi tutar.
pub struct Replay {
    /// Kayıtta bulunan örnek adları (sıralı).
    ///
    /// `hello.instances` bunları ilan eder: oynatım motoru sembol tablosunu
    /// yazmadan bağlanan bir istemci de hangi kaydı dinlediğini görmeli.
    pub instances: Vec<String>,
    /// Örneği çözülemeyen simüle olaylarda kullanılan varsayılan ad.
    ///
    /// Sembollü işlemlerde `src` canlıdaki gibi sembolün örneğinden gelir;
    /// bu yalnızca hiçbir örneğe bağlanamayan durumların etiketi.
    pub primary: Arc<str>,
    /// İstenen kapsam (epoch ms). `hello` bunları ilan eder.
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    /// Simüle hesap/pozisyon/emir durumu.
    pub sim: Mutex<Sim>,
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
        balance: f64,
        done: tokio::sync::watch::Receiver<Option<ReplayEnd>>,
        start: Arc<crate::replay::StartGate>,
    ) -> Self {
        Self {
            instances: instances.to_vec(),
            // Kayıt boş örnek listesiyle yüklenemez; yine de bir ad
            // uydurmaktansa kipin adını kullanıyoruz.
            primary: Arc::from(instances.first().map(String::as_str).unwrap_or("replay")),
            from_ms,
            to_ms,
            sim: Mutex::new(Sim::new(balance)),
            done,
            start,
        }
    }

    /// Simüle olayın `src` alanı: sembolün çözüldüğü örnek, yoksa varsayılan.
    fn src_of(&self, instance: Option<&str>) -> Arc<str> {
        match instance {
            Some(i) if self.instances.iter().any(|k| k == i) => Arc::from(i),
            _ => self.primary.clone(),
        }
    }
}

/// Simüle pozisyon.
#[derive(Debug, Clone)]
pub struct SimPos {
    ticket: u64,
    /// Emri gönderen istemcinin wire kimliği (canlıdaki `magic` karşılığı).
    client_id: u64,
    symbol: String,
    buy: bool,
    volume: f64,
    price_open: f64,
    sl: f64,
    tp: f64,
    time_msc: i64,
    comment: String,
    /// Açılış anında okunan sözleşme büyüklüğü (kâr hesabı için).
    contract_size: f64,
}

/// Simüle bekleyen emir.
///
/// **Tetiklenme modellenmiyor**: fiyat seviyeye gelse bile bu emir kendi
/// kendine pozisyona dönüşmez. Bekleyen emrin gönderim/iptal yolunu test
/// etmek için var; dolum simülasyonu yalnızca piyasa emirlerindedir.
#[derive(Debug, Clone)]
pub struct SimOrd {
    ticket: u64,
    client_id: u64,
    symbol: String,
    kind: &'static str,
    volume: f64,
    price: f64,
    stoplimit: f64,
    sl: f64,
    tp: f64,
    time_setup_msc: i64,
    expiration: i64,
    comment: String,
}

/// Simüle broker durumu.
///
/// # Bu bir broker DEĞİLDİR
///
/// Modellenen tek şey, kayıttaki o anki bid/ask'ten yapılan dolum ve ondan
/// çıkan basit kâr/zarar. **Modellenmeyenler**: kayma, komisyon, swap, kur
/// çevrimi, marjin/teminat tamamlama, bekleyen emir tetiklenmesi, kısmi
/// dolum, likidite. Simüle dolum GERÇEK DOLUM DEĞİLDİR ve simüle bakiye bir
/// stratejinin gerçek getirisi sayılamaz.
#[derive(Debug)]
pub struct Sim {
    balance: f64,
    /// Bilet sayacı. 1'den başlar: 0 "bilet yok" demektir.
    next_ticket: u64,
    positions: Vec<SimPos>,
    pending: Vec<SimOrd>,
}

impl Sim {
    pub fn new(balance: f64) -> Self {
        Self { balance, next_ticket: 1, positions: Vec::new(), pending: Vec::new() }
    }

    /// Sıradaki bilet. Belirlenimci: aynı kayıt + aynı istekler → aynı
    /// biletler.
    fn ticket(&mut self) -> u64 {
        let t = self.next_ticket;
        self.next_ticket += 1;
        t
    }
}

/// Simüle kâr/zarar — **modelin tamamı budur**.
///
/// `(çıkış − giriş) × hacim × sözleşme_büyüklüğü`, yöne göre işaretli.
///
/// **Sözleşme büyüklüğü kayıtta YOKTUR** (`symbols-*.jsonl` bu alanı
/// taşımaz), yani pratikte 1 kabul edilir ve sonuç "fiyat farkı × hacim"
/// birimindedir — PARA DEĞİL. Sembol adına bakıp 100000 varsaymak sessizce
/// yanlış bir para değeri üretirdi; yanlış birimden yanlış para daha
/// tehlikeli.
///
/// Kâr, sembolün kotasyon para biriminde hesaplanır ve hesap para birimine
/// **çevrilmez**; komisyon, swap ve kayma yoktur. Bu kasıtlı: modellenmeyen
/// bir maliyeti tahmin etmektense hiç eklememek, sonucun ne olduğu konusunda
/// dürüst kalmayı sağlar.
fn sim_profit(buy: bool, entry: f64, exit: f64, volume: f64, contract_size: f64) -> f64 {
    let cs = if contract_size > 0.0 { contract_size } else { 1.0 };
    let dir = if buy { 1.0 } else { -1.0 };
    dir * (exit - entry) * volume * cs
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
/// Canlı ile replay arasındaki fark burada, **tek yerde** doğar: `mode` ve
/// kapsam alanları. Kanal adları, mesaj biçimleri ve alan adları aynı kalır —
/// sinyal sisteminin kod yolu iki kipte de aynı olmalı.
fn hello_msg(ctx: &Ctx, level: Level) -> ServerMsg {
    let replay = ctx.replay.as_deref();
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
        mode: if replay.is_some() { "replay" } else { "live" },
        instances,
        // Replay'de emir yürütme daima "açık": emirler reddedilmez, simüle
        // edilir. `--enable-trading` orada bir kapı değil, çünkü kapatacağı
        // bir risk yok — gerçek emir zaten hiç gönderilmiyor.
        trading: replay.is_some() || ctx.trading,
        // Piyasa verisi daima token'sız; yalnızca hesap ve emir kilitli.
        public_feed: true,
        auth_required_for_trading: ctx.token.is_some(),
        level: level.name(),
        replay_from_ms: replay.and_then(|r| r.from_ms),
        replay_to_ms: replay.and_then(|r| r.to_ms),
    }
}

/// Replay kipinde `candles` cevabının açıklaması; canlıda `None`.
fn replay_hist_note(ctx: &Ctx) -> Option<String> {
    ctx.replay.as_ref().map(|_| crate::replay::HIST_NOTE.to_owned())
}

fn replay_done(end: ReplayEnd) -> ServerMsg {
    ServerMsg::ReplayDone {
        ticks: end.ticks,
        // 0 "kayıt boştu" demek; sıfır zaman damgası göndermek uydurma olurdu.
        last_ms: (end.last_ms != 0).then_some(end.last_ms),
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
                // Replay kipinde emir olayının TEK kaynağı simüle motordur
                // (paylaşılan bellek okuyucusu hiç çalışmaz), canlıda ise
                // simüle olay hiç üretilmez. Bayrağı kipe bakarak basmak bu
                // yüzden doğru — ve tek bir yerde kalıyor.
                sim: ctx.replay.is_some(),
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
            candles_from_store(ctx, symbol, tf, count, "off", replay_hist_note(ctx))
        }

        // --- hesap yüzeyi: replay'de simüle durum ---
        //
        // Alan adları ve biçimler canlıyla AYNI; yalnızca değerlerin kaynağı
        // farklı. İstemci kodu iki kipte de aynı yolu yürümeli.
        ClientMsg::Account => match ctx.replay.as_deref() {
            Some(r) => vec![ServerMsg::Account { items: sim_accounts(ctx, r) }],
            None => vec![ServerMsg::Account { items: collect_accounts(ctx) }],
        },

        ClientMsg::Positions => {
            let (items, total, truncated) = match ctx.replay.as_deref() {
                Some(r) => sim_positions(ctx, r),
                None => collect_positions(ctx),
            };
            vec![ServerMsg::Positions { items, total, truncated }]
        }

        ClientMsg::Orders => {
            let (items, total, truncated) = match ctx.replay.as_deref() {
                Some(r) => sim_orders(ctx, r),
                None => collect_orders(ctx),
            };
            vec![ServerMsg::Orders { items, total, truncated }]
        }

        // --- emir yüzeyi ---
        //
        // Replay'de emirler REDDEDİLMEZ, simüle edilir: yoksa sinyal
        // sisteminin emir yolu hiç test edilemezdi. Bu dallanma aynı zamanda
        // güvenlik sınırıdır — sim yolunda `Cmd` üretilmez.
        ClientMsg::Order(req) => match ctx.replay.as_deref() {
            Some(r) => sim_submit_order(req, ctx, r),
            None => vec![submit_order(req, ctx)],
        },

        ClientMsg::Cancel { id, ticket } => match ctx.replay.as_deref() {
            Some(r) => sim_cancel(ctx, r, &id, ticket),
            None => vec![submit_simple(ctx, &id, action::REMOVE, ticket, 0.0, 0.0, 0.0)],
        },
        ClientMsg::Close { id, ticket, volume } => match ctx.replay.as_deref() {
            Some(r) => sim_close(ctx, r, &id, ticket, volume),
            None => {
                vec![submit_simple(ctx, &id, action::CLOSE_POSITION, ticket, volume, 0.0, 0.0)]
            }
        },
        ClientMsg::ModifySltp { id, ticket, sl, tp } => match ctx.replay.as_deref() {
            Some(r) => sim_modify(ctx, r, &id, ticket, sl, tp),
            None => vec![submit_simple(ctx, &id, action::SLTP, ticket, 0.0, sl, tp)],
        },
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
        return candles_from_store(ctx, symbol, tf, count, "off", replay_hist_note(ctx));
    };
    // Replay'de MT5 geçmişi YOKTUR: çekilecek bir terminal yok. Sebebi
    // söylemeden `hist: "off"` demek, istemcinin "Service'i açmayı unuttum"
    // sanmasına yol açardı.
    if ctx.replay.is_some() {
        return candles_from_store(ctx, symbol, tf, count, "off", replay_hist_note(ctx));
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
        sim: ctx.replay.is_some(),
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
    // GÜVENLİK SINIRI — replay kipinde paylaşılan belleğe HİÇBİR ŞEY yazılmaz.
    //
    // Bu noktaya gelinmesi bir programlama hatasıdır (`handle_client_msg`
    // simülasyon yoluna sapmalıydı). Panik atmıyoruz ama komutu da
    // göndermiyoruz: kayıttan oynatım sırasında canlı bir emrin çıkması,
    // bu projede kabul edilebilecek en pahalı kaza olurdu.
    if ctx.replay.is_some() {
        return rejected(ctx, id, "replay kipinde gercek emir gonderilmez");
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

// ---------------------------------------------------------------------------
// Simüle emir motoru — YALNIZCA replay kipinde
// ---------------------------------------------------------------------------
//
// Neden simüle ediyoruz da reddetmiyoruz: emirleri reddeden bir replay,
// sinyal sisteminin emir yolunu HİÇ test edemez — kaydın tek işe yaramadığı
// yer tam da en pahalı hataların çıktığı yer olurdu.
//
// Neden yine de gerçeğe benzemiyor: kayma, komisyon, swap, kur çevrimi,
// marjin ve bekleyen emir tetiklenmesi MODELLENMİYOR (bkz. [`Sim`]). Simüle
// dolum GERÇEK DOLUM DEĞİLDİR.
//
// Hangi doğrulamalar canlıyla aynı, hangileri değil:
//
// - AYNI olanlar — İSTEMCİNİN İSTEĞİNE dair olan her şey: bilinmeyen sembol,
//   geçersiz `side`/`type`, hacim ızgarası, fiyat ızgarası, çift kimlik.
//   İstemci iki kipte de aynı hatayı görmeli.
// - ATLANAN — BROKER BAĞLANTISINA dair olanlar: doldurma modu (`filling`),
//   hesap izinleri, canlı para kilidi. Simüle dolumun doldurma moduyla işi
//   yok; üstelik `filling_mask`/`exec_mode` kayıttan gelir ve eksik bir
//   kayıt yüzünden canlıda kabul edilen emri replay'de reddetmek, kod
//   yolunu test etmek yerine sahte bir hata üretirdi.

/// `TRADE_RETCODE_PLACED` — istek sunucuya iletildi. **Dolum DEĞİL.**
const RET_ACK: u32 = 10008;
/// `TRADE_RETCODE_DONE` — istek tamamlandı.
const RET_DONE: u32 = 10009;

/// Kayıttaki O ANKİ fiyat.
///
/// Kaynak canlıdakiyle aynı: sembol kaydına işlenmiş son tick. Replay'de bu
/// kaydı oynatım motoru besler. Fiyat yoksa `None` döner ve emir reddedilir —
/// **uydurulmuş bir fiyattan dolum yapmak**, simülasyonu sessizce yalancı
/// yapardı.
fn sim_last(ctx: &Ctx, symbol: &str) -> Option<crate::state::LastTick> {
    let filter = [symbol.to_owned()];
    ctx.registry.snapshot(&filter).into_iter().next().map(|(_, _, t)| t)
}

/// Fiyatı sembolün ızgarasına oturt; kayıt o sembolü tanımıyorsa olduğu gibi.
fn sim_norm(ctx: &Ctx, symbol: &str, p: f64) -> f64 {
    if p == 0.0 {
        return 0.0;
    }
    match ctx.registry.resolve_any(symbol).and_then(|(i, id)| ctx.registry.symbol(&i, id)) {
        Some(e) => sinyal_proto::normalize_price(p, e.tick_size, e.digits),
        None => p,
    }
}

/// Replay'de emir kapısı.
///
/// Canlı kapılardan geçmez (`--enable-trading`, canlı para kilidi, hesap
/// izinleri): hiçbiri burada bir riski kapatmıyor, çünkü gerçek emir zaten
/// gönderilmiyor. **Idempotency denetimi aynen korunur** — çift kimlik
/// davranışı kod yolunun parçasıdır ve replay'de de aynı görünmelidir.
fn sim_gate(ctx: &Ctx, id: &str) -> Result<u64, ServerMsg> {
    ctx.orders.register(id).map_err(|_| duplicate(ctx, id))
}

/// Simüle emir olayının değişken alanları.
#[derive(Default)]
struct SimEvt {
    order: u64,
    deal: u64,
    position: u64,
    volume: f64,
    price: f64,
}

/// Emir olaylarını **canlıyla aynı sırada** yayınla: `ack`(10008) → `txn`(10009).
///
/// Sıra tesadüf değil: istemci `ack`i dolum sanmamalı (bkz. [`OrderEvent`]).
/// Replay bu ayrımı da yeniden üretmezse, ayrımı yanlış kuran bir istemci
/// hatası ancak canlıda ortaya çıkardı.
fn sim_ack_then_txn(ctx: &Ctx, src: &Arc<str>, client_id: u64, txn: SimEvt) {
    let publish = |kind: &'static str, retcode: u32, e: &SimEvt| {
        let _ = ctx.events.send(FeedEvent::Order {
            instance: src.clone(),
            client_id,
            kind,
            retcode,
            order: e.order,
            deal: e.deal,
            position: e.position,
            volume: e.volume,
            price: e.price,
            comment: String::new(),
        });
    };
    publish("ack", RET_ACK, &SimEvt { order: txn.order, ..Default::default() });
    publish("txn", RET_DONE, &txn);
}

/// `queued` — isteğin kabul edildiği, canlıdaki ile aynı ilk cevap.
fn sim_queued(ctx: &Ctx, src: &Arc<str>, id: &str) -> ServerMsg {
    ServerMsg::Order(event(ctx, id, "queued", src))
}

/// Bir sembolün ait olduğu örnek — `src` alanı canlıdaki gibi buradan gelir.
fn sim_src(ctx: &Ctx, r: &Replay, symbol: &str) -> Arc<str> {
    r.src_of(ctx.registry.resolve_any(symbol).map(|(i, _)| i).as_deref())
}

fn sim_submit_order(req: OrderReq, ctx: &Ctx, r: &Replay) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, &req.id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    // Doğrulama sırası canlıyla AYNI: istemci iki kipte de aynı hatayı görsün.
    let (is_pending, otype) = match resolve_order_type(&req) {
        Ok(v) => v,
        Err(why) => return vec![rejected(ctx, &req.id, why)],
    };
    let Some((instance, symbol_id)) = ctx.registry.resolve_any(&req.symbol) else {
        return vec![rejected(ctx, &req.id, "sembol bulunamadi")];
    };
    let Some(entry) = ctx.registry.symbol(&instance, symbol_id) else {
        return vec![rejected(ctx, &req.id, "sembol kaydi okunamadi")];
    };
    let volume = match sinyal_proto::normalize_volume(
        req.volume,
        entry.volume_min,
        entry.volume_max,
        entry.volume_step,
    ) {
        Ok(v) => v,
        Err(e) => return vec![rejected(ctx, &req.id, &format!("hacim: {e}"))],
    };
    let norm = |p: f64| {
        if p == 0.0 {
            0.0
        } else {
            sinyal_proto::normalize_price(p, entry.tick_size, entry.digits)
        }
    };
    let buy = matches!(
        otype,
        order_type::BUY | order_type::BUY_LIMIT | order_type::BUY_STOP | order_type::BUY_STOP_LIMIT
    );
    let comment = if req.comment.is_empty() { short(&req.id) } else { req.comment.clone() };
    let src = r.src_of(Some(&instance));

    let mut sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());

    if is_pending {
        let price = norm(req.price);
        if price <= 0.0 {
            return vec![rejected(ctx, &req.id, "bekleyen emir fiyat ister")];
        }
        let ticket = sim.ticket();
        sim.pending.push(SimOrd {
            ticket,
            client_id: wire,
            symbol: req.symbol.clone(),
            kind: order_kind_name(otype),
            volume,
            price,
            stoplimit: norm(req.stoplimit),
            sl: norm(req.sl),
            tp: norm(req.tp),
            // Kaydın saati; yoksa 0 — yerel saat yazmak kaydın zamanıyla
            // çelişen bir damga üretirdi.
            time_setup_msc: sim_last(ctx, &req.symbol).map(|l| l.time_msc).unwrap_or(0),
            expiration: req.expiration,
            comment,
        });
        drop(sim);
        sim_ack_then_txn(
            ctx,
            &src,
            wire,
            SimEvt { order: ticket, volume, price, ..Default::default() },
        );
        return vec![sim_queued(ctx, &src, &req.id)];
    }

    // Piyasa emri: **alış ASK'ten, satış BID'den** dolar. Kayma yok.
    let Some(last) = sim_last(ctx, &req.symbol) else {
        return vec![rejected(ctx, &req.id, "kayitta bu an icin fiyat yok — simule dolum yapilamaz")];
    };
    let price = if buy { last.ask } else { last.bid };
    if price <= 0.0 {
        return vec![rejected(ctx, &req.id, "kayitta bu an icin fiyat yok — simule dolum yapilamaz")];
    }

    let ticket = sim.ticket();
    let deal = sim.ticket();
    sim.positions.push(SimPos {
        ticket,
        client_id: wire,
        symbol: req.symbol.clone(),
        buy,
        volume,
        price_open: price,
        sl: norm(req.sl),
        tp: norm(req.tp),
        time_msc: last.time_msc,
        comment,
        contract_size: entry.contract_size,
    });
    drop(sim);

    // MT5'te yeni pozisyonun kimliği, onu açan emrin biletidir.
    sim_ack_then_txn(ctx, &src, wire, SimEvt { order: ticket, deal, position: ticket, volume, price });
    vec![sim_queued(ctx, &src, &req.id)]
}

fn sim_close(ctx: &Ctx, r: &Replay, id: &str, ticket: u64, volume: f64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    let mut sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    let Some(ix) = sim.positions.iter().position(|p| p.ticket == ticket) else {
        return vec![rejected(ctx, id, "simule pozisyon bulunamadi")];
    };
    let (buy, entry_price, cs, pos_vol, symbol) = {
        let p = &sim.positions[ix];
        (p.buy, p.price_open, p.contract_size, p.volume, p.symbol.clone())
    };
    // Alış pozisyonu BID'den, satış pozisyonu ASK'ten kapanır.
    let price = match sim_last(ctx, &symbol) {
        Some(l) if buy && l.bid > 0.0 => l.bid,
        Some(l) if !buy && l.ask > 0.0 => l.ask,
        _ => return vec![rejected(ctx, id, "kayitta bu an icin fiyat yok — simule kapanis yapilamaz")],
    };

    // 0 veya fazlası "tamamını kapat" demek (canlıdaki ile aynı gevşeklik).
    let vol = if volume <= 0.0 || volume >= pos_vol { pos_vol } else { volume };
    sim.balance += sim_profit(buy, entry_price, price, vol, cs);
    if vol >= pos_vol {
        sim.positions.remove(ix);
    } else {
        sim.positions[ix].volume -= vol;
    }
    let ord = sim.ticket();
    let deal = sim.ticket();
    drop(sim);

    let src = sim_src(ctx, r, &symbol);
    sim_ack_then_txn(ctx, &src, wire, SimEvt { order: ord, deal, position: ticket, volume: vol, price });
    vec![sim_queued(ctx, &src, id)]
}

fn sim_cancel(ctx: &Ctx, r: &Replay, id: &str, ticket: u64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    let mut sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    let Some(ix) = sim.pending.iter().position(|o| o.ticket == ticket) else {
        return vec![rejected(ctx, id, "simule bekleyen emir bulunamadi")];
    };
    let o = sim.pending.remove(ix);
    drop(sim);

    let src = sim_src(ctx, r, &o.symbol);
    sim_ack_then_txn(
        ctx,
        &src,
        wire,
        SimEvt { order: o.ticket, volume: o.volume, price: o.price, ..Default::default() },
    );
    vec![sim_queued(ctx, &src, id)]
}

fn sim_modify(ctx: &Ctx, r: &Replay, id: &str, ticket: u64, sl: f64, tp: f64) -> Vec<ServerMsg> {
    let wire = match sim_gate(ctx, id) {
        Ok(w) => w,
        Err(m) => return vec![m],
    };
    let mut sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(p) = sim.positions.iter().position(|p| p.ticket == ticket) {
        let symbol = sim.positions[p].symbol.clone();
        let (nsl, ntp) = (sim_norm(ctx, &symbol, sl), sim_norm(ctx, &symbol, tp));
        sim.positions[p].sl = nsl;
        sim.positions[p].tp = ntp;
        drop(sim);
        let src = sim_src(ctx, r, &symbol);
        sim_ack_then_txn(ctx, &src, wire, SimEvt { position: ticket, ..Default::default() });
        return vec![sim_queued(ctx, &src, id)];
    }
    if let Some(o) = sim.pending.iter().position(|o| o.ticket == ticket) {
        let symbol = sim.pending[o].symbol.clone();
        let (nsl, ntp) = (sim_norm(ctx, &symbol, sl), sim_norm(ctx, &symbol, tp));
        sim.pending[o].sl = nsl;
        sim.pending[o].tp = ntp;
        drop(sim);
        let src = sim_src(ctx, r, &symbol);
        sim_ack_then_txn(ctx, &src, wire, SimEvt { order: ticket, ..Default::default() });
        return vec![sim_queued(ctx, &src, id)];
    }
    vec![rejected(ctx, id, "simule pozisyon/emir bulunamadi")]
}

/// Simüle pozisyonun o anki fiyatı (kapanış tarafından) — yoksa açılış fiyatı.
fn sim_mark(ctx: &Ctx, p: &SimPos) -> f64 {
    sim_last(ctx, &p.symbol)
        .map(|l| if p.buy { l.bid } else { l.ask })
        .filter(|v| *v > 0.0)
        .unwrap_or(p.price_open)
}

/// Sentetik hesap durumu.
///
/// Alan adları canlıyla birebir aynı; değerlerin **anlamı** farklı ve bu
/// `hello.mode == "replay"` ile ilan edilmiştir. `mode` daima `demo`: simüle
/// bir hesabın canlı para kilidinin yanlış tarafına düşmesi kabul edilemez.
///
/// Kayıtta birden çok örnek olsa bile **tek** hesap dönülür: simüle bakiye
/// tek bir cüzdandır, onu örnek başına tekrarlamak aynı parayı iki kez
/// saydırırdı.
fn sim_accounts(ctx: &Ctx, r: &Replay) -> Vec<AccountInfo> {
    let sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    // `+ 0.0`: boş bir f64 toplamı `-0.0` verir ve tel üzerinde `"profit":-0.0`
    // görünürdü. Sayısal olarak aynı ama "-0.00 kâr" gösteren bir istemci
    // arayüzü, olmayan bir zararı varmış gibi gösterir.
    let profit: f64 = sim
        .positions
        .iter()
        .map(|p| sim_profit(p.buy, p.price_open, sim_mark(ctx, p), p.volume, p.contract_size))
        .sum::<f64>()
        + 0.0;
    let equity = sim.balance + profit;
    vec![AccountInfo {
        src: r.primary.to_string(),
        login: 0,
        server: "replay".into(),
        company: "replay".into(),
        // Para birimi kayıtta yok; uydurmak yerine boş bırakılıyor.
        currency: String::new(),
        leverage: 0,
        balance: sim.balance,
        credit: 0.0,
        profit,
        equity,
        // Marjin modellenmiyor: 0 "marjin yok" değil "hesaplanmıyor" demek.
        margin: 0.0,
        margin_free: equity,
        margin_level: 0.0,
        mode: "demo",
        // Aynı sembolde birden çok pozisyon tutulabiliyor — davranış hedging.
        margin_mode: "hedging",
        so_mode: "unknown",
        margin_so_call: 0.0,
        margin_so_so: 0.0,
        can_trade: true,
        blocked_by: None,
        age_ms: 0,
    }]
}

fn sim_positions(ctx: &Ctx, r: &Replay) -> (Vec<PositionInfo>, u32, bool) {
    let sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    let items: Vec<PositionInfo> = sim
        .positions
        .iter()
        .map(|p| {
            let cur = sim_mark(ctx, p);
            PositionInfo {
                // Canlıdaki gibi: pozisyonun `src`'si sembolün örneği.
                src: sim_src(ctx, r, &p.symbol).to_string(),
                ticket: p.ticket,
                identifier: p.ticket,
                client_id: p.client_id,
                symbol: p.symbol.clone(),
                side: if p.buy { "buy" } else { "sell" },
                volume: p.volume,
                price_open: p.price_open,
                price_current: cur,
                sl: p.sl,
                tp: p.tp,
                profit: sim_profit(p.buy, p.price_open, cur, p.volume, p.contract_size),
                // Swap modellenmiyor; 0 burada "hesaplanmıyor" demek.
                swap: 0.0,
                time_msc: p.time_msc,
                comment: p.comment.clone(),
            }
        })
        .collect();
    let total = items.len() as u32;
    // Simüle liste hiç kesilmez: tavan yok, hepsi bellekte.
    (items, total, false)
}

fn sim_orders(ctx: &Ctx, r: &Replay) -> (Vec<OrderInfo>, u32, bool) {
    let sim = r.sim.lock().unwrap_or_else(|e| e.into_inner());
    let items: Vec<OrderInfo> = sim
        .pending
        .iter()
        .map(|o| OrderInfo {
            src: sim_src(ctx, r, &o.symbol).to_string(),
            ticket: o.ticket,
            client_id: o.client_id,
            symbol: o.symbol.clone(),
            kind: o.kind,
            volume_initial: o.volume,
            // Tetiklenme modellenmiyor: kalan hacim daima ilk hacim.
            volume_current: o.volume,
            price: o.price,
            stoplimit: o.stoplimit,
            sl: o.sl,
            tp: o.tp,
            time_setup_msc: o.time_setup_msc,
            expiration: o.expiration,
            comment: o.comment.clone(),
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
            replay: None,
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
        let (c_tx, c_rx) = std::sync::mpsc::channel::<Cmd>();
        ctx.cmd_tx.insert("mt5-1".into(), c_tx);
        let (done_tx, done_rx) = tokio::sync::watch::channel(None);
        ctx.replay = Some(Arc::new(Replay::new(
            &["mt5-1".to_string()],
            Some(1_700_000_000_000),
            Some(1_700_000_600_000),
            balance,
            done_rx,
            Arc::new(crate::replay::StartGate::new()),
        )));
        (ctx, c_rx, done_tx)
    }

    /// EURUSD'yi kayıttan gelmiş gibi tanıt ve bir fiyat yaz.
    fn seed_symbol(ctx: &Ctx, bid: f64, ask: f64) {
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
        ctx.registry.set_symbols("mt5-1", vec![e]);
        ctx.registry.update_last(
            "mt5-1",
            1,
            crate::state::LastTick { bid, ask, last: 0.0, time_msc: 1_700_000_000_500 },
        );
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
            comment: String::new(),
        };
        let msg = to_wire(&ev, &ctx).unwrap();
        match &msg {
            ServerMsg::Order(e) => assert!(!e.sim, "canli olay simule isaretlenemez"),
            other => panic!("beklenmeyen: {other:?}"),
        }
        let j = serde_json::to_string(&msg).unwrap();
        assert!(!j.contains("sim"), "canli olayda sim alani HIC olmamali: {j}");

        // Reddedilen canlı emirler de temiz olmalı.
        let j = serde_json::to_string(&rejected(&ctx, "o1", "test")).unwrap();
        assert!(!j.contains("sim"), "{j}");
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
}
