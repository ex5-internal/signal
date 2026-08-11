//! Paylaşımlı bellek okuyucusu.
//!
//! # Neden oturumu tek thread sahipleniyor
//!
//! Halkalar SPSC'dir ve `Session` bunu çalışma zamanında denetler: oturuma
//! erişen ilk thread "sahip" olur, başka thread'den gelen işlem reddedilir.
//! Bu yüzden WebSocket görevlerinin doğrudan `push_cmd` çağırması **çalışmaz**
//! — emirler bir kanal üzerinden okuyucu thread'ine iletilir ve o gönderir.
//!
//! Bu aynı zamanda daha temiz bir tasarım: paylaşımlı belleğe tek bir yerden
//! dokunuluyor.

use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sinyal_bridge::Session;
use sinyal_proto::{book_type, Cmd, Tick};
use sinyal_shm::{qpc, qpc_delta_nanos};
use tokio::sync::broadcast;

use crate::join::OrderJoin;
use crate::state::{LastTick, Registry};

/// Abonelere yayılan olay.
#[derive(Debug, Clone)]
pub enum FeedEvent {
    Tick {
        instance: Arc<str>,
        symbol: Arc<str>,
        bid: f64,
        ask: f64,
        last: f64,
        time_msc: i64,
        /// EA'nın yakalamasından bu olayın üretilmesine kadar geçen süre.
        lat_us: u64,
    },
    Book {
        instance: Arc<str>,
        symbol: Arc<str>,
        time_msc: i64,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
    },
    Order {
        instance: Arc<str>,
        client_id: u64,
        kind: &'static str,
        retcode: u32,
        order: u64,
        deal: u64,
        position: u64,
        volume: f64,
        price: f64,
        comment: String,
    },
}

/// Okuyucu thread'inin durumu (teşhis).
#[derive(Debug, Clone, Copy, Default)]
pub struct ReaderStats {
    pub connected: bool,
    pub ticks: u64,
    pub books: u64,
    pub orders: u64,
    /// EA'nın halkaya yazamadığı (kaybettiği) tick sayısı.
    pub ea_tick_loss: u64,
    /// Okunmayı bekleyen tick sayısı — biz mi geride kalıyoruz.
    pub backlog: u64,
    /// Kimligi henuz cozulememis emir olayi sayisi.
    pub join_pending: u64,
    /// Geriye donuk atfedilen olay sayisi.
    pub join_late: u64,
    /// Hicbir emrimize baglanamayan (elle yapilmis) olay sayisi.
    pub join_unattributed: u64,
}

/// Bir örnek için okuyucu thread'i başlat.
///
/// Thread, EA gelene kadar bağlanmayı yeniden dener; bu beklenen bir
/// durumdur (çekirdek terminalden önce başlamış olabilir).
pub fn spawn_reader(
    instance: String,
    registry: Arc<Registry>,
    tx: broadcast::Sender<FeedEvent>,
    cmd_rx: Receiver<Cmd>,
    stats: Arc<std::sync::Mutex<ReaderStats>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("sinyal-rd-{instance}"))
        .spawn(move || reader_loop(instance, registry, tx, cmd_rx, stats))
        .expect("okuyucu thread'i başlatılamadı")
}

fn reader_loop(
    instance: String,
    registry: Arc<Registry>,
    tx: broadcast::Sender<FeedEvent>,
    cmd_rx: Receiver<Cmd>,
    stats: Arc<std::sync::Mutex<ReaderStats>>,
) {
    let inst: Arc<str> = Arc::from(instance.as_str());

    // EA'yı bekle. Bağlanamamak hata değil — terminal henüz açılmamış olabilir.
    let session = loop {
        match Session::open(&instance) {
            Ok(s) => {
                eprintln!("[{instance}] köprüye bağlanıldı");
                break s;
            }
            Err(e) => {
                eprintln!("[{instance}] bekleniyor: {e}");
                std::thread::sleep(Duration::from_secs(2));
                // Komut kuyruğu bu sırada dolabilir; boşaltıp reddedilmelerini
                // sağlıyoruz ki istemci sonsuza kadar beklemesin.
                while cmd_rx.try_recv().is_ok() {}
            }
        }
    };

    {
        let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
        s.connected = true;
    }

    // Sembol tablosunu ilk anda yükle — tick akışını isimlendirebilmek için.
    refresh_symbols(&session, &registry, &instance);
    let mut last_sym_refresh = Instant::now();

    let mut join = OrderJoin::new();
    let mut batch = vec![Tick::default(); 512];
    let mut idle_rounds: u32 = 0;

    loop {
        let mut did_work = false;

        // --- 1) Tick akışı ---
        // Güvenlik: bu thread oturumun sahibi; SPSC sözleşmesi korunuyor.
        let n = unsafe { session.pop_tick_batch(&mut batch) };
        if n > 0 {
            did_work = true;
            let now = qpc();
            for t in &batch[..n] {
                let Some(name) = registry.name_of(&instance, t.symbol_id) else {
                    // Sembol tablosu henüz yayımlanmamış olabilir; bir sonraki
                    // yenilemede isimlenecek. Tick'i atmak yerine atlıyoruz —
                    // isimsiz göndermek istemciyi yanıltırdı.
                    continue;
                };
                registry.update_last(
                    &instance,
                    t.symbol_id,
                    LastTick { bid: t.bid, ask: t.ask, last: t.last, time_msc: t.time_msc },
                );
                // Abone yoksa `send` hata döner — bu normal, yok sayılır.
                let _ = tx.send(FeedEvent::Tick {
                    instance: inst.clone(),
                    symbol: Arc::from(name.as_str()),
                    bid: t.bid,
                    ask: t.ask,
                    last: t.last,
                    time_msc: t.time_msc,
                    lat_us: qpc_delta_nanos(t.recv_qpc, now) / 1_000,
                });
            }
            let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
            s.ticks += n as u64;
        }

        // --- 2) Derinlik ---
        while let Some(b) = unsafe { session.pop_book() } {
            did_work = true;
            let Some(name) = registry.name_of(&instance, b.symbol_id) else { continue };
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for lv in b.levels() {
                match lv.kind {
                    book_type::BUY | book_type::BUY_MARKET => bids.push([lv.price, lv.volume_real]),
                    book_type::SELL | book_type::SELL_MARKET => asks.push([lv.price, lv.volume_real]),
                    // Bilinmeyen tür: reddetmek yerine atlıyoruz (MetaQuotes
                    // ileride enum'a değer ekleyebilir).
                    _ => {}
                }
            }
            // En iyi fiyat önce.
            bids.sort_by(|a, b| b[0].total_cmp(&a[0]));
            asks.sort_by(|a, b| a[0].total_cmp(&b[0]));
            let _ = tx.send(FeedEvent::Book {
                instance: inst.clone(),
                symbol: Arc::from(name.as_str()),
                time_msc: b.time_msc,
                bids,
                asks,
            });
            let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
            s.books += 1;
        }

        // --- 3) Emir sonuçları ---
        //
        // Ham olaylar geç-bağlama eşleştiricisinden geçer: EA `client_id`
        // atamaz (sıcak yolda tablo taraması yasak), kimlik burada kurulur.
        // Çözülemeyen olaylar kısa süre bekletilir ve bağ kurulunca geriye
        // dönük atfedilir.
        while let Some(r) = unsafe { session.pop_res() } {
            did_work = true;
            for ev in join.on_event(r) {
                emit_order(&tx, &inst, &ev);
                let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
                s.orders += 1;
            }
        }

        // Süresi dolan (hiç bağ kurulamayan) olayları kimliksiz yayımla —
        // atmıyoruz, "bize ait değil / elle yapılmış" olarak gidiyorlar.
        for ev in join.expire() {
            emit_order(&tx, &inst, &ev);
        }

        // --- 4) Giden emirler ---
        loop {
            match cmd_rx.try_recv() {
                Ok(cmd) => {
                    did_work = true;
                    // Güvenlik: bu thread oturumun sahibi.
                    if !unsafe { session.push_cmd(&cmd) } {
                        eprintln!(
                            "[{instance}] komut halkası dolu — emir gönderilemedi (client_id={})",
                            cmd.client_id
                        );
                    }
                }
                Err(TryRecvError::Empty) => break,
                // Gönderen taraf kapandı: kapanış zamanı.
                Err(TryRecvError::Disconnected) => return,
            }
        }

        // --- 5) Periyodik bakım ---
        if last_sym_refresh.elapsed() >= Duration::from_secs(1) {
            refresh_symbols(&session, &registry, &instance);

            // Durum görüntüsü: hesap + açık pozisyonlar + bekleyen emirler.
            // Aynı zamanda eşleştiriciyi besler — `POSITION_MAGIC` bizim
            // `client_id`'mizi taşıdığı için çekirdek yeniden başlasa bile
            // bağlar buradan yeniden kurulur (mutabakatın temeli).
            if let Some(st) = session.state() {
                join.seed_from_positions(&st.positions);
                join.seed_from_orders(&st.orders);
                if st.truncated {
                    eprintln!(
                        "[{instance}] UYARI: durum listesi kesildi (pozisyon {}/{}, emir {}/{})",
                        st.positions.len(),
                        st.pos_total,
                        st.orders.len(),
                        st.ord_total
                    );
                }
                registry.set_state(&instance, st);
            }

            last_sym_refresh = Instant::now();
            let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
            s.ea_tick_loss = session.tick_push_failures();
            s.backlog = session.tick_backlog();
            s.join_pending = join.pending_len() as u64;
            s.join_late = join.resolved_late;
            s.join_unattributed = join.unattributed;
        }

        // --- 6) Bekleme stratejisi ---
        // Önce kısa süre dönerek bekle (düşük gecikme), sonra uykuya geç
        // (boş piyasada CPU yakmamak için). Sıkı döngü p50'yi düşürür ama
        // seans dışında bir çekirdeği %100'de tutardı.
        if did_work {
            idle_rounds = 0;
        } else {
            idle_rounds = idle_rounds.saturating_add(1);
            if idle_rounds < 20_000 {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(Duration::from_micros(200));
            }
        }
    }
}

/// Bir emir olayini abonelere yayinla.
fn emit_order(tx: &broadcast::Sender<FeedEvent>, inst: &Arc<str>, r: &sinyal_proto::Res) {
    let kind = match r.kind {
        sinyal_proto::res_kind::SEND_ACK => "ack",
        sinyal_proto::res_kind::TRADE_TXN => "txn",
        sinyal_proto::res_kind::REJECTED => "rejected",
        sinyal_proto::res_kind::DUPLICATE => "duplicate",
        _ => "unknown",
    };
    let _ = tx.send(FeedEvent::Order {
        instance: inst.clone(),
        client_id: r.client_id,
        kind,
        retcode: r.retcode,
        order: r.order,
        deal: r.deal,
        position: r.position,
        volume: r.volume,
        price: r.price,
        comment: sinyal_proto::read_fixed_str(&r.comment).to_owned(),
    });
}

fn refresh_symbols(session: &Session, registry: &Registry, instance: &str) {
    if let Some(list) = session.symbols() {
        if !list.is_empty() {
            registry.set_symbols(instance, list);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Yalnızca testler kapasite belirtir: testlerde EA rolünü biz oynuyoruz ve
    // küçük halkalarla çalışmak istiyoruz. Üretimde kapasite keşfedilir.
    use sinyal_bridge::Capacities;
    use sinyal_proto::{kind, sym_flag, write_fixed_str, SymbolEntry};

    fn unique(tag: &str) -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        format!("src{}-{}-{}", tag, std::process::id(), N.fetch_add(1, Ordering::Relaxed))
    }

    fn small() -> Capacities {
        Capacities { ticks: 256, books: 16, cmds: 16, results: 16, symbols: 16, positions: 8, orders: 8 }
    }

    fn sym(id: u32, name: &str) -> SymbolEntry {
        let mut e = SymbolEntry {
            symbol_id: id,
            digits: 5,
            volume_step: 0.01,
            flags: sym_flag::READY,
            ..Default::default()
        };
        write_fixed_str(&mut e.name, name);
        e
    }

    /// EA rolünü taklit eden yardımcı: kendi thread'inde oturum kurar.
    ///
    /// Session'ın thread koruması yüzünden EA tarafı da tek bir thread'den
    /// sürülmeli — testin kendisi bu sözleşmeye uymak zorunda.
    struct FakeEa {
        tx: std::sync::mpsc::Sender<Box<dyn FnOnce(&Session) + Send>>,
        _handle: std::thread::JoinHandle<()>,
    }

    impl FakeEa {
        fn start(instance: String, caps: Capacities) -> Self {
            let (tx, rx) = std::sync::mpsc::channel::<Box<dyn FnOnce(&Session) + Send>>();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                let s = Session::create(&instance, caps).expect("EA oturumu");
                ready_tx.send(()).ok();
                while let Ok(f) = rx.recv() {
                    f(&s);
                }
            });
            ready_rx.recv().expect("EA hazır olmalı");
            Self { tx, _handle: handle }
        }

        fn run(&self, f: impl FnOnce(&Session) + Send + 'static) {
            self.tx.send(Box::new(f)).expect("EA thread'i yaşamalı");
        }
    }

    #[test]
    fn reader_names_ticks_using_the_symbol_table() {
        let inst = unique("names");
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(0, "EURUSD")]) }.unwrap();
        });

        let registry = Arc::new(Registry::new());
        let (tx, mut rx) = broadcast::channel(256);
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, stats);

        // Okuyucunun bağlanıp sembol tablosunu almasını bekle.
        std::thread::sleep(Duration::from_millis(300));

        ea.run(|s| {
            let t = Tick {
                time_msc: 1_700_000_000_000,
                bid: 1.1000,
                ask: 1.1002,
                recv_qpc: qpc(),
                symbol_id: 0,
                kind: kind::TICK,
                ..Default::default()
            };
            assert!(unsafe { s.push_tick(&t) });
        });

        let ev = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), rx.recv()).await
            })
            .expect("zaman aşımı")
            .expect("olay gelmeli");

        match ev {
            FeedEvent::Tick { symbol, bid, .. } => {
                assert_eq!(&*symbol, "EURUSD", "tick sembol adıyla isimlendirilmeli");
                assert!((bid - 1.1000).abs() < 1e-12);
            }
            other => panic!("beklenmeyen olay: {other:?}"),
        }
    }

    #[test]
    fn reader_splits_book_into_sorted_bids_and_asks() {
        let inst = unique("book");
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(0, "XAUUSD")]) }.unwrap();
        });

        let registry = Arc::new(Registry::new());
        let (tx, mut rx) = broadcast::channel(256);
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, stats);
        std::thread::sleep(Duration::from_millis(300));

        ea.run(|s| {
            // Karışık sırada, iki taraf iç içe.
            let prices = [2000.5, 1999.0, 2001.0, 1998.0];
            let vols = [1.0, 2.0, 3.0, 4.0];
            let kinds = [book_type::SELL, book_type::BUY, book_type::SELL, book_type::BUY];
            assert!(unsafe { s.push_book_flat(0, 42, qpc(), &prices, &vols, &kinds) });
        });

        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let ev = rt
            .block_on(async {
                loop {
                    match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                        Ok(Ok(FeedEvent::Book { bids, asks, .. })) => return Some((bids, asks)),
                        Ok(Ok(_)) => continue,
                        _ => return None,
                    }
                }
            })
            .expect("derinlik olayı gelmeli");

        let (bids, asks) = ev;
        // Alışlar yüksekten alçağa, satışlar alçaktan yükseğe.
        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 2);
        assert!(bids[0][0] > bids[1][0], "alışlar en iyi fiyat önce olmalı");
        assert!(asks[0][0] < asks[1][0], "satışlar en iyi fiyat önce olmalı");
        assert!((bids[0][0] - 1999.0).abs() < 1e-9);
        assert!((asks[0][0] - 2000.5).abs() < 1e-9);
    }

    #[test]
    fn commands_reach_the_ea_through_the_reader_thread() {
        // ASIL NOKTA: WebSocket görevleri push_cmd'yi DOĞRUDAN çağıramaz —
        // Session'ın thread koruması reddeder. Kanal üzerinden geçmeli.
        let inst = unique("cmd");
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(0, "EURUSD")]) }.unwrap();
        });

        let registry = Arc::new(Registry::new());
        let (tx, _rx) = broadcast::channel(256);
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, stats);
        std::thread::sleep(Duration::from_millis(300));

        cmd_tx
            .send(Cmd {
                client_id: 777,
                volume: 0.1,
                symbol_id: 0,
                action: sinyal_proto::action::PENDING,
                order_type: sinyal_proto::order_type::BUY_LIMIT,
                ..Default::default()
            })
            .unwrap();

        // EA tarafında komutu bekle.
        let (got_tx, got_rx) = std::sync::mpsc::channel();
        for _ in 0..50 {
            let got_tx = got_tx.clone();
            ea.run(move |s| {
                if let Some(c) = unsafe { s.pop_cmd() } {
                    got_tx.send(c).ok();
                }
            });
            if let Ok(c) = got_rx.recv_timeout(Duration::from_millis(100)) {
                assert_eq!(c.client_id, 777);
                assert_eq!(c.order_type, sinyal_proto::order_type::BUY_LIMIT);
                return;
            }
        }
        panic!("komut EA'ya ulaşmadı");
    }
}
