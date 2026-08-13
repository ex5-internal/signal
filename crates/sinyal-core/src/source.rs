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

use crate::candles::CandleStore;
use crate::history::{HistClient, HistCmd, HistReply, HistStatus, DEFAULT_TIMEOUT};
use crate::join::OrderJoin;
use crate::record::RecorderHandle;
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
        /// Olay ANINDAKİ piyasa fiyatı — 0 ise ölçüm yok.
        ///
        /// Giriş maliyetini `spread` ile `kayma` diye AYIRMANIN tek yolu.
        /// `price` tek başına "ne kadar kaybettim"i söyler ama "neden"i
        /// söylemez: `ask - bid` spread'i, `price - ask` ise kaymayı verir.
        /// Bu ayrım olmadan motorun önerdiği fiyatın ulaşılamaz olması ile
        /// spread'in genişlemesi aynı sayıya karışır.
        bid: f64,
        ask: f64,
        /// MT5 `ENUM_ORDER_STATE` — 0 (`STARTED`) ölçüm yok demektir.
        order_state: u8,
        /// MT5 `ENUM_TRADE_TRANSACTION_TYPE` — 0 (`ORDER_ADD`) ölçüm yok.
        txn_type: u8,
        comment: String,
    },
    /// **Broker'a yazılan stop DOĞRULANAMADI.**
    ///
    /// `modify_sltp` kabul edilmiş olabilir ama broker'ın uygulamış olması
    /// AYRI bir sorudur: `stops_level`/`freeze_level` ihlali, requote ya da
    /// "invalid stops" (10016) emri sessizce düşürür ve pozisyon KORUMASIZ
    /// kalır. Komuttan sonra bir pencere boyunca durum yayınındaki `sl`
    /// izlenir; istenen değere ulaşmazsa bu olay üretilir.
    ///
    /// **Hata DEĞİL, UYARI.** İstemci kendi yedek stop'unu devrede tutmalı
    /// ve bunu BİLMELİ — sessiz kalmak, korumasız bir pozisyonu korunmuş
    /// sanmak demektir.
    SltpUnverified {
        instance: Arc<str>,
        /// `modify_sltp` isteğinin istemci kimliği.
        id: String,
        ticket: u64,
        /// Komutta İSTENEN stop.
        want_sl: f64,
        /// Durum yayınındaki GERÇEK stop. Pozisyon hiç bulunamadıysa `None`
        /// — "0 idi" ile "bakamadık" aynı şey değil.
        actual_sl: Option<f64>,
        /// Bakılan durum görüntüsünün yaşı (ms).
        ///
        /// Büyükse sorun broker'da değil KÖPRÜDE olabilir: zombi bir köprü
        /// durumu dondurur ve stop gerçekte kurulmuş olsa bile burada eski
        /// değer görünür. İki nedeni ayırmanın tek yolu bu sayı.
        state_age_ms: Option<u64>,
        /// İnsan okunur sebep.
        why: &'static str,
    },
    /// Bir mum kapandı.
    ///
    /// Yalnızca KAPANAN bar yayılır; açık barın her tick'te güncellenmesini
    /// yayınlamak tick akışını ikiye katlardı. İstemci açık barı zaten tick
    /// akışından kendisi güncelleyebilir.
    Candle {
        symbol: Arc<str>,
        tf: &'static str,
        bar: crate::candles::Bar,
    },
}

/// Geçmiş kanalını yeniden bağlamayı deneme aralığı.
///
/// Service çekirdekten sonra başlayabilir; bir kez deneyip vazgeçmek,
/// "servisi açtım ama hâlâ bar gelmiyor" demek olurdu.
const HIST_RETRY: Duration = Duration::from_secs(5);

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
    /// Gecmis kanali (MQL5 Service) bagli mi.
    pub hist_ready: bool,
    /// Gonderilen gecmis istegi sayisi.
    pub hist_reqs: u64,
    /// Depoya islenen MT5 bari sayisi.
    pub hist_bars: u64,
    /// EKSIK teslim edilen istek sayisi — bar halkasi dolmus olabilir.
    pub hist_incomplete: u64,
    /// Service'in hata dondurdugu istek sayisi.
    pub hist_failed: u64,
    /// Zaman asimina ugrayan istek sayisi.
    pub hist_timeout: u64,
    /// Su an cevabi beklenen gecmis istegi sayisi.
    pub hist_pending: u64,
    /// Tick kaydi acik mi (`--record`).
    pub rec_on: bool,
    /// Diske YAZILAN tick sayisi.
    pub rec_written: u64,
    /// Kayit kanali dolu (ya da yazma basarisiz) oldugu icin DUSURULEN tick
    /// sayisi.
    ///
    /// Sifir olmayan her deger gercek bir kayiptir: kayit dosyasi o kadar
    /// tick eksik. Sessiz kalmasi, aylar sonra "kayitta delik var" demekti.
    pub rec_dropped: u64,
    /// Kayit sirasinda alinan dosya hatasi sayisi.
    pub rec_errors: u64,
    /// `symbols-*.jsonl` dosyasina eklenen satir sayisi.
    pub rec_symbol_lines: u64,
}

/// Bir örnek için okuyucu thread'i başlat.
///
/// Thread, EA gelene kadar bağlanmayı yeniden dener; bu beklenen bir
/// durumdur (çekirdek terminalden önce başlamış olabilir).
#[allow(clippy::too_many_arguments)]
pub fn spawn_reader(
    instance: String,
    registry: Arc<Registry>,
    tx: broadcast::Sender<FeedEvent>,
    cmd_rx: Receiver<Cmd>,
    hist_rx: Receiver<HistCmd>,
    stats: Arc<std::sync::Mutex<ReaderStats>>,
    candles: Arc<std::sync::Mutex<CandleStore>>,
    recorder: Option<Arc<RecorderHandle>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("sinyal-rd-{instance}"))
        .spawn(move || {
            reader_loop(instance, registry, tx, cmd_rx, hist_rx, stats, candles, recorder)
        })
        .expect("okuyucu thread'i başlatılamadı")
}

#[allow(clippy::too_many_arguments)]
fn reader_loop(
    instance: String,
    registry: Arc<Registry>,
    tx: broadcast::Sender<FeedEvent>,
    cmd_rx: Receiver<Cmd>,
    hist_rx: Receiver<HistCmd>,
    stats: Arc<std::sync::Mutex<ReaderStats>>,
    candles: Arc<std::sync::Mutex<CandleStore>>,
    recorder: Option<Arc<RecorderHandle>>,
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
                // Geçmiş istekleri de CEVAPLANMALI: sessizce yutmak, WebSocket
                // görevini zaman aşımına kadar bekletirdi.
                while let Ok(h) = hist_rx.try_recv() {
                    let _ = h.reply.send(HistReply::Unavailable);
                }
            }
        }
    };

    {
        let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
        s.connected = true;
    }

    // Sembol tablosunu ilk anda yükle — tick akışını isimlendirebilmek için.
    refresh_symbols(&session, &registry, &instance, recorder.as_deref());
    let mut last_sym_refresh = Instant::now();
    // Durum yoklaması sembol yenilemesinden ayrı: sıklığı korelasyon
    // ihtiyacına göre uyarlanıyor (bkz. aşağıdaki `state_due`).
    let mut last_state_refresh = Instant::now() - Duration::from_secs(1);

    let mut join = OrderJoin::new();
    let mut batch = vec![Tick::default(); 512];
    let mut idle_rounds: u32 = 0;

    // Geçmiş kanalı BU thread'den bağlanmalı: taşıyıcı, kendisine erişen ilk
    // thread'i sahip yapar ve başkasının işlemini sessizce reddeder. main'de
    // bağlanıp buraya taşımak, hata vermeyen ama hiç bar gelmeyen bir kurulum
    // üretirdi.
    let mut hist = crate::hist_bridge::attach(&instance)
        .map(|link| HistClient::new(link, DEFAULT_TIMEOUT));
    if hist.is_some() {
        eprintln!("[{instance}] gecmis kanali bagli — MT5 barlari acik");
    } else if !crate::hist_bridge::COMPILED_IN {
        eprintln!(
            "[{instance}] gecmis kanali bu ikiliye derlenmedi (mt5-hist kapali) \
             — mumlar yalniz tick'ten uretilecek"
        );
    }
    let mut last_hist_try = Instant::now();
    // İstek → cevap kanalı. `HistClient` tokio'yu tanımıyor; eşleşmeyi burada
    // tutuyoruz ki geçmiş mantığı paylaşımlı bellek ve çalışma zamanı olmadan
    // test edilebilsin.
    let mut hist_waiters: std::collections::HashMap<
        u32,
        tokio::sync::oneshot::Sender<HistReply>,
    > = std::collections::HashMap::new();

    loop {
        let mut did_work = false;

        // --- 1) Tick akışı ---
        // Güvenlik: bu thread oturumun sahibi; SPSC sözleşmesi korunuyor.
        let n = unsafe { session.pop_tick_batch(&mut batch) };
        if n > 0 {
            did_work = true;
            let now = qpc();
            for t in &batch[..n] {
                // Kayıt: TEK bir kanal push'u, sıcak yolda I/O YOK. Kanal
                // doluysa kayıt düşer ve sayılır (bkz. record.rs) — okuyucuyu
                // bekletmek EA'nın halkasını doldurup canlı tick kaybettirirdi.
                //
                // İsimlendirmeden ÖNCE: ham tick her hâlükârda kaydedilir.
                // Sembol adı zaten kayıttan değil `symbols-*.jsonl`'dan
                // çözülüyor, bu yüzden tablo henüz gelmemişken atılan tick
                // kayıtta bir delik olurdu.
                if let Some(r) = &recorder {
                    r.push_tick(t);
                }
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
                let sym: Arc<str> = Arc::from(name.as_str());

                // Mum deposunu besle; kapanan barlar ayrıca yayılır.
                let closed = {
                    let mut cs = candles.lock().unwrap_or_else(|e| e.into_inner());
                    cs.on_tick(&name, t.bid, t.ask, t.time_msc)
                };
                for cb in closed {
                    let _ = tx.send(FeedEvent::Candle {
                        symbol: Arc::from(cb.symbol.as_str()),
                        tf: cb.tf,
                        bar: cb.bar,
                    });
                }

                // Abone yoksa `send` hata döner — bu normal, yok sayılır.
                let _ = tx.send(FeedEvent::Tick {
                    instance: inst.clone(),
                    symbol: sym,
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

        // --- 4b) Geçmiş bar kanalı ---
        //
        // Üç iş: gelen istekleri service'e ilet, gelen barları depoya işle,
        // süresi dolanları kapat. Hiçbiri tick akışını bloklamaz — geçmiş
        // gecikmesi fiyat gecikmesinden önemsizdir.
        loop {
            match hist_rx.try_recv() {
                Ok(h) => {
                    did_work = true;
                    let Some(client) = hist.as_mut() else {
                        // Service yok: özellik kapalı, hata değil. Ama
                        // CEVAPSIZ bırakmıyoruz.
                        let _ = h.reply.send(HistReply::Unavailable);
                        continue;
                    };
                    match client.request(&h.symbol, h.symbol_id, &h.tf, h.count, h.to_msc) {
                        Ok(id) => {
                            hist_waiters.insert(id, h.reply);
                            let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
                            s.hist_reqs += 1;
                        }
                        Err(e) => {
                            let _ = h.reply.send(HistReply::Refused(e.to_string()));
                        }
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        if let Some(client) = hist.as_mut() {
            let mut done = client.drain();
            done.extend(client.expire());
            for out in done {
                did_work = true;
                let n = out.bars.len();
                // Hata ve zaman aşımı bir ÇEKİM DEĞİLDİR: boş sonuçlarını
                // depoya yazmak, hiç alınamamış bir seriyi "broker bar
                // vermedi" diye alınmış göstermek olurdu. Başarılı ama boş
                // cevap ise kaydedilir — o gerçekten bir cevaptır.
                if n > 0 || matches!(out.status, HistStatus::Complete) {
                    // Barlar AYRI bir seriye giriyor: MT5'in serisi (genelde
                    // BID) ile tick'ten ürettiğimiz (MID) aynı şey değil,
                    // karıştırmak istemciyi yanıltırdı.
                    let mut cs = candles.lock().unwrap_or_else(|e| e.into_inner());
                    cs.ingest_mt5(&out.symbol, out.tf, &out.bars);
                }
                {
                    let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
                    s.hist_bars += n as u64;
                    match out.status {
                        HistStatus::Complete => {}
                        HistStatus::Incomplete { .. } => s.hist_incomplete += 1,
                        HistStatus::Failed(_) => s.hist_failed += 1,
                        HistStatus::TimedOut { .. } => s.hist_timeout += 1,
                    }
                }
                if !matches!(out.status, HistStatus::Complete) {
                    eprintln!(
                        "[{instance}] gecmis {} {}: {} — {}",
                        out.symbol,
                        out.tf,
                        out.status.name(),
                        out.status.detail().unwrap_or_default()
                    );
                }
                // Zaman aşımı da bir CEVAPTIR: bekleyen taraf sonsuza kadar
                // asılı kalmaz.
                if let Some(reply) = hist_waiters.remove(&out.req_id) {
                    let _ = reply.send(HistReply::Done { status: out.status, bars: n });
                }
            }
        }

        // --- 5) Durum görüntüsü (uyarlanır sıklık) ---
        //
        // Durum yalnızca hesap/pozisyon sorguları için değil, **korelasyon için
        // de** okunur: `POSITION_MAGIC` bizim `client_id`'mizi taşır, yani
        // "bilet → client_id" bağı buradan kurulur. Çekirdek yeniden başlasa
        // bile bağlar bu yolla geri gelir — mutabakatın temeli.
        //
        // Sıklık uyarlanır: normalde saniyede bir yeter, ama atfedilmeyi
        // bekleyen olay varken 1 saniye beklemek dolum olaylarının kimliksiz
        // yayımlanmasına yol açardı. Bekleyen varken 2 ms'de bir yokluyoruz;
        // EA `OnTrade`'de durumu "kirli" işaretleyip bir sonraki timer'da
        // (~16 ms) yayımladığı için bağ pratikte iki hane ms içinde kurulur.
        let state_due = if join.pending_len() > 0 {
            last_state_refresh.elapsed() >= Duration::from_millis(2)
        } else {
            last_state_refresh.elapsed() >= Duration::from_secs(1)
        };
        if state_due {
            if let Some(st) = session.state() {
                // Tohumlama yeni bağ kurabilir; bunun üzerine artık
                // çözülebilen bekleyen olayları HEMEN yayımla. (Bu adım
                // eksikti: bağ kuruluyor ama kuyruktakiler taranmıyordu, bu
                // yüzden açılış olayları süresi dolup kimliksiz gidiyordu.)
                for ev in join.seed_from_positions(&st.positions) {
                    did_work = true;
                    emit_order(&tx, &inst, &ev);
                }
                for ev in join.seed_from_orders(&st.orders) {
                    did_work = true;
                    emit_order(&tx, &inst, &ev);
                }
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
            last_state_refresh = Instant::now();
        }

        // --- 6) Periyodik bakım ---
        if last_sym_refresh.elapsed() >= Duration::from_secs(1) {
            refresh_symbols(&session, &registry, &instance, recorder.as_deref());

            last_sym_refresh = Instant::now();
            let mut s = stats.lock().unwrap_or_else(|e| e.into_inner());
            s.ea_tick_loss = session.tick_push_failures();
            s.backlog = session.tick_backlog();
            s.join_pending = join.pending_len() as u64;
            s.join_late = join.resolved_late;
            s.join_unattributed = join.unattributed;
            s.hist_ready = hist.is_some();
            s.hist_pending = hist.as_ref().map_or(0, |h| h.pending_len() as u64);
            if let Some(r) = &recorder {
                let c = r.counts();
                s.rec_on = true;
                s.rec_written = c.written;
                s.rec_dropped = c.dropped;
                s.rec_errors = c.errors;
                s.rec_symbol_lines = c.sym_lines;
            }
        }

        // Service çekirdekten sonra başlayabilir; bir kez deneyip vazgeçmek
        // "servisi açtım ama hâlâ bar gelmiyor" demek olurdu.
        if hist.is_none() && last_hist_try.elapsed() >= HIST_RETRY {
            last_hist_try = Instant::now();
            if let Some(link) = crate::hist_bridge::attach(&instance) {
                eprintln!("[{instance}] gecmis kanali bagli — MT5 barlari acik");
                hist = Some(HistClient::new(link, DEFAULT_TIMEOUT));
            }
        }

        // --- 7) Bekleme stratejisi ---
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

/// Ham `Res`ten tel üzerindeki `kind` etiketini türet.
///
/// # Neden `expired` burada doğuyor
///
/// MT5 süre dolumunu AYRI bir olay türü olarak bildirmez: bekleyen emrin
/// süresi dolduğunda `TRADE_TRANSACTION_ORDER_DELETE` gelir — dolan bir
/// emrin sildiği olayın TA KENDİSİ. İkisini ayıran tek alan `order_state`.
/// Bu ayrım yapılmazsa süresi dolmuş bir emir istemciye sıradan bir `txn`
/// olarak görünür ve `retcode` 10009 olduğu için DOLMUŞ sanılır.
fn order_kind(r: &sinyal_proto::Res) -> &'static str {
    // Sıra önemli: süre dolumu bir TRADE_TXN kılığında gelir, bu yüzden
    // genel `txn` etiketinden ÖNCE ayıklanmalı.
    if r.kind == sinyal_proto::res_kind::TRADE_TXN
        && r.txn_type == sinyal_proto::txn_type::ORDER_DELETE
        && r.order_state == sinyal_proto::order_state::EXPIRED
    {
        return "expired";
    }
    match r.kind {
        sinyal_proto::res_kind::SEND_ACK => "ack",
        sinyal_proto::res_kind::TRADE_TXN => "txn",
        sinyal_proto::res_kind::REJECTED => "rejected",
        sinyal_proto::res_kind::DUPLICATE => "duplicate",
        _ => "unknown",
    }
}

/// Bir emir olayini abonelere yayinla.
fn emit_order(tx: &broadcast::Sender<FeedEvent>, inst: &Arc<str>, r: &sinyal_proto::Res) {
    let _ = tx.send(FeedEvent::Order {
        instance: inst.clone(),
        client_id: r.client_id,
        kind: order_kind(r),
        retcode: r.retcode,
        order: r.order,
        deal: r.deal,
        position: r.position,
        volume: r.volume,
        price: r.price,
        bid: r.bid,
        ask: r.ask,
        order_state: r.order_state,
        txn_type: r.txn_type,
        comment: sinyal_proto::read_fixed_str(&r.comment).to_owned(),
    });
}

fn refresh_symbols(
    session: &Session,
    registry: &Registry,
    instance: &str,
    recorder: Option<&RecorderHandle>,
) {
    if let Some(list) = session.symbols() {
        if !list.is_empty() {
            // Kaydediciye HER yenilemede gönderiyoruz; diske yazma kararını
            // (içerik değişti mi) o veriyor. Değişiklik denetimini burada
            // yapmak, kaydedicinin diskte ne olduğu bilgisini yok sayardı —
            // ve düşen bir görüntü kalıcı bir boşluğa dönüşürdü.
            if let Some(r) = recorder {
                r.push_symbols(list.clone());
            }
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

    /// UÇTAN UCA: EA'nın yazdığı tick, okuyucu döngüsünden geçip DİSKE
    /// düşüyor mu.
    ///
    /// `record.rs` kaydedicinin kendisini kanıtlıyor; buradaki asıl mesele
    /// **bağlantı**: sıcak yoldaki tek satırlık `push_tick` çağrısı ve sembol
    /// tablosunun kaydediciye iletilmesi. İkisi de sessizce düşebilir
    /// (kaydedici mükemmel çalışır, kimse ona tick vermez) ve sonuç "kayıt
    /// açıktı ama dizin boş" olurdu.
    #[test]
    fn recorded_ticks_travel_from_the_ring_to_the_disk() {
        let inst = unique("rec");
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(0, "EURUSD")]) }.unwrap();
        });

        let dir = std::env::temp_dir().join(format!("sinyal-src-rec-{inst}"));
        let _ = std::fs::remove_dir_all(&dir);
        let rec = Arc::new(crate::record::Recorder::start(&dir, &inst).expect("kayit acilmali"));

        let registry = Arc::new(Registry::new());
        let (tx, _rx) = broadcast::channel(256);
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (_hist_tx, hist_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let _rd = spawn_reader(
            inst.clone(),
            registry.clone(),
            tx,
            cmd_rx,
            hist_rx,
            stats.clone(),
            Arc::new(std::sync::Mutex::new(CandleStore::new())),
            Some(rec.clone()),
        );
        std::thread::sleep(Duration::from_millis(300));

        ea.run(|s| {
            for i in 0..5u32 {
                let t = Tick {
                    time_msc: 1_700_000_000_000 + i as i64,
                    bid: 1.1000 + i as f64 * 0.0001,
                    ask: 1.1002 + i as f64 * 0.0001,
                    recv_qpc: qpc(),
                    symbol_id: 0,
                    kind: kind::TICK,
                    ..Default::default()
                };
                assert!(unsafe { s.push_tick(&t) });
            }
        });

        // Okuyucunun beşini de işlemesini bekle.
        let mut seen = 0;
        for _ in 0..100 {
            seen = stats.lock().unwrap_or_else(|e| e.into_inner()).ticks;
            if seen >= 5 {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(seen, 5, "okuyucu tick'leri islemeliydi");

        // `stop` tamponu yazdırır ve kilidi bırakır.
        assert!(rec.stop());
        let counts = rec.counts();
        assert_eq!(counts.written, 5, "bes tick de diske yazilmaliydi: {counts:?}");
        assert_eq!(counts.dropped, 0);

        let days = crate::record::list_days(&dir, &inst).unwrap();
        assert_eq!(days.len(), 1, "tek gun dosyasi bekleniyordu: {days:?}");
        let recs =
            crate::record::load_ticks(&crate::record::tick_path(&dir, &inst, days[0])).unwrap();
        assert_eq!(recs.len(), 5);
        assert_eq!(recs[0].time_msc, 1_700_000_000_000);
        assert!((recs[4].bid - 1.1004).abs() < 1e-12, "fiyat bozulmadan gitmeli");
        // Sembol tablosu da kaydediciye ulaşmalı: isimlendirme replay'de
        // buradan çözülüyor.
        assert_eq!(counts.sym_lines, 1, "sembol tablosu bir kez yazilmali");

        let _ = std::fs::remove_dir_all(&dir);
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
        let (_hist_tx, hist_rx) = std::sync::mpsc::channel();
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, hist_rx, stats, Arc::new(std::sync::Mutex::new(CandleStore::new())), None);

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
        let (_hist_tx, hist_rx) = std::sync::mpsc::channel();
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, hist_rx, stats, Arc::new(std::sync::Mutex::new(CandleStore::new())), None);
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
    fn history_requests_are_always_answered_even_without_a_service() {
        // ASIL NOKTA: geçmiş kanalı yokken isteği sessizce yutmak, WebSocket
        // görevini zaman aşımına kadar askıda bırakırdı. "Özellik kapalı"
        // cevabı, cevapsızlıktan iyidir.
        let inst = unique("hist");
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(0, "EURUSD")]) }.unwrap();
        });

        let registry = Arc::new(Registry::new());
        let (tx, _rx) = broadcast::channel(256);
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (hist_tx, hist_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let _rd = spawn_reader(
            inst.clone(),
            registry.clone(),
            tx,
            cmd_rx,
            hist_rx,
            stats,
            Arc::new(std::sync::Mutex::new(CandleStore::new())),
            None,
        );
        std::thread::sleep(Duration::from_millis(300));

        let (rtx, rrx) = tokio::sync::oneshot::channel();
        hist_tx
            .send(HistCmd {
                symbol: "EURUSD".into(),
                symbol_id: 0,
                tf: "M1".into(),
                count: 100,
                to_msc: 0,
                reply: rtx,
            })
            .unwrap();

        let reply = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), rrx).await })
            .expect("cevapsiz kalmamali")
            .expect("kanal dusmemeli");

        // Bu örnek için hiçbir Service çalışmıyor (ve `mt5-hist` kapalıysa
        // kanal hiç derlenmemiştir) — iki durumda da cevap gelmeli.
        assert_eq!(reply, HistReply::Unavailable);
    }

    /// Service rolünü taklit eden yardımcı — EA'nınki gibi tek thread'e
    /// kilitli, çünkü `HistSession` de oturuma erişen ilk thread'i sahip yapar.
    #[cfg(feature = "mt5-hist")]
    struct FakeService {
        tx: std::sync::mpsc::Sender<Box<dyn FnOnce(&sinyal_bridge::HistSession) + Send>>,
        _handle: std::thread::JoinHandle<()>,
    }

    #[cfg(feature = "mt5-hist")]
    impl FakeService {
        fn start(instance: String) -> Self {
            let (tx, rx) = std::sync::mpsc::channel::<
                Box<dyn FnOnce(&sinyal_bridge::HistSession) + Send>,
            >();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                let s = sinyal_bridge::HistSession::create(&instance).expect("service oturumu");
                ready_tx.send(()).ok();
                while let Ok(f) = rx.recv() {
                    f(&s);
                }
            });
            ready_rx.recv().expect("service hazir olmali");
            Self { tx, _handle: handle }
        }

        fn run(&self, f: impl FnOnce(&sinyal_bridge::HistSession) + Send + 'static) {
            self.tx.send(Box::new(f)).expect("service thread'i yasamali");
        }
    }

    /// UÇTAN UCA: istek paylaşımlı bellekten service'e gider, barlar geri
    /// gelir ve **MT5 kaynaklı ayrı seriye** işlenir.
    ///
    /// Sahte taşıyıcıyla yapılan testler eşleştirme mantığını kanıtlar; bu
    /// test halka yerleşimini, `Cell<BarRec>` slot boyutunu ve thread
    /// sahipliğini de kanıtlıyor — üçü de yalnızca çalışma zamanında bozulur.
    #[cfg(feature = "mt5-hist")]
    #[test]
    fn history_flows_through_real_shared_memory_into_the_mt5_series() {
        use sinyal_proto::{bar_flag, timeframe, BarRec};

        let inst = unique("histe2e");
        // Service segmentleri OLUŞTURUR; çekirdek yalnızca bağlanır.
        let svc = FakeService::start(inst.clone());
        let ea = FakeEa::start(inst.clone(), small());
        ea.run(|s| {
            unsafe { s.set_symbols(&[sym(4, "EURUSD")]) }.unwrap();
        });

        let registry = Arc::new(Registry::new());
        let (tx, _rx) = broadcast::channel(256);
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (hist_tx, hist_rx) = std::sync::mpsc::channel();
        let stats = Arc::new(std::sync::Mutex::new(ReaderStats::default()));
        let store = Arc::new(std::sync::Mutex::new(CandleStore::new()));
        let _rd = spawn_reader(
            inst.clone(),
            registry.clone(),
            tx,
            cmd_rx,
            hist_rx,
            stats,
            store.clone(),
            None,
        );
        std::thread::sleep(Duration::from_millis(300));

        let (rtx, rrx) = tokio::sync::oneshot::channel();
        hist_tx
            .send(HistCmd {
                symbol: "EURUSD".into(),
                symbol_id: 4,
                tf: "M5".into(),
                count: 3,
                to_msc: 0,
                reply: rtx,
            })
            .unwrap();

        // Service isteği alıp barları yayınlıyor.
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        for _ in 0..50 {
            let seen_tx = seen_tx.clone();
            svc.run(move |s| {
                if let Some(req) = unsafe { s.pop_req() } {
                    for i in 0..3u32 {
                        let b = BarRec {
                            time_msc: 1_700_000_000_000 + i as i64 * 300_000,
                            open: 1.10,
                            high: 1.11,
                            low: 1.09,
                            close: 1.10 + i as f64 * 0.001,
                            tick_volume: 7,
                            req_id: req.req_id,
                            symbol_id: req.symbol_id,
                            timeframe: req.timeframe,
                            index: i,
                            total: 3,
                            flags: if i == 2 { bar_flag::LAST } else { 0 },
                            ..Default::default()
                        };
                        assert!(unsafe { s.push_bar(&b) }, "bar halkasi dolmamali");
                    }
                    seen_tx.send(req).ok();
                }
            });
            if let Ok(req) = seen_rx.recv_timeout(Duration::from_millis(100)) {
                assert_eq!(req.symbol_id, 4);
                assert_eq!(req.timeframe, timeframe::M5, "MT5 ham degeri gitmeli");
                assert_eq!(req.count, 3);
                assert_ne!(req.req_id, 0);
                break;
            }
        }

        let reply = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async { tokio::time::timeout(Duration::from_secs(10), rrx).await })
            .expect("zaman asimi")
            .expect("cevap gelmeli");
        assert_eq!(reply, HistReply::Done { status: HistStatus::Complete, bars: 3 });

        // Barlar MT5 serisine girdi ve kaynağı öyle bildiriliyor.
        let cs = store.lock().unwrap();
        let view = cs.get("EURUSD", "M5", 10);
        assert_eq!(view.src, crate::candles::BarSource::Mt5);
        assert_eq!(view.bars.len(), 3);
        assert!((view.bars[2].c - 1.102).abs() < 1e-9);
        // Tick tarafına HİÇ dokunulmamalı.
        assert!(cs.tick_bars("EURUSD", "M5", 10).is_empty());
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
        let (_hist_tx, hist_rx) = std::sync::mpsc::channel();
        let _rd = spawn_reader(inst.clone(), registry.clone(), tx, cmd_rx, hist_rx, stats, Arc::new(std::sync::Mutex::new(CandleStore::new())), None);
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

    // -----------------------------------------------------------------------
    // `kind` türetimi — süre dolumu bir `txn` kılığında gelir
    // -----------------------------------------------------------------------

    fn res(kind: u8) -> sinyal_proto::Res {
        sinyal_proto::Res { kind, ..Default::default() }
    }

    #[test]
    fn expired_pending_order_is_not_reported_as_a_fill() {
        // MT5 süre dolumunu ayrı bir olay türü olarak bildirmez: ORDER_DELETE
        // + ORDER_STATE_EXPIRED gelir ve `retcode` 10009 (DONE) olabilir —
        // çünkü emrin YERLEŞTİRİLMESİ başarılıydı. Ayrım yapılmazsa istemci
        // bunu dolum sanar ve olmayan bir pozisyonu yönetmeye çalışır.
        let r = sinyal_proto::Res {
            retcode: 10009,
            txn_type: sinyal_proto::txn_type::ORDER_DELETE,
            order_state: sinyal_proto::order_state::EXPIRED,
            ..res(sinyal_proto::res_kind::TRADE_TXN)
        };
        assert_eq!(order_kind(&r), "expired");
    }

    #[test]
    fn a_filled_order_leaving_the_list_stays_a_txn() {
        // Dolan emir de ORDER_DELETE ile listeden düşer. `expired`i yalnızca
        // txn_type'a bakarak ayırmak, HER dolumu "expired" yapardı.
        let r = sinyal_proto::Res {
            txn_type: sinyal_proto::txn_type::ORDER_DELETE,
            order_state: sinyal_proto::order_state::FILLED,
            ..res(sinyal_proto::res_kind::TRADE_TXN)
        };
        assert_eq!(order_kind(&r), "txn");
    }

    #[test]
    fn expired_state_outside_an_order_delete_is_still_a_txn() {
        // Simetrik tuzak: yalnızca order_state'e bakmak da yeterli değil.
        let r = sinyal_proto::Res {
            txn_type: sinyal_proto::txn_type::HISTORY_ADD,
            order_state: sinyal_proto::order_state::EXPIRED,
            ..res(sinyal_proto::res_kind::TRADE_TXN)
        };
        assert_eq!(order_kind(&r), "txn");
    }

    #[test]
    fn existing_kind_labels_are_unchanged() {
        // `expired` eklenirken mevcut sözleşme BOZULMAMALI: istemciler bu
        // dört etiketi zaten işliyor.
        assert_eq!(order_kind(&res(sinyal_proto::res_kind::SEND_ACK)), "ack");
        assert_eq!(order_kind(&res(sinyal_proto::res_kind::TRADE_TXN)), "txn");
        assert_eq!(order_kind(&res(sinyal_proto::res_kind::REJECTED)), "rejected");
        assert_eq!(order_kind(&res(sinyal_proto::res_kind::DUPLICATE)), "duplicate");
        assert_eq!(order_kind(&res(200)), "unknown");
    }

    #[test]
    fn expired_is_derived_only_from_a_live_transaction() {
        // SEND_ACK'te order_state/txn_type ANLAMSIZDIR (EA sıfır bırakır ama
        // gelecekte bir alan sızarsa `ack` sessizce `expired`e dönüşmemeli).
        let r = sinyal_proto::Res {
            txn_type: sinyal_proto::txn_type::ORDER_DELETE,
            order_state: sinyal_proto::order_state::EXPIRED,
            ..res(sinyal_proto::res_kind::SEND_ACK)
        };
        assert_eq!(order_kind(&r), "ack");
    }
}
