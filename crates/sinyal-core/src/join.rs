//! Emir olaylarını istemci kimliğine bağlama (geç bağlama).
//!
//! # Neden burada, EA'da değil
//!
//! `MqlTradeTransaction` yapısında `magic` ve `comment` **yoktur**, ve
//! `MqlTradeResult.request_id` yalnızca `TRADE_TRANSACTION_REQUEST` olayında
//! doludur. Yani dolum bilgisini taşıyan `DEAL_ADD` olayı, tek başına
//! bakıldığında hangi emre ait olduğunu söylemez.
//!
//! EA'da tablo taraması yapmak seçenek değil: terminalin işlem kuyruğu 1024
//! elemanlı ve işleyici yavaşladığında **eski olaylar sessizce ezilir**. Bu
//! yüzden EA ham alanları iletir, eşleştirme burada yapılır.
//!
//! # Nasıl
//!
//! Üç yönlü bir tablo tutulur: `request_id → client_id`,
//! `order ticket → client_id`, `position ticket → client_id`.
//!
//! Olayların sırası **garantili değildir** (dokümante). `DEAL_ADD`, onu
//! açıklayan `REQUEST` olayından ÖNCE gelebilir. Bu yüzden çözülemeyen olaylar
//! kısa süreli bir bekleme tamponunda tutulur; bağ kurulduğunda **geriye
//! dönük** atfedilip yayımlanırlar. Pencere dolarsa olay `client_id = 0` ile
//! yayımlanır — "bize ait değil / elle yapılmış" anlamına gelir. **Olay asla
//! atılmaz**; sessizce kaybetmek, istemcinin emrinin durumunu hiç öğrenememesi
//! demek olurdu.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use sinyal_proto::{res_kind, Res};

/// Çözülemeyen olayın bekleyeceği süre.
///
/// Bu süre sonunda olay `client_id = 0` ile yayımlanır. Uzun tutmak gecikme,
/// kısa tutmak yanlış "bize ait değil" etiketi demektir; 5 saniye MT5'in
/// olay sırası düzensizliği için fazlasıyla yeterli.
const PENDING_WINDOW: Duration = Duration::from_secs(5);

/// Bekleme tamponunun üst sınırı — patolojik bir durumda belleği korur.
const MAX_PENDING: usize = 4096;

/// Eşleme tablolarının üst sınırı. Aşılırsa en eski girdiler düşer.
const MAX_BINDINGS: usize = 16_384;

/// Geç bağlamalı emir eşleştirici.
pub struct OrderJoin {
    by_request: HashMap<u32, u64>,
    by_order: HashMap<u64, u64>,
    by_position: HashMap<u64, u64>,
    /// Henüz çözülemeyen olaylar (geliş sırasıyla).
    pending: Vec<(Instant, Res)>,
    /// Teşhis.
    pub resolved_late: u64,
    pub unattributed: u64,
}

impl Default for OrderJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderJoin {
    pub fn new() -> Self {
        Self {
            by_request: HashMap::new(),
            by_order: HashMap::new(),
            by_position: HashMap::new(),
            pending: Vec::new(),
            resolved_late: 0,
            unattributed: 0,
        }
    }

    /// Emir gönderirken çağrılır: `client_id` biliniyor ama henüz bilet yok.
    pub fn on_dispatch(&mut self, client_id: u64) {
        // Şimdilik yalnızca kayıt tutulacak bir şey yok — bağ, `SEND_ACK`
        // geldiğinde `request_id` ile kurulur. Yine de tabloları budayalım.
        let _ = client_id;
        self.prune();
    }

    /// Durum anlık görüntüsündeki pozisyonlardan bağ kur.
    ///
    /// `POSITION_MAGIC` bizim gönderdiğimiz emirlerde `client_id` taşır. Bu
    /// yol, çekirdek yeniden başladığında veya olaylar kaybolduğunda bağların
    /// yeniden kurulmasını sağlar — mutabakatın temeli.
    pub fn seed_from_positions(&mut self, positions: &[sinyal_proto::PositionRec]) {
        for p in positions {
            if p.magic != 0 {
                self.by_position.entry(p.ticket).or_insert(p.magic);
                self.by_position.entry(p.identifier).or_insert(p.magic);
            }
        }
        self.prune();
    }

    /// Durum anlık görüntüsündeki bekleyen emirlerden bağ kur.
    pub fn seed_from_orders(&mut self, orders: &[sinyal_proto::OrderRec]) {
        for o in orders {
            if o.magic != 0 {
                self.by_order.entry(o.ticket).or_insert(o.magic);
            }
        }
        self.prune();
    }

    /// Bir olayı işle. Yayımlanmaya hazır olayları döndürür.
    ///
    /// Dönen liste, bu olayın kendisini ve/veya bu olay sayesinde artık
    /// çözülebilen ESKİ bekleyen olayları içerebilir.
    pub fn on_event(&mut self, mut r: Res) -> Vec<Res> {
        // 1) Bu olay yeni bağ kuruyor mu?
        self.learn(&r);

        // 2) Kendisini çözmeyi dene.
        let mut out = Vec::new();
        match self.resolve(&r) {
            Some(cid) => {
                r.client_id = cid;
                out.push(r);
            }
            None => {
                // SEND_ACK/REJECTED/DUPLICATE zaten client_id taşır (çekirdek
                // üretti); yalnızca TRADE_TXN çözümsüz kalabilir.
                if r.client_id != 0 {
                    out.push(r);
                } else if self.pending.len() < MAX_PENDING {
                    self.pending.push((Instant::now(), r));
                } else {
                    // Tampon dolu — atmak yerine kimliksiz yayımla.
                    self.unattributed += 1;
                    out.push(r);
                }
            }
        }

        // 3) Yeni bağlar sayesinde çözülebilen bekleyenleri serbest bırak.
        self.flush_resolvable(&mut out);
        out
    }

    /// Süresi dolan bekleyenleri kimliksiz olarak yayımla.
    ///
    /// Periyodik çağrılmalıdır; aksi halde hiç `REQUEST` gelmeyen bir olay
    /// (ör. terminalden elle kapatma) tamponda sonsuza kadar kalır.
    pub fn expire(&mut self) -> Vec<Res> {
        let now = Instant::now();
        let mut out = Vec::new();
        let mut keep = Vec::with_capacity(self.pending.len());
        for (at, r) in self.pending.drain(..) {
            if now.duration_since(at) >= PENDING_WINDOW {
                self.unattributed += 1;
                out.push(r);
            } else {
                keep.push((at, r));
            }
        }
        self.pending = keep;
        out
    }

    /// Olaydan öğrenilebilecek bağları kur.
    fn learn(&mut self, r: &Res) {
        // `SEND_ACK` çekirdek tarafından üretilir ve client_id'yi taşır; asıl
        // bağ noktası burasıdır.
        if r.client_id != 0 {
            if r.request_id != 0 {
                self.by_request.insert(r.request_id, r.client_id);
            }
            if r.order != 0 {
                self.by_order.insert(r.order, r.client_id);
            }
            if r.position != 0 {
                self.by_position.insert(r.position, r.client_id);
            }
            return;
        }

        // Kimliksiz bir olay, bilinen bir `request_id` taşıyorsa biletleri
        // öğrenebiliriz.
        if r.request_id != 0 {
            if let Some(&cid) = self.by_request.get(&r.request_id) {
                if r.order != 0 {
                    self.by_order.insert(r.order, cid);
                }
                if r.position != 0 {
                    self.by_position.insert(r.position, cid);
                }
            }
        }

        // Hedging'de açılış emrinin bileti pozisyon bileti olur; biri
        // biliniyorsa diğerini de bağla.
        if r.order != 0 && r.position != 0 {
            if let Some(&cid) = self.by_order.get(&r.order) {
                self.by_position.entry(r.position).or_insert(cid);
            } else if let Some(&cid) = self.by_position.get(&r.position) {
                self.by_order.entry(r.order).or_insert(cid);
            }
        }
    }

    /// Olayı çözmeyi dene.
    fn resolve(&self, r: &Res) -> Option<u64> {
        if r.client_id != 0 {
            return Some(r.client_id);
        }
        if r.request_id != 0 {
            if let Some(&c) = self.by_request.get(&r.request_id) {
                return Some(c);
            }
        }
        if r.order != 0 {
            if let Some(&c) = self.by_order.get(&r.order) {
                return Some(c);
            }
        }
        if r.position != 0 {
            if let Some(&c) = self.by_position.get(&r.position) {
                return Some(c);
            }
        }
        None
    }

    /// Artık çözülebilen bekleyenleri çıkar.
    fn flush_resolvable(&mut self, out: &mut Vec<Res>) {
        if self.pending.is_empty() {
            return;
        }
        let mut keep = Vec::with_capacity(self.pending.len());
        for (at, mut r) in std::mem::take(&mut self.pending) {
            match self.resolve(&r) {
                Some(cid) => {
                    r.client_id = cid;
                    self.resolved_late += 1;
                    out.push(r);
                }
                None => keep.push((at, r)),
            }
        }
        self.pending = keep;
    }

    fn prune(&mut self) {
        // Basit koruma: sınırı aşarsa tabloyu boşalt. Bağlar durum anlık
        // görüntüsünden yeniden kurulabildiği için bu kayıp kalıcı değil.
        if self.by_order.len() > MAX_BINDINGS {
            self.by_order.clear();
        }
        if self.by_position.len() > MAX_BINDINGS {
            self.by_position.clear();
        }
        if self.by_request.len() > MAX_BINDINGS {
            self.by_request.clear();
        }
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(client_id: u64, request_id: u32, order: u64) -> Res {
        Res {
            client_id,
            request_id,
            order,
            kind: res_kind::SEND_ACK,
            retcode: 10008,
            ..Default::default()
        }
    }

    fn txn(request_id: u32, order: u64, deal: u64, position: u64) -> Res {
        Res {
            client_id: 0,
            request_id,
            order,
            deal,
            position,
            kind: res_kind::TRADE_TXN,
            ..Default::default()
        }
    }

    #[test]
    fn ack_binds_and_later_txn_resolves() {
        let mut j = OrderJoin::new();
        let out = j.on_event(ack(42, 7, 0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].client_id, 42);

        // Ara olay: request_id dolu, client_id boş.
        let out = j.on_event(txn(7, 900, 0, 0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].client_id, 42, "request_id ile çözülmeli");

        // Sonraki olayda request_id YOK, yalnızca bilet var.
        let out = j.on_event(txn(0, 900, 901, 900));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].client_id, 42, "order bileti ile çözülmeli");
    }

    #[test]
    fn out_of_order_event_is_attributed_retroactively() {
        // ASIL SORUN: DEAL_ADD, onu açıklayan REQUEST'ten ÖNCE gelebilir.
        // Sıra garantisi YOK (dokümante).
        let mut j = OrderJoin::new();

        // Önce kimliksiz dolum olayı — çözülemez, beklemeye alınır.
        let out = j.on_event(txn(0, 900, 901, 900));
        assert!(out.is_empty(), "çözülemeyen olay hemen yayımlanmamalı");
        assert_eq!(j.pending_len(), 1);

        // Sonra bağı kuran ack gelir.
        let out = j.on_event(ack(42, 7, 900));
        assert_eq!(out.len(), 2, "ack + geriye dönük çözülen olay");
        assert!(out.iter().all(|r| r.client_id == 42));
        assert_eq!(j.pending_len(), 0);
        assert_eq!(j.resolved_late, 1);
    }

    #[test]
    fn foreign_event_is_published_not_dropped() {
        // Terminalden elle kapatılan bir pozisyonun olayı bize ait değildir.
        // ATILMAMALI — istemci "bilinmeyen işlem" olarak görmeli.
        let mut j = OrderJoin::new();
        let out = j.on_event(txn(0, 555, 556, 555));
        assert!(out.is_empty(), "önce beklemeye alınır");

        // Süre dolunca kimliksiz yayımlanır.
        std::thread::sleep(Duration::from_millis(10));
        // Süreyi zorlamak yerine tamponu doğrudan yaşlandıralım.
        j.pending[0].0 = Instant::now() - PENDING_WINDOW - Duration::from_secs(1);
        let out = j.expire();
        assert_eq!(out.len(), 1, "olay kaybolmamalı");
        assert_eq!(out[0].client_id, 0, "bize ait değil olarak işaretlenmeli");
        assert_eq!(j.unattributed, 1);
    }

    #[test]
    fn position_magic_seeds_binding_after_core_restart() {
        // Çekirdek yeniden başladığında olay tabloları boştur; bağlar durum
        // anlık görüntüsündeki POSITION_MAGIC'ten yeniden kurulur.
        let mut j = OrderJoin::new();
        let mut p = sinyal_proto::PositionRec {
            ticket: 900,
            identifier: 900,
            magic: 42,
            ..Default::default()
        };
        p.volume = 0.01;
        j.seed_from_positions(&[p]);

        let out = j.on_event(txn(0, 0, 901, 900));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].client_id, 42, "magic üzerinden çözülmeli");
    }

    #[test]
    fn order_magic_seeds_binding_for_pending_orders() {
        let mut j = OrderJoin::new();
        let o = sinyal_proto::OrderRec { ticket: 777, magic: 99, ..Default::default() };
        j.seed_from_orders(&[o]);

        let out = j.on_event(txn(0, 777, 0, 0));
        assert_eq!(out[0].client_id, 99);
    }

    #[test]
    fn hedging_links_order_ticket_to_position_ticket() {
        // Hedging'de açılış emrinin bileti pozisyon bileti olur (gözlemimizde
        // order=942420795 ve position=942420795 aynıydı).
        let mut j = OrderJoin::new();
        j.on_event(ack(42, 7, 900));
        // order biliniyor, position ilk kez görülüyor
        j.on_event(txn(0, 900, 0, 900));
        // artık yalnızca position taşıyan olay da çözülmeli
        let out = j.on_event(txn(0, 0, 0, 900));
        assert_eq!(out[0].client_id, 42);
    }

    #[test]
    fn pending_buffer_does_not_grow_without_bound() {
        let mut j = OrderJoin::new();
        for i in 0..(MAX_PENDING + 100) {
            let out = j.on_event(txn(0, 100_000 + i as u64, 0, 0));
            // Tampon dolduktan sonra olaylar kimliksiz YAYIMLANIR, atılmaz.
            if i >= MAX_PENDING {
                assert_eq!(out.len(), 1, "tampon dolunca olay yayımlanmalı");
            }
        }
        assert!(j.pending_len() <= MAX_PENDING);
        assert!(j.unattributed > 0);
    }

    #[test]
    fn events_that_already_carry_client_id_pass_through() {
        let mut j = OrderJoin::new();
        let r = Res { client_id: 5, kind: res_kind::REJECTED, ..Default::default() };
        let out = j.on_event(r);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].client_id, 5);
        assert_eq!(j.pending_len(), 0);
    }
}
