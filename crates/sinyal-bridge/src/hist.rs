//! Geçmiş bar kanalının köprü oturumu — MQL5 **Service** ile çekirdek arası.
//!
//! Bu modül de [`crate::session`] gibi saf Rust'tır (C ABI içermez), böylece
//! testlerden doğrudan kullanılabilir. C ABI sarmalayıcısı [`crate::ffi`]
//! içindedir.
//!
//! # Neden EA'nın oturumundan AYRI
//!
//! `CopyRates` EA/script içinde çağıran thread'i **bloklar** ve zaman aşımı
//! belgelenmemiştir; canlı hesaplarda 30-60 saniyelik donmalar raporlanmıştır.
//! EA tek thread olduğu için böyle bir çağrı tick toplamayı tamamen durdururdu.
//! Bu yüzden geçmiş okuma ayrı bir MQL5 Service'e taşındı: kendi thread'inde
//! çalışır ve `Sleep()` serbest olduğu için dolu halkada yeniden deneyebilir.
//!
//! Service **ayrı bir yazardır** ve EA'nın tek-yazar kilidine DOKUNMAZ: kendi
//! kilidi `{instance}.hist` adını kullanır. Aynı kilidi paylaşsalardı EA ile
//! service birbirini dışlar, ikisinden yalnızca biri başlayabilirdi — üstelik
//! hata mesajı "bu örneği başka bir yazar tutuyor" olurdu ve sebebi hiç
//! anlaşılmazdı.
//!
//! # Yönler
//!
//! | Halka | Yazar | Okur |
//! |---|---|---|
//! | `hreq` | çekirdek ([`HistSession::attach`]) | service ([`HistSession::create`]) |
//! | `bars` | service | çekirdek |
//!
//! İki taraf aynı tipi kullanır ama **karşı yönün** fonksiyonlarını
//! çağırmamalıdır: her halka SPSC'dir ve ikinci bir yazar veriyi bozar.
//! Service yalnızca [`HistSession::pop_req`] + [`HistSession::push_bar`],
//! çekirdek yalnızca [`HistSession::push_req`] + [`HistSession::pop_bar`]
//! çağırır.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use sinyal_proto::{capacity, BarRec, HistReq, Ring, SegmentNames};
use sinyal_shm::{current_thread_id, InstanceLock, SharedMem};

use crate::session::{check_pow2, init_or_attach_ring, open_ring_auto, SessionError};

/// Geçmiş kanalının halka kapasiteleri.
///
/// Varsayılanlar [`sinyal_proto::capacity`] ile aynıdır; testler küçük
/// değerlerle çalışabilsin diye ayarlanabilir bırakıldı.
#[derive(Debug, Clone, Copy)]
pub struct HistCapacities {
    /// İstek halkası slot sayısı. İstekler seyrek ve küçük.
    pub reqs: u64,
    /// Bar halkası slot sayısı. Geçmiş akışı patlamalıdır (tek istek binlerce
    /// bar üretir), bu yüzden cömert.
    pub bars: u64,
}

impl Default for HistCapacities {
    fn default() -> Self {
        Self { reqs: capacity::HIST_REQS, bars: capacity::BARS }
    }
}

/// Service'in tek-yazar kilidinin adı.
///
/// EA'nınkinden **farklı** olmak zorunda: [`InstanceLock::acquire`] adı
/// `Local\Sinyal.{ad}.lock` biçimine sokar, dolayısıyla EA `{instance}`,
/// service `{instance}.hist` kullanınca iki ayrı mutex oluşur ve birbirlerini
/// engellemezler.
fn lock_name(instance: &str) -> String {
    format!("{instance}.hist")
}

/// Geçmiş kanalının bir ucunu temsil eden oturum.
///
/// # Segment sahipliği
///
/// Segmentleri **service oluşturur** (veri kaynağı odur); çekirdek yalnızca
/// bağlanır ve service gelene kadar yeniden dener. EA oturumundaki kalıbın
/// aynısı: tek sahip olması "kim başlatır" yarışını ortadan kaldırır.
pub struct HistSession {
    // Eşlemeler sahiplenilmeli: düşerlerse halkaların işaret ettiği bellek
    // eşlemesiz kalır. Alan sırası ÖNEMLİ — Rust alanları bildirim sırasıyla
    // düşürür, bu yüzden halkalar eşlemelerden ÖNCE geliyor.
    reqs: Ring<HistReq>,
    bars: Ring<BarRec>,

    _mem_reqs: SharedMem,
    _mem_bars: SharedMem,

    /// Yalnızca service tarafında dolu. Düştüğünde ad serbest kalır.
    _lock: Option<InstanceLock>,

    /// Bu oturuma erişen ilk thread'in kimliği (0 = henüz belirlenmedi).
    ///
    /// SPSC sözleşmesinin çalışma zamanı denetimi; [`crate::Session`]'daki
    /// `check_thread` ile aynı gerekçe. Service'in kendi döngüsü tek thread
    /// olduğu için ihlal beklenmiyor, ama beklenmedik bir yoldan ihlal
    /// edilirse sessiz veri bozulması yerine SAYILABİLİR bir olay üretmeli.
    owner_thread: AtomicU32,
    /// Yanlış thread'den gelip REDDEDİLEN işlem sayısı.
    thread_violations: AtomicU64,

    instance: String,
}

impl core::fmt::Debug for HistSession {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HistSession")
            .field("instance", &self.instance)
            .field("req_backlog", &self.req_backlog())
            .field("bar_backlog", &self.bar_backlog())
            .field("bar_push_failures", &self.bar_push_failures())
            .finish_non_exhaustive()
    }
}

impl HistSession {
    /// Service tarafı: segmentleri oluştur (veya var olana bağlan).
    ///
    /// `hreq` halkasını **okur**, `bars` halkasını **yazar**.
    ///
    /// Kapasiteler varsayılandır; test/ayar için [`HistSession::create_with`]
    /// kullan.
    pub fn create(instance: &str) -> Result<Self, SessionError> {
        Self::create_with(instance, HistCapacities::default())
    }

    /// Service tarafı, kapasiteleri açıkça vererek.
    pub fn create_with(instance: &str, caps: HistCapacities) -> Result<Self, SessionError> {
        check_pow2("hist_reqs", caps.reqs)?;
        check_pow2("bars", caps.bars)?;

        // Tek-yazar kilidini HER ŞEYDEN ÖNCE al: segmentleri kurup sonra kilidin
        // alınamadığını fark etmek, ikinci service'in halka başlıklarına
        // dokunmasına fırsat verirdi.
        //
        // Ad EA'nınkinden farklı — EA'nın kilidine DOKUNMUYORUZ.
        let lock = InstanceLock::acquire(&lock_name(instance))?;

        let names = SegmentNames::new(instance);

        let mem_reqs = SharedMem::create_or_open(
            &names.hist_reqs,
            sinyal_proto::ring::bytes_needed::<HistReq>(caps.reqs),
        )?;
        let mem_bars = SharedMem::create_or_open(
            &names.bars,
            sinyal_proto::ring::bytes_needed::<BarRec>(caps.bars),
        )?;

        // Güvenlik: her eşleme en az `bytes_needed` bayt ve 64'e hizalı;
        // `was_created` sayesinde yalnızca YENİ segmentlerde başlık yazılıyor,
        // böylece çalışan bir akışın sayaçları sıfırlanmıyor.
        let reqs = unsafe { init_or_attach_ring(&mem_reqs, caps.reqs)? };
        let bars = unsafe { init_or_attach_ring(&mem_bars, caps.bars)? };

        Ok(Self {
            reqs,
            bars,
            _mem_reqs: mem_reqs,
            _mem_bars: mem_bars,
            _lock: Some(lock),
            owner_thread: AtomicU32::new(0),
            thread_violations: AtomicU64::new(0),
            instance: instance.to_owned(),
        })
    }

    /// Çekirdek tarafı: var olan segmentlere bağlan.
    ///
    /// `hreq` halkasına **yazar**, `bars` halkasından **okur**.
    ///
    /// Kapasiteler **keşfedilir**, tahmin edilmez: her segmentin başlığı önce
    /// tek başına okunur (`peek_header`), gerçek kapasite oradan alınır ve
    /// segment o boyutta eşlenir. Tahmin edip yanlış boyutta eşlemek, sebebi
    /// hiç anlaşılmayan `ERROR_ACCESS_DENIED` üretirdi.
    ///
    /// Service henüz başlamadıysa [`SessionError::Shm`] döner — çağıran yeniden
    /// denemelidir, bu beklenen bir durumdur. (Hata metni "EA henüz başlamamış
    /// olabilir" der; bu kanalda karşılığı service'tir.)
    pub fn attach(instance: &str) -> Result<Self, SessionError> {
        let names = SegmentNames::new(instance);

        // Güvenlik: her önizleme eşlemesi en az başlık kadar; `peek_header`
        // imza/sürüm doğrulaması, `attach` ayrıca slot boyutu doğrulaması
        // yapıyor — `Cell<BarRec>` 128 bayt olduğu için yanlış segment adına
        // bağlanmak burada yakalanır.
        let (mem_reqs, reqs) = unsafe { open_ring_auto::<HistReq>(&names.hist_reqs)? };
        let (mem_bars, bars) = unsafe { open_ring_auto::<BarRec>(&names.bars)? };

        Ok(Self {
            reqs,
            bars,
            _mem_reqs: mem_reqs,
            _mem_bars: mem_bars,
            // Çekirdek `bars` halkasının OKUYUCUSU; kilit YAZAR tekliğini
            // korur, okuyucuyu kilitlemek service'i hiç başlatmazdı.
            _lock: None,
            owner_thread: AtomicU32::new(0),
            thread_violations: AtomicU64::new(0),
            instance: instance.to_owned(),
        })
    }

    /// Çağıran thread'in bu oturumun sahibi olduğunu doğrula.
    ///
    /// İlk çağrıda sahibi belirler; sonrakilerde karşılaştırır. `false`
    /// dönerse çağıran işlemi YAPMAMALIDIR — yapmak SPSC sözleşmesini ihlal
    /// eder ve veriyi bozar.
    #[inline]
    fn check_thread(&self) -> bool {
        let me = current_thread_id();
        let owner = self.owner_thread.load(Ordering::Relaxed);
        if owner == me {
            return true;
        }
        if owner == 0
            && self
                .owner_thread
                .compare_exchange(0, me, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            return true;
        }
        self.thread_violations.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Yanlış thread'den gelip reddedilen işlem sayısı. 0 olmalı.
    #[inline]
    pub fn thread_violations(&self) -> u64 {
        self.thread_violations.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn instance(&self) -> &str {
        &self.instance
    }

    // --- Çekirdek tarafı: iste, barları topla ---

    /// Geçmiş isteği gönder (çekirdek üretir). Halka doluysa `false`.
    ///
    /// İstekler seyrek olduğu için dolu bulmak neredeyse imkânsızdır; olursa
    /// service tamamen durmuş demektir ve çağıran yeniden denemelidir.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i (çekirdek) çağırmalıdır.
    #[inline]
    pub unsafe fn push_req(&self, r: &HistReq) -> bool {
        if !self.check_thread() {
            return false;
        }
        unsafe { self.reqs.push(r) }
    }

    /// Bar oku (çekirdek tüketir). Halka boşsa `None`.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_bar(&self) -> Option<BarRec> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.bars.pop() }.map(|(_, b)| b)
    }

    /// En fazla `out.len()` barı toplu oku; okunan sayıyı döndürür.
    ///
    /// Geçmiş akışı patlamalıdır (tek istek binlerce bar); bar başına iki
    /// atomik erişim yerine parti başına iki erişim yapmak tüketici tarafının
    /// maliyetini belirgin düşürür.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_bar_batch(&self, out: &mut [BarRec]) -> usize {
        if !self.check_thread() {
            return 0;
        }
        unsafe { self.bars.pop_batch(out) }
    }

    // --- Service tarafı: isteği al, barları yayınla ---

    /// Bekleyen geçmiş isteğini al (service tüketir). Yoksa `None`.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i (service döngüsü) çağırmalıdır.
    #[inline]
    pub unsafe fn pop_req(&self) -> Option<HistReq> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.reqs.pop() }.map(|(_, r)| r)
    }

    /// Bar yayınla (service üretir). Halka doluysa `false` — bar YAZILMADI.
    ///
    /// # Burada tick yolundan FARKLI davranıyoruz
    ///
    /// Tick yazarı (EA) dolu halkada yeniden DENEMEZ: ticaret terminalinin
    /// thread'ini bekletmek gecikme için yaptığımız her şeyi geçersiz kılardı
    /// ve kaybedilen tek bir tick'in yerine hemen yenisi gelir.
    ///
    /// Geçmiş barı öyle değil: kaybolan bar bir daha ASLA gelmez, seride kalıcı
    /// bir delik açar. Service kendi thread'inde çalıştığı ve `Sleep()`
    /// kullanabildiği için `false` dönüşünde kısa bir uykuyla **yeniden
    /// DENEMELİDİR**. Bu fonksiyon bilinçli olarak bloklamaz; yeniden deneme
    /// kararı çağırana bırakılır çünkü uyku süresi MQL5 tarafının işidir.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i (service) çağırmalıdır.
    #[inline]
    pub unsafe fn push_bar(&self, b: &BarRec) -> bool {
        if !self.check_thread() {
            return false;
        }
        unsafe { self.bars.push(b) }
    }

    // --- Teşhis ---

    /// Halka dolu bulunduğu için reddedilen `push_bar` sayısı.
    ///
    /// Service yeniden denediği için bu **kayıp değil, geri basınç** ölçüsüdür:
    /// çekirdeğin barları ne sıklıkla geriden takip ettiğini gösterir. Gerçek
    /// kayıp [`BarRec::total`](sinyal_proto::BarRec::total) ile teslim edilen
    /// bar sayısının karşılaştırılmasından anlaşılır.
    #[inline]
    pub fn bar_push_failures(&self) -> u64 {
        self.bars.push_failures()
    }

    /// Halka dolu bulunduğu için reddedilen `push_req` sayısı.
    #[inline]
    pub fn req_push_failures(&self) -> u64 {
        self.reqs.push_failures()
    }

    /// Okunmayı bekleyen bar sayısı — çekirdeğin geride kalıp kalmadığı.
    #[inline]
    pub fn bar_backlog(&self) -> u64 {
        self.bars.len()
    }

    /// Service tarafından alınmayı bekleyen istek sayısı.
    #[inline]
    pub fn req_backlog(&self) -> u64 {
        self.reqs.len()
    }

    /// Bar halkasının kapasitesi (service'in kurduğu değer).
    #[inline]
    pub fn bar_capacity(&self) -> u64 {
        self.bars.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinyal_proto::{bar_flag, timeframe};

    fn small_caps() -> HistCapacities {
        HistCapacities { reqs: 8, bars: 16 }
    }

    fn unique(tag: &str) -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        format!("h{}-{}-{}", tag, std::process::id(), N.fetch_add(1, Ordering::Relaxed))
    }

    fn sample_bar(req_id: u32, index: u32, total: u32, close: f64) -> BarRec {
        BarRec {
            time_msc: 1_700_000_000_000 + index as i64 * 60_000,
            open: close - 0.5,
            high: close + 1.0,
            low: close - 1.0,
            close,
            tick_volume: 42,
            real_volume: 0,
            req_id,
            symbol_id: 3,
            timeframe: timeframe::M1,
            spread: 12,
            flags: if index + 1 == total { bar_flag::LAST | bar_flag::PARTIAL } else { 0 },
            index,
            total,
            ..Default::default()
        }
    }

    #[test]
    fn request_and_bars_cross_the_boundary_end_to_end() {
        let inst = unique("stream");
        // Service segmentleri kurar, çekirdek sonradan bağlanır.
        let svc = HistSession::create_with(&inst, small_caps()).unwrap();
        let core = HistSession::attach(&inst).unwrap();

        // Çekirdek 3 M1 barı istiyor.
        let req = HistReq {
            to_msc: 0,
            req_id: 77,
            symbol_id: 3,
            timeframe: timeframe::M1,
            count: 3,
            ..Default::default()
        };
        assert!(unsafe { core.push_req(&req) });

        let got = unsafe { svc.pop_req() }.expect("service isteği almalı");
        assert_eq!(got.req_id, 77);
        assert_eq!(got.timeframe, timeframe::M1);
        assert_eq!(got.count, 3);
        assert!(unsafe { svc.pop_req() }.is_none(), "istek iki kez teslim edilmemeli");

        // Service barları yayınlıyor — fiziksel sıra eskiden yeniye.
        for i in 0..3u32 {
            assert!(unsafe { svc.push_bar(&sample_bar(77, i, 3, 1.10 + i as f64 * 0.01)) });
        }

        let mut bars = Vec::new();
        while let Some(b) = unsafe { core.pop_bar() } {
            bars.push(b);
        }
        assert_eq!(bars.len(), 3);
        assert_eq!(bars[0].index, 0, "index 0 EN ESKİ bar olmalı");
        assert_eq!(bars[0].req_id, 77);
        assert!(bars[0].is_coherent());
        assert!(!bars[0].is_last());
        // CopyRates'in son barı oluşmakta olandır.
        assert!(bars[2].is_last() && bars[2].is_partial());
        assert_eq!(bars[2].total, 3, "toplam sayı eksik teslimatı yakalar");
        // Broker gerçek hacim yayınlamıyor — 0 "hacim yok" sanılmamalı.
        assert_eq!(bars[2].real_volume_opt(), None);
        assert_eq!(svc.bar_push_failures(), 0);
    }

    #[test]
    fn batch_read_delivers_bars_in_order() {
        let inst = unique("batch");
        let svc = HistSession::create_with(&inst, small_caps()).unwrap();
        let core = HistSession::attach(&inst).unwrap();

        for i in 0..5u32 {
            assert!(unsafe { svc.push_bar(&sample_bar(1, i, 5, 100.0 + i as f64)) });
        }
        let mut out = [BarRec::default(); 8];
        let n = unsafe { core.pop_bar_batch(&mut out) };
        assert_eq!(n, 5);
        assert_eq!(out[0].index, 0);
        assert_eq!(out[4].index, 4);
        assert_eq!(unsafe { core.pop_bar_batch(&mut out) }, 0, "boş halka 0 döner");
    }

    #[test]
    fn full_bar_ring_refuses_write_without_erroring() {
        // Halka dolduğunda `push_bar` HATA DEĞİL `false` döner: service kısa bir
        // `Sleep` ile yeniden deneyebilsin diye. Hata döndürseydik service
        // isteği iptal eder ve barlar kalıcı kaybolurdu.
        let inst = unique("full");
        let svc = HistSession::create_with(&inst, small_caps()).unwrap();
        let cap = small_caps().bars;

        for i in 0..cap {
            assert!(unsafe { svc.push_bar(&sample_bar(1, i as u32, cap as u32, 1.0)) });
        }
        assert!(!unsafe { svc.push_bar(&sample_bar(1, 999, 999, 1.0)) }, "dolu halka reddetmeli");
        assert_eq!(svc.bar_push_failures(), 1);
        assert_eq!(svc.bar_backlog(), cap);

        // Çekirdek okuyunca yer açılmalı ve yeniden deneme BAŞARILI olmalı —
        // service'in yapması gereken tam olarak bu.
        let core = HistSession::attach(&inst).unwrap();
        assert!(unsafe { core.pop_bar() }.is_some());
        assert!(unsafe { svc.push_bar(&sample_bar(1, 999, 999, 1.0)) }, "yer açılınca yazmalı");
    }

    #[test]
    fn operations_from_a_foreign_thread_are_refused_not_corrupting() {
        let inst = unique("thread");
        let svc = HistSession::create_with(&inst, small_caps()).unwrap();

        // Sahibi bu thread olarak belirle.
        assert!(unsafe { svc.push_bar(&sample_bar(1, 0, 1, 1.0)) });
        assert_eq!(svc.thread_violations(), 0);

        let svc = std::sync::Arc::new(svc);
        let svc2 = svc.clone();
        let accepted =
            std::thread::spawn(move || unsafe { svc2.push_bar(&sample_bar(2, 0, 1, 2.0)) })
                .join()
                .unwrap();
        assert!(!accepted, "yabancı thread'in yazımı reddedilmeli");
        assert_eq!(svc.thread_violations(), 1, "ihlal sayılmalı");

        // Okuma da reddedilmeli — SPSC iki yönde de tek taraflıdır.
        let svc3 = svc.clone();
        let popped = std::thread::spawn(move || unsafe { svc3.pop_req() }).join().unwrap();
        assert!(popped.is_none());
        assert_eq!(svc.thread_violations(), 2);

        // Reddedilen yazım halkaya SIZMAMALI.
        let core = HistSession::attach(&inst).unwrap();
        assert_eq!(unsafe { core.pop_bar() }.unwrap().req_id, 1);
        assert!(unsafe { core.pop_bar() }.is_none());
    }

    #[test]
    fn hist_lock_does_not_collide_with_the_ea_lock() {
        // ASIL KRİTİK DAVRANIŞ: service ile EA aynı örnek adını kullanır ama
        // ayrı kilitler alır. Aynı kilit olsaydı ikisinden yalnızca biri
        // başlayabilirdi.
        let inst = unique("locks");
        let caps = crate::Capacities {
            ticks: 64,
            books: 8,
            cmds: 8,
            results: 8,
            symbols: 16,
            positions: 8,
            orders: 8,
        };
        let _ea = crate::Session::create(&inst, caps).expect("EA açılmalı");
        let _svc = HistSession::create_with(&inst, small_caps()).expect("service de açılmalı");

        // Ters sıra da çalışmalı (service önce başlarsa EA engellenmemeli).
        let inst2 = unique("locks2");
        let _svc2 = HistSession::create_with(&inst2, small_caps()).unwrap();
        assert!(crate::Session::create(&inst2, caps).is_ok(), "service EA'yı engellememeli");
    }

    #[test]
    fn second_service_on_same_instance_is_refused() {
        // Bar halkası SPSC: ikinci bir service yazarı veriyi bozardı.
        let inst = unique("dupsvc");
        let _first = HistSession::create_with(&inst, small_caps()).unwrap();
        let err = HistSession::create_with(&inst, small_caps()).unwrap_err();
        assert!(
            matches!(err, SessionError::Shm(sinyal_shm::ShmError::InstanceBusy(_))),
            "ikinci service reddedilmeli, bulunan: {err:?}"
        );
    }

    #[test]
    fn attach_before_service_starts_fails_and_is_retryable() {
        let inst = unique("notyet");
        let err = HistSession::attach(&inst).unwrap_err();
        assert!(matches!(err, SessionError::Shm(_)));

        let _svc = HistSession::create_with(&inst, small_caps()).unwrap();
        assert!(HistSession::attach(&inst).is_ok());
        assert!(HistSession::attach(&inst).is_ok(), "birden çok okuyucu olabilir");
    }

    #[test]
    fn writer_lock_is_released_when_session_drops() {
        // Service yeniden başladığında kilit takılı kalmamalı.
        let inst = unique("lockrelease");
        {
            let _svc = HistSession::create_with(&inst, small_caps()).unwrap();
            assert!(HistSession::create_with(&inst, small_caps()).is_err());
        }
        assert!(HistSession::create_with(&inst, small_caps()).is_ok(), "kilit serbest kalmalı");
    }

    #[test]
    fn rejects_non_power_of_two_capacity() {
        let inst = unique("badcap");
        let caps = HistCapacities { reqs: 8, bars: 100 };
        assert!(matches!(
            HistSession::create_with(&inst, caps),
            Err(SessionError::BadCapacity("bars", 100))
        ));
    }

    #[test]
    fn attaching_to_a_tick_segment_is_refused_by_slot_size() {
        // Adlar aynı örnekten türüyor ve `RING_MAGIC` tüm halkalarda aynı;
        // yanlış segmente bağlanmayı yakalayan tek çalışma-zamanı denetimi slot
        // boyutudur. `Cell<BarRec>` = 128, `Cell<Tick>` = 64.
        let inst = unique("wrongseg");
        let caps = crate::Capacities {
            ticks: 64,
            books: 8,
            cmds: 8,
            results: 8,
            symbols: 16,
            positions: 8,
            orders: 8,
        };
        let _ea = crate::Session::create(&inst, caps).unwrap();
        // EA `hreq`/`bars` segmentlerini KURMAZ; bağlanma açılışta başarısız olur.
        assert!(matches!(HistSession::attach(&inst), Err(SessionError::Shm(_))));

        // Doğrudan tick segmentine bar halkası olarak bağlanmak da reddedilmeli.
        let names = SegmentNames::new(&inst);
        let mem = SharedMem::open(&names.ticks, sinyal_proto::ring::RING_HEADER_SIZE).unwrap();
        assert!(matches!(
            unsafe { Ring::<BarRec>::attach(mem.as_ptr()) },
            Err(sinyal_proto::RingError::SlotSizeMismatch { .. })
        ));
    }

    #[test]
    fn service_restart_preserves_unread_bars() {
        // Çekirdek hâlâ bağlıyken service yeniden başlarsa okunmamış barlar
        // kaybolmamalı ve sayaçlar sıfırlanmamalı.
        let inst = unique("restart");
        let svc1 = HistSession::create_with(&inst, small_caps()).unwrap();
        let core = HistSession::attach(&inst).unwrap();

        assert!(unsafe { svc1.push_bar(&sample_bar(1, 0, 2, 1.0)) });
        assert!(unsafe { svc1.push_bar(&sample_bar(1, 1, 2, 2.0)) });
        assert_eq!(unsafe { core.pop_bar() }.unwrap().index, 0);

        drop(svc1);
        let svc2 = HistSession::create_with(&inst, small_caps()).unwrap();
        assert!(unsafe { svc2.push_bar(&sample_bar(2, 0, 1, 3.0)) });

        assert_eq!(unsafe { core.pop_bar() }.unwrap().index, 1, "okunmamış bar kaybolmamalı");
        assert_eq!(unsafe { core.pop_bar() }.unwrap().req_id, 2, "yeni bar arkaya eklenmeli");
    }
}
