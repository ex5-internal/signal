//! Köprü oturumu — bir EA örneğinin tüm paylaşımlı bellek segmentleri.
//!
//! Bu modül saf Rust'tır (C ABI içermez), böylece testlerden ve
//! `latency-bench`'ten doğrudan kullanılabilir. C ABI sarmalayıcısı
//! [`crate::ffi`] içindedir.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use sinyal_proto::{
    capacity, Book, BookLevel, Cmd, Res, Ring, SegmentNames, SymbolEntry, SymbolTable, Tick,
    MAX_BOOK_DEPTH,
};
use sinyal_shm::{current_thread_id, InstanceLock, SharedMem};

/// Bir köprü oturumunun kapasiteleri.
///
/// Varsayılanlar [`sinyal_proto::capacity`] ile aynıdır; testler küçük
/// değerlerle çalışabilsin diye ayarlanabilir bırakıldı.
#[derive(Debug, Clone, Copy)]
pub struct Capacities {
    pub ticks: u64,
    pub books: u64,
    pub cmds: u64,
    pub results: u64,
    pub symbols: u32,
}

impl Default for Capacities {
    fn default() -> Self {
        Self {
            ticks: capacity::TICKS,
            books: capacity::BOOKS,
            cmds: capacity::CMDS,
            results: capacity::RESULTS,
            symbols: capacity::SYMBOLS,
        }
    }
}

/// Köprü kurulumunda oluşabilecek hatalar.
#[derive(Debug)]
pub enum SessionError {
    Shm(sinyal_shm::ShmError),
    Ring(sinyal_proto::RingError),
    SymTab(sinyal_proto::SymTabError),
    /// Kapasite 2'nin kuvveti değil.
    BadCapacity(&'static str, u64),
    /// İşlem, oturumun sahibi olmayan bir thread'den geldi ve REDDEDİLDİ.
    ///
    /// Halkalar SPSC'dir; ikinci bir thread'in yazmasına izin vermek veriyi
    /// bozardı. Bu bir yapılandırma hatasıdır.
    WrongThread,
}

impl core::fmt::Display for SessionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Shm(e) => write!(f, "paylaşımlı bellek: {e}"),
            Self::Ring(e) => write!(f, "halka: {e}"),
            Self::SymTab(e) => write!(f, "sembol tablosu: {e}"),
            Self::BadCapacity(which, v) => {
                write!(f, "{which} kapasitesi 2'nin kuvveti olmalı, verilen {v}")
            }
            Self::WrongThread => write!(
                f,
                "işlem oturumun sahibi olmayan bir thread'den geldi ve reddedildi \
                 — halka tek yazarlıdır (SPSC), ikinci thread veriyi bozardı"
            ),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<sinyal_shm::ShmError> for SessionError {
    fn from(e: sinyal_shm::ShmError) -> Self {
        Self::Shm(e)
    }
}
impl From<sinyal_proto::RingError> for SessionError {
    fn from(e: sinyal_proto::RingError) -> Self {
        Self::Ring(e)
    }
}
impl From<sinyal_proto::SymTabError> for SessionError {
    fn from(e: sinyal_proto::SymTabError) -> Self {
        Self::SymTab(e)
    }
}

/// Bir EA örneğinin köprü oturumu.
///
/// # Segment sahipliği
///
/// Segmentleri **EA oluşturur** (veri kaynağı odur); çekirdek yalnızca açar ve
/// EA gelene kadar yeniden dener. Tek sahip olması, "kim başlatır" yarışını
/// tamamen ortadan kaldırır.
///
/// Terminal yeniden başlatıldığında çekirdek segmenti hâlâ açık tutuyor
/// olabilir — bu durumda [`SharedMem::was_created`] `false` döner ve halka
/// başlığı **yeniden başlatılmaz**; sayaçlar korunur, tüketici akışın
/// ortasında kopmuş sayılmaz.
pub struct Session {
    // Eşlemeler sahiplenilmeli: düşerlerse halkaların işaret ettiği bellek
    // eşlemesiz kalır. Alan sırası önemli — Rust alanları bildirim sırasıyla
    // düşürür, bu yüzden halkalar eşlemelerden ÖNCE gelir.
    ticks: Ring<Tick>,
    books: Ring<Book>,
    cmds: Ring<Cmd>,
    results: Ring<Res>,
    symtab: SymbolTable,

    _mem_ticks: SharedMem,
    _mem_books: SharedMem,
    _mem_cmds: SharedMem,
    _mem_results: SharedMem,
    _mem_symbols: SharedMem,

    /// Yalnızca yazar (EA) tarafında dolu. Düştüğünde örnek adı serbest kalır.
    _lock: Option<InstanceLock>,

    /// Bu oturuma erişen ilk thread'in kimliği (0 = henüz belirlenmedi).
    ///
    /// SPSC sözleşmesinin çalışma zamanı denetimi. İsimli mutex zaten ikinci
    /// bir EA örneğini engelliyor; bu ikinci savunma hattı, sözleşmenin
    /// beklenmedik bir yoldan ihlalini (ör. MT5'in gelecekte olay modelini
    /// değiştirmesi) sessiz veri bozulması yerine SAYILABİLİR bir olaya
    /// çevirir.
    owner_thread: AtomicU32,
    /// Yanlış thread'den gelip REDDEDİLEN işlem sayısı.
    thread_violations: AtomicU64,

    instance: String,
}

impl core::fmt::Debug for Session {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Session")
            .field("instance", &self.instance)
            .field("tick_backlog", &self.tick_backlog())
            .field("tick_push_failures", &self.tick_push_failures())
            .finish_non_exhaustive()
    }
}

impl Session {
    /// EA tarafı: segmentleri oluştur (veya var olana bağlan) ve halkaları kur.
    pub fn create(instance: &str, caps: Capacities) -> Result<Self, SessionError> {
        check_pow2("ticks", caps.ticks)?;
        check_pow2("books", caps.books)?;
        check_pow2("cmds", caps.cmds)?;
        check_pow2("results", caps.results)?;

        // Tek-yazar kilidini HER ŞEYDEN ÖNCE al. Segmentleri kurup sonra
        // kilidin alınamadığını fark etmek, ikinci EA'nın halka başlıklarına
        // dokunmasına fırsat verirdi.
        let lock = InstanceLock::acquire(instance)?;

        let names = SegmentNames::new(instance);

        let mem_ticks =
            SharedMem::create_or_open(&names.ticks, sinyal_proto::ring::bytes_needed::<Tick>(caps.ticks))?;
        let mem_books =
            SharedMem::create_or_open(&names.books, sinyal_proto::ring::bytes_needed::<Book>(caps.books))?;
        let mem_cmds =
            SharedMem::create_or_open(&names.cmds, sinyal_proto::ring::bytes_needed::<Cmd>(caps.cmds))?;
        let mem_results = SharedMem::create_or_open(
            &names.results,
            sinyal_proto::ring::bytes_needed::<Res>(caps.results),
        )?;
        let mem_symbols = SharedMem::create_or_open(
            &names.symbols,
            sinyal_proto::symtab::bytes_needed(caps.symbols),
        )?;

        // Güvenlik: her eşleme en az `bytes_needed` bayt ve 64'e hizalı;
        // `was_created` ile yalnızca YENİ segmentlerde başlık yazılıyor, böylece
        // çalışan bir akışın sayaçları sıfırlanmıyor.
        let ticks = unsafe { init_or_attach_ring(&mem_ticks, caps.ticks)? };
        let books = unsafe { init_or_attach_ring(&mem_books, caps.books)? };
        let cmds = unsafe { init_or_attach_ring(&mem_cmds, caps.cmds)? };
        let results = unsafe { init_or_attach_ring(&mem_results, caps.results)? };
        let symtab = unsafe {
            if mem_symbols.was_created() {
                SymbolTable::init(mem_symbols.as_ptr(), caps.symbols)
            } else {
                SymbolTable::attach(mem_symbols.as_ptr())?
            }
        };

        Ok(Self {
            ticks,
            books,
            cmds,
            results,
            symtab,
            _mem_ticks: mem_ticks,
            _mem_books: mem_books,
            _mem_cmds: mem_cmds,
            _mem_results: mem_results,
            _mem_symbols: mem_symbols,
            _lock: Some(lock),
            owner_thread: AtomicU32::new(0),
            thread_violations: AtomicU64::new(0),
            instance: instance.to_owned(),
        })
    }

    /// Çekirdek tarafı: var olan segmentlere bağlan.
    ///
    /// Kapasiteler **keşfedilir**, tahmin edilmez: her segmentin başlığı önce
    /// tek başına okunur, gerçek kapasite oradan alınır ve segment o boyutta
    /// eşlenir.
    ///
    /// Bu, EA ile çekirdeğin farklı kapasite yapılandırmalarıyla çalışması
    /// durumunda ortaya çıkan ve sebebi hiç anlaşılmayan
    /// `ERROR_ACCESS_DENIED` hatasını tamamen ortadan kaldırır.
    ///
    /// EA henüz başlamadıysa [`SessionError::Shm`] döner — çağıran yeniden
    /// denemelidir, bu beklenen bir durumdur.
    pub fn open(instance: &str) -> Result<Self, SessionError> {
        let names = SegmentNames::new(instance);

        // Güvenlik: her önizleme eşlemesi en az başlık kadar; `peek_header`
        // imza/sürüm doğrulaması yapıyor.
        let (mem_ticks, ticks) = unsafe { open_ring_auto::<Tick>(&names.ticks)? };
        let (mem_books, books) = unsafe { open_ring_auto::<Book>(&names.books)? };
        let (mem_cmds, cmds) = unsafe { open_ring_auto::<Cmd>(&names.cmds)? };
        let (mem_results, results) = unsafe { open_ring_auto::<Res>(&names.results)? };

        let sym_cap = {
            let probe = SharedMem::open(&names.symbols, sinyal_proto::symtab::SYMTAB_HEADER_SIZE)?;
            unsafe { sinyal_proto::symtab::peek_header(probe.as_ptr()) }?
        };
        let mem_symbols =
            SharedMem::open(&names.symbols, sinyal_proto::symtab::bytes_needed(sym_cap))?;
        let symtab = unsafe { SymbolTable::attach(mem_symbols.as_ptr())? };

        Ok(Self {
            ticks,
            books,
            cmds,
            results,
            symtab,
            _mem_ticks: mem_ticks,
            _mem_books: mem_books,
            _mem_cmds: mem_cmds,
            _mem_results: mem_results,
            _mem_symbols: mem_symbols,
            // Çekirdek okuyucudur, kilit almaz: kilit YAZAR tekliğini korur.
            // Çekirdeği de kilitlemek, EA'nın hiç başlayamamasına yol açardı.
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
    ///
    /// Maliyet: sıcak yolda tek bir relaxed atomik okuma (önbellekte sıcak,
    /// ~1 ns). Sessiz veri bozulmasına karşı bu bedel kabul edilebilir.
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
    ///
    /// Sıfırdan farklıysa SPSC sözleşmesi ihlal edilmiş demektir — veri
    /// bozulmadı (işlem reddedildi) ama yapılandırma yanlış.
    #[inline]
    pub fn thread_violations(&self) -> u64 {
        self.thread_violations.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn instance(&self) -> &str {
        &self.instance
    }

    // --- EA tarafı: piyasa verisi yayınla ---

    /// Tick yayınla. Halka doluysa `false` — mesaj KAYBOLUR.
    ///
    /// EA bilinçli olarak yeniden denemez: ticaret terminalinin thread'ini
    /// bekletmek, gecikmeyi düşürmek için yaptığımız her şeyi geçersiz kılardı.
    /// Kayıp [`Session::tick_push_failures`] ile izlenir.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i çağırmalıdır (EA thread'i).
    #[inline]
    pub unsafe fn push_tick(&self, t: &Tick) -> bool {
        // Yanlış thread'den gelirse YAZMA. `false` her iki durumda da
        // "yazılmadı" demek olduğu için çağıran açısından anlam tutarlı;
        // ayrımı `thread_violations()` sayacı verir.
        if !self.check_thread() {
            return false;
        }
        unsafe { self.ticks.push(t) }
    }

    /// Derinlik anlık görüntüsü yayınla.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn push_book(&self, b: &Book) -> bool {
        if !self.check_thread() {
            return false;
        }
        unsafe { self.books.push(b) }
    }

    /// Derinlik tablosunu düz dizilerden kur ve yayınla.
    ///
    /// MQL5'in `MqlBookInfo` dizisini doğrudan geçirmek yerine düz dizilerle
    /// çalışıyoruz: MQL5 yapı yerleşiminin C ile eşleştiğini varsaymak yerine
    /// kopyalama maliyetini (birkaç yüz bayt) kabul ediyoruz. Bu, protokoldeki
    /// en kırılgan noktayı tamamen ortadan kaldırıyor.
    ///
    /// Broker [`MAX_BOOK_DEPTH`]'ten fazla seviye verirse baştan (en iyi
    /// fiyatlar) kesilir ve `truncated` işaretlenir.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i çağırmalıdır.
    pub unsafe fn push_book_flat(
        &self,
        symbol_id: u32,
        time_msc: i64,
        recv_qpc: u64,
        prices: &[f64],
        volumes: &[f64],
        kinds: &[u8],
    ) -> bool {
        let given = prices.len().min(volumes.len()).min(kinds.len());
        let depth = given.min(MAX_BOOK_DEPTH);

        let mut b = Book {
            time_msc,
            recv_qpc,
            symbol_id,
            depth: depth as u16,
            kind: sinyal_proto::kind::BOOK,
            truncated: u8::from(given > MAX_BOOK_DEPTH),
            ..Default::default()
        };
        for i in 0..depth {
            b.levels[i] = BookLevel {
                price: prices[i],
                volume_real: volumes[i],
                kind: kinds[i],
                _pad: [0; 7],
            };
        }
        unsafe { self.push_book(&b) }
    }

    /// Sembol tablosunu topluca değiştir.
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir yazar thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn set_symbols(&self, entries: &[SymbolEntry]) -> Result<(), SessionError> {
        if !self.check_thread() {
            return Err(SessionError::WrongThread);
        }
        unsafe { self.symtab.replace_all(entries) }.map_err(SessionError::SymTab)
    }

    /// Bekleyen emir komutunu al (EA tarafı tüketir).
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_cmd(&self) -> Option<Cmd> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.cmds.pop() }.map(|(_, c)| c)
    }

    /// Emir sonucu / ticaret olayı yayınla (EA tarafı üretir).
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn push_res(&self, r: &Res) -> bool {
        if !self.check_thread() {
            return false;
        }
        unsafe { self.results.push(r) }
    }

    // --- Çekirdek tarafı: tüket ---

    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_tick(&self) -> Option<Tick> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.ticks.pop() }.map(|(_, t)| t)
    }

    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_tick_batch(&self, out: &mut [Tick]) -> usize {
        if !self.check_thread() {
            return 0;
        }
        unsafe { self.ticks.pop_batch(out) }
    }

    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_book(&self) -> Option<Book> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.books.pop() }.map(|(_, b)| b)
    }

    /// # Güvenlik
    /// Yalnızca TEK bir tüketici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn pop_res(&self) -> Option<Res> {
        if !self.check_thread() {
            return None;
        }
        unsafe { self.results.pop() }.map(|(_, r)| r)
    }

    /// Emir komutu gönder (çekirdek tarafı üretir).
    ///
    /// # Güvenlik
    /// Yalnızca TEK bir üretici thread'i çağırmalıdır.
    #[inline]
    pub unsafe fn push_cmd(&self, c: &Cmd) -> bool {
        if !self.check_thread() {
            return false;
        }
        unsafe { self.cmds.push(c) }
    }

    /// Sembol tablosunun tutarlı anlık görüntüsü.
    #[inline]
    pub fn symbols(&self) -> Option<Vec<SymbolEntry>> {
        self.symtab.snapshot(64)
    }

    // --- Teşhis ---

    /// Halka dolu olduğu için KAYBEDİLEN tick sayısı.
    ///
    /// EA yeniden denemediği için bu doğrudan kayıp demektir; Faz 1'in
    /// "kayıp = 0" kriteri tam olarak bunu ölçer.
    #[inline]
    pub fn tick_push_failures(&self) -> u64 {
        self.ticks.push_failures()
    }

    #[inline]
    pub fn book_push_failures(&self) -> u64 {
        self.books.push_failures()
    }

    #[inline]
    pub fn res_push_failures(&self) -> u64 {
        self.results.push_failures()
    }

    #[inline]
    pub fn cmd_push_failures(&self) -> u64 {
        self.cmds.push_failures()
    }

    /// Okunmayı bekleyen tick sayısı — tüketicinin geride kalıp kalmadığını
    /// gösterir.
    #[inline]
    pub fn tick_backlog(&self) -> u64 {
        self.ticks.len()
    }
}

/// Var olan bir halkayı, kapasitesini KEŞFEDEREK aç.
///
/// İki aşama: önce yalnızca başlık kadar eşle ve kapasiteyi oku, sonra doğru
/// boyutta yeniden eşle. Önizleme eşlemesi ikinciden önce serbest bırakılır.
///
/// # Güvenlik
/// Çağıran, dönen `SharedMem`'i `Ring` yaşadığı sürece canlı tutmalıdır.
unsafe fn open_ring_auto<T: Copy>(name: &str) -> Result<(SharedMem, Ring<T>), SessionError> {
    let capacity = {
        let probe = SharedMem::open(name, sinyal_proto::ring::RING_HEADER_SIZE)?;
        // Güvenlik: önizleme en az başlık kadar eşlendi.
        let meta = unsafe { sinyal_proto::ring::peek_header(probe.as_ptr()) }?;
        meta.capacity
    };
    let mem = SharedMem::open(name, sinyal_proto::ring::bytes_needed::<T>(capacity))?;
    // Güvenlik: eşleme tam boyutta; `attach` slot boyutunu da doğruluyor.
    let ring = unsafe { Ring::<T>::attach(mem.as_ptr())? };
    Ok((mem, ring))
}

/// Segment yeniyse halkayı kur, değilse var olana bağlan.
///
/// # Güvenlik
/// `mem` en az `bytes_needed::<T>(capacity)` bayt eşlenmiş olmalı.
unsafe fn init_or_attach_ring<T: Copy>(
    mem: &SharedMem,
    capacity: u64,
) -> Result<Ring<T>, SessionError> {
    unsafe {
        if mem.was_created() {
            Ok(Ring::<T>::init(mem.as_ptr(), capacity))
        } else {
            Ring::<T>::attach(mem.as_ptr()).map_err(SessionError::Ring)
        }
    }
}

fn check_pow2(which: &'static str, v: u64) -> Result<(), SessionError> {
    if v == 0 || !v.is_power_of_two() {
        Err(SessionError::BadCapacity(which, v))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinyal_proto::{filling, kind, write_fixed_str};

    fn small_caps() -> Capacities {
        Capacities { ticks: 64, books: 8, cmds: 8, results: 8, symbols: 16 }
    }

    fn unique(tag: &str) -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        format!("t{}-{}-{}", tag, std::process::id(), N.fetch_add(1, Ordering::Relaxed))
    }

    fn sample_tick(id: u32, bid: f64) -> Tick {
        Tick {
            time_msc: 1_700_000_000_000,
            bid,
            ask: bid + 0.0002,
            last: 0.0,
            volume_real: 0.0,
            recv_qpc: sinyal_shm::qpc(),
            symbol_id: id,
            flags: sinyal_proto::tick_flag::BID | sinyal_proto::tick_flag::ASK,
            kind: kind::TICK,
            _pad: 0,
        }
    }

    #[test]
    fn create_then_open_and_stream_ticks() {
        let inst = unique("stream");
        let ea = Session::create(&inst, small_caps()).unwrap();
        // Çekirdek ayrı bir "süreç" gibi bağlanıyor.
        let core = Session::open(&inst).unwrap();

        for i in 0..10u32 {
            assert!(unsafe { ea.push_tick(&sample_tick(i, 1.1000 + i as f64 * 0.0001)) });
        }

        let mut got = Vec::new();
        while let Some(t) = unsafe { core.pop_tick() } {
            got.push(t);
        }
        assert_eq!(got.len(), 10);
        assert_eq!(got[0].symbol_id, 0);
        assert_eq!(got[9].symbol_id, 9);
        assert!((got[9].bid - 1.1009).abs() < 1e-9);
        assert_eq!(ea.tick_push_failures(), 0);
    }

    #[test]
    fn open_before_ea_starts_fails_and_is_retryable() {
        let inst = unique("notyet");
        let err = Session::open(&inst).unwrap_err();
        assert!(matches!(err, SessionError::Shm(_)));
        // Mesaj operatöre ne olduğunu söylemeli — bu beklenen bir durum.
        assert!(err.to_string().contains("EA henüz başlamamış olabilir"));

        // EA başlayınca aynı çağrı çalışmalı.
        let _ea = Session::create(&inst, small_caps()).unwrap();
        assert!(Session::open(&inst).is_ok());
    }

    #[test]
    fn commands_flow_core_to_ea_and_results_flow_back() {
        let inst = unique("orders");
        let ea = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();

        // Çekirdek bir LIMIT emri gönderiyor — eski adaptörde bu hiç yoktu.
        let mut cmd = Cmd {
            client_id: 42,
            volume: 0.10,
            price: 1.0950,
            symbol_id: 3,
            action: sinyal_proto::action::PENDING,
            order_type: sinyal_proto::order_type::BUY_LIMIT,
            filling: filling::AUTO,
            ..Default::default()
        };
        write_fixed_str(&mut cmd.comment, "test-limit");
        assert!(unsafe { core.push_cmd(&cmd) });

        let got = unsafe { ea.pop_cmd() }.expect("EA komutu almalı");
        assert_eq!(got.client_id, 42);
        assert_eq!(got.order_type, sinyal_proto::order_type::BUY_LIMIT);
        assert_eq!(got.filling, filling::AUTO);
        assert_eq!(sinyal_proto::read_fixed_str(&got.comment), "test-limit");
        assert!(unsafe { ea.pop_cmd() }.is_none(), "komut iki kez teslim edilmemeli");

        // EA sonucu geri gönderiyor.
        let res = Res {
            client_id: 42,
            order: 998877,
            retcode: 10009,
            kind: sinyal_proto::res_kind::TRADE_TXN,
            ..Default::default()
        };
        assert!(unsafe { ea.push_res(&res) });
        let back = unsafe { core.pop_res() }.expect("çekirdek sonucu almalı");
        assert_eq!(back.client_id, 42);
        assert_eq!(back.order, 998877);
        assert_eq!(back.retcode, 10009);
    }

    #[test]
    fn symbol_table_crosses_the_boundary() {
        let inst = unique("symtab");
        let ea = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();

        let mut e = SymbolEntry {
            symbol_id: 0,
            digits: 2,
            point: 0.01,
            volume_min: 0.01,
            volume_step: 0.01,
            volume_max: 100.0,
            // Yalnız FOK destekleyen bir sembol — eski adaptörün IOC'yi sabit
            // kodlaması tam burada emirleri reddettiriyordu.
            filling_mode: 0b01,
            ..Default::default()
        };
        write_fixed_str(&mut e.name, "GOLD#");
        unsafe { ea.set_symbols(&[e]) }.unwrap();

        let snap = core.symbols().expect("tutarlı görüntü alınmalı");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].name_str(), "GOLD#");
        assert_eq!(snap[0].filling_mode, 0b01);
    }

    #[test]
    fn book_flat_builds_snapshot_and_marks_truncation() {
        let inst = unique("book");
        let ea = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();

        let prices: Vec<f64> = (0..5).map(|i| 1.10 + i as f64 * 0.0001).collect();
        let volumes: Vec<f64> = (0..5).map(|i| (i + 1) as f64).collect();
        let kinds = vec![
            sinyal_proto::book_type::BUY,
            sinyal_proto::book_type::BUY,
            sinyal_proto::book_type::SELL,
            sinyal_proto::book_type::SELL,
            sinyal_proto::book_type::SELL,
        ];
        assert!(unsafe { ea.push_book_flat(7, 123, 456, &prices, &volumes, &kinds) });

        let b = unsafe { core.pop_book() }.expect("derinlik gelmeli");
        assert_eq!(b.symbol_id, 7);
        assert_eq!(b.depth, 5);
        assert_eq!(b.truncated, 0);
        assert_eq!(b.levels().len(), 5);
        assert!((b.levels()[0].price - 1.10).abs() < 1e-12);
        assert_eq!(b.levels()[4].kind, sinyal_proto::book_type::SELL);
    }

    #[test]
    fn book_flat_truncates_deep_broker_books_without_overflowing() {
        let inst = unique("booktrunc");
        let ea = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();

        let n = MAX_BOOK_DEPTH + 20;
        let prices: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let volumes: Vec<f64> = vec![1.0; n];
        let kinds: Vec<u8> = vec![sinyal_proto::book_type::BUY; n];
        assert!(unsafe { ea.push_book_flat(1, 0, 0, &prices, &volumes, &kinds) });

        let b = unsafe { core.pop_book() }.unwrap();
        assert_eq!(b.depth as usize, MAX_BOOK_DEPTH);
        assert_eq!(b.truncated, 1, "kesilme işaretlenmeli");
        // En iyi fiyatlar korunmalı (baştan kesiyoruz).
        assert!((b.levels()[0].price - 0.0).abs() < 1e-12);
    }

    #[test]
    fn book_flat_uses_shortest_array_when_lengths_disagree() {
        // Çağıran tutarsız uzunluklar verirse en kısasına göre davranmalı —
        // aksi halde sınır dışı okuma olurdu.
        let inst = unique("bookmismatch");
        let ea = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();

        assert!(unsafe {
            ea.push_book_flat(1, 0, 0, &[1.0, 2.0, 3.0], &[10.0, 20.0], &[0, 1, 0, 1])
        });
        let b = unsafe { core.pop_book() }.unwrap();
        assert_eq!(b.depth, 2, "en kısa dizi belirleyici olmalı");
    }

    #[test]
    fn tick_loss_is_counted_when_consumer_stalls() {
        // Tüketici hiç okumazsa EA kaybetmeye başlar ve bunu RAPOR eder.
        let inst = unique("loss");
        let ea = Session::create(&inst, small_caps()).unwrap();

        let cap = small_caps().ticks;
        for i in 0..cap {
            assert!(unsafe { ea.push_tick(&sample_tick(i as u32, 1.0)) });
        }
        assert_eq!(ea.tick_push_failures(), 0);
        assert_eq!(ea.tick_backlog(), cap);

        for _ in 0..5 {
            assert!(!unsafe { ea.push_tick(&sample_tick(0, 1.0)) });
        }
        assert_eq!(ea.tick_push_failures(), 5, "kayıp sayılmalı");
    }

    #[test]
    fn second_writer_on_same_instance_is_refused() {
        // SPSC sözleşmesinin asıl garantisi. İki EA aynı örnek adıyla
        // açılırsa iki thread aynı halkaya yazar ve veri bozulur.
        let inst = unique("dupwriter");
        let _first = Session::create(&inst, small_caps()).unwrap();

        let err = Session::create(&inst, small_caps()).unwrap_err();
        assert!(
            matches!(err, SessionError::Shm(sinyal_shm::ShmError::InstanceBusy(_))),
            "ikinci yazar reddedilmeli, bulunan: {err:?}"
        );
        // Mesaj operatöre çözümü söylemeli.
        assert!(err.to_string().contains("FARKLI bir örnek adı"));
    }

    #[test]
    fn core_can_open_while_ea_holds_the_writer_lock() {
        // Kilit YAZAR tekliğini korur; okuyucuyu engellememeli, aksi halde
        // çekirdek hiç bağlanamazdı.
        let inst = unique("readerok");
        let _ea = Session::create(&inst, small_caps()).unwrap();
        assert!(Session::open(&inst).is_ok());
        assert!(Session::open(&inst).is_ok(), "birden çok okuyucu olabilir");
    }

    #[test]
    fn writer_lock_is_released_when_session_drops() {
        // EA yeniden başladığında kilit takılı kalmamalı.
        let inst = unique("lockrelease");
        {
            let _ea = Session::create(&inst, small_caps()).unwrap();
            assert!(Session::create(&inst, small_caps()).is_err());
        }
        assert!(Session::create(&inst, small_caps()).is_ok(), "kilit serbest kalmalı");
    }

    #[test]
    fn operations_from_a_foreign_thread_are_refused_not_corrupting() {
        // MQL5 modeli bunu üretmez (tek EA = tek thread) ama sözleşme
        // beklenmedik bir yoldan ihlal edilirse sessizce veri bozmak yerine
        // reddedip saymalıyız.
        let inst = unique("wrongthread");
        let ea = Session::create(&inst, small_caps()).unwrap();

        // Sahibi bu thread olarak belirle.
        assert!(unsafe { ea.push_tick(&sample_tick(1, 1.0)) });
        assert_eq!(ea.thread_violations(), 0);

        let ea = std::sync::Arc::new(ea);
        let ea2 = ea.clone();
        let accepted = std::thread::spawn(move || unsafe { ea2.push_tick(&sample_tick(2, 2.0)) })
            .join()
            .unwrap();

        assert!(!accepted, "yabancı thread'in yazımı reddedilmeli");
        assert_eq!(ea.thread_violations(), 1, "ihlal sayılmalı");

        // Sahip thread çalışmaya devam edebilmeli ve halkada yalnızca
        // onun yazdığı tick olmalı.
        let core = Session::open(&inst).unwrap();
        assert_eq!(unsafe { core.pop_tick() }.unwrap().symbol_id, 1);
        assert!(unsafe { core.pop_tick() }.is_none(), "reddedilen yazım sızmamalı");
    }

    #[test]
    fn rejects_non_power_of_two_capacity() {
        let inst = unique("badcap");
        let caps = Capacities { ticks: 100, ..small_caps() };
        assert!(matches!(
            Session::create(&inst, caps),
            Err(SessionError::BadCapacity("ticks", 100))
        ));
    }

    #[test]
    fn reattaching_preserves_stream_position() {
        // Terminal yeniden başladığında (çekirdek segmenti açık tutarken)
        // sayaçlar SIFIRLANMAMALI — aksi halde tüketici akışın ortasında
        // kopar ve eski verileri yeniden okur.
        let inst = unique("restart");
        let core = Session::open(&inst);
        assert!(core.is_err(), "önce EA başlamalı");

        let ea1 = Session::create(&inst, small_caps()).unwrap();
        let core = Session::open(&inst).unwrap();
        assert!(unsafe { ea1.push_tick(&sample_tick(1, 1.0)) });
        assert!(unsafe { ea1.push_tick(&sample_tick(2, 2.0)) });
        assert_eq!(unsafe { core.pop_tick() }.unwrap().symbol_id, 1);

        // EA "yeniden başlıyor" — çekirdek hâlâ bağlı olduğu için segment yaşıyor.
        drop(ea1);
        let ea2 = Session::create(&inst, small_caps()).unwrap();
        assert!(unsafe { ea2.push_tick(&sample_tick(3, 3.0)) });

        // Okunmamış tick kaybolmamalı ve yeni tick onun ARKASINA eklenmeli.
        assert_eq!(unsafe { core.pop_tick() }.unwrap().symbol_id, 2);
        assert_eq!(unsafe { core.pop_tick() }.unwrap().symbol_id, 3);
    }
}
