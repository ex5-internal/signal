//! Süreçler arası tek-yazar/tek-okur (SPSC) halka tampon.
//!
//! Yazar MT5 terminalinin içindeki EA thread'i (`SinyalBridge.dll` üzerinden),
//! okuyucu ayrı bir süreçteki `sinyal-core`. Bellek Windows dosya eşlemesiyle
//! paylaşılır, bu yüzden yapıların içinde işaretçi/`Box`/`Vec` OLAMAZ ve tüm
//! erişim ham işaretçiler üzerinden yapılır.
//!
//! # Neden bu tasarım
//!
//! Yazar **asla bloklamaz** — ticaret terminalinin thread'ini yavaş bir
//! okuyucu yüzünden bekletmek kabul edilemez. Halka dolduğunda `push` mesajı
//! yazmaz, `false` döner ve [`RingHeader::push_failures`] sayacını artırır;
//! kaybı kabul edip etmemek çağırana bırakılır.
//!
//! Dolduğunda en ESKİyi ezen (overwrite) bir tasarım tazelik açısından daha
//! iyi olurdu, ancak okuyucunun tam o slotu okurken ezilmesi yırtık okumaya
//! (torn read) yol açar; bunu engellemek için slot başına seqlock gerekir.
//! Bilinçli olarak basit olanı seçtik: halka cömert boyutlandırılıyor
//! (tick için 1M slot ≈ 64 MB, 100k tick/sn'de ~10 saniyelik tampon), bu
//! yüzden dolma pratikte yalnızca okuyucu tamamen durduğunda olur — ki o
//! durumda zaten sistem bozuktur ve sayaç alarm üretir.
//!
//! Güncel fiyat tazeliği halka seviyesinde DEĞİL, çekirdekteki "sembol başına
//! son tick" tablosunda çözülür; yalnızca anlık fiyatla ilgilenen tüketiciler
//! oradan okur ve birikmiş kuyruğun arkasında kalmaz.
//!
//! # Bellek yerleşimi
//!
//! ```text
//! [RingHeader: 256 bayt][Cell<T> × capacity]
//! ```

use core::sync::atomic::{AtomicU64, Ordering};

/// Halka başlığındaki imza — yanlış segmente bağlanmayı yakalar.
pub const RING_MAGIC: u64 = 0x5359_4E59_414C_5231; // "SYNYALR1"

/// Yerleşim sürümü. Mesaj yapıları değişirse artır; uyumsuz sürüme bağlanma
/// sessizce bozuk veri okumak yerine hata verir.
///
/// - v1: ilk sürüm
/// - v2: `SymbolEntry`'ye `trade_exemode`/`stops_level`/`freeze_level`/
///   `ticks_bookdepth`/`volume_limit`/`flags` eklendi (doğru doldurma modu
///   seçimi bunlar olmadan yapılamıyordu), `Cmd.magic` u32→u64,
///   `Res`'e `request_id` eklendi.
pub const RING_VERSION: u32 = 3;

/// Başlık boyutu; slotlar bu ofsetten başlar (64'ün katı — hizalama korunur).
pub const RING_HEADER_SIZE: usize = 256;

/// Halka başlığı. Üretici ve tüketici sayaçları ayrı önbellek satırlarında
/// tutulur; aksi halde her iki taraf aynı satırı sürekli geçersiz kılar
/// (false sharing) ve gecikme belirgin şekilde artar.
#[repr(C, align(64))]
pub struct RingHeader {
    // --- satır 0: değişmeyen meta ---
    pub magic: u64,
    pub version: u32,
    pub slot_size: u32,
    /// Slot sayısı — daima 2'nin kuvveti (maske ile indeksleme için).
    pub capacity: u64,
    pub _pad0: [u8; 40],

    // --- satır 1: yalnız üretici yazar ---
    /// Yazılacak bir sonraki indeks (monoton, sarmaz).
    pub head: AtomicU64,
    /// Halka dolu bulunduğu için reddedilen `push` sayısı.
    ///
    /// **Anlamı üreticiye bağlıdır** ve bunu karıştırmak metriği işe yaramaz
    /// hale getirir:
    ///
    /// - **Yeniden denemeyen üretici** (üretimdeki EA köprüsü — ticaret
    ///   thread'ini bekletemez, mesajı bırakır): bu sayı doğrudan **kalıcı
    ///   olarak kaybedilmiş mesaj** sayısıdır. Sıfırdan farklıysa alarm.
    /// - **Yeniden deneyen üretici** (testler, toplu besleme): mesaj
    ///   kaybolmaz; bu yalnızca "halka kaç kez dolu bulundu" göstergesidir,
    ///   yani geri basınç ölçüsüdür.
    ///
    /// Üretim yolunda tek yazar EA köprüsü olduğu ve o asla yeniden denemediği
    /// için, orada bu sayaç "kayıp tick" ile birebir aynıdır.
    pub push_failures: AtomicU64,
    pub _pad1: [u8; 48],

    // --- satır 2: yalnız tüketici yazar ---
    /// Okunacak bir sonraki indeks (monoton, sarmaz).
    pub tail: AtomicU64,
    pub _pad2: [u8; 56],

    // --- satır 3: yedek ---
    pub _pad3: [u8; 64],
}

/// Halkadaki tek slot: üretici tarafından atanan sıra numarası + yük.
///
/// `seq`, `head`'in o anki değeridir. Tüketici bunu boşluk tespiti ve kayıt
/// (replay) ilişkilendirmesi için kullanabilir; halka zaten sıralı teslim
/// ettiğinden doğruluk için gerekli değildir, teşhis içindir.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Cell<T: Copy> {
    pub seq: u64,
    pub val: T,
}

/// Verilen slot sayısı için gereken toplam bayt.
pub const fn bytes_needed<T: Copy>(capacity: u64) -> usize {
    RING_HEADER_SIZE + (core::mem::size_of::<Cell<T>>() * capacity as usize)
}

/// Halka başlığındaki meta veri.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingMeta {
    pub version: u32,
    pub slot_size: u32,
    pub capacity: u64,
}

/// Yalnızca başlığı okuyup kapasiteyi öğren.
///
/// # Neden gerekli
///
/// Tüketici, üreticinin hangi kapasiteyle kurduğunu **bilemez**. Varsayılan
/// bir değer tahmin edip o boyutta eşlemeye çalışmak, segment daha küçükse
/// `MapViewOfFile`'ın `ERROR_ACCESS_DENIED` (5) ile başarısız olmasına yol
/// açar — sebebi hiç anlaşılmayan bir hata.
///
/// Bunun yerine önce [`RING_HEADER_SIZE`] baytlık bir önizleme eşlemesi
/// yapılır, kapasite buradan okunur, sonra doğru boyutta yeniden eşlenir.
///
/// # Güvenlik
///
/// `base` en az [`RING_HEADER_SIZE`] bayt geçerli, okunabilir ve 64'e hizalı
/// belleğe işaret etmelidir.
pub unsafe fn peek_header(base: *const u8) -> Result<RingMeta, RingError> {
    let hdr = base.cast::<RingHeader>();
    let (magic, version, slot_size, capacity) = unsafe {
        core::sync::atomic::fence(Ordering::Acquire);
        ((*hdr).magic, (*hdr).version, (*hdr).slot_size, (*hdr).capacity)
    };
    if magic != RING_MAGIC {
        return Err(RingError::BadMagic(magic));
    }
    if version != RING_VERSION {
        return Err(RingError::VersionMismatch { found: version, expected: RING_VERSION });
    }
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(RingError::BadCapacity(capacity));
    }
    Ok(RingMeta { version, slot_size, capacity })
}

/// Paylaşılan bellekteki bir halkaya ham görünüm.
///
/// # Güvenlik
///
/// `base`, en az `bytes_needed::<T>(capacity)` bayt geçerli ve 64'e hizalı
/// eşlenmiş belleğe işaret etmelidir ve `Ring` yaşadığı sürece eşlenmiş
/// kalmalıdır. Aynı segmente **en fazla bir** üretici ve **en fazla bir**
/// tüketici bağlanabilir — SPSC varsayımı ihlal edilirse veri bozulur.
pub struct Ring<T: Copy> {
    hdr: *mut RingHeader,
    cells: *mut Cell<T>,
    capacity: u64,
    mask: u64,
}

// Ham işaretçi tutuyor; paylaşılan bellek segmenti süreç ömrü boyunca geçerli
// ve erişim atomiklerle senkronize. Thread'ler arası taşınabilir.
unsafe impl<T: Copy> Send for Ring<T> {}
unsafe impl<T: Copy> Sync for Ring<T> {}

impl<T: Copy> Ring<T> {
    /// Yeni bir halkayı SIFIRDAN kur (başlığı yazar).
    ///
    /// Yalnızca segmenti ilk oluşturan taraf çağırmalıdır. Var olan bir
    /// segmente bağlanmak için [`Ring::attach`] kullan.
    ///
    /// # Panics
    /// `capacity` 2'nin kuvveti değilse veya 0 ise.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn init(base: *mut u8, capacity: u64) -> Self {
        assert!(capacity.is_power_of_two(), "capacity 2'nin kuvveti olmalı");
        let hdr = base.cast::<RingHeader>();
        unsafe {
            // Başlığı tamamen sıfırla — segment yeniden kullanılıyor olabilir.
            core::ptr::write_bytes(base, 0, RING_HEADER_SIZE);
            (*hdr).version = RING_VERSION;
            (*hdr).slot_size = core::mem::size_of::<Cell<T>>() as u32;
            (*hdr).capacity = capacity;
            // `magic` EN SON yazılır: başlığın geri kalanı hazır olmadan
            // bağlanmaya çalışan bir tüketici yarı kurulmuş halka görmesin.
            core::sync::atomic::fence(Ordering::Release);
            (*hdr).magic = RING_MAGIC;
        }
        Self {
            hdr,
            cells: unsafe { base.add(RING_HEADER_SIZE).cast::<Cell<T>>() },
            capacity,
            mask: capacity - 1,
        }
    }

    /// Var olan bir halkaya bağlan; başlığı doğrular.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn attach(base: *mut u8) -> Result<Self, RingError> {
        let hdr = base.cast::<RingHeader>();
        let (magic, version, slot_size, capacity) = unsafe {
            core::sync::atomic::fence(Ordering::Acquire);
            ((*hdr).magic, (*hdr).version, (*hdr).slot_size, (*hdr).capacity)
        };
        if magic != RING_MAGIC {
            return Err(RingError::BadMagic(magic));
        }
        if version != RING_VERSION {
            return Err(RingError::VersionMismatch { found: version, expected: RING_VERSION });
        }
        let expect_slot = core::mem::size_of::<Cell<T>>() as u32;
        if slot_size != expect_slot {
            return Err(RingError::SlotSizeMismatch { found: slot_size, expected: expect_slot });
        }
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(RingError::BadCapacity(capacity));
        }
        Ok(Self {
            hdr,
            cells: unsafe { base.add(RING_HEADER_SIZE).cast::<Cell<T>>() },
            capacity,
            mask: capacity - 1,
        })
    }

    #[inline]
    fn hdr(&self) -> &RingHeader {
        // Güvenlik: `hdr` kurulum sırasında doğrulandı ve segment eşlenmiş kalıyor.
        unsafe { &*self.hdr }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Halka dolu bulunduğu için reddedilen `push` sayısı.
    ///
    /// Yorumu için [`RingHeader::push_failures`] belgesine bak — yeniden
    /// denemeyen bir üretici için bu "kaybedilmiş mesaj", yeniden deneyen bir
    /// üretici için yalnızca "geri basınç" demektir.
    #[inline]
    pub fn push_failures(&self) -> u64 {
        self.hdr().push_failures.load(Ordering::Relaxed)
    }

    /// Okunmayı bekleyen mesaj sayısı.
    #[inline]
    pub fn len(&self) -> u64 {
        let head = self.hdr().head.load(Ordering::Acquire);
        let tail = self.hdr().tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bir mesaj yaz. Halka doluysa mesaj YAZILMAZ ve `false` döner.
    ///
    /// Asla bloklamaz — dönüş değerini yok saymak, sessizce veri kaybetmek
    /// demektir. Çağıran ya yeniden denemeli ya da kaybı bilinçli kabul
    /// etmelidir (EA köprüsü ikincisini yapar: ticaret thread'i bekletilemez).
    ///
    /// Yalnızca üretici tarafı çağırmalıdır.
    ///
    /// # Güvenlik
    /// Aynı anda yalnızca TEK bir üretici bu fonksiyonu çağırabilir.
    #[must_use = "false dönerse mesaj YAZILMADI — yok saymak sessiz veri kaybıdır"]
    pub unsafe fn push(&self, val: &T) -> bool {
        let hdr = self.hdr();
        // Yalnız üretici `head`'i yazdığı için Relaxed okuma yeterli.
        let head = hdr.head.load(Ordering::Relaxed);
        // Tüketicinin ilerlemesini görmek için Acquire.
        let tail = hdr.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= self.capacity {
            hdr.push_failures.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let cell = unsafe { self.cells.add((head & self.mask) as usize) };
        unsafe {
            core::ptr::write(core::ptr::addr_of_mut!((*cell).seq), head);
            core::ptr::write(core::ptr::addr_of_mut!((*cell).val), *val);
        }
        // Release: yük yazımları `head` görünür olmadan ÖNCE tamamlanmış olur.
        // Bu olmadan tüketici ilerlemiş bir `head` görüp yarım yazılmış slot
        // okuyabilirdi.
        hdr.head.store(head.wrapping_add(1), Ordering::Release);
        true
    }

    /// Bir mesaj oku. Halka boşsa `None`.
    ///
    /// Yalnızca tüketici tarafı çağırmalıdır.
    ///
    /// # Güvenlik
    /// Aynı anda yalnızca TEK bir tüketici bu fonksiyonu çağırabilir.
    pub unsafe fn pop(&self) -> Option<(u64, T)> {
        let hdr = self.hdr();
        // Yalnız tüketici `tail`'i yazdığı için Relaxed okuma yeterli.
        let tail = hdr.tail.load(Ordering::Relaxed);
        // Acquire: üreticinin yük yazımlarını görmeyi garanti eder.
        let head = hdr.head.load(Ordering::Acquire);
        if tail == head {
            return None;
        }
        let cell = unsafe { self.cells.add((tail & self.mask) as usize) };
        let out = unsafe {
            (
                core::ptr::read(core::ptr::addr_of!((*cell).seq)),
                core::ptr::read(core::ptr::addr_of!((*cell).val)),
            )
        };
        // Release: slot okuması `tail` ilerlemeden önce bitmiş olur; aksi halde
        // üretici biz okurken slotu yeniden kullanabilirdi.
        hdr.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(out)
    }

    /// En fazla `out.len()` mesajı toplu oku; okunan sayıyı döndürür.
    ///
    /// Mesaj başına iki atomik erişim yerine parti başına iki erişim yapar —
    /// yoğun tick akışında tüketici tarafının maliyetini belirgin düşürür.
    ///
    /// # Güvenlik
    /// Aynı anda yalnızca TEK bir tüketici bu fonksiyonu çağırabilir.
    pub unsafe fn pop_batch(&self, out: &mut [T]) -> usize {
        if out.is_empty() {
            return 0;
        }
        let hdr = self.hdr();
        let tail = hdr.tail.load(Ordering::Relaxed);
        let head = hdr.head.load(Ordering::Acquire);
        let available = head.wrapping_sub(tail);
        if available == 0 {
            return 0;
        }
        let n = (available as usize).min(out.len());
        for (i, slot) in out.iter_mut().enumerate().take(n) {
            let cell = unsafe { self.cells.add(((tail.wrapping_add(i as u64)) & self.mask) as usize) };
            *slot = unsafe { core::ptr::read(core::ptr::addr_of!((*cell).val)) };
        }
        hdr.tail.store(tail.wrapping_add(n as u64), Ordering::Release);
        n
    }
}

/// Halkaya bağlanırken oluşabilecek hatalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError {
    /// İmza tutmuyor — segment henüz kurulmamış veya yanlış segment.
    BadMagic(u64),
    VersionMismatch { found: u32, expected: u32 },
    /// Slot boyutu farklı — iki taraf farklı mesaj tanımıyla derlenmiş.
    /// `gen-mqh` ile MQL5 başlığını yenilemeyi unutmuş olabilirsin.
    SlotSizeMismatch { found: u32, expected: u32 },
    BadCapacity(u64),
}

impl core::fmt::Display for RingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic(m) => write!(
                f,
                "halka imzası geçersiz (0x{m:016X}) — segment kurulmamış veya yanlış segment"
            ),
            Self::VersionMismatch { found, expected } => write!(
                f,
                "halka sürümü uyumsuz: bulunan {found}, beklenen {expected}"
            ),
            Self::SlotSizeMismatch { found, expected } => write!(
                f,
                "slot boyutu uyumsuz: bulunan {found}, beklenen {expected} — \
                 iki taraf farklı mesaj tanımıyla derlenmiş (gen-mqh ile MQL5 başlığını yenile)"
            ),
            Self::BadCapacity(c) => write!(f, "kapasite geçersiz ({c}) — 2'nin kuvveti olmalı"),
        }
    }
}

impl std::error::Error for RingError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test için yığında değil, hizalanmış bir tamponda halka kurar.
    struct TestBuf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
    }

    impl TestBuf {
        fn new(bytes: usize) -> Self {
            let layout = std::alloc::Layout::from_size_align(bytes, 64).unwrap();
            // Güvenlik: boyut > 0 ve hizalama geçerli.
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null());
            Self { ptr, layout }
        }
    }

    impl Drop for TestBuf {
        fn drop(&mut self) {
            unsafe { std::alloc::dealloc(self.ptr, self.layout) };
        }
    }

    #[test]
    fn header_is_four_cache_lines() {
        assert_eq!(core::mem::size_of::<RingHeader>(), RING_HEADER_SIZE);
        assert_eq!(core::mem::align_of::<RingHeader>(), 64);
    }

    #[test]
    fn push_pop_roundtrip() {
        let buf = TestBuf::new(bytes_needed::<u64>(8));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 8) };

        assert!(ring.is_empty());
        for i in 0..5u64 {
            assert!(unsafe { ring.push(&(i * 10)) });
        }
        assert_eq!(ring.len(), 5);

        for i in 0..5u64 {
            let (seq, val) = unsafe { ring.pop() }.expect("mesaj olmalı");
            assert_eq!(seq, i);
            assert_eq!(val, i * 10);
        }
        assert!(ring.is_empty());
        assert!(unsafe { ring.pop() }.is_none());
    }

    #[test]
    fn rejects_newest_when_full_and_counts_it() {
        let buf = TestBuf::new(bytes_needed::<u64>(4));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 4) };

        for i in 0..4u64 {
            assert!(unsafe { ring.push(&i) }, "kapasite dolana kadar kabul etmeli");
        }
        // Beşinci reddedilmeli — EN ESKİ korunur, EN YENİ geri çevrilir.
        assert!(!unsafe { ring.push(&99) });
        assert_eq!(ring.push_failures(), 1);

        // İlk dördü hâlâ orada ve sıralı; 99 hiç yazılmamış olmalı.
        for i in 0..4u64 {
            assert_eq!(unsafe { ring.pop() }.unwrap().1, i);
        }
        assert!(unsafe { ring.pop() }.is_none(), "reddedilen mesaj halkaya sızmamalı");
    }

    #[test]
    fn wraps_around_indefinitely() {
        let buf = TestBuf::new(bytes_needed::<u64>(4));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 4) };

        // Kapasitenin çok üstünde mesaj geçir — maske ile indeksleme sarmalı.
        for i in 0..1000u64 {
            assert!(unsafe { ring.push(&i) });
            assert_eq!(unsafe { ring.pop() }.unwrap().1, i);
        }
        assert_eq!(ring.push_failures(), 0);
    }

    #[test]
    fn pop_batch_reads_multiple() {
        let buf = TestBuf::new(bytes_needed::<u64>(16));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 16) };

        for i in 0..10u64 {
            assert!(unsafe { ring.push(&i) });
        }
        let mut out = [0u64; 4];
        assert_eq!(unsafe { ring.pop_batch(&mut out) }, 4);
        assert_eq!(out, [0, 1, 2, 3]);

        let mut big = [0u64; 32];
        assert_eq!(unsafe { ring.pop_batch(&mut big) }, 6, "kalan 6 mesaj");
        assert_eq!(&big[..6], &[4, 5, 6, 7, 8, 9]);
        assert_eq!(unsafe { ring.pop_batch(&mut big) }, 0, "boş halka 0 döner");
    }

    #[test]
    fn attach_validates_header() {
        let buf = TestBuf::new(bytes_needed::<u64>(8));
        let _ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 8) };

        // Doğru tiple bağlanmak çalışır.
        assert!(unsafe { Ring::<u64>::attach(buf.ptr) }.is_ok());

        // Farklı slot boyutlu bir tiple bağlanmak YAKALANIR — bu, iki tarafın
        // farklı mesaj tanımıyla derlenmesine karşı asıl korumamız.
        #[repr(C)]
        #[derive(Clone, Copy)]
        struct Other([u8; 24]);
        assert!(matches!(
            unsafe { Ring::<Other>::attach(buf.ptr) },
            Err(RingError::SlotSizeMismatch { .. })
        ));
    }

    #[test]
    fn attach_rejects_uninitialised_memory() {
        let buf = TestBuf::new(bytes_needed::<u64>(8));
        // init çağrılmadı → sıfır bellek → imza tutmaz.
        assert!(matches!(
            unsafe { Ring::<u64>::attach(buf.ptr) },
            Err(RingError::BadMagic(0))
        ));
    }

    #[test]
    fn retrying_producer_loses_nothing_across_threads() {
        // Halkayı kapasitesinin çok üstünde mesajla zorlar. Üretici dolu
        // halkada YENİDEN DENER, bu yüzden hiçbir mesaj kaybolmamalı ve sıra
        // korunmalıdır. `push_failures` burada kayıp DEĞİL, geri basınç
        // ölçüsüdür ve sıfırdan büyük olması beklenir.
        const N: u64 = 200_000;
        let buf = TestBuf::new(bytes_needed::<u64>(1024));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, 1024) };
        let ring = std::sync::Arc::new(ring);

        let producer = {
            let ring = ring.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while i < N {
                    if unsafe { ring.push(&i) } {
                        i += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            })
        };

        let mut expected = 0u64;
        while expected < N {
            if let Some((seq, val)) = unsafe { ring.pop() } {
                assert_eq!(val, expected, "mesajlar sırasını korumalı");
                assert_eq!(seq, expected, "sıra numarası head ile aynı olmalı");
                expected += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
        assert!(unsafe { ring.pop() }.is_none(), "fazladan mesaj olmamalı");
        // Kasıtlı olarak `push_failures == 0` İDDİA ETMİYORUZ: küçük bir halkayı
        // zorlarken doluluk normaldir ve yeniden deneyen üretici için kayıp
        // anlamına gelmez. Kaybın olmadığının kanıtı yukarıdaki sıra ve sayı
        // denetimidir.
    }

    #[test]
    fn non_retrying_producer_push_failures_equal_actual_loss() {
        // Üretimdeki EA köprüsünün davranışı: ticaret thread'i bekletilemez,
        // bu yüzden dolu halkada mesaj BIRAKILIR. Bu üretici için
        // `push_failures` doğrudan kaybedilen mesaj sayısıdır — Faz 1'in
        // "kayıp = 0" kriteri tam olarak bunu ölçer.
        const CAP: u64 = 8;
        const SENT: u64 = 20;
        let buf = TestBuf::new(bytes_needed::<u64>(CAP));
        let ring: Ring<u64> = unsafe { Ring::init(buf.ptr, CAP) };

        let mut accepted = 0u64;
        for i in 0..SENT {
            if unsafe { ring.push(&i) } {
                accepted += 1;
            }
            // Yeniden DENEMİYORUZ — EA gibi davranıyoruz.
        }

        let mut received = 0u64;
        while unsafe { ring.pop() }.is_some() {
            received += 1;
        }

        assert_eq!(accepted, CAP, "yalnızca kapasite kadarı kabul edilmeli");
        assert_eq!(received, accepted, "kabul edilen her mesaj okunabilmeli");
        assert_eq!(
            ring.push_failures(),
            SENT - accepted,
            "sayaç gerçekten kaybedilen mesaj sayısını vermeli"
        );
    }
}
