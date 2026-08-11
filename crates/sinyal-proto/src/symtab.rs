//! Sembol tablosu segmenti — EA yazar, çekirdek okur.
//!
//! Halkalardan farklı olarak bu bir akış değil, **durum**: EA açılışta (ve
//! sembol listesi değiştiğinde) doldurur, çekirdek istediği zaman okur.
//!
//! Çekirdeğin emir doğrulaması (hacim yuvarlama, doldurma modu seçimi, fiyat
//! normalizasyonu) tamamen bu tabloya dayanır — broker özellikleri asla tahmin
//! edilmez. Eski adaptörün `ORDER_FILLING_IOC`'yi sabit kodlaması tam da bu
//! tablonun olmamasından kaynaklanıyordu.
//!
//! Tutarlı okuma için tablo düzeyinde bir seqlock kullanılır: EA güncellemeye
//! başlarken `generation`'ı tek sayıya, bitirince çift sayıya çeker. Okuyucu
//! önce ve sonra `generation`'ı okur; tek sayı gördüyse veya değer değiştiyse
//! yeniden dener. Böylece yarı güncellenmiş bir tablo asla kullanılmaz.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::msg::SymbolEntry;

/// Sembol tablosu segmentinin imzası.
pub const SYMTAB_MAGIC: u64 = 0x5359_4E59_414C_5331; // "SYNYALS1"

/// Yerleşim sürümü — [`crate::ring::RING_VERSION`] ile birlikte artırılır.
pub const SYMTAB_VERSION: u32 = 3;

/// Başlık boyutu; kayıtlar bu ofsetten başlar.
pub const SYMTAB_HEADER_SIZE: usize = 64;

/// Tek bir EA örneğinin taşıyabileceği azami sembol sayısı.
///
/// 4096, "tüm pair'ler" hedefinin çok üstünde; bir MT5 terminalinin Market
/// Watch'ında pratikte bunun onda biri kadar sembol tutulur.
pub const MAX_SYMBOLS: usize = 4096;

/// Sembol tablosu başlığı.
#[repr(C, align(64))]
pub struct SymbolTableHeader {
    pub magic: u64,
    pub version: u32,
    /// Bu segmentin taşıyabileceği azami kayıt sayısı.
    pub capacity: u32,
    /// Geçerli kayıt sayısı.
    pub count: AtomicU32,
    pub _pad0: u32,
    /// Seqlock sayacı: TEK = güncelleniyor, ÇİFT = tutarlı.
    pub generation: AtomicU64,
    pub _pad1: [u8; 32],
}

/// Sembol tablosuna ham görünüm.
///
/// # Güvenlik
///
/// `base`, en az [`bytes_needed`] bayt geçerli ve 64'e hizalı eşlenmiş belleğe
/// işaret etmeli ve `SymbolTable` yaşadığı sürece eşlenmiş kalmalıdır. Yalnızca
/// TEK bir yazar (EA) olabilir; okuyucu sayısı sınırsızdır.
pub struct SymbolTable {
    hdr: *mut SymbolTableHeader,
    entries: *mut SymbolEntry,
    capacity: u32,
}

unsafe impl Send for SymbolTable {}
unsafe impl Sync for SymbolTable {}

/// Verilen kapasite için gereken toplam bayt.
pub const fn bytes_needed(capacity: u32) -> usize {
    SYMTAB_HEADER_SIZE + core::mem::size_of::<SymbolEntry>() * capacity as usize
}

/// Yalnızca başlığı okuyup kapasiteyi öğren.
///
/// Gerekçesi [`crate::ring::peek_header`] ile aynı: tüketici, üreticinin
/// hangi kapasiteyle kurduğunu tahmin etmemelidir.
///
/// # Güvenlik
/// `base` en az [`SYMTAB_HEADER_SIZE`] bayt geçerli belleğe işaret etmelidir.
pub unsafe fn peek_header(base: *const u8) -> Result<u32, SymTabError> {
    let hdr = base.cast::<SymbolTableHeader>();
    let (magic, version, capacity) = unsafe {
        core::sync::atomic::fence(Ordering::Acquire);
        ((*hdr).magic, (*hdr).version, (*hdr).capacity)
    };
    if magic != SYMTAB_MAGIC {
        return Err(SymTabError::BadMagic(magic));
    }
    if version != SYMTAB_VERSION {
        return Err(SymTabError::VersionMismatch { found: version, expected: SYMTAB_VERSION });
    }
    if capacity == 0 || capacity as usize > MAX_SYMBOLS {
        return Err(SymTabError::BadCapacity(capacity));
    }
    Ok(capacity)
}

impl SymbolTable {
    /// Segmenti sıfırdan kur (başlığı yazar). Yalnızca EA tarafı çağırır.
    ///
    /// # Panics
    /// `capacity` 0 ise veya [`MAX_SYMBOLS`]'ü aşıyorsa.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn init(base: *mut u8, capacity: u32) -> Self {
        assert!(capacity > 0 && capacity as usize <= MAX_SYMBOLS, "kapasite 1..={MAX_SYMBOLS} olmalı");
        let hdr = base.cast::<SymbolTableHeader>();
        unsafe {
            core::ptr::write_bytes(base, 0, SYMTAB_HEADER_SIZE);
            (*hdr).version = SYMTAB_VERSION;
            (*hdr).capacity = capacity;
            // `magic` en son — yarı kurulmuş tabloya bağlanma olmasın.
            core::sync::atomic::fence(Ordering::Release);
            (*hdr).magic = SYMTAB_MAGIC;
        }
        Self {
            hdr,
            entries: unsafe { base.add(SYMTAB_HEADER_SIZE).cast::<SymbolEntry>() },
            capacity,
        }
    }

    /// Var olan segmente bağlan; başlığı doğrular.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn attach(base: *mut u8) -> Result<Self, SymTabError> {
        let hdr = base.cast::<SymbolTableHeader>();
        let (magic, version, capacity) = unsafe {
            core::sync::atomic::fence(Ordering::Acquire);
            ((*hdr).magic, (*hdr).version, (*hdr).capacity)
        };
        if magic != SYMTAB_MAGIC {
            return Err(SymTabError::BadMagic(magic));
        }
        if version != SYMTAB_VERSION {
            return Err(SymTabError::VersionMismatch { found: version, expected: SYMTAB_VERSION });
        }
        if capacity == 0 || capacity as usize > MAX_SYMBOLS {
            return Err(SymTabError::BadCapacity(capacity));
        }
        Ok(Self {
            hdr,
            entries: unsafe { base.add(SYMTAB_HEADER_SIZE).cast::<SymbolEntry>() },
            capacity,
        })
    }

    #[inline]
    fn hdr(&self) -> &SymbolTableHeader {
        unsafe { &*self.hdr }
    }

    #[inline]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Tabloyu topluca değiştir (seqlock'u doğru sırayla sürer).
    ///
    /// Yazma sırasında okuyucular tutarsız veri görmez; yalnızca yeniden
    /// denerler. Yalnızca EA tarafı çağırır.
    ///
    /// # Güvenlik
    /// Aynı anda yalnızca TEK yazar çağırabilir.
    pub unsafe fn replace_all(&self, items: &[SymbolEntry]) -> Result<(), SymTabError> {
        if items.len() > self.capacity as usize {
            return Err(SymTabError::TooManySymbols {
                found: items.len() as u32,
                capacity: self.capacity,
            });
        }
        let hdr = self.hdr();
        let gen = hdr.generation.load(Ordering::Relaxed);
        // TEK sayı: "güncelleniyor". Okuyucular bunu görünce bekler.
        hdr.generation.store(gen.wrapping_add(1), Ordering::Relaxed);
        core::sync::atomic::fence(Ordering::Release);

        unsafe {
            core::ptr::copy_nonoverlapping(items.as_ptr(), self.entries, items.len());
        }
        hdr.count.store(items.len() as u32, Ordering::Relaxed);

        // ÇİFT sayı: "tutarlı". Release, yukarıdaki tüm yazımların görünür
        // olmasını garanti eder.
        hdr.generation.store(gen.wrapping_add(2), Ordering::Release);
        Ok(())
    }

    /// Tablonun tutarlı bir anlık görüntüsünü al.
    ///
    /// Yazar tam o sırada güncelliyorsa `attempts` kez yeniden dener; hâlâ
    /// tutarlı bir görüntü alınamazsa `None` döner (çağıran sonra tekrar
    /// denemelidir — bu bir hata değil, geçici çekişmedir).
    pub fn snapshot(&self, attempts: usize) -> Option<Vec<SymbolEntry>> {
        let hdr = self.hdr();
        for _ in 0..attempts.max(1) {
            let g1 = hdr.generation.load(Ordering::Acquire);
            if g1 & 1 != 0 {
                // Yazar içeride — bekle ve tekrar dene.
                core::hint::spin_loop();
                continue;
            }
            let count = hdr.count.load(Ordering::Relaxed).min(self.capacity) as usize;
            let mut out = Vec::with_capacity(count);
            unsafe {
                let src = core::slice::from_raw_parts(self.entries, count);
                out.extend_from_slice(src);
            }
            core::sync::atomic::fence(Ordering::Acquire);
            if hdr.generation.load(Ordering::Relaxed) == g1 {
                return Some(out);
            }
            // Yazar araya girdi — okuduğumuz karışık olabilir, at ve tekrar dene.
        }
        None
    }

    /// Tablonun kaç kez güncellendiği (teşhis için).
    #[inline]
    pub fn generation(&self) -> u64 {
        self.hdr().generation.load(Ordering::Acquire)
    }
}

/// Sembol tablosuna bağlanırken/yazarken oluşabilecek hatalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymTabError {
    BadMagic(u64),
    VersionMismatch { found: u32, expected: u32 },
    BadCapacity(u32),
    TooManySymbols { found: u32, capacity: u32 },
}

impl core::fmt::Display for SymTabError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic(m) => write!(
                f,
                "sembol tablosu imzası geçersiz (0x{m:016X}) — segment kurulmamış veya yanlış segment"
            ),
            Self::VersionMismatch { found, expected } => {
                write!(f, "sembol tablosu sürümü uyumsuz: bulunan {found}, beklenen {expected}")
            }
            Self::BadCapacity(c) => write!(f, "sembol tablosu kapasitesi geçersiz ({c})"),
            Self::TooManySymbols { found, capacity } => write!(
                f,
                "sembol sayısı kapasiteyi aşıyor: {found} > {capacity}"
            ),
        }
    }
}

impl std::error::Error for SymTabError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::write_fixed_str;

    struct TestBuf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
    }

    impl TestBuf {
        fn new(bytes: usize) -> Self {
            let layout = std::alloc::Layout::from_size_align(bytes, 64).unwrap();
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

    fn entry(id: u32, name: &str) -> SymbolEntry {
        let mut e = SymbolEntry { symbol_id: id, digits: 5, volume_step: 0.01, ..Default::default() };
        write_fixed_str(&mut e.name, name);
        e
    }

    #[test]
    fn header_is_one_cache_line() {
        assert_eq!(core::mem::size_of::<SymbolTableHeader>(), SYMTAB_HEADER_SIZE);
        assert_eq!(core::mem::align_of::<SymbolTableHeader>(), 64);
    }

    #[test]
    fn replace_and_snapshot_roundtrip() {
        let buf = TestBuf::new(bytes_needed(64));
        let tab = unsafe { SymbolTable::init(buf.ptr, 64) };

        assert_eq!(tab.snapshot(4).unwrap().len(), 0, "yeni tablo boş");

        let items = vec![entry(0, "EURUSD"), entry(1, "GOLD#"), entry(2, "XAUUSD.m")];
        unsafe { tab.replace_all(&items) }.unwrap();

        let snap = tab.snapshot(4).unwrap();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].name_str(), "EURUSD");
        assert_eq!(snap[1].name_str(), "GOLD#");
        assert_eq!(snap[2].name_str(), "XAUUSD.m");
        assert_eq!(snap[2].symbol_id, 2);
    }

    #[test]
    fn generation_is_even_after_write() {
        let buf = TestBuf::new(bytes_needed(8));
        let tab = unsafe { SymbolTable::init(buf.ptr, 8) };
        assert_eq!(tab.generation() % 2, 0, "başlangıçta tutarlı");

        unsafe { tab.replace_all(&[entry(0, "EURUSD")]) }.unwrap();
        assert_eq!(tab.generation(), 2);
        assert_eq!(tab.generation() % 2, 0, "yazımdan sonra tutarlı");

        unsafe { tab.replace_all(&[entry(0, "EURUSD"), entry(1, "GBPUSD")]) }.unwrap();
        assert_eq!(tab.generation(), 4);
    }

    #[test]
    fn rejects_more_symbols_than_capacity() {
        let buf = TestBuf::new(bytes_needed(2));
        let tab = unsafe { SymbolTable::init(buf.ptr, 2) };
        let items = vec![entry(0, "A"), entry(1, "B"), entry(2, "C")];
        assert!(matches!(
            unsafe { tab.replace_all(&items) },
            Err(SymTabError::TooManySymbols { found: 3, capacity: 2 })
        ));
    }

    #[test]
    fn attach_validates_and_sees_writer_data() {
        let buf = TestBuf::new(bytes_needed(16));
        let tab = unsafe { SymbolTable::init(buf.ptr, 16) };
        unsafe { tab.replace_all(&[entry(7, "USDJPY")]) }.unwrap();

        // Ayrı bir "süreç" aynı belleğe bağlanıyor.
        let reader = unsafe { SymbolTable::attach(buf.ptr) }.unwrap();
        let snap = reader.snapshot(4).unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].name_str(), "USDJPY");
        assert_eq!(snap[0].symbol_id, 7);
    }

    #[test]
    fn attach_rejects_uninitialised_memory() {
        let buf = TestBuf::new(bytes_needed(8));
        assert!(matches!(
            unsafe { SymbolTable::attach(buf.ptr) },
            Err(SymTabError::BadMagic(0))
        ));
    }

    #[test]
    fn snapshot_never_returns_torn_data_under_concurrent_writes() {
        // Yazar iki farklı tutarlı durum arasında gidip gelir; okuyucu asla
        // karışık bir durum görmemeli (ya hep A ya hep B).
        use std::sync::atomic::{AtomicBool, Ordering as O};
        use std::sync::Arc;

        let buf = TestBuf::new(bytes_needed(64));
        let tab = Arc::new(unsafe { SymbolTable::init(buf.ptr, 64) });
        let stop = Arc::new(AtomicBool::new(false));

        let state_a: Vec<SymbolEntry> = (0..32).map(|i| entry(i, "AAAAAA")).collect();
        let state_b: Vec<SymbolEntry> = (0..32).map(|i| entry(i, "BBBBBB")).collect();
        unsafe { tab.replace_all(&state_a) }.unwrap();

        let writer = {
            let tab = tab.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut flip = false;
                while !stop.load(O::Relaxed) {
                    let s = if flip { &state_b } else { &state_a };
                    unsafe { tab.replace_all(s) }.unwrap();
                    flip = !flip;
                }
            })
        };

        for _ in 0..20_000 {
            if let Some(snap) = tab.snapshot(64) {
                // Tüm kayıtlar AYNI duruma ait olmalı — karışım yırtık okumadır.
                let first = snap[0].name_str().to_owned();
                assert!(
                    snap.iter().all(|e| e.name_str() == first),
                    "yırtık okuma: tabloda karışık durum görüldü"
                );
            }
        }
        stop.store(true, O::Relaxed);
        writer.join().unwrap();
    }
}
