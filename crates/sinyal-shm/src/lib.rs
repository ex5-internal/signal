//! Windows paylaşımlı bellek eşlemesi ve yüksek çözünürlüklü saat.
//!
//! Bu kütüphane `SinyalBridge.dll` içinde **MT5 terminalinin süreç alanına**
//! yüklenir. Bu yüzden bilinçli olarak sıfır bağımlılıklıdır: gereken sekiz
//! Win32 fonksiyonu doğrudan bildirilir. Bir bağımlılık ağacını ticaret
//! terminalinin içine taşımak hem gereksiz hem de öngörülemez.
//!
//! # Adlandırma
//!
//! Segment adları `Local\` ile başlar — oturuma hapsedilir. `Global\` yönetici
//! hakkı ister ve segmenti makine geneline açar; EA ile çekirdek aynı oturumda
//! çalıştığı için buna gerek yok.
//!
//! # Hizalama
//!
//! `MapViewOfFile` daima ayırma ayrıntı düzeyine (64 KB) hizalı adres döndürür,
//! dolayısıyla halkaların istediği 64 baytlık hizalama garanti altındadır.

#![cfg(windows)]

use core::ffi::c_void;

mod ffi {
    use core::ffi::c_void;

    // BCryptGenRandom bcrypt.dll'de; digerleri kernel32'de (varsayilan).
    #[link(name = "bcrypt")]
    extern "system" {
        pub fn BCryptGenRandom(
            h_algorithm: *mut c_void,
            pb_buffer: *mut u8,
            cb_buffer: u32,
            dw_flags: u32,
        ) -> i32;
    }

    pub const INVALID_HANDLE_VALUE: isize = -1;
    pub const PAGE_READWRITE: u32 = 0x04;
    pub const FILE_MAP_ALL_ACCESS: u32 = 0x000F_001F;
    pub const ERROR_ALREADY_EXISTS: u32 = 183;

    extern "system" {
        pub fn CreateMutexW(
            lp_attributes: *mut c_void,
            b_initial_owner: i32,
            lp_name: *const u16,
        ) -> isize;

        pub fn GetCurrentThreadId() -> u32;

        pub fn CreateFileMappingW(
            h_file: isize,
            lp_attributes: *mut c_void,
            fl_protect: u32,
            dw_maximum_size_high: u32,
            dw_maximum_size_low: u32,
            lp_name: *const u16,
        ) -> isize;

        pub fn OpenFileMappingW(
            dw_desired_access: u32,
            b_inherit_handle: i32,
            lp_name: *const u16,
        ) -> isize;

        pub fn MapViewOfFile(
            h_file_mapping_object: isize,
            dw_desired_access: u32,
            dw_file_offset_high: u32,
            dw_file_offset_low: u32,
            dw_number_of_bytes_to_map: usize,
        ) -> *mut c_void;

        pub fn UnmapViewOfFile(lp_base_address: *const c_void) -> i32;
        pub fn CloseHandle(h_object: isize) -> i32;
        pub fn GetLastError() -> u32;
        pub fn QueryPerformanceCounter(lp_performance_count: *mut i64) -> i32;
        pub fn QueryPerformanceFrequency(lp_frequency: *mut i64) -> i32;
    }
}

/// Paylaşımlı bellek işlemlerinde oluşabilecek hatalar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShmError {
    /// Segment adı geçersiz (boş veya NUL içeriyor).
    InvalidName(String),
    /// `CreateFileMappingW` başarısız.
    CreateFailed { name: String, code: u32 },
    /// `OpenFileMappingW` başarısız — segment henüz yok olabilir.
    OpenFailed { name: String, code: u32 },
    /// `MapViewOfFile` başarısız.
    MapFailed { name: String, code: u32 },
    /// Var olan segmente bağlanıldı ama beklenenden küçük.
    ///
    /// Bu genellikle iki tarafın farklı kapasite yapılandırmasıyla çalıştığını
    /// gösterir; sessizce devam etmek sınır dışı okumaya yol açardı.
    TooSmall { name: String, requested: usize, actual: usize },
    /// Bu örnek adını başka bir yazar tutuyor.
    ///
    /// Halkalar SPSC olduğu için ikinci bir yazara izin vermek veri bozardı.
    InstanceBusy(String),
}

impl core::fmt::Display for ShmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidName(n) => write!(f, "segment adı geçersiz: {n:?}"),
            Self::CreateFailed { name, code } => {
                write!(f, "paylaşımlı bellek oluşturulamadı ({name}): Win32 hata {code}")
            }
            Self::OpenFailed { name, code } => write!(
                f,
                "paylaşımlı bellek açılamadı ({name}): Win32 hata {code} \
                 — EA henüz başlamamış olabilir"
            ),
            Self::MapFailed { name, code } => {
                write!(f, "paylaşımlı bellek eşlenemedi ({name}): Win32 hata {code}")
            }
            Self::TooSmall { name, requested, actual } => write!(
                f,
                "paylaşımlı bellek beklenenden küçük ({name}): istenen {requested}, bulunan {actual} \
                 — iki taraf farklı kapasite yapılandırmasıyla çalışıyor"
            ),
            Self::InstanceBusy(inst) => write!(
                f,
                "'{inst}' örneğini başka bir yazar tutuyor — aynı örnek adıyla ikinci bir EA \
                 çalıştırılamaz (halka tek yazarlıdır, ikinci yazar veriyi bozar). \
                 Her terminale/grafiğe FARKLI bir örnek adı ver."
            ),
        }
    }
}

impl std::error::Error for ShmError {}

/// Eşlenmiş bir paylaşımlı bellek segmenti.
///
/// `Drop` görünümü kaldırır ve tanıtıcıyı kapatır. Segment, ona bağlı SON
/// süreç de kapandığında Windows tarafından yok edilir — yani çekirdek yeniden
/// başlarken EA hâlâ bağlıysa veri kaybolmaz.
pub struct SharedMem {
    handle: isize,
    ptr: *mut u8,
    size: usize,
    name: String,
    created: bool,
}

// Ham işaretçi tutuyor; segment `SharedMem` yaşadığı sürece eşlenmiş kalır.
// İçeriğe eşzamanlı erişimin güvenliği halka/tablo katmanının sorumluluğunda.
unsafe impl Send for SharedMem {}
unsafe impl Sync for SharedMem {}

impl SharedMem {
    /// Segmenti oluştur; zaten varsa ona bağlan.
    ///
    /// EA tarafı bunu kullanır: terminal yeniden başlatıldığında segment
    /// çekirdek tarafından hâlâ açık tutuluyor olabilir.
    ///
    /// [`SharedMem::was_created`] ile gerçekten yeni mi oluşturduğunu
    /// öğrenebilirsin — halka başlığını yalnızca yeni oluşturulduğunda
    /// başlatmak gerekir, aksi halde akış ortasında sayaçlar sıfırlanır.
    pub fn create_or_open(name: &str, size: usize) -> Result<Self, ShmError> {
        let wide = to_wide(name)?;
        let (hi, lo) = split_size(size);
        // Güvenlik: `wide` NUL sonlandırılmış geçerli bir UTF-16 dizisi.
        let handle = unsafe {
            ffi::CreateFileMappingW(
                ffi::INVALID_HANDLE_VALUE,
                core::ptr::null_mut(),
                ffi::PAGE_READWRITE,
                hi,
                lo,
                wide.as_ptr(),
            )
        };
        if handle == 0 {
            return Err(ShmError::CreateFailed { name: name.to_owned(), code: last_error() });
        }
        // GetLastError'ı CreateFileMappingW'den HEMEN sonra okumak şart:
        // araya giren herhangi bir çağrı değeri ezer.
        let already_existed = last_error() == ffi::ERROR_ALREADY_EXISTS;
        Self::map(handle, name, size, !already_existed)
    }

    /// Var olan bir segmente bağlan. Segment yoksa hata döner.
    ///
    /// Çekirdek tarafı bunu kullanır: EA'nın segmenti kurmuş olması beklenir.
    pub fn open(name: &str, size: usize) -> Result<Self, ShmError> {
        let wide = to_wide(name)?;
        // Güvenlik: `wide` NUL sonlandırılmış geçerli bir UTF-16 dizisi.
        let handle =
            unsafe { ffi::OpenFileMappingW(ffi::FILE_MAP_ALL_ACCESS, 0, wide.as_ptr()) };
        if handle == 0 {
            return Err(ShmError::OpenFailed { name: name.to_owned(), code: last_error() });
        }
        Self::map(handle, name, size, false)
    }

    fn map(handle: isize, name: &str, size: usize, created: bool) -> Result<Self, ShmError> {
        // Güvenlik: `handle` geçerli bir dosya eşleme tanıtıcısı.
        let ptr = unsafe { ffi::MapViewOfFile(handle, ffi::FILE_MAP_ALL_ACCESS, 0, 0, size) };
        if ptr.is_null() {
            let code = last_error();
            unsafe { ffi::CloseHandle(handle) };
            return Err(ShmError::MapFailed { name: name.to_owned(), code });
        }
        Ok(Self { handle, ptr: ptr.cast::<u8>(), size, name: name.to_owned(), created })
    }

    /// Segmentin başlangıcına işaretçi. En az 64 bayta hizalıdır.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Eşlenen bayt sayısı.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Bu çağrı segmenti YENİ mi oluşturdu (yoksa var olana mı bağlandı)?
    ///
    /// Halka başlığını yalnızca `true` ise başlat — aksi halde çalışan bir
    /// akışın `head`/`tail` sayaçlarını sıfırlayıp tüketiciyi bozarsın.
    #[inline]
    pub fn was_created(&self) -> bool {
        self.created
    }
}

impl Drop for SharedMem {
    fn drop(&mut self) {
        // Güvenlik: her ikisi de bu yapının sahip olduğu geçerli kaynaklar ve
        // yalnızca burada bir kez serbest bırakılıyor.
        unsafe {
            ffi::UnmapViewOfFile(self.ptr.cast::<c_void>());
            ffi::CloseHandle(self.handle);
        }
    }
}

impl core::fmt::Debug for SharedMem {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SharedMem")
            .field("name", &self.name)
            .field("size", &self.size)
            .field("created", &self.created)
            .finish_non_exhaustive()
    }
}

fn to_wide(name: &str) -> Result<Vec<u16>, ShmError> {
    if name.is_empty() || name.contains('\0') {
        return Err(ShmError::InvalidName(name.to_owned()));
    }
    let mut v: Vec<u16> = name.encode_utf16().collect();
    v.push(0);
    Ok(v)
}

fn split_size(size: usize) -> (u32, u32) {
    let s = size as u64;
    ((s >> 32) as u32, (s & 0xFFFF_FFFF) as u32)
}

fn last_error() -> u32 {
    // Güvenlik: yan etkisiz, thread-yerel bir değeri okur.
    unsafe { ffi::GetLastError() }
}

// ---------------------------------------------------------------------------
// Tek-örnek kilidi
// ---------------------------------------------------------------------------

/// Bir örnek adı için tek-yazar kilidi (isimli Windows mutex'i).
///
/// # Neden doğruluk şartı
///
/// Halkalar SPSC'dir: **tek** yazar varsayarlar. MQL5 dokümanı her EA'nın
/// kendi thread'inde çalıştığını söylüyor — yani aynı `instance` adıyla iki
/// grafiğe iki EA yüklenirse iki AYRI thread aynı halkaya yazar ve veri
/// sessizce bozulur. Bu, opsiyonel bir sertleştirme değil; SPSC sözleşmesinin
/// tek uygulanabilir garantisidir.
///
/// Süreç çökerse Windows tanıtıcıyı kapatır ve mutex yok olur; kilit takılı
/// kalmaz.
pub struct InstanceLock {
    handle: isize,
    name: String,
}

unsafe impl Send for InstanceLock {}
unsafe impl Sync for InstanceLock {}

impl InstanceLock {
    /// Kilidi al. Başka bir süreç/thread zaten tutuyorsa
    /// [`ShmError::InstanceBusy`] döner.
    pub fn acquire(instance: &str) -> Result<Self, ShmError> {
        let name = format!("Local\\Sinyal.{instance}.lock");
        let wide = to_wide(&name)?;
        // Güvenlik: `wide` NUL sonlandırılmış geçerli bir UTF-16 dizisi.
        let handle = unsafe { ffi::CreateMutexW(core::ptr::null_mut(), 1, wide.as_ptr()) };
        if handle == 0 {
            return Err(ShmError::CreateFailed { name, code: last_error() });
        }
        // GetLastError'ı CreateMutexW'den HEMEN sonra oku — araya giren çağrı
        // değeri ezer.
        if last_error() == ffi::ERROR_ALREADY_EXISTS {
            // Mutex vardı: sahibi başkası. Kendi tanıtıcımızı kapatıp
            // reddediyoruz.
            unsafe { ffi::CloseHandle(handle) };
            return Err(ShmError::InstanceBusy(instance.to_owned()));
        }
        Ok(Self { handle, name })
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // Güvenlik: sahip olduğumuz geçerli tanıtıcı, yalnızca burada kapanır.
        unsafe { ffi::CloseHandle(self.handle) };
    }
}

impl core::fmt::Debug for InstanceLock {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InstanceLock").field("name", &self.name).finish()
    }
}

/// Kriptografik olarak güvenli rastgele baytlar.
///
/// Token üretimi için. `rand` benzeri bir bağımlılık eklemek yerine sistemin
/// kendi CSPRNG'si kullanılır — bu kütüphane MT5 sürecine yükleniyor ve
/// bağımlılıksız kalması gerekiyor.
///
/// Sistem rastgelelik veremezse `None` döner; **tahmin edilebilir bir değere
/// düşmez**. Zayıf bir token üretip onu güçlü sanmak, hiç üretmemekten kötüdür.
pub fn random_bytes(out: &mut [u8]) -> Option<()> {
    const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;
    if out.is_empty() {
        return Some(());
    }
    // Güvenlik: `out` geçerli ve yazılabilir; uzunluk doğru veriliyor.
    let status = unsafe {
        ffi::BCryptGenRandom(
            core::ptr::null_mut(),
            out.as_mut_ptr(),
            out.len() as u32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    // NTSTATUS: 0 = STATUS_SUCCESS.
    if status == 0 {
        Some(())
    } else {
        None
    }
}

/// URL-güvenli base64 (dolgusuz) — token metni için.
pub fn base64url(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut s = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        s.push(A[(n >> 18) as usize & 63] as char);
        s.push(A[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            s.push(A[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            s.push(A[n as usize & 63] as char);
        }
    }
    s
}

/// Çağıran thread'in işletim sistemi kimliği.
///
/// SPSC sözleşmesinin çalışma zamanı denetimi için: halkaya yazan thread
/// değişirse bu bir yapılandırma hatasıdır ve sessiz veri bozulması yerine
/// görünür bir sayaç üretmelidir.
#[inline]
pub fn current_thread_id() -> u32 {
    // Güvenlik: yan etkisiz, argümansız bir Win32 çağrısı.
    unsafe { ffi::GetCurrentThreadId() }
}

// ---------------------------------------------------------------------------
// Saat
// ---------------------------------------------------------------------------

/// Yüksek çözünürlüklü sayaç (`QueryPerformanceCounter`) okuması.
///
/// Gecikme ölçümünün temeli: EA tick'i yakaladığı anda bunu damgalar, çekirdek
/// okuduğu anda tekrar damgalar. İki süreç aynı makinede olduğu için sayaç
/// ortaktır ve **duvar saati kaymasından etkilenmez** — broker sunucu saatiyle
/// (`time_msc`) ölçüm yapmak yanıltıcı olurdu.
#[inline]
pub fn qpc() -> u64 {
    let mut v: i64 = 0;
    // Güvenlik: geçerli bir `i64`'e yazıyor; QPC Windows XP'den beri daima başarılı.
    unsafe { ffi::QueryPerformanceCounter(&mut v) };
    v as u64
}

/// Sayaç frekansı (tick/saniye). Sistem açıkken sabittir.
#[inline]
pub fn qpc_frequency() -> u64 {
    let mut v: i64 = 0;
    // Güvenlik: geçerli bir `i64`'e yazıyor.
    unsafe { ffi::QueryPerformanceFrequency(&mut v) };
    // 0 dönmesi mümkün değil ama bölme hatasına karşı korumalıyız.
    if v <= 0 {
        1
    } else {
        v as u64
    }
}

/// İki QPC damgası arasındaki farkı nanosaniyeye çevir.
///
/// `end` `start`'tan küçükse 0 döner (sayaç geriye gitmez, ama farklı
/// çekirdeklerden okunan damgalar nadiren birkaç tick ters görünebilir).
#[inline]
pub fn qpc_delta_nanos(start: u64, end: u64) -> u64 {
    let freq = qpc_frequency();
    let delta = end.saturating_sub(start);
    // 128-bit ara değer: delta * 1e9 tek başına u64'ü aşabilir.
    ((delta as u128 * 1_000_000_000u128) / freq as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Testler arası çakışmayı önlemek için benzersiz segment adı.
    fn unique(tag: &str) -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        format!(
            "Local\\SinyalTest.{}.{}.{}",
            tag,
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[test]
    fn create_map_write_read() {
        let name = unique("basic");
        let shm = SharedMem::create_or_open(&name, 4096).unwrap();
        assert!(shm.was_created(), "ilk çağrı segmenti oluşturmalı");
        assert_eq!(shm.size(), 4096);

        unsafe {
            core::ptr::write(shm.as_ptr().cast::<u64>(), 0xDEAD_BEEF_CAFE_F00D);
        }
        let back = unsafe { core::ptr::read(shm.as_ptr().cast::<u64>()) };
        assert_eq!(back, 0xDEAD_BEEF_CAFE_F00D);
    }

    #[test]
    fn second_attach_sees_same_memory_and_reports_not_created() {
        let name = unique("shared");
        let a = SharedMem::create_or_open(&name, 4096).unwrap();
        assert!(a.was_created());

        // İkinci bağlanma var olan segmenti bulmalı — bu ayrım kritik: yeni
        // oluşturulmadıysa halka başlığı SIFIRLANMAMALI.
        let b = SharedMem::create_or_open(&name, 4096).unwrap();
        assert!(!b.was_created(), "var olan segmente bağlanınca created=false olmalı");

        unsafe { core::ptr::write(a.as_ptr().cast::<u32>(), 0x1234_5678) };
        let seen = unsafe { core::ptr::read(b.as_ptr().cast::<u32>()) };
        assert_eq!(seen, 0x1234_5678, "iki görünüm aynı belleği göstermeli");
    }

    #[test]
    fn open_existing_segment_works() {
        let name = unique("openexisting");
        let _owner = SharedMem::create_or_open(&name, 4096).unwrap();
        let opened = SharedMem::open(&name, 4096).unwrap();
        assert!(!opened.was_created());
    }

    #[test]
    fn open_missing_segment_fails_clearly() {
        let name = unique("missing");
        let err = SharedMem::open(&name, 4096).unwrap_err();
        assert!(matches!(err, ShmError::OpenFailed { .. }));
        // Mesaj çekirdek operatörüne ne yapacağını söylemeli.
        assert!(err.to_string().contains("EA henüz başlamamış olabilir"));
    }

    #[test]
    fn segment_dies_when_last_handle_closes() {
        let name = unique("lifetime");
        {
            let _shm = SharedMem::create_or_open(&name, 4096).unwrap();
            assert!(SharedMem::open(&name, 4096).is_ok(), "yaşarken açılabilmeli");
        }
        // Son tanıtıcı kapandı → segment yok olmalı.
        assert!(SharedMem::open(&name, 4096).is_err(), "son tanıtıcıdan sonra yok olmalı");
    }

    #[test]
    fn rejects_invalid_names() {
        assert!(matches!(
            SharedMem::create_or_open("", 4096),
            Err(ShmError::InvalidName(_))
        ));
        assert!(matches!(
            SharedMem::create_or_open("Local\\bad\0name", 4096),
            Err(ShmError::InvalidName(_))
        ));
    }

    #[test]
    fn mapping_is_at_least_cache_line_aligned() {
        // Halkalar 64 baytlık hizalama varsayıyor; MapViewOfFile 64 KB ayrıntı
        // düzeyinde hizaladığı için bu garanti, ama varsayımı test ediyoruz.
        let name = unique("align");
        let shm = SharedMem::create_or_open(&name, 1 << 20).unwrap();
        assert_eq!(shm.as_ptr() as usize % 64, 0, "eşleme 64 bayta hizalı olmalı");
    }

    #[test]
    fn large_segment_over_4gb_boundary_math_is_correct() {
        // 32 bitlik yüksek/alçak ayrımının doğruluğu — 4 GB üstü segmentlerde
        // yanlış bölme sessizce küçük bir segment oluştururdu.
        assert_eq!(split_size(0), (0, 0));
        assert_eq!(split_size(4096), (0, 4096));
        assert_eq!(split_size(0xFFFF_FFFF), (0, 0xFFFF_FFFF));
        assert_eq!(split_size(0x1_0000_0000), (1, 0));
        assert_eq!(split_size(0x1_2345_6789), (1, 0x2345_6789));
    }

    #[test]
    fn instance_lock_blocks_a_second_writer() {
        // SPSC sözleşmesinin tek uygulanabilir garantisi: aynı örnek adına
        // ikinci bir yazar giremez.
        let inst = format!("locktest-{}-{}", std::process::id(), line!());
        let first = InstanceLock::acquire(&inst).unwrap();

        let err = InstanceLock::acquire(&inst).unwrap_err();
        assert!(matches!(err, ShmError::InstanceBusy(_)));
        // Mesaj operatöre ne yapması gerektiğini söylemeli.
        let msg = err.to_string();
        assert!(msg.contains("FARKLI bir örnek adı"), "yönlendirme eksik: {msg}");

        drop(first);
        // Serbest bıraktıktan sonra tekrar alınabilmeli — EA yeniden
        // başladığında kilit takılı kalmamalı.
        assert!(InstanceLock::acquire(&inst).is_ok());
    }

    #[test]
    fn different_instances_do_not_block_each_other() {
        // Broker başına ayrı terminal çalıştırmanın temeli: farklı örnek
        // adları birbirini engellememeli.
        let a = InstanceLock::acquire(&format!("lockA-{}", std::process::id())).unwrap();
        let b = InstanceLock::acquire(&format!("lockB-{}", std::process::id())).unwrap();
        assert_ne!(a.name(), b.name());
    }

    #[test]
    fn thread_id_is_stable_within_a_thread_and_differs_across_threads() {
        let mine = current_thread_id();
        assert_ne!(mine, 0);
        assert_eq!(mine, current_thread_id(), "aynı thread'de sabit olmalı");

        let other = std::thread::spawn(current_thread_id).join().unwrap();
        assert_ne!(mine, other, "farklı thread farklı kimlik vermeli");
    }

    #[test]
    fn random_bytes_are_filled_and_not_repeating() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        random_bytes(&mut a).expect("sistem CSPRNG çalışmalı");
        random_bytes(&mut b).expect("sistem CSPRNG çalışmalı");
        assert_ne!(a, [0u8; 32], "tampon doldurulmalı");
        assert_ne!(a, b, "iki çağrı aynı değeri vermemeli");
        assert!(random_bytes(&mut []).is_some(), "boş dilim sorun olmamalı");
    }

    #[test]
    fn base64url_encodes_without_padding_or_unsafe_chars() {
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(b"foobar"), "Zm9vYmFy");

        // URL/komut satırı güvenli olmalı: '+' '/' '=' ÇIKMAMALI.
        let mut buf = [0u8; 96];
        random_bytes(&mut buf).unwrap();
        let s = base64url(&buf);
        assert!(!s.contains('+') && !s.contains('/') && !s.contains('='), "güvensiz karakter: {s}");
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn generated_token_has_enough_entropy_to_be_unguessable() {
        // 32 bayt = 256 bit; base64url'de 43 karakter.
        let mut buf = [0u8; 32];
        random_bytes(&mut buf).unwrap();
        let t = base64url(&buf);
        assert_eq!(t.len(), 43, "32 bayt 43 karakter olmalı, bulunan {}", t.len());
    }

    #[test]
    fn qpc_advances_monotonically() {
        let a = qpc();
        let b = qpc();
        assert!(b >= a, "QPC geriye gitmemeli");
        assert!(qpc_frequency() > 0);
    }

    #[test]
    fn qpc_delta_converts_to_plausible_nanos() {
        let start = qpc();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let nanos = qpc_delta_nanos(start, qpc());
        // 20 ms uyku: çok gevşek sınırlar (yüklü CI makinesi olabilir), amaç
        // birim dönüşümünün büyüklük sırasının doğru olduğunu göstermek.
        assert!(
            (10_000_000..500_000_000).contains(&nanos),
            "20ms ≈ 2e7 ns bekleniyordu, bulunan {nanos}"
        );
    }

    #[test]
    fn qpc_delta_handles_reversed_stamps() {
        // Farklı çekirdeklerden okunan damgalar nadiren ters görünebilir;
        // taşma yerine 0 dönmeli.
        assert_eq!(qpc_delta_nanos(1000, 500), 0);
        assert_eq!(qpc_delta_nanos(100, 100), 0);
    }
}
