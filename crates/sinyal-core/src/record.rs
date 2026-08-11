//! Tick kaydedicisi — diske **append-only**, AYRI thread.
//!
//! # Neden ayrı thread
//!
//! Okuyucu döngüsü (`source.rs`) tick başına ~250 µs'lik bir gecikme
//! bütçesiyle çalışıyor. Diske yazmak bu bütçeyi tek bir `write` çağrısıyla
//! aşabilir: NTFS'te bir metadata güncellemesi, bir antivirüs kancası veya
//! dolmuş bir yazma önbelleği milisaniyeler sürer. Okuyucu o sırada halkadan
//! tick çekmiyorsa **EA'nın halkası dolar ve tick KALICI kaybolur** — yani
//! kayıt tutma çabası canlı akışı bozar. Bu yüzden okuyucu yalnızca kanala
//! `try_send` yapar; kanal doluysa kayıt DÜŞÜRÜLÜR ve sayılır.
//!
//! Öncelik sırası açıktır: **canlı akış > kayıt**. Düşen kayıt telemetriye
//! çıkar; sessiz kayıp yoktur.
//!
//! # Kayıt biçimi — SABİT
//!
//! ```text
//! <data-dir>/<instance>/ticks-YYYYMMDD.bin      sabit 48 baytlık kayıtlar
//! <data-dir>/<instance>/symbols-YYYYMMDD.jsonl  sembol tablosu görüntüleri
//! <data-dir>/.lock                              tek yazar kilidi
//! ```
//!
//! Gün sınırı **UTC** ve tarih kaydın YEREL ALIM zamanından türetilir
//! ([`TickRec::recv_ms`]) — broker saatinden değil. Broker saati atlarsa
//! (sunucu düzeltmesi, yaz saati) dosya sınırı kaymaz.
//!
//! ## Dosya başlığı YOK — kasıtlı
//!
//! **Dosya uzunluğu tek doğruluk kaynağıdır.** Yükleme `len - (len % 48)`
//! bayta kırpar, yani çökme anında yarım yazılmış kayıt sessizce atılır
//! ([`load_ticks`]). Başlıkta kayıt sayısı tutsaydık, çökmeden sonra başlık
//! gerçekle çelişirdi ve hangisine inanılacağı belirsiz kalırdı. `fsync`
//! yapmıyoruz (her tickte fsync akışı öldürürdü): çökmede son ≤200 ms
//! kaybolabilir, ama kırpma kuralı sayesinde dosya **her zaman** okunabilir.
//!
//! ## İki saat KASITLI
//!
//! - [`TickRec::time_msc`] broker sunucu saatidir ve tüketiciye giden zamandır.
//!   Aynı değeri taşıyan ardışık tickler olabilir (broker ms çözünürlüğünde
//!   iki tick aynı damgayı alır).
//! - [`TickRec::recv_ms`] yerel alım zamanıdır ve **replay temposu** yalnızca
//!   bundan yeniden üretilebilir. Kaynağı EA'nın `recv_qpc` damgasıdır: QPC
//!   monoton ve süreçler arası ortaktır, duvar saati düzeltmelerinden
//!   etkilenmez. Bir kez alınan (duvar saati, QPC) çapasıyla epoch ms'ye
//!   çevrilir — sıcak yolda saat okuma YOK.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sinyal_proto::{sym_flag, SymbolEntry, Tick};
use sinyal_shm::{qpc, qpc_frequency};

// `SymbolInfo` artik kullanilmiyor: kayit `replay::SymbolItem` yaziyor cunku
// tick kayitlarindaki `symbol_id` ancak o tipteki `id` alaniyla coazulebilir.

/// Tek bir tick kaydının bayt boyu. **Değiştirilemez** — kayıt biçiminin
/// tamamı bu sabite dayanır (kırpma, indeksleme, dosya uzunluğu aritmetiği).
pub const REC_SIZE: usize = 48;

const MS_PER_DAY: i64 = 86_400_000;

/// Tampon bu boyuta ulaşınca yazılır.
///
/// 64 KB ≈ 1365 tick. Sakin bir seansta bu saatler alır, o yüzden süre sınırı
/// da var: ikisinden hangisi önce dolarsa.
const FLUSH_BYTES: usize = 64 * 1024;

/// Tampon en geç bu sürede bir yazılır.
///
/// Çökmede kaybedilebilecek azami veri bu kadardır. Daha kısası sistem çağrısı
/// sayısını, daha uzunu kayıp penceresini büyütür.
const FLUSH_EVERY: Duration = Duration::from_millis(200);

/// Dosya açılamadığında yeniden denemeden önce beklenen süre.
///
/// Disk dolduğunda veya dizin silindiğinde her tick için `open` denemek
/// yazıcıyı sistem çağrısında boğardı.
const OPEN_RETRY: Duration = Duration::from_secs(1);

/// Kanal kapasitesi (tick adedi). 64 Ki tick ≈ 3,6 MB bellek.
///
/// Bu, yazıcının kısa bir takılmasını (disk uykudan uyanıyor, antivirüs
/// tarıyor) tick kaybetmeden soğurmak için tampon. Daha büyüğü, kalıcı olarak
/// yetişemeyen bir diskte yalnızca kaybı geciktirirdi.
pub const DEFAULT_QUEUE: usize = 1 << 16;

// ---------------------------------------------------------------------------
// Kayıt
// ---------------------------------------------------------------------------

/// Diskteki tek tick kaydı — **tam 48 bayt**.
///
/// Alan sırası ve ofsetleri kayıt biçiminin parçasıdır; okuyan taraf (replay)
/// bunlara göre yazılmıştır. Bayt düzeni little-endian olarak AÇIKÇA
/// kodlanır ([`TickRec::to_bytes`]) — `repr(C)` düzenine güvenip belleği
/// olduğu gibi dökmek, kaydı derleyici/mimari ayrıntısına bağlardı.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TickRec {
    /// Yerel alım zamanı, epoch ms. **Replay temposu bundan üretilir.**
    pub recv_ms: i64,
    /// Broker sunucu saati, epoch ms. Tüketiciye giden zaman budur.
    pub time_msc: i64,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    /// O anki sembol tablosundaki indeks; isim `symbols-*.jsonl`'dan çözülür.
    pub symbol_id: u32,
    /// `sinyal_proto::tick_flag::*` maskesi.
    pub flags: u16,
    /// `sinyal_proto::kind::*` (telde `u8`, kayıtta hizalama için `u16`).
    pub kind: u16,
}

// Kayıt boyutu biçimin kendisidir: bir sapma tüm dosyayı kaydırır.
const _: () = assert!(std::mem::size_of::<TickRec>() == REC_SIZE);
const _: () = assert!(std::mem::align_of::<TickRec>() == 8);

impl TickRec {
    /// Halkadan gelen ham tick'ten kayıt üret.
    ///
    /// `recv_ms` burada verilir çünkü QPC → epoch dönüşümü çapayı bilen
    /// yazıcıya aittir.
    #[inline]
    fn from_tick(t: &Tick, recv_ms: i64) -> Self {
        Self {
            recv_ms,
            time_msc: t.time_msc,
            bid: t.bid,
            ask: t.ask,
            last: t.last,
            symbol_id: t.symbol_id,
            flags: t.flags,
            kind: t.kind as u16,
        }
    }

    /// Diskteki gösterimi (little-endian, sabit ofsetler).
    #[inline]
    pub fn to_bytes(self) -> [u8; REC_SIZE] {
        let mut b = [0u8; REC_SIZE];
        b[0..8].copy_from_slice(&self.recv_ms.to_le_bytes());
        b[8..16].copy_from_slice(&self.time_msc.to_le_bytes());
        b[16..24].copy_from_slice(&self.bid.to_le_bytes());
        b[24..32].copy_from_slice(&self.ask.to_le_bytes());
        b[32..40].copy_from_slice(&self.last.to_le_bytes());
        b[40..44].copy_from_slice(&self.symbol_id.to_le_bytes());
        b[44..46].copy_from_slice(&self.flags.to_le_bytes());
        b[46..48].copy_from_slice(&self.kind.to_le_bytes());
        b
    }

    /// 48 baytlık bir kaydı çöz.
    #[inline]
    pub fn from_bytes(b: &[u8; REC_SIZE]) -> Self {
        #[inline]
        fn a8(b: &[u8; REC_SIZE], at: usize) -> [u8; 8] {
            [
                b[at], b[at + 1], b[at + 2], b[at + 3], b[at + 4], b[at + 5], b[at + 6],
                b[at + 7],
            ]
        }
        Self {
            recv_ms: i64::from_le_bytes(a8(b, 0)),
            time_msc: i64::from_le_bytes(a8(b, 8)),
            bid: f64::from_le_bytes(a8(b, 16)),
            ask: f64::from_le_bytes(a8(b, 24)),
            last: f64::from_le_bytes(a8(b, 32)),
            symbol_id: u32::from_le_bytes([b[40], b[41], b[42], b[43]]),
            flags: u16::from_le_bytes([b[44], b[45]]),
            kind: u16::from_le_bytes([b[46], b[47]]),
        }
    }
}

// ---------------------------------------------------------------------------
// Okuma tarafı (replay ve testler)
// ---------------------------------------------------------------------------

/// Bayt dizisini kayıtlara çöz; **tam olmayan son kaydı atar**.
///
/// Kırpma burada yapılır ve sessizdir: yarım kayıt, yazarın çökmesinden
/// başka bir anlama gelmez ve tek doğru davranış onu yok saymaktır.
#[allow(dead_code)] // replay tarafının giriş kapısı; bugün testler kullanıyor
pub fn decode_all(bytes: &[u8]) -> Vec<TickRec> {
    let n = bytes.len() / REC_SIZE;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut b = [0u8; REC_SIZE];
        b.copy_from_slice(&bytes[i * REC_SIZE..(i + 1) * REC_SIZE]);
        out.push(TickRec::from_bytes(&b));
    }
    out
}

/// Bir gün dosyasını yükle. Yarım yazılmış son kayıt **hata değildir**.
#[allow(dead_code)] // replay tarafının giriş kapısı
pub fn load_ticks(path: &Path) -> std::io::Result<Vec<TickRec>> {
    Ok(decode_all(&fs::read(path)?))
}

/// `<data-dir>/<instance>/ticks-YYYYMMDD.bin`
#[allow(dead_code)]
pub fn tick_path(data_dir: &Path, instance: &str, stamp: u32) -> PathBuf {
    data_dir.join(instance).join(format!("ticks-{stamp:08}.bin"))
}

/// `<data-dir>/<instance>/symbols-YYYYMMDD.jsonl`
#[allow(dead_code)]
pub fn symbols_path(data_dir: &Path, instance: &str, stamp: u32) -> PathBuf {
    data_dir.join(instance).join(format!("symbols-{stamp:08}.jsonl"))
}

/// Bu örnek için kayıtlı günler (YYYYMMDD), artan sırada.
///
/// Dizin yoksa boş liste döner — "henüz kayıt yok" bir hata değildir.
#[allow(dead_code)]
pub fn list_days(data_dir: &Path, instance: &str) -> std::io::Result<Vec<u32>> {
    let dir = data_dir.join(instance);
    let rd = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for ent in rd.flatten() {
        let name = ent.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("ticks-") else { continue };
        let Some(stamp) = rest.strip_suffix(".bin") else { continue };
        if stamp.len() != 8 {
            continue;
        }
        if let Ok(v) = stamp.parse::<u32>() {
            out.push(v);
        }
    }
    out.sort_unstable();
    Ok(out)
}

// ---------------------------------------------------------------------------
// UTC takvim
// ---------------------------------------------------------------------------

/// Epoch ms → `YYYYMMDD` (UTC).
pub fn day_stamp(ms: i64) -> u32 {
    stamp_of_day(ms.div_euclid(MS_PER_DAY))
}

/// `YYYYMMDD` → o günün 00:00 UTC epoch ms değeri.
///
/// Replay'in `--replay-date` + `--replay-from HH:MM` hesabı bunu ister.
/// Geçersiz damgada `None` döner — takvimde olmayan bir günü sessizce
/// yuvarlamak, kaydın var olmayan bir bölümünü oynatmak demekti.
#[allow(dead_code)]
pub fn day_start_ms(stamp: u32) -> Option<i64> {
    let y = (stamp / 10_000) as i64;
    let m = ((stamp / 100) % 100) as i64;
    let d = (stamp % 100) as i64;
    if !(1..=12).contains(&m) || d <= 0 || d > days_in_month(y, m) as i64 {
        return None;
    }
    Some(days_from_civil(y, m, d) * MS_PER_DAY)
}

/// Epoch günü → `YYYYMMDD`.
fn stamp_of_day(day: i64) -> u32 {
    let (y, m, d) = civil_from_days(day);
    (y as u32) * 10_000 + m * 100 + d
}

/// Howard Hinnant'ın `civil_from_days` algoritması (proleptik Gregoryen).
///
/// Kendi takvim aritmetiğimizi yazıyoruz çünkü bu ikili için `chrono`
/// eklemek, tek ihtiyacı "UTC gün sınırı" olan bir bağımlılık ağacı demekti.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `civil_from_days` tersi.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// Dosyayı 48'in katına geri çek (kısmi yazımdan sonra onarım).
///
/// Kırpılan bayt sayısını döner; dosya zaten hizalıysa `None`. Yeniden
/// açıyoruz çünkü append kipiyle açılmış bir tanıtıcı Windows'ta
/// `set_len` için gereken erişimi taşımayabilir.
fn align_file(path: &Path) -> std::io::Result<Option<u64>> {
    let f = OpenOptions::new().write(true).open(path)?;
    let len = f.metadata()?.len();
    let extra = len % REC_SIZE as u64;
    if extra == 0 {
        return Ok(None);
    }
    f.set_len(len - extra)?;
    Ok(Some(extra))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Hatalar
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum RecError {
    /// Kayıt dizinini başka bir yazar tutuyor.
    Busy { dir: PathBuf },
    /// Örnek adı dosya yolunda kullanılamaz.
    BadInstance(String),
    Io { what: &'static str, path: PathBuf, source: std::io::Error },
}

impl std::fmt::Display for RecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy { dir } => write!(
                f,
                "kayit dizinini ({}) baska bir yazar tutuyor — kilit: {}. \
                 Ayni dizine ikinci bir kaydedici serpistirilmis append yapar ve \
                 dosyayi SESSIZCE bozar. Diger daemon'i durdur ya da AYRI bir \
                 --record dizini ver.",
                dir.display(),
                dir.join(LOCK_NAME).display()
            ),
            Self::BadInstance(s) => write!(
                f,
                "ornek adi ({s:?}) dosya yolunda kullanilamaz — kayit dizini \
                 <data-dir>/<instance> olarak aciliyor"
            ),
            Self::Io { what, path, source } => {
                write!(f, "kayit {what} basarisiz ({}): {source}", path.display())
            }
        }
    }
}

impl std::error::Error for RecError {}

// ---------------------------------------------------------------------------
// Tek yazar kilidi
// ---------------------------------------------------------------------------

const LOCK_NAME: &str = ".lock";

/// `<data-dir>/.lock` — dizin başına tek yazar.
///
/// `sinyal_shm::InstanceLock` kalıbı: kilidi **işletim sistemi** tutar, süreç
/// ölünce tanıtıcı kapanır ve kilit kendiliğinden serbest kalır. Takılı kalan
/// bir kilit dosyası temizlemek zorunda değilsin.
///
/// Windows'ta `share_mode(0)` ile açıyoruz: dosyayı ikinci bir açan — aynı
/// süreçteki bir başka `Recorder` dahil — `ERROR_SHARING_VIOLATION` alır.
/// Kilit dizin düzeyindedir çünkü asıl korunan şey bir dosya değil, kayıt
/// **dizininin bütünlüğü**: iki daemon aynı dizine yazarsa `ticks-*.bin`
/// dosyaları serpiştirilmiş append yüzünden bozulur ve bu aylar sonra
/// "grafik saçmalıyor" olarak fark edilir.
#[derive(Debug)]
struct DirLock {
    _file: File,
    #[cfg_attr(windows, allow(dead_code))]
    path: PathBuf,
}

impl DirLock {
    fn acquire(data_dir: &Path) -> Result<Self, RecError> {
        let path = data_dir.join(LOCK_NAME);
        match open_exclusive(&path) {
            Ok(mut f) => {
                // Tanılama için sahibini yaz. Kilit tutulurken dosya
                // okunamaz (paylaşım yok); değeri, çökme sonrası kalan bir
                // dosyayı incelerken ortaya çıkar.
                let _ = writeln!(f, "pid={} at_ms={}", std::process::id(), now_ms());
                let _ = f.flush();
                Ok(Self { _file: f, path })
            }
            Err(e) if is_busy(&e) => Err(RecError::Busy { dir: data_dir.to_path_buf() }),
            Err(e) => Err(RecError::Io { what: "kilit acma", path, source: e }),
        }
    }
}

#[cfg(windows)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    // share_mode(0): okuma/yazma/silme paylaşımı YOK.
    OpenOptions::new().read(true).write(true).create(true).share_mode(0).open(path)
}

#[cfg(not(windows))]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    // Bu ikili pratikte yalnızca Windows'ta çalışır (köprü MT5 sürecine
    // giriyor). Derlenebilir kalmak için "varsa reddet" semantiği; DİKKAT:
    // burada çökme sonrası kilit dosyası kalır ve elle silinmesi gerekir —
    // Windows yolundaki kendiliğinden serbest kalma garantisi YOKTUR.
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(windows)]
fn is_busy(e: &std::io::Error) -> bool {
    // 32 = ERROR_SHARING_VIOLATION, 33 = ERROR_LOCK_VIOLATION
    matches!(e.raw_os_error(), Some(32) | Some(33))
}

#[cfg(not(windows))]
fn is_busy(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::AlreadyExists
}

#[cfg(not(windows))]
impl Drop for DirLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

// ---------------------------------------------------------------------------
// Sayaçlar
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecStats {
    written: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    sym_lines: AtomicU64,
}

/// Kaydedicinin telemetriye çıkan sayaçları.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecCounts {
    /// Diske yazılan tick sayısı.
    pub written: u64,
    /// Kanal dolu olduğu (veya yazma başarısız olduğu) için **düşürülen**
    /// tick sayısı. Sessiz kayıp yok: bu sayaç raporlanır.
    pub dropped: u64,
    /// Dosya açma/yazma hatası sayısı.
    pub errors: u64,
    /// `symbols-*.jsonl` dosyasına eklenen satır sayısı.
    pub sym_lines: u64,
}

// ---------------------------------------------------------------------------
// Kanal mesajları
// ---------------------------------------------------------------------------

enum RecMsg {
    Tick(Tick),
    /// Sembol tablosunun tamamı. Değişiklik denetimi YAZICIDA yapılır:
    /// diskte ne olduğunu bilen odur ve düşen bir görüntü bir sonraki
    /// yenilemede (1 sn) kendiliğinden telafi edilir.
    Symbols(Vec<SymbolEntry>),
    /// "Tamponu yaz ve haber ver" — bkz. [`RecorderHandle::flush`].
    #[allow(dead_code)]
    Flush(std::sync::mpsc::Sender<()>),
    Stop,
}

// ---------------------------------------------------------------------------
// Saat çapası
// ---------------------------------------------------------------------------

/// QPC damgasını epoch ms'ye çeviren çapa.
///
/// Bir kez alınır. Duvar saati sonradan düzeltilse bile kayıttaki tempo
/// bozulmaz: aralıklar QPC'den gelir, yalnızca başlangıç noktası duvar
/// saatindendir.
#[derive(Debug, Clone, Copy)]
struct Clock {
    anchor_qpc: u64,
    anchor_ms: i64,
    freq: u64,
}

impl Clock {
    fn new() -> Self {
        Self { anchor_qpc: qpc(), anchor_ms: now_ms(), freq: qpc_frequency().max(1) }
    }

    #[inline]
    fn epoch_ms(&self, recv_qpc: u64) -> i64 {
        if recv_qpc == 0 {
            // EA damgası yoksa (eski ikili, sentetik tick) duvar saatine düş.
            return now_ms();
        }
        let d = recv_qpc as i128 - self.anchor_qpc as i128;
        self.anchor_ms + ((d * 1000) / self.freq as i128) as i64
    }
}

// ---------------------------------------------------------------------------
// Kaydedici
// ---------------------------------------------------------------------------

/// Kaydediciyi başlatan giriş noktası.
pub struct Recorder;

impl Recorder {
    /// Kaydı başlat: dizini kilitle, yazıcı thread'ini kur.
    pub fn start(data_dir: &Path, instance: &str) -> Result<RecorderHandle, RecError> {
        Self::start_with_capacity(data_dir, instance, DEFAULT_QUEUE)
    }

    /// Kanal kapasitesini açıkça vererek başlat.
    ///
    /// Kapasite, yazıcının kısa takılmalarını soğuran tampondur; testler
    /// dolma davranışını görebilmek için küçük değer verir.
    pub fn start_with_capacity(
        data_dir: &Path,
        instance: &str,
        capacity: usize,
    ) -> Result<RecorderHandle, RecError> {
        if !instance_ok(instance) {
            return Err(RecError::BadInstance(instance.to_owned()));
        }
        fs::create_dir_all(data_dir).map_err(|e| RecError::Io {
            what: "dizin olusturma",
            path: data_dir.to_path_buf(),
            source: e,
        })?;

        // Kilit ÖNCE: dizine tek bir yazar girer.
        let lock = DirLock::acquire(data_dir)?;

        let dir = data_dir.join(instance);
        fs::create_dir_all(&dir).map_err(|e| RecError::Io {
            what: "dizin olusturma",
            path: dir.clone(),
            source: e,
        })?;

        let stats = Arc::new(RecStats::default());
        // sync_channel: sınırlı ve `try_send` ile ASLA bloklamayan tek std
        // kanalı. Sınırsız kanal, yetişemeyen bir diskte belleği yerdi.
        let (tx, rx) = sync_channel::<RecMsg>(capacity.max(1));

        let writer = Writer {
            instance: instance.to_owned(),
            dir: dir.clone(),
            clock: Clock::new(),
            buf: Vec::with_capacity(FLUSH_BYTES + REC_SIZE),
            file: None,
            file_path: PathBuf::new(),
            day: i64::MIN,
            open_retry_at: None,
            last_flush: Instant::now(),
            last_items: None,
            stats: stats.clone(),
            _lock: lock,
        };

        let join = std::thread::Builder::new()
            .name(format!("sinyal-rec-{instance}"))
            .spawn(move || writer_loop(rx, writer))
            .map_err(|e| RecError::Io {
                what: "yazici thread'i baslatma",
                path: dir.clone(),
                source: e,
            })?;

        Ok(RecorderHandle { tx, stats, dir, join: Mutex::new(Some(join)) })
    }
}

/// Örnek adı dosya yolunda güvenli mi.
fn instance_ok(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && !s.contains(|c: char| {
            c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
        })
}

/// Okuyucu thread'inin tuttuğu tutamak.
///
/// Tüm gönderme yolları **bloklamaz**: kanal doluysa mesaj düşürülür ve
/// sayılır.
pub struct RecorderHandle {
    tx: SyncSender<RecMsg>,
    stats: Arc<RecStats>,
    dir: PathBuf,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl RecorderHandle {
    /// SICAK YOL: tick'i kayıt kanalına koy.
    ///
    /// Yaptığı tek iş bir `try_send`. Kanal doluysa tick **düşürülür** ve
    /// sayılır — okuyucuyu bekletmek canlı akışı riske atardı.
    #[inline]
    pub fn push_tick(&self, t: &Tick) {
        if self.tx.try_send(RecMsg::Tick(*t)).is_err() {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Sembol tablosu görüntüsü ver (saniyede bir; sıcak yol değil).
    ///
    /// Düşerse kayıp değildir: yazıcı değişiklik denetimini kendi yaptığı
    /// için tablo bir sonraki yenilemede yine gönderilir ve değişiklik o
    /// zaman yazılır.
    pub fn push_symbols(&self, entries: Vec<SymbolEntry>) {
        let _ = self.tx.try_send(RecMsg::Symbols(entries));
    }

    pub fn counts(&self) -> RecCounts {
        RecCounts {
            written: self.stats.written.load(Ordering::Relaxed),
            dropped: self.stats.dropped.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
            sym_lines: self.stats.sym_lines.load(Ordering::Relaxed),
        }
    }

    /// Kayıt dizini (`<data-dir>/<instance>`).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Tamponu diske yazdır ve bitmesini bekle. Zaman aşımında `false`.
    ///
    /// Kapanışta [`RecorderHandle::stop`] zaten yazdırıyor; bu, kaydı
    /// durdurmadan diske bakmak isteyen çağıran (ve testler) içindir.
    #[allow(dead_code)]
    pub fn flush(&self, timeout: Duration) -> bool {
        let (ack_tx, ack_rx) = std::sync::mpsc::channel();
        if !self.offer(RecMsg::Flush(ack_tx), timeout) {
            return false;
        }
        ack_rx.recv_timeout(timeout).is_ok()
    }

    /// Yazıcıyı durdur ve bekle. Temiz kapandıysa `true`.
    ///
    /// Kapanışta çağrılır: tamponda bekleyen ≤200 ms'lik kaydı kurtarır.
    pub fn stop(&self) -> bool {
        let _ = self.offer(RecMsg::Stop, Duration::from_secs(2));
        let handle = self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        match handle {
            Some(h) => h.join().is_ok(),
            // Zaten durdurulmuş: tekrar çağırmak hata değil.
            None => true,
        }
    }

    /// Doluyken beklemeden pes etmeyen gönderim — yalnızca kontrol mesajları
    /// için (kapanış/flush). Tick yolunda ASLA kullanılmaz.
    fn offer(&self, msg: RecMsg, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut msg = msg;
        loop {
            match self.tx.try_send(msg) {
                Ok(()) => return true,
                Err(TrySendError::Full(m)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    msg = m;
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }
    }

    /// Testler için: gerçek yazıcı olmadan, alıcısı elde tutulan bir tutamak.
    /// Kanal dolduğunda düşürme davranışını belirlenimci sınamayı sağlar.
    #[cfg(test)]
    fn detached(capacity: usize) -> (Self, Receiver<RecMsg>) {
        let (tx, rx) = sync_channel::<RecMsg>(capacity);
        let h = Self {
            tx,
            stats: Arc::new(RecStats::default()),
            dir: PathBuf::new(),
            join: Mutex::new(None),
        };
        (h, rx)
    }
}

impl Drop for RecorderHandle {
    fn drop(&mut self) {
        // Tutamak ölüyorsa yazıcıyı düzgün kapat: aksi halde tamponda bekleyen
        // kayıtlar kaybolurdu.
        self.stop();
    }
}

impl std::fmt::Debug for RecorderHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecorderHandle")
            .field("dir", &self.dir)
            .field("counts", &self.counts())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Yazıcı thread'i
// ---------------------------------------------------------------------------

struct Writer {
    instance: String,
    dir: PathBuf,
    clock: Clock,
    buf: Vec<u8>,
    file: Option<File>,
    /// Açık dosyanın yolu — yalnızca hata mesajı için.
    file_path: PathBuf,
    /// Açık dosyanın UTC günü (epoch günü).
    day: i64,
    /// Açma başarısızsa bu ana kadar yeniden deneme.
    open_retry_at: Option<Instant>,
    last_flush: Instant,
    /// En son YAZILAN sembol tablosunun JSON gösterimi. Değişiklik denetimi
    /// bunun üzerinden: aynı içerik ikinci kez yazılmaz.
    last_items: Option<String>,
    stats: Arc<RecStats>,
    /// Kilit yazıcıyla birlikte yaşar: thread bitince serbest kalır.
    _lock: DirLock,
}

fn writer_loop(rx: Receiver<RecMsg>, mut w: Writer) {
    loop {
        let wait = FLUSH_EVERY.saturating_sub(w.last_flush.elapsed());
        match rx.recv_timeout(wait) {
            Ok(RecMsg::Tick(t)) => w.on_tick(&t),
            Ok(RecMsg::Symbols(v)) => w.on_symbols(&v),
            Ok(RecMsg::Flush(ack)) => {
                w.flush();
                let _ = ack.send(());
            }
            Ok(RecMsg::Stop) => break,
            Err(RecvTimeoutError::Timeout) => w.flush(),
            // Gönderen taraf kapandı: son tamponu yazıp çık.
            Err(RecvTimeoutError::Disconnected) => break,
        }
        // Kesintisiz akışta `recv_timeout` hiç zaman aşımına uğramaz; süre
        // sınırını burada da denetliyoruz.
        if w.last_flush.elapsed() >= FLUSH_EVERY {
            w.flush();
        }
    }
    w.flush();
}

impl Writer {
    fn on_tick(&mut self, t: &Tick) {
        let recv_ms = self.clock.epoch_ms(t.recv_qpc);
        let day = recv_ms.div_euclid(MS_PER_DAY);
        if !self.ensure_file(day) {
            // Dosya yok: sessizce yutmuyoruz.
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.buf.extend_from_slice(&TickRec::from_tick(t, recv_ms).to_bytes());
        if self.buf.len() >= FLUSH_BYTES {
            self.flush();
        }
    }

    /// İstenen güne ait dosyayı açık hale getir.
    fn ensure_file(&mut self, day: i64) -> bool {
        if self.file.is_some() && self.day == day {
            return true;
        }
        // Tamponda bekleyenler ESKİ güne ait: dosyayı değiştirmeden önce yaz.
        self.flush();
        self.file = None;

        if let Some(at) = self.open_retry_at {
            if Instant::now() < at {
                return false;
            }
        }
        let path = self.dir.join(format!("ticks-{:08}.bin", stamp_of_day(day)));
        // append: var olan günün üstüne yazmaz, sonuna ekler. Daemon gün
        // içinde yeniden başlarsa kayıt kaldığı yerden devam eder.
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => {
                self.file = Some(f);
                self.file_path = path;
                self.day = day;
                self.open_retry_at = None;
                true
            }
            Err(e) => {
                self.note("dosya acilamadi", &path, &e);
                self.open_retry_at = Some(Instant::now() + OPEN_RETRY);
                false
            }
        }
    }

    fn flush(&mut self) {
        self.last_flush = Instant::now();
        if self.buf.is_empty() {
            return;
        }
        let recs = (self.buf.len() / REC_SIZE) as u64;
        // `f` ödüncünü match'in içine taşımıyoruz: hata dalında `self`'in
        // başka alanlarına yazmak gerekiyor.
        let res = match self.file.as_mut() {
            Some(f) => f.write_all(&self.buf),
            None => {
                self.stats.dropped.fetch_add(recs, Ordering::Relaxed);
                self.buf.clear();
                return;
            }
        };
        match res {
            Ok(()) => {
                self.stats.written.fetch_add(recs, Ordering::Relaxed);
            }
            Err(e) => {
                let path = std::mem::take(&mut self.file_path);
                self.note("yazma", &path, &e);
                self.stats.dropped.fetch_add(recs, Ordering::Relaxed);
                // Tanıtıcıyı bırak: bir sonraki tick yeniden açmayı dener.
                self.file = None;
                self.open_retry_at = Some(Instant::now() + OPEN_RETRY);

                // KRİTİK: `write_all` başarısız olmadan önce kısmi yazmış
                // olabilir (disk doldu). O baytları öylece bırakırsak, dosyanın
                // ORTASINDA yarım bir kayıt kalır ve sonrasına eklenen her şey
                // kayar — yükleyicinin kırpma kuralı yalnızca dosya SONUNDAKİ
                // yarım kaydı kurtarır. Bu yüzden dosyayı hemen 48'in katına
                // geri çekiyoruz: bir kayıt kaybetmek, bir günü okunamaz
                // yapmaktan iyidir.
                match align_file(&path) {
                    Ok(Some(trimmed)) => eprintln!(
                        "[{}] KAYIT: kismi yazim {} bayt kirpildi ({})",
                        self.instance,
                        trimmed,
                        path.display()
                    ),
                    Ok(None) => {}
                    Err(e2) => self.note("kismi yazim kirpma", &path, &e2),
                }
            }
        }
        self.buf.clear();
    }

    fn on_symbols(&mut self, entries: &[SymbolEntry]) {
        // Alanlar `wire::SymbolInfo` ile aynı — ARTI `id`.
        //
        // `id` OLMAZSA KAYIT OKUNAMAZ. Tick kayıtları sembolü `symbol_id` ile
        // tutar (adı 507 tick boyunca tekrarlamak anlamsız olurdu); replay o
        // sayıyı ada çevirmek için bu eşlemeye muhtaçtır. `wire::SymbolInfo`
        // bu alanı taşımaz çünkü WebSocket istemcisi sembolü adıyla görür ve
        // dahili kimliği bilmesi gerekmez.
        //
        // İlk sürüm doğrudan `SymbolInfo` yazdı ve replay `missing field: id`
        // ile yüklemeyi reddetti — birim testleri geçmişti, çünkü yazıcı ve
        // okuyucu ayrı ayrı test edilmişti. Gerçek tur olmadan yakalanmazdı.
        let items: Vec<crate::replay::SymbolItem> = entries
            .iter()
            .map(|e| crate::replay::SymbolItem {
                id: e.symbol_id,
                s: e.name_str().to_owned(),
                digits: e.digits,
                point: e.point,
                tick_size: e.tick_size,
                volume_min: e.volume_min,
                volume_max: e.volume_max,
                volume_step: e.volume_step,
                // OLMAZSA REPLAY'DE MARJIN VE KÂR SESSİZCE YANLIŞ ÇIKAR:
                // simülatör kârı `fiyat farkı × hacim × contract_size` ile
                // hesaplıyor ve alan 0 gelirse forex'te 100 000 kat küçük bir
                // kâr, pratikte de sonsuz kaldıraç doğar. Kayıt bu alanı
                // taşımadığı sürece replay'in kâr eğrisi anlamsızdı.
                contract_size: e.contract_size,
                exec_mode: e.trade_exemode,
                filling_mask: e.filling_mode,
                stops_level: e.stops_level,
                book_depth: e.ticks_bookdepth,
                polled_only: e.flags & sym_flag::POLLED_ONLY != 0,
                chart: e.flags & sym_flag::CHART != 0,
                ready: e.flags & sym_flag::READY != 0,
                src: self.instance.clone(),
            })
            .collect();

        let Ok(items_json) = serde_json::to_string(&items) else {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // DEĞİŞMEDİYSE YAZMA: tablo saniyede bir yenileniyor, her yenilemeyi
        // yazmak günde ~86 bin özdeş satır demekti.
        if self.last_items.as_deref() == Some(items_json.as_str()) {
            return;
        }

        let at_ms = now_ms();
        let path = self.dir.join(format!("symbols-{:08}.jsonl", day_stamp(at_ms)));
        let line = format!("{{\"at_ms\":{at_ms},\"items\":{items_json}}}\n");
        // Satır seyrek (yalnızca değişimde): dosyayı açık tutmuyoruz.
        let res = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| f.write_all(line.as_bytes()));
        match res {
            Ok(()) => {
                self.last_items = Some(items_json);
                self.stats.sym_lines.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => self.note("sembol satiri yazma", &path, &e),
        }
    }

    /// Hatayı say ve boğmadan bildir.
    fn note(&self, what: &str, path: &Path, e: &std::io::Error) {
        let n = self.stats.errors.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n % 1000 == 0 {
            eprintln!(
                "[{}] KAYIT hatasi ({what}, {}): {e} — toplam {n} hata",
                self.instance,
                path.display()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Testler
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinyal_proto::{kind, tick_flag, write_fixed_str};

    /// Test dizini: kapanışta silinir.
    ///
    /// Kaydedici tutamağı bu yapıdan SONRA tanımlanmalı (Rust yerelleri ters
    /// sırada düşürür): kilit dosyası açıkken dizin silinemez.
    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::AtomicU32;
            static N: AtomicU32 = AtomicU32::new(0);
            let p = std::env::temp_dir().join(format!(
                "sinyal-rec-{}-{}-{}",
                tag,
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tick(i: u64, at: u64) -> Tick {
        Tick {
            time_msc: 1_700_000_000_000 + i as i64,
            bid: 1.1 + i as f64 * 1e-5,
            ask: 1.2 + i as f64 * 1e-5,
            last: if i % 3 == 0 { 0.0 } else { 1.15 },
            recv_qpc: at,
            symbol_id: (i % 7) as u32,
            flags: tick_flag::BID | tick_flag::ASK,
            kind: kind::TICK,
            ..Default::default()
        }
    }

    fn sym(id: u32, name: &str) -> SymbolEntry {
        let mut e = SymbolEntry {
            symbol_id: id,
            digits: 5,
            point: 1e-5,
            volume_step: 0.01,
            // Simülatörün parayla ilişkisi bu alandan geçiyor; testlerde 0
            // bırakmak, kaybını görünmez kılan şeyin ta kendisiydi.
            contract_size: 100_000.0,
            flags: sym_flag::READY | sym_flag::CONTRACT_DATA,
            ..Default::default()
        };
        write_fixed_str(&mut e.name, name);
        e
    }

    /// Dizindeki tek `symbols-*.jsonl` dosyası.
    fn symbols_file(dir: &Path, instance: &str) -> PathBuf {
        let mut found: Vec<PathBuf> = fs::read_dir(dir.join(instance))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("symbols-") && n.ends_with(".jsonl"))
            })
            .collect();
        found.sort();
        assert_eq!(found.len(), 1, "tek sembol dosyasi bekleniyordu: {found:?}");
        found.pop().unwrap()
    }

    #[test]
    fn record_layout_is_exactly_48_bytes_at_the_documented_offsets() {
        // Bu düzen üç bileşenin ortak sözleşmesi (kaydedici, yükleyici,
        // replay). Bir alanın kayması dosyayı sessizce yanlış okutur.
        let r = TickRec {
            recv_ms: 0x0102_0304_0506_0708,
            time_msc: 0x1112_1314_1516_1718,
            bid: 1.5,
            ask: 2.5,
            last: 3.5,
            symbol_id: 0xAABB_CCDD,
            flags: 0x1234,
            kind: 0x5678,
        };
        let b = r.to_bytes();
        assert_eq!(b.len(), 48);
        assert_eq!(&b[0..8], &0x0102_0304_0506_0708i64.to_le_bytes());
        assert_eq!(&b[8..16], &0x1112_1314_1516_1718i64.to_le_bytes());
        assert_eq!(&b[16..24], &1.5f64.to_le_bytes());
        assert_eq!(&b[24..32], &2.5f64.to_le_bytes());
        assert_eq!(&b[32..40], &3.5f64.to_le_bytes());
        assert_eq!(&b[40..44], &0xAABB_CCDDu32.to_le_bytes());
        assert_eq!(&b[44..46], &0x1234u16.to_le_bytes());
        assert_eq!(&b[46..48], &0x5678u16.to_le_bytes());
        assert_eq!(TickRec::from_bytes(&b), r, "kodla-coz turu birebir olmali");
    }

    #[test]
    fn written_ticks_read_back_bit_for_bit() {
        let dir = TmpDir::new("rt");
        let rec = Recorder::start(dir.path(), "mt5-rt").unwrap();

        // QPC damgaları küçük adımlarla artıyor: aynı UTC günü içinde kal.
        let base = qpc();
        let mut pushed = Vec::new();
        for i in 0..1_000u64 {
            let t = tick(i, base + i * 10);
            rec.push_tick(&t);
            pushed.push(t);
        }
        assert!(rec.stop(), "yazici temiz kapanmali");

        let days = list_days(dir.path(), "mt5-rt").unwrap();
        assert_eq!(days.len(), 1, "tek gun bekleniyordu: {days:?}");
        let path = tick_path(dir.path(), "mt5-rt", days[0]);
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            (pushed.len() * REC_SIZE) as u64,
            "dosya uzunlugu kayit sayisinin tam kati olmali"
        );

        let back = load_ticks(&path).unwrap();
        assert_eq!(back.len(), pushed.len());
        for (i, (r, t)) in back.iter().zip(&pushed).enumerate() {
            assert_eq!(r.time_msc, t.time_msc, "kayit {i}");
            // Fiyatlar BİT düzeyinde aynı olmalı: yuvarlama kabul edilemez.
            assert_eq!(r.bid.to_bits(), t.bid.to_bits(), "kayit {i}");
            assert_eq!(r.ask.to_bits(), t.ask.to_bits(), "kayit {i}");
            assert_eq!(r.last.to_bits(), t.last.to_bits(), "kayit {i}");
            assert_eq!(r.symbol_id, t.symbol_id, "kayit {i}");
            assert_eq!(r.flags, t.flags, "kayit {i}");
            assert_eq!(r.kind, t.kind as u16, "kayit {i}");
        }
        // recv_ms: sıra korunmuş ve makul bir duvar saati değeri.
        assert!(
            back.windows(2).all(|w| w[1].recv_ms >= w[0].recv_ms),
            "yerel alim zamani geriye gitmemeli"
        );
        assert!(
            (back[0].recv_ms - now_ms()).abs() < 60_000,
            "recv_ms simdiki zamana yakin olmali: {}",
            back[0].recv_ms
        );
        assert_eq!(rec.counts().written, pushed.len() as u64);
        assert_eq!(rec.counts().dropped, 0);
    }

    #[test]
    fn a_half_written_record_is_trimmed_instead_of_failing() {
        // Çökme anında yarım kayıt kalır. Dosya uzunluğu tek doğruluk
        // kaynağı: kırp ve devam et. Hata vermek, bir günlük kaydı
        // okunamaz yapardı.
        let dir = TmpDir::new("trim");
        let path = dir.path().join("ticks-20260101.bin");
        let mut bytes = Vec::new();
        for i in 0..3i64 {
            bytes.extend_from_slice(
                &TickRec { recv_ms: i, time_msc: i * 2, bid: i as f64, ..Default::default() }
                    .to_bytes(),
            );
        }
        bytes.extend_from_slice(&[0xAB; 17]); // yarım kayıt
        fs::write(&path, &bytes).unwrap();

        let back = load_ticks(&path).expect("kirpilmali, HATA VERMEMELI");
        assert_eq!(back.len(), 3, "yarim kayit atilmali");
        assert_eq!(back[2].recv_ms, 2);
        assert_eq!(back[2].time_msc, 4);

        // Sınır durumları: tek başına yarım kayıt ve boş dosya.
        assert!(decode_all(&[0u8; 47]).is_empty());
        assert!(decode_all(&[]).is_empty());
    }

    #[test]
    fn a_partial_write_is_repaired_so_later_appends_stay_aligned() {
        // Kırpma kuralı yalnızca dosya SONUNDAKİ yarım kaydı kurtarır. Disk
        // dolduğunda yarım kalan yazımı olduğu gibi bırakırsak, sonrasına
        // eklenen her kayıt kayar ve o günün tamamı okunamaz hale gelir.
        let dir = TmpDir::new("align");
        let path = dir.path().join("ticks-20260101.bin");
        let mut bytes = vec![0u8; 3 * REC_SIZE];
        bytes.extend_from_slice(&[0xCD; 20]); // yarım kayıt
        fs::write(&path, &bytes).unwrap();

        assert_eq!(align_file(&path).unwrap(), Some(20), "fazlalik kirpilmali");
        assert_eq!(fs::metadata(&path).unwrap().len(), (3 * REC_SIZE) as u64);
        // İkinci çağrı bir şey yapmamalı.
        assert_eq!(align_file(&path).unwrap(), None);

        // Onarımdan sonra eklenen kayıt doğru hizada okunur.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&TickRec { recv_ms: 42, ..Default::default() }.to_bytes()).unwrap();
        drop(f);
        let back = load_ticks(&path).unwrap();
        assert_eq!(back.len(), 4);
        assert_eq!(back[3].recv_ms, 42);
    }

    #[test]
    fn a_full_queue_drops_ticks_and_counts_every_one() {
        // Belirlenimci: alıcı hiç boşaltılmıyor, kapasite biliniyor.
        let (h, rx) = RecorderHandle::detached(4);
        for i in 0..10 {
            h.push_tick(&tick(i, 1000 + i));
        }
        let c = h.counts();
        assert_eq!(c.dropped, 6, "kapasite 4, 10 push -> 6 dusmeli");
        assert_eq!(c.written, 0, "yazici yok, hicbiri yazilmis olamaz");

        let mut queued = 0;
        while rx.try_recv().is_ok() {
            queued += 1;
        }
        assert_eq!(queued, 4, "kanalda tam kapasite kadar tick beklemeli");
        drop(rx);
    }

    #[test]
    fn a_burst_over_a_tiny_queue_never_loses_a_tick_silently() {
        // Kanal kasten çok küçük: yazıcı yetişemeyecek. Beklenti panik ETMEMESİ
        // ve her tick'in ya diskte ya sayaçta olması.
        let dir = TmpDir::new("burst");
        let rec = Recorder::start_with_capacity(dir.path(), "mt5-b", 8).unwrap();
        const N: u64 = 50_000;
        let base = qpc();
        for i in 0..N {
            rec.push_tick(&tick(i, base + i));
        }
        assert!(rec.stop(), "yazici panik ETMEMELI");

        let c = rec.counts();
        assert_eq!(c.written + c.dropped, N, "her tick ya yazilmali ya SAYILMALI");
        assert!(c.written > 0, "hicbiri yazilmamis olamaz");
        assert_eq!(c.errors, 0, "diske yazmada hata beklenmiyor");

        let days = list_days(dir.path(), "mt5-b").unwrap();
        assert_eq!(days.len(), 1);
        let path = tick_path(dir.path(), "mt5-b", days[0]);
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            c.written * REC_SIZE as u64,
            "dosyadaki kayit sayisi sayacla tutmali"
        );
        assert_eq!(load_ticks(&path).unwrap().len() as u64, c.written);
    }

    #[test]
    fn crossing_utc_midnight_opens_a_second_file() {
        let dir = TmpDir::new("day");
        let rec = Recorder::start(dir.path(), "mt5-d").unwrap();

        let base = qpc();
        let freq = qpc_frequency().max(1);
        rec.push_tick(&tick(0, base));

        // Gün sonuna kalan süre + 1 dakika ilerideki bir QPC damgası üret:
        // kaydedicinin çapası bu ana çok yakın olduğu için bu damga kesinlikle
        // ERTESİ güne düşer.
        let to_midnight = MS_PER_DAY - now_ms().rem_euclid(MS_PER_DAY);
        let ahead = ((to_midnight + 60_000) as u64).saturating_mul(freq) / 1000;
        rec.push_tick(&tick(1, base + ahead));
        assert!(rec.stop());

        let days = list_days(dir.path(), "mt5-d").unwrap();
        assert_eq!(days.len(), 2, "gun degisiminde ikinci dosya acilmali: {days:?}");
        assert_eq!(
            day_start_ms(days[1]).unwrap() - day_start_ms(days[0]).unwrap(),
            MS_PER_DAY,
            "iki dosya ardisik UTC gunu olmali: {days:?}"
        );
        // Her gün kendi kaydını almalı — sınırda tampon karışmamalı.
        assert_eq!(load_ticks(&tick_path(dir.path(), "mt5-d", days[0])).unwrap().len(), 1);
        assert_eq!(load_ticks(&tick_path(dir.path(), "mt5-d", days[1])).unwrap().len(), 1);
        assert_eq!(rec.counts().written, 2);
    }

    #[test]
    fn a_second_recorder_on_the_same_directory_is_refused() {
        // İki daemon aynı dizine yazarsa serpiştirilmiş append dosyayı
        // SESSİZCE bozar; hata vermek tek doğru davranış.
        let dir = TmpDir::new("lock");
        let first = Recorder::start(dir.path(), "a").unwrap();

        let err = Recorder::start(dir.path(), "b").expect_err("ikinci yazar REDDEDILMELI");
        assert!(matches!(err, RecError::Busy { .. }), "beklenen Busy, gelen: {err:?}");
        let msg = err.to_string();
        assert!(msg.contains(".lock"), "mesaj kilidi gostermeli: {msg}");
        assert!(msg.contains("--record"), "mesaj ne yapilacagini soylemeli: {msg}");

        // Kilit bırakılınca yeniden alınabilmeli.
        assert!(first.stop());
        drop(first);
        let again = Recorder::start(dir.path(), "b").expect("kilit birakilinca alinabilmeli");
        assert!(again.stop());
    }

    #[test]
    fn the_symbol_table_is_written_on_change_not_on_every_refresh() {
        let dir = TmpDir::new("sym");
        let rec = Recorder::start(dir.path(), "mt5-s").unwrap();

        let a = vec![sym(0, "EURUSD")];
        // Üç özdeş yenileme -> TEK satır.
        rec.push_symbols(a.clone());
        rec.push_symbols(a.clone());
        rec.push_symbols(a.clone());
        assert!(rec.flush(Duration::from_secs(5)));

        let mut b = a.clone();
        b.push(sym(1, "XAUUSD"));
        rec.push_symbols(b);
        assert!(rec.stop());

        let path = symbols_file(dir.path(), "mt5-s");
        let text = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "yalniz degisimler yazilmali: {text}");
        assert_eq!(rec.counts().sym_lines, 2);

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(first["at_ms"].as_i64().unwrap() > 1_600_000_000_000);
        assert_eq!(first["items"].as_array().unwrap().len(), 1);
        assert_eq!(second["items"].as_array().unwrap().len(), 2);
        assert_eq!(first["items"][0]["s"], "EURUSD");
        assert_eq!(second["items"][1]["s"], "XAUUSD");
        // Alan adları `wire::SymbolInfo` ile aynı olmalı: replay'in `symbols`
        // cevabı bu satırlardan üretilecek.
        for k in ["digits", "point", "volume_step", "polled_only", "chart", "ready", "src"] {
            assert!(first["items"][0].get(k).is_some(), "{k} alani eksik: {}", lines[0]);
        }
        assert_eq!(first["items"][0]["src"], "mt5-s");
    }

    #[test]
    fn a_real_recording_is_loadable_by_the_real_replay_loader() {
        // KAYDEDİCİ İLE OKUYUCU ARASINDAKİ DİKİŞ.
        //
        // Bu dosyanın diğer testleri kaydediciyi KENDİ yükleyicisiyle, replay
        // testleri de okuyucuyu KENDİ yazdığı dosyayla doğruluyor. İki taraf
        // birbirini hiç anlamasa bile o testlerin HEPSİ yeşil kalırdı — ve
        // gerçekten kalmıştı: ilk sürüm sembol tablosunu `wire::SymbolInfo`
        // olarak yazıyordu, okuyucu ise `id` alanını arıyordu. Birim testleri
        // bunu göremez, çünkü kimse gerçek bir turu atmıyordu.
        //
        // Bu test o turu atar: ÜRETİMDEKİ kaydedici yazar, ÜRETİMDEKİ
        // yükleyici okur.
        let dir = TmpDir::new("e2e");
        let inst = "mt5-1";
        let date_stamp = day_stamp(now_ms());

        let rec = Recorder::start(dir.path(), inst).expect("kayit acilmali");
        rec.push_symbols(vec![sym(0, "EURUSD"), sym(1, "XAUUSD")]);

        // Tick'ler 1 ms arayla: tempo `recv_ms` farkından üretildiği için
        // damgaların gerçekten ilerlemesi gerekiyor.
        let freq = qpc_frequency().max(1);
        let base = qpc();
        let pushed: Vec<Tick> = (0..64u64)
            .map(|i| {
                let mut t = tick(i, base + i * freq / 1000);
                t.symbol_id = (i % 2) as u32;
                t
            })
            .collect();
        for t in &pushed {
            rec.push_tick(t);
        }
        assert!(rec.stop(), "kayit temiz kapanmali");
        assert_eq!(rec.counts().dropped, 0, "bu boyutta dusen olmamali");

        let days = list_days(dir.path(), inst).unwrap();
        assert!(
            days.contains(&date_stamp),
            "kaydedici beklenen gun dosyasini acmali: {days:?} icinde {date_stamp} yok"
        );

        // --- ÜRETİMDEKİ REPLAY YÜKLEYİCİSİ ---
        let opts = crate::replay::ReplayOpts {
            speed: 0.0,
            ..crate::replay::ReplayOpts::new(dir.path(), format!("{date_stamp:08}"))
        };
        let loaded = crate::replay::load(&opts)
            .expect("kaydedicinin urettigi dizin replay tarafindan YUKLENEBILMELI");

        assert_eq!(loaded.instances, vec![inst.to_string()]);
        assert_eq!(loaded.len(), pushed.len(), "her kayit geri gelmeli");

        // Fiyatlar ve broker saati BİT DÜZEYİNDE aynı dönmeli: kayıt biçimi
        // yuvarlama yapmaz, yaparsa strateji kayıttan farklı bir seri görür.
        for (item, w) in loaded.items.iter().zip(&pushed) {
            assert_eq!(item.rec.bid, w.bid, "bid birebir");
            assert_eq!(item.rec.ask, w.ask, "ask birebir");
            assert_eq!(item.rec.last, w.last);
            assert_eq!(item.rec.time_msc, w.time_msc, "broker saati korunmali");
            assert_eq!(item.rec.symbol_id, w.symbol_id);
            assert_eq!(item.rec.flags, w.flags);
        }

        // Tempo `recv_ms`ten üretiliyor: damgalar ARTAN olmalı, yoksa replay
        // kaydı yanlış hızda oynatır.
        for pair in loaded.items.windows(2) {
            assert!(
                pair[1].rec.recv_ms >= pair[0].rec.recv_ms,
                "recv_ms geri gitmemeli: {} -> {}",
                pair[0].rec.recv_ms,
                pair[1].rec.recv_ms
            );
        }

        // ASIL TUZAK: `symbol_id` → ad eşlemesi. Bu kopmuşsa replay hiçbir
        // tick'i isimlendiremez ve akış SESSİZCE boşalır.
        let snaps = &loaded.symbols[0];
        assert_eq!(snaps.len(), 1, "tek bir tablo goruntusu bekleniyordu");
        let items = &snaps[0].items;
        let name_of = |id: u32| {
            items.iter().find(|i| i.id == id).map(|i| i.s.as_str()).unwrap_or("<YOK>")
        };
        assert_eq!(name_of(0), "EURUSD", "symbol_id 0 cozulemedi: {items:?}");
        assert_eq!(name_of(1), "XAUUSD", "symbol_id 1 cozulemedi: {items:?}");

        // İKİNCİ TUZAK — `contract_size`. Ad eşlemesi gibi kopunca akışı
        // durdurmaz; simülasyonu SESSİZCE YALANCI yapar. Alan kayda
        // yazılmadığı için replay'de 0 geliyordu ve:
        //   marjin = hacim × 0 × fiyat / kaldıraç → serbest marjin denetimi
        //   (10019) hiç tetiklenmez, yani sonsuz kaldıraç;
        //   kâr = fiyat farkı × hacim × 0 → forex'te 100 000 kat küçük, yani
        //   kâr eğrisi anlamsız.
        // Hiçbir test kırmıyordu: kaydedici testleri alanı yazmadığını,
        // simülatör testleri de kendi kurduğu sembolle çalıştığı için
        // eksikliği göremiyordu.
        for it in items {
            assert_eq!(
                it.contract_size, 100_000.0,
                "contract_size kayittan geri gelmeli, yoksa replay'in marjini ve kari \
                 sessizce yanlis olur: {it:?}"
            );
        }
        // Sözleşme büyüklüğü motorun gördüğü hâle kadar taşınmalı: kayıt →
        // SymbolEntry → SimSymbol zincirinin herhangi bir halkası düşerse
        // yukarıdaki iddia yeşil kalırdı.
        let entry = items[0].to_entry();
        assert_eq!(entry.contract_size, 100_000.0, "SymbolEntry'ye aktarilmali");
        assert_ne!(entry.flags & sym_flag::CONTRACT_DATA, 0, "sozlesme verisi ilan edilmeli");
        assert_eq!(
            crate::sim::SimSymbol::from_entry(&entry).contract_size,
            100_000.0,
            "SimSymbol'e aktarilmali — marjin ve kar bu degere dayaniyor"
        );
    }

    #[test]
    fn utc_day_arithmetic_round_trips() {
        // Kendi takvim aritmetiğimizi kullanıyoruz; artık gün ve yüzyıl
        // kuralları burada kanıtlanmalı.
        assert_eq!(day_stamp(0), 1970_01_01);
        assert_eq!(day_stamp(MS_PER_DAY - 1), 1970_01_01);
        assert_eq!(day_stamp(MS_PER_DAY), 1970_01_02);
        assert_eq!(day_stamp(-1), 1969_12_31, "epoch oncesi de dogru olmali");
        assert_eq!(day_stamp(1_709_164_800_000), 2024_02_29, "artik gun");
        assert_eq!(day_start_ms(1970_01_01), Some(0));
        assert_eq!(day_start_ms(2024_02_29), Some(1_709_164_800_000));
        assert_eq!(day_start_ms(2023_02_29), None, "artik olmayan yilda 29 Subat yok");
        assert_eq!(day_start_ms(2024_13_01), None);
        assert_eq!(day_start_ms(2024_00_10), None);

        // 40 yıl boyunca gün ↔ damga dönüşümü tutmalı.
        for day in 0..14_600i64 {
            let ms = day * MS_PER_DAY;
            let stamp = day_stamp(ms);
            assert_eq!(day_start_ms(stamp), Some(ms), "gun {day} damga {stamp}");
        }
    }

    #[test]
    fn instance_names_that_escape_the_data_dir_are_refused() {
        let dir = TmpDir::new("badname");
        for bad in ["", ".", "..", "a/b", "a\\b", "c:evil", "q?"] {
            let err = Recorder::start(dir.path(), bad)
                .expect_err("yol kacisi kabul edilemez: {bad}");
            assert!(matches!(err, RecError::BadInstance(_)), "{bad}: {err:?}");
        }
    }
}
