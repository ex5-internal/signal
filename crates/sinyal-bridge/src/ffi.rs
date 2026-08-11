//! MQL5'in `#import` ile çağırdığı C ABI.
//!
//! # Panik güvenliği
//!
//! Dışa açık HER fonksiyon `catch_unwind` ile sarılıdır. Bu isteğe bağlı
//! değil: Rust 1.81'den beri `extern "C"` sınırından unwind etmek süreci
//! **abort** eder — yani köprüdeki bir panik, açık pozisyonları olan MT5
//! terminalini öldürürdü. Panik yerine [`SINYAL_ERR_PANIC`] dönüyoruz; EA bunu
//! loglar ve terminal ayakta kalır.
//!
//! # Dönüş sözleşmesi
//!
//! - `> 0` başarı (genellikle [`SINYAL_OK`])
//! - `0` "şu an yapılamadı" — halka dolu ya da boş. Hata DEĞİL.
//! - `< 0` hata; ayrıntı için `SinyalLastError` çağır.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};
use std::sync::Mutex;

use sinyal_proto::{BarRec, Book, BookLevel, Cmd, HistReq, Res, SymbolEntry, Tick, MAX_BOOK_DEPTH};

use crate::hist::HistSession;
use crate::session::{Capacities, Session};

/// İşlem başarılı.
pub const SINYAL_OK: i32 = 1;
/// Halka dolu (push) veya boş (pop). Hata değil.
pub const SINYAL_WOULD_BLOCK: i32 = 0;
/// Tanıtıcı geçersiz — `SinyalOpen` başarısız olmuş veya çift kapatılmış.
pub const SINYAL_ERR_HANDLE: i32 = -1;
/// Köprüde panik yakalandı. Terminal öldürülmedi; bu bir hatadır, bildir.
pub const SINYAL_ERR_PANIC: i32 = -2;
/// Argümanlar geçersiz (NULL işaretçi, negatif uzunluk vb.).
pub const SINYAL_ERR_ARGS: i32 = -3;
/// Diğer hata; ayrıntı `SinyalLastError`'da.
pub const SINYAL_ERR_OTHER: i32 = -4;

/// `SinyalSizeof` için yapı kodları — MQL5 tarafındaki `sizeof()` ile
/// karşılaştırılır. Uyumsuzluk, iki tarafın farklı sürümle derlendiğini
/// gösterir ve EA'nın başlamayı reddetmesi gerekir.
pub mod sizeof_what {
    pub const TICK: i32 = 1;
    pub const BOOK_LEVEL: i32 = 2;
    pub const BOOK: i32 = 3;
    pub const CMD: i32 = 4;
    pub const RES: i32 = 5;
    pub const SYMBOL_ENTRY: i32 = 6;
    pub const ACCOUNT: i32 = 7;
    pub const POSITION: i32 = 8;
    pub const ORDER: i32 = 9;
    /// Geçmiş kanalı — service `HistReq`'i bu kodla doğrular.
    pub const HIST_REQ: i32 = 10;
    /// Geçmiş kanalı — service `BarRec`'i bu kodla doğrular.
    pub const BAR_REC: i32 = 11;
    /// Protokol sürümü (boyut değil).
    pub const PROTO_VERSION: i32 = 100;
}

/// Aynı terminal sürecinde aynı anda açılabilecek azami oturum sayısı.
///
/// Her chart'taki EA örneği bir slot kullanır; 64 pratikte fazlasıyla yeterli.
const MAX_SESSIONS: usize = 64;

/// Bir oturum slotu.
///
/// # Neden ham işaretçi DÖNDÜRMÜYORUZ
///
/// İlk tasarım tanıtıcı olarak `Box::into_raw` işaretçisini döndürüyor ve
/// içindeki bir imza alanını okuyarak doğruluyordu. Bu **temelden yanlıştı**:
/// bir işaretçiyi doğrulamak için önce onu dereference etmek gerekiyordu, yani
/// MQL5 tarafındaki bir yazım hatası (ör. `0x1`) doğrudan geçersiz adrese
/// erişime dönüşüyordu — hizalanmamış/eşlenmemiş bellek okuması, yani açık
/// pozisyonları olan bir ticaret terminalinin çökmesi.
///
/// Şimdi tanıtıcı bir **indeks + kuşak** çiftidir. Doğrulama yalnızca kendi
/// tablomuzdaki atomik değerleri okur; çağırandan gelen sayı asla adres olarak
/// kullanılmaz. Yalnızca kendi sakladığımız işaretçiyi dereference ederiz.
///
/// Tip parametresi sayesinde her oturum türü KENDİ tablosunu kullanır: bir
/// `Session` tanıtıcısının `HistSession` olarak çözülmesi tip düzeyinde
/// imkânsız, ayrıca [`TAG_HIST`] biti sayısal olarak da ayırıyor.
struct Slot<T> {
    /// Slot kullanımda mı (tahsis yarışını çözer).
    claimed: AtomicBool,
    /// Her tahsiste artar; eski tanıtıcıları anında geçersizleştirir.
    generation: AtomicU32,
    ptr: AtomicPtr<T>,
}

impl<T> Slot<T> {
    const fn new() -> Self {
        Self {
            claimed: AtomicBool::new(false),
            generation: AtomicU32::new(0),
            ptr: AtomicPtr::new(core::ptr::null_mut()),
        }
    }
}

static SLOTS: [Slot<Handle>; MAX_SESSIONS] = [const { Slot::new() }; MAX_SESSIONS];
static HIST_SLOTS: [Slot<HistHandle>; MAX_SESSIONS] = [const { Slot::new() }; MAX_SESSIONS];

struct Handle {
    session: Session,
}

struct HistHandle {
    session: HistSession,
}

/// EA oturumu tanıtıcılarının tür etiketi.
const TAG_SESSION: u64 = 0;
/// Geçmiş oturumu tanıtıcılarının tür etiketi.
///
/// # Neden gerekli
///
/// İki tablo da aynı indeks/kuşak alanını kullanıyor, dolayısıyla `SinyalOpen`
/// ile alınan bir tanıtıcı sayısal olarak geçerli bir geçmiş tanıtıcısına eşit
/// olabilirdi. Tip güvenliği bellek hatasını engeller ama YANLIŞ OTURUMA
/// yazmayı engellemez — barlar başka bir örneğin halkasına giderdi. Bu bit
/// karışıklığı en baştan imkânsız kılıyor: etiketi tutmayan tanıtıcı çözülmez.
const TAG_HIST: u64 = 0x4000_0000;
/// Etiket bitini indeks bitlerinden ayıran maske.
const TAG_MASK: u64 = 0x4000_0000;

/// Tanıtıcı = (kuşak << 32) | tür etiketi | indeks. Kuşak 1'den başladığı için
/// geçerli bir tanıtıcı asla 0 olmaz — 0 güvenle "geçersiz" anlamına gelir.
#[inline]
fn encode(index: usize, generation: u32, tag: u64) -> i64 {
    (((generation as u64) << 32) | tag | index as u64) as i64
}

/// Tanıtıcıyı çöz. Tür etiketi beklenenden farklıysa `None` — başka türün
/// tanıtıcısı buraya sızmaz.
#[inline]
fn decode(h: i64, tag: u64) -> Option<(usize, u32)> {
    let v = h as u64;
    let low = v & 0xFFFF_FFFF;
    if low & TAG_MASK != tag {
        return None;
    }
    Some(((low & !TAG_MASK) as usize, (v >> 32) as u32))
}

static LAST_ERROR: Mutex<String> = Mutex::new(String::new());

fn set_error(msg: impl Into<String>) {
    if let Ok(mut g) = LAST_ERROR.lock() {
        *g = msg.into();
    }
}

/// Yeni bir oturumu tabloya yerleştir; tanıtıcı döner. Yer yoksa 0.
fn alloc_in<T>(table: &'static [Slot<T>], value: T, tag: u64) -> i64 {
    let raw = Box::into_raw(Box::new(value));
    for (i, slot) in table.iter().enumerate() {
        if slot
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // Kuşağı işaretçiden ÖNCE artır: böylece bu slota ait eski bir
            // tanıtıcı, yeni işaretçi görünür olmadan geçersizleşir.
            let generation = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
            slot.ptr.store(raw, Ordering::Release);
            return encode(i, generation, tag);
        }
    }
    // Yer yok — sızdırmamak için kutuyu geri al.
    drop(unsafe { Box::from_raw(raw) });
    set_error(format!("açık oturum sayısı sınırına ulaşıldı ({MAX_SESSIONS})"));
    0
}

/// Tanıtıcıyı çöz. Geçersizse `None` — hiçbir durumda çağırandan gelen sayı
/// adres olarak kullanılmaz.
fn resolve_in<T>(table: &'static [Slot<T>], h: i64, tag: u64) -> Option<&'static T> {
    if h == 0 {
        return None;
    }
    let (index, generation) = decode(h, tag)?;
    let slot = table.get(index)?;
    // Önce kuşak: eşleşmiyorsa tanıtıcı bayat, işaretçiye hiç bakmayız.
    if slot.generation.load(Ordering::Acquire) != generation {
        return None;
    }
    let p = slot.ptr.load(Ordering::Acquire);
    if p.is_null() {
        return None;
    }
    // Güvenlik: `p` bizim sakladığımız, `alloc_in`'de kutulanmış geçerli bir
    // işaretçi. Kuşak denetimi onun hâlâ bu tanıtıcıya ait olduğunu gösteriyor.
    Some(unsafe { &*p })
}

/// Slotu boşalt ve oturumu serbest bırak. Tanıtıcı geçersizse `false`.
///
/// # Eşzamanlılık
/// Aynı tanıtıcı için kapatma ile diğer çağrılar EŞZAMANLI OLMAMALIDIR.
/// MQL5 modelinde bu doğal olarak sağlanır: `OnDeinit` ile `OnTick`/`OnTimer`
/// aynı EA thread'inde sıralı çalışır; service tarafında da tek bir döngü var.
fn free_in<T>(table: &'static [Slot<T>], h: i64, tag: u64) -> bool {
    if h == 0 {
        return false;
    }
    let Some((index, generation)) = decode(h, tag) else {
        return false;
    };
    let Some(slot) = table.get(index) else {
        return false;
    };
    if slot.generation.load(Ordering::Acquire) != generation {
        return false;
    }
    // Kuşağı artırmak tanıtıcıyı ANINDA geçersizleştirir — çift kapatma ve
    // kapatma sonrası kullanım burada yakalanır.
    slot.generation.fetch_add(1, Ordering::AcqRel);
    let p = slot.ptr.swap(core::ptr::null_mut(), Ordering::AcqRel);
    if !p.is_null() {
        // Güvenlik: `alloc_in`'de kutulanan işaretçi, yalnızca burada ve bir
        // kez geri alınıyor (swap onu tablodan çıkardı).
        drop(unsafe { Box::from_raw(p) });
    }
    slot.claimed.store(false, Ordering::Release);
    true
}

#[inline]
fn alloc_slot(session: Session) -> i64 {
    alloc_in(&SLOTS, Handle { session }, TAG_SESSION)
}

#[inline]
fn resolve(h: i64) -> Option<&'static Handle> {
    resolve_in(&SLOTS, h, TAG_SESSION)
}

#[inline]
fn free_slot(h: i64) -> bool {
    free_in(&SLOTS, h, TAG_SESSION)
}

#[inline]
fn alloc_hist_slot(session: HistSession) -> i64 {
    alloc_in(&HIST_SLOTS, HistHandle { session }, TAG_HIST)
}

#[inline]
fn resolve_hist(h: i64) -> Option<&'static HistHandle> {
    resolve_in(&HIST_SLOTS, h, TAG_HIST)
}

#[inline]
fn free_hist_slot(h: i64) -> bool {
    free_in(&HIST_SLOTS, h, TAG_HIST)
}

/// MQL5 `string` (UTF-16, NUL sonlandırılmış) → Rust `String`.
///
/// # Güvenlik
/// `p` NUL sonlandırılmış geçerli bir UTF-16 dizisine işaret etmelidir.
unsafe fn wide_to_string(p: *const u16) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let mut len = 0usize;
    // Kaçak bir işaretçide sonsuza kadar taramamak için üst sınır.
    const MAX: usize = 1024;
    while len < MAX {
        if unsafe { *p.add(len) } == 0 {
            break;
        }
        len += 1;
    }
    let slice = unsafe { core::slice::from_raw_parts(p, len) };
    String::from_utf16(slice).ok()
}

/// Panik yakalayıcı sarmalayıcı: panik olursa süreci abort etmek yerine
/// [`SINYAL_ERR_PANIC`] döner.
fn guard<F: FnOnce() -> i32>(f: F) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            set_error("köprüde panik yakalandı (terminal korundu)");
            SINYAL_ERR_PANIC
        }
    }
}

// ---------------------------------------------------------------------------
// Yaşam döngüsü
// ---------------------------------------------------------------------------

/// Köprü oturumunu aç. Başarıda tanıtıcı, hatada 0 döner.
///
/// Kapasiteler 0 verilirse varsayılan kullanılır. Segmentleri EA oluşturur.
///
/// # Güvenlik
/// `instance` NUL sonlandırılmış geçerli bir UTF-16 dizisi olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalOpen(
    instance: *const u16,
    tick_cap: u32,
    book_cap: u32,
    cmd_cap: u32,
    res_cap: u32,
    sym_cap: u32,
) -> i64 {
    let r = catch_unwind(AssertUnwindSafe(|| {
        let Some(name) = (unsafe { wide_to_string(instance) }) else {
            set_error("instance adı geçersiz veya NULL");
            return 0i64;
        };
        if name.trim().is_empty() {
            set_error("instance adı boş olamaz");
            return 0i64;
        }

        let d = Capacities::default();
        let caps = Capacities {
            ticks: if tick_cap == 0 { d.ticks } else { tick_cap as u64 },
            books: if book_cap == 0 { d.books } else { book_cap as u64 },
            cmds: if cmd_cap == 0 { d.cmds } else { cmd_cap as u64 },
            results: if res_cap == 0 { d.results } else { res_cap as u64 },
            symbols: if sym_cap == 0 { d.symbols } else { sym_cap },
            positions: d.positions,
            orders: d.orders,
        };

        match Session::create(&name, caps) {
            Ok(session) => alloc_slot(session),
            Err(e) => {
                set_error(format!("oturum açılamadı ({name}): {e}"));
                0
            }
        }
    }));
    match r {
        Ok(v) => v,
        Err(_) => {
            set_error("SinyalOpen sırasında panik yakalandı");
            0
        }
    }
}

/// Oturumu kapat ve segment görünümlerini serbest bırak.
///
/// Çift kapatma güvenlidir: kuşak sayacı ilk kapatmada arttığı için ikinci
/// çağrı [`SINYAL_ERR_HANDLE`] alır, serbest bırakılmış belleğe dokunmaz.
///
/// # Eşzamanlılık
/// Aynı tanıtıcı için diğer köprü çağrılarıyla eşzamanlı çağrılmamalıdır;
/// MQL5'te `OnDeinit` aynı EA thread'inde sıralı çalıştığı için bu doğaldır.
#[no_mangle]
pub extern "C" fn SinyalClose(h: i64) -> i32 {
    guard(|| {
        if h == 0 {
            return SINYAL_OK;
        }
        if free_slot(h) {
            SINYAL_OK
        } else {
            set_error("SinyalClose: tanıtıcı geçersiz veya zaten kapatılmış");
            SINYAL_ERR_HANDLE
        }
    })
}

/// Yapı boyutlarını sorgula — EA açılışta MQL5 `sizeof()` ile karşılaştırır.
///
/// Bu, Rust ile MQL5 arasındaki yerleşim uyumunu çalışma zamanında doğrulayan
/// TEK mekanizmadır; başlık üretilmiş olsa bile iki taraf farklı sürümle
/// derlenmiş olabilir.
#[no_mangle]
pub extern "C" fn SinyalSizeof(what: i32) -> i32 {
    use core::mem::size_of;
    match what {
        sizeof_what::TICK => size_of::<Tick>() as i32,
        sizeof_what::BOOK_LEVEL => size_of::<BookLevel>() as i32,
        sizeof_what::BOOK => size_of::<Book>() as i32,
        sizeof_what::CMD => size_of::<Cmd>() as i32,
        sizeof_what::RES => size_of::<Res>() as i32,
        sizeof_what::SYMBOL_ENTRY => size_of::<SymbolEntry>() as i32,
        sizeof_what::ACCOUNT => size_of::<sinyal_proto::AccountSnapshot>() as i32,
        sizeof_what::POSITION => size_of::<sinyal_proto::PositionRec>() as i32,
        sizeof_what::ORDER => size_of::<sinyal_proto::OrderRec>() as i32,
        sizeof_what::HIST_REQ => size_of::<HistReq>() as i32,
        sizeof_what::BAR_REC => size_of::<BarRec>() as i32,
        sizeof_what::PROTO_VERSION => sinyal_proto::ring::RING_VERSION as i32,
        _ => SINYAL_ERR_ARGS,
    }
}

/// Yüksek çözünürlüklü sayaç. EA tick'i yakaladığı anda bunu damgalar.
#[no_mangle]
pub extern "C" fn SinyalQpc() -> u64 {
    sinyal_shm::qpc()
}

/// Sayaç frekansı (tick/saniye) — EA tarafında süre hesabı için.
#[no_mangle]
pub extern "C" fn SinyalQpcFrequency() -> u64 {
    sinyal_shm::qpc_frequency()
}

/// Son hata metnini UTF-16 olarak `buf`'a yaz; yazılan karakter sayısını döner.
///
/// # Güvenlik
/// `buf` en az `cap` adet `u16` için geçerli yazılabilir bellek olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalLastError(buf: *mut u16, cap: i32) -> i32 {
    guard(|| {
        if buf.is_null() || cap <= 1 {
            return SINYAL_ERR_ARGS;
        }
        let msg = LAST_ERROR.lock().map(|g| g.clone()).unwrap_or_default();
        let wide: Vec<u16> = msg.encode_utf16().collect();
        // NUL için bir yer ayır.
        let n = wide.len().min(cap as usize - 1);
        unsafe {
            core::ptr::copy_nonoverlapping(wide.as_ptr(), buf, n);
            *buf.add(n) = 0;
        }
        n as i32
    })
}

// ---------------------------------------------------------------------------
// Piyasa verisi (EA → çekirdek)
// ---------------------------------------------------------------------------

/// Tick yayınla.
///
/// `recv_qpc` burada, MQL5'e dönüp gelmeden ölçülür — böylece damga yakalama
/// anına mümkün olduğunca yakın olur.
///
/// Dönüş: [`SINYAL_OK`], veya halka doluysa [`SINYAL_WOULD_BLOCK`] (tick
/// KAYBEDİLDİ — EA bunu saymalı ve loglamalıdır).
///
/// # Güvenlik
/// `h` geçerli bir tanıtıcı olmalı; yalnızca tek bir EA thread'i çağırmalıdır.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn SinyalPushTick(
    h: i64,
    symbol_id: u32,
    time_msc: i64,
    bid: f64,
    ask: f64,
    last: f64,
    volume_real: f64,
    flags: u16,
    kind: u8,
) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalPushTick: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        let t = Tick {
            time_msc,
            bid,
            ask,
            last,
            volume_real,
            recv_qpc: sinyal_shm::qpc(),
            symbol_id,
            flags,
            kind,
            _pad: 0,
        };
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda (tek EA thread'i).
        if unsafe { handle.session.push_tick(&t) } {
            SINYAL_OK
        } else {
            SINYAL_WOULD_BLOCK
        }
    })
}

/// Derinlik anlık görüntüsü yayınla.
///
/// MQL5 `MqlBookInfo` dizisi yerine üç düz dizi alıyoruz: MQL5 yapı
/// yerleşiminin C ile eşleştiğini varsaymak protokoldeki en kırılgan nokta
/// olurdu; birkaç yüz baytlık kopya bunu tamamen ortadan kaldırıyor.
///
/// # Güvenlik
/// Üç dizi de en az `depth` eleman içermelidir; `h` geçerli olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalPushBook(
    h: i64,
    symbol_id: u32,
    time_msc: i64,
    prices: *const f64,
    volumes: *const f64,
    kinds: *const u8,
    depth: i32,
) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalPushBook: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        if prices.is_null() || volumes.is_null() || kinds.is_null() || depth < 0 {
            set_error("SinyalPushBook: dizi NULL veya derinlik negatif");
            return SINYAL_ERR_ARGS;
        }
        let qpc = sinyal_shm::qpc();
        // Kesme burada YAPILIR: MQL5 tarafından gelen derinlik
        // MAX_BOOK_DEPTH'ten büyük olsa bile yalnızca o kadarını okuyoruz.
        let n = (depth as usize).min(MAX_BOOK_DEPTH);
        let (p, v, k) = unsafe {
            (
                core::slice::from_raw_parts(prices, n),
                core::slice::from_raw_parts(volumes, n),
                core::slice::from_raw_parts(kinds, n),
            )
        };
        let mut b = Book {
            time_msc,
            recv_qpc: qpc,
            symbol_id,
            depth: n as u16,
            kind: sinyal_proto::kind::BOOK,
            truncated: u8::from(depth as usize > MAX_BOOK_DEPTH),
            ..Default::default()
        };
        for i in 0..n {
            b.levels[i] = BookLevel { price: p[i], volume_real: v[i], kind: k[i], _pad: [0; 7] };
        }
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        if unsafe { handle.session.push_book(&b) } {
            SINYAL_OK
        } else {
            SINYAL_WOULD_BLOCK
        }
    })
}

/// Sembol tablosunu topluca değiştir.
///
/// # Güvenlik
/// `entries` en az `count` adet [`SymbolEntry`] içermelidir. MQL5 tarafındaki
/// yapı yerleşimi Rust ile aynı olmalıdır — EA açılışta `SinyalSizeof` ile
/// bunu doğrulamalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalSetSymbols(h: i64, entries: *const SymbolEntry, count: i32) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalSetSymbols: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        if count < 0 || (count > 0 && entries.is_null()) {
            set_error("SinyalSetSymbols: dizi NULL veya sayı negatif");
            return SINYAL_ERR_ARGS;
        }
        let slice = if count == 0 {
            &[][..]
        } else {
            unsafe { core::slice::from_raw_parts(entries, count as usize) }
        };
        // Güvenlik: tek yazar sözleşmesi çağıranın sorumluluğunda.
        match unsafe { handle.session.set_symbols(slice) } {
            Ok(()) => SINYAL_OK,
            Err(e) => {
                set_error(format!("sembol tablosu yazılamadı: {e}"));
                SINYAL_ERR_OTHER
            }
        }
    })
}

/// Hesap + açık pozisyonlar + bekleyen emirleri topluca yayımla.
///
/// # Boş dizi geçirme
///
/// MQL5'te sıfır uzunluklu bir dizinin tampon adresi tanımsızdır. Bu yüzden EA
/// dizileri **daima en az 1 elemanlı** ayırmalı ve gerçek sayıyı `npos`/`nord`
/// ile ayrı geçirmelidir. Burada işaretçiler NULL olamaz ama sayı 0 olabilir.
///
/// `pos_total`/`ord_total` terminaldeki GERÇEK sayılardır; listeden büyükse
/// kesilme olmuştur ve çekirdek bunu istemciye bildirir.
///
/// # Güvenlik
/// Diziler en az `npos`/`nord` eleman içermelidir; `h` geçerli olmalıdır.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn SinyalSetState(
    h: i64,
    account: *const sinyal_proto::AccountSnapshot,
    positions: *const sinyal_proto::PositionRec,
    npos: i32,
    pos_total: i32,
    orders: *const sinyal_proto::OrderRec,
    nord: i32,
    ord_total: i32,
    built_at_msc: i64,
    unstable: u8,
) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalSetState: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        if account.is_null() || npos < 0 || nord < 0 || pos_total < 0 || ord_total < 0 {
            set_error("SinyalSetState: NULL hesap veya negatif sayı");
            return SINYAL_ERR_ARGS;
        }
        if (npos > 0 && positions.is_null()) || (nord > 0 && orders.is_null()) {
            set_error("SinyalSetState: sayı > 0 iken dizi NULL");
            return SINYAL_ERR_ARGS;
        }
        let acc = unsafe { core::ptr::read(account) };
        let ps = if npos == 0 {
            &[][..]
        } else {
            unsafe { core::slice::from_raw_parts(positions, npos as usize) }
        };
        let os = if nord == 0 {
            &[][..]
        } else {
            unsafe { core::slice::from_raw_parts(orders, nord as usize) }
        };
        // Güvenlik: tek yazar sözleşmesi çağıranın sorumluluğunda.
        match unsafe {
            handle.session.set_state(
                &acc,
                ps,
                os,
                pos_total as u32,
                ord_total as u32,
                built_at_msc,
                unstable != 0,
            )
        } {
            Ok(()) => SINYAL_OK,
            Err(e) => {
                set_error(format!("durum yayımlanamadı: {e}"));
                SINYAL_ERR_OTHER
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Emirler
// ---------------------------------------------------------------------------

/// Bekleyen emir komutunu al.
///
/// Dönüş: komut varsa [`SINYAL_OK`], yoksa [`SINYAL_WOULD_BLOCK`].
///
/// # Güvenlik
/// `out` geçerli yazılabilir bir [`Cmd`] olmalıdır; yalnızca tek bir EA
/// thread'i çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalPopCmd(h: i64, out: *mut Cmd) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalPopCmd: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        if out.is_null() {
            set_error("SinyalPopCmd: çıkış işaretçisi NULL");
            return SINYAL_ERR_ARGS;
        }
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        match unsafe { handle.session.pop_cmd() } {
            Some(c) => {
                unsafe { core::ptr::write(out, c) };
                SINYAL_OK
            }
            None => SINYAL_WOULD_BLOCK,
        }
    })
}

/// Emir sonucu / ticaret olayı yayınla.
///
/// # Güvenlik
/// `res` geçerli bir [`Res`] olmalıdır; yalnızca tek bir EA thread'i
/// çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalPushRes(h: i64, res: *const Res) -> i32 {
    guard(|| {
        let Some(handle) = resolve(h) else {
            set_error("SinyalPushRes: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE;
        };
        if res.is_null() {
            set_error("SinyalPushRes: işaretçi NULL");
            return SINYAL_ERR_ARGS;
        }
        let r = unsafe { core::ptr::read(res) };
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        if unsafe { handle.session.push_res(&r) } {
            SINYAL_OK
        } else {
            SINYAL_WOULD_BLOCK
        }
    })
}

// ---------------------------------------------------------------------------
// Geçmiş kanalı (service ↔ çekirdek)
// ---------------------------------------------------------------------------
//
// Bu blok AYRI bir oturum tipidir ve mevcut `SinyalOpen` imzasına DOKUNMAZ:
// MQL5 `#import` bildirimlerinde imza doğrulaması yapılmadığı için var olan bir
// fonksiyonun aritesini değiştirmek, eski EA ile yeni DLL buluştuğunda sessiz
// yığın bozulmasına yol açardı.
//
// Dönüş sözleşmesi buradaki altı fonksiyon için `long`'dur (MQL5 `long` = 64
// bit): `> 0` başarı, `0` "şu an yapılamadı", `< 0` hata. Tek istisna
// [`SinyalHistClose`]: sözleşme gereği başarıda **0** döner.

/// Service tarafı: geçmiş segmentlerini oluştur ve oturumu aç.
///
/// Başarıda `> 0` tanıtıcı, hatada `< 0` döner (ayrıntı `SinyalLastError`'da).
/// Kapasiteler varsayılandır — geçmiş akışının boyutu EA'nın yapılandırmasına
/// bağlı değil.
///
/// Bu oturum EA'nın tek-yazar kilidine DOKUNMAZ; kendi kilidi
/// `{instance}.hist` adını kullanır, böylece EA ile service aynı örnek adıyla
/// yan yana çalışabilir.
///
/// # Güvenlik
/// `instance` NUL sonlandırılmış geçerli bir UTF-16 dizisi olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistOpen(instance: *const u16) -> i64 {
    // Güvenlik: `instance` üzerindeki sözleşme çağırandan bu fonksiyona,
    // oradan da değişmeden `hist_open`'a aktarılıyor.
    unsafe { hist_open(instance, false) }
}

/// Çekirdek tarafı: var olan geçmiş segmentlerine bağlan.
///
/// Service henüz başlamadıysa hata döner — çağıran yeniden denemelidir, bu
/// beklenen bir durumdur.
///
/// # Güvenlik
/// `instance` NUL sonlandırılmış geçerli bir UTF-16 dizisi olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistAttach(instance: *const u16) -> i64 {
    // Güvenlik: yukarıdaki `SinyalHistOpen` ile aynı sözleşme.
    unsafe { hist_open(instance, true) }
}

/// `SinyalHistOpen`/`SinyalHistAttach` ortak gövdesi.
///
/// # Güvenlik
/// `instance` NUL sonlandırılmış geçerli bir UTF-16 dizisi olmalıdır.
unsafe fn hist_open(instance: *const u16, attach: bool) -> i64 {
    let r = catch_unwind(AssertUnwindSafe(|| {
        let Some(name) = (unsafe { wide_to_string(instance) }) else {
            set_error("instance adı geçersiz veya NULL");
            return SINYAL_ERR_ARGS as i64;
        };
        if name.trim().is_empty() {
            set_error("instance adı boş olamaz");
            return SINYAL_ERR_ARGS as i64;
        }
        let opened =
            if attach { HistSession::attach(&name) } else { HistSession::create(&name) };
        match opened {
            Ok(session) => {
                let h = alloc_hist_slot(session);
                // `alloc_hist_slot` yer yoksa 0 döner; sözleşme hata için
                // NEGATİF değer istiyor.
                if h == 0 {
                    SINYAL_ERR_OTHER as i64
                } else {
                    h
                }
            }
            Err(e) => {
                let what = if attach { "bağlanılamadı" } else { "açılamadı" };
                set_error(format!("geçmiş oturumu {what} ({name}): {e}"));
                SINYAL_ERR_OTHER as i64
            }
        }
    }));
    match r {
        Ok(v) => v,
        Err(_) => {
            set_error("SinyalHistOpen sırasında panik yakalandı");
            SINYAL_ERR_PANIC as i64
        }
    }
}

/// Geçmiş oturumunu kapat. Başarıda **0**, geçersiz tanıtıcıda negatif döner.
///
/// Dönüş değeri diğer export'lardan farklı ([`SINYAL_OK`] değil 0): FFI
/// sözleşmesi böyle sabitlendi, MQL5 tarafı `!= 0` ile hata arıyor.
///
/// Çift kapatma güvenlidir: kuşak sayacı ilk kapatmada arttığı için ikinci
/// çağrı hata alır ve serbest bırakılmış belleğe dokunmaz.
#[no_mangle]
pub extern "C" fn SinyalHistClose(h: i64) -> i64 {
    let r = catch_unwind(AssertUnwindSafe(|| {
        if h == 0 {
            return 0i64;
        }
        if free_hist_slot(h) {
            0
        } else {
            set_error("SinyalHistClose: tanıtıcı geçersiz veya zaten kapatılmış");
            SINYAL_ERR_HANDLE as i64
        }
    }));
    match r {
        Ok(v) => v,
        Err(_) => {
            set_error("SinyalHistClose sırasında panik yakalandı");
            SINYAL_ERR_PANIC as i64
        }
    }
}

/// Service tarafı: bekleyen geçmiş isteğini al.
///
/// Dönüş: `1` istek var (`out` dolduruldu), `0` istek yok, `< 0` hata.
/// İstek yokluğu HATA DEĞİLDİR — service bunu görünce kısa bir `Sleep` ile
/// döngüsüne devam eder.
///
/// # Güvenlik
/// `out` geçerli yazılabilir bir [`HistReq`] olmalıdır; yalnızca tek bir
/// service thread'i çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistPopReq(h: i64, out: *mut HistReq) -> i64 {
    hist_guard(|| {
        let Some(handle) = resolve_hist(h) else {
            set_error("SinyalHistPopReq: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE as i64;
        };
        if out.is_null() {
            set_error("SinyalHistPopReq: çıkış işaretçisi NULL");
            return SINYAL_ERR_ARGS as i64;
        }
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda (tek service
        // thread'i).
        match unsafe { handle.session.pop_req() } {
            Some(r) => {
                unsafe { core::ptr::write(out, r) };
                SINYAL_OK as i64
            }
            None => SINYAL_WOULD_BLOCK as i64,
        }
    })
}

/// Service tarafı: bar yayınla.
///
/// Dönüş: `1` yazıldı, `0` halka DOLU, `< 0` hata.
///
/// **`0` dönüşü hata değildir ve YOK SAYILAMAZ.** Bar yazılmamıştır ve
/// kaybolan bir geçmiş barı bir daha asla gelmez — seride kalıcı delik açar.
/// Service `Sleep(1)` gibi kısa bir uykudan sonra AYNI barla yeniden
/// denemelidir (`OnTimer` yasak, ama `Sleep` service içinde serbesttir).
///
/// # Güvenlik
/// `bar` geçerli bir [`BarRec`] olmalıdır; yalnızca tek bir service thread'i
/// çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistPushBar(h: i64, bar: *const BarRec) -> i64 {
    hist_guard(|| {
        let Some(handle) = resolve_hist(h) else {
            set_error("SinyalHistPushBar: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE as i64;
        };
        if bar.is_null() {
            set_error("SinyalHistPushBar: işaretçi NULL");
            return SINYAL_ERR_ARGS as i64;
        }
        let b = unsafe { core::ptr::read(bar) };
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        if unsafe { handle.session.push_bar(&b) } {
            SINYAL_OK as i64
        } else {
            SINYAL_WOULD_BLOCK as i64
        }
    })
}

/// Çekirdek tarafı: geçmiş isteği gönder.
///
/// Dönüş: `1` yazıldı, `0` halka dolu (yeniden dene), `< 0` hata.
///
/// # Güvenlik
/// `req` geçerli bir [`HistReq`] olmalıdır; yalnızca tek bir çekirdek thread'i
/// çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistPushReq(h: i64, req: *const HistReq) -> i64 {
    hist_guard(|| {
        let Some(handle) = resolve_hist(h) else {
            set_error("SinyalHistPushReq: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE as i64;
        };
        if req.is_null() {
            set_error("SinyalHistPushReq: işaretçi NULL");
            return SINYAL_ERR_ARGS as i64;
        }
        let r = unsafe { core::ptr::read(req) };
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        if unsafe { handle.session.push_req(&r) } {
            SINYAL_OK as i64
        } else {
            SINYAL_WOULD_BLOCK as i64
        }
    })
}

/// Çekirdek tarafı: bar oku.
///
/// Dönüş: `1` bar var (`out` dolduruldu), `0` bar yok, `< 0` hata.
///
/// # Güvenlik
/// `out` geçerli yazılabilir bir [`BarRec`] olmalıdır; yalnızca tek bir
/// çekirdek thread'i çağırmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalHistPopBar(h: i64, out: *mut BarRec) -> i64 {
    hist_guard(|| {
        let Some(handle) = resolve_hist(h) else {
            set_error("SinyalHistPopBar: tanıtıcı geçersiz");
            return SINYAL_ERR_HANDLE as i64;
        };
        if out.is_null() {
            set_error("SinyalHistPopBar: çıkış işaretçisi NULL");
            return SINYAL_ERR_ARGS as i64;
        }
        // Güvenlik: SPSC sözleşmesi çağıranın sorumluluğunda.
        match unsafe { handle.session.pop_bar() } {
            Some(b) => {
                unsafe { core::ptr::write(out, b) };
                SINYAL_OK as i64
            }
            None => SINYAL_WOULD_BLOCK as i64,
        }
    })
}

/// [`guard`]'ın 64 bit dönen sürümü — geçmiş export'ları `long` döndürüyor.
fn hist_guard<F: FnOnce() -> i64>(f: F) -> i64 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            set_error("köprüde panik yakalandı (terminal korundu)");
            SINYAL_ERR_PANIC as i64
        }
    }
}

// ---------------------------------------------------------------------------
// Teşhis
// ---------------------------------------------------------------------------

/// Halka dolu olduğu için KAYBEDİLEN tick sayısı. 0 olmalı; değilse alarm.
///
/// # Güvenlik
/// `h` geçerli bir tanıtıcı olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalTickPushFailures(h: i64) -> u64 {
    let r = catch_unwind(AssertUnwindSafe(|| match resolve(h) {
        Some(handle) => handle.session.tick_push_failures(),
        None => u64::MAX,
    }));
    r.unwrap_or(u64::MAX)
}

/// Okunmayı bekleyen tick sayısı — çekirdeğin geride kalıp kalmadığı.
///
/// # Güvenlik
/// `h` geçerli bir tanıtıcı olmalıdır.
#[no_mangle]
pub unsafe extern "C" fn SinyalTickBacklog(h: i64) -> u64 {
    let r = catch_unwind(AssertUnwindSafe(|| match resolve(h) {
        Some(handle) => handle.session.tick_backlog(),
        None => u64::MAX,
    }));
    r.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    fn unique(tag: &str) -> String {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        format!("ffi{}-{}-{}", tag, std::process::id(), N.fetch_add(1, Ordering::Relaxed))
    }

    /// Küçük kapasiteli bir oturum aç (testler 64 MB ayırmasın).
    fn open_small(name: &str) -> i64 {
        let w = wide(name);
        unsafe { SinyalOpen(w.as_ptr(), 64, 8, 8, 8, 16) }
    }

    #[test]
    fn open_push_close_roundtrip() {
        let name = unique("basic");
        let h = open_small(&name);
        assert_ne!(h, 0, "oturum açılmalı");

        let rc = unsafe {
            SinyalPushTick(h, 3, 1_700_000_000_000, 1.1000, 1.1002, 0.0, 0.0, 0x06, 1)
        };
        assert_eq!(rc, SINYAL_OK);

        // Çekirdek tarafı aynı segmentten okumalı.
        let core = Session::open(&name).unwrap();
        let t = unsafe { core.pop_tick() }.expect("tick gelmeli");
        assert_eq!(t.symbol_id, 3);
        assert!((t.bid - 1.1000).abs() < 1e-12);
        assert_ne!(t.recv_qpc, 0, "köprü QPC damgası basmalı");

        assert_eq!(SinyalClose(h), SINYAL_OK);
    }

    #[test]
    fn sizeof_matches_rust_layout() {
        // EA'nın açılışta yapacağı denetimin ta kendisi.
        assert_eq!(SinyalSizeof(sizeof_what::TICK), 56);
        assert_eq!(SinyalSizeof(sizeof_what::BOOK_LEVEL), 24);
        assert_eq!(SinyalSizeof(sizeof_what::BOOK), 824);
        assert_eq!(SinyalSizeof(sizeof_what::CMD), 184);
        assert_eq!(SinyalSizeof(sizeof_what::RES), 184);
        assert_eq!(SinyalSizeof(sizeof_what::SYMBOL_ENTRY), 192);
        assert_eq!(SinyalSizeof(sizeof_what::ACCOUNT), 512);
        assert_eq!(SinyalSizeof(sizeof_what::POSITION), 192);
        assert_eq!(SinyalSizeof(sizeof_what::ORDER), 192);
        assert_eq!(SinyalSizeof(sizeof_what::PROTO_VERSION), 3);
        assert_eq!(SinyalSizeof(9999), SINYAL_ERR_ARGS, "bilinmeyen kod hata dönmeli");
    }

    #[test]
    fn bogus_handle_is_rejected_not_dereferenced() {
        // MQL5 tarafındaki bir hata rastgele sayı geçirebilir; çökmemeliyiz.
        for h in [0i64, 1, -1, 0x1234_5678] {
            let rc = unsafe { SinyalPushTick(h, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) };
            assert_eq!(rc, SINYAL_ERR_HANDLE, "tanıtıcı {h} reddedilmeli");
        }
    }

    #[test]
    fn handle_is_invalid_after_close_and_slot_reuse_does_not_resurrect_it() {
        // EA yeniden başladığında eski tanıtıcı elde kalabilir. Slot yeniden
        // kullanıldığında o eski tanıtıcının YENİ oturuma erişmesi felaket
        // olurdu (yanlış broker'a emir göndermek). Kuşak sayacı bunu keser.
        let name_a = unique("stale-a");
        let h_old = open_small(&name_a);
        assert_ne!(h_old, 0);
        assert_eq!(SinyalClose(h_old), SINYAL_OK);

        // Kapatıldıktan sonra kullanım reddedilmeli.
        assert_eq!(
            unsafe { SinyalPushTick(h_old, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_ERR_HANDLE,
            "kapatılmış tanıtıcı kullanılamamalı"
        );

        // Aynı slot yeni bir oturuma verilse bile eski tanıtıcı geçersiz kalmalı.
        let name_b = unique("stale-b");
        let h_new = open_small(&name_b);
        assert_ne!(h_new, 0);
        assert_ne!(h_new, h_old, "kuşak arttığı için tanıtıcı farklı olmalı");
        assert_eq!(
            unsafe { SinyalPushTick(h_old, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_ERR_HANDLE,
            "eski tanıtıcı yeni oturuma erişmemeli"
        );
        assert_eq!(
            unsafe { SinyalPushTick(h_new, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_OK
        );
        assert_eq!(SinyalClose(h_new), SINYAL_OK);
    }

    #[test]
    fn many_concurrent_sessions_get_distinct_handles() {
        // Her chart'taki EA örneği kendi slotunu almalı; tanıtıcılar
        // çakışmamalı.
        let mut handles = Vec::new();
        for i in 0..8 {
            let h = open_small(&unique(&format!("multi{i}")));
            assert_ne!(h, 0, "{i}. oturum açılmalı");
            handles.push(h);
        }
        let uniq: std::collections::HashSet<i64> = handles.iter().copied().collect();
        assert_eq!(uniq.len(), handles.len(), "tanıtıcılar benzersiz olmalı");
        for h in handles {
            assert_eq!(SinyalClose(h), SINYAL_OK);
        }
    }

    #[test]
    fn double_close_is_safe() {
        let name = unique("dblclose");
        let h = open_small(&name);
        assert_eq!(SinyalClose(h), SINYAL_OK);
        // İkinci kapatma imzası silindiği için geçersiz olarak yakalanmalı —
        // serbest bırakılmış belleği ikinci kez serbest bırakmamalı.
        assert_eq!(SinyalClose(h), SINYAL_ERR_HANDLE);
    }

    #[test]
    fn open_rejects_empty_and_null_instance() {
        let w = wide("   ");
        assert_eq!(unsafe { SinyalOpen(w.as_ptr(), 64, 8, 8, 8, 16) }, 0);
        assert_eq!(unsafe { SinyalOpen(core::ptr::null(), 64, 8, 8, 8, 16) }, 0);

        let mut buf = [0u16; 256];
        let n = unsafe { SinyalLastError(buf.as_mut_ptr(), buf.len() as i32) };
        assert!(n > 0, "hata metni yazılmalı");
        let msg = String::from_utf16_lossy(&buf[..n as usize]);
        assert!(msg.contains("instance"), "mesaj sorunu açıklamalı: {msg}");
    }

    #[test]
    fn push_book_truncates_and_marks() {
        let name = unique("book");
        let h = open_small(&name);
        let core = Session::open(&name).unwrap();

        let n = MAX_BOOK_DEPTH + 10;
        let prices: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vols: Vec<f64> = vec![1.0; n];
        let kinds: Vec<u8> = vec![1; n];
        let rc = unsafe {
            SinyalPushBook(h, 5, 111, prices.as_ptr(), vols.as_ptr(), kinds.as_ptr(), n as i32)
        };
        assert_eq!(rc, SINYAL_OK);

        let b = unsafe { core.pop_book() }.unwrap();
        assert_eq!(b.depth as usize, MAX_BOOK_DEPTH);
        assert_eq!(b.truncated, 1);
        SinyalClose(h);
    }

    #[test]
    fn push_book_rejects_null_arrays() {
        let name = unique("booknull");
        let h = open_small(&name);
        let v = [1.0f64];
        let k = [1u8];
        assert_eq!(
            unsafe { SinyalPushBook(h, 1, 0, core::ptr::null(), v.as_ptr(), k.as_ptr(), 1) },
            SINYAL_ERR_ARGS
        );
        assert_eq!(
            unsafe { SinyalPushBook(h, 1, 0, v.as_ptr(), v.as_ptr(), k.as_ptr(), -5) },
            SINYAL_ERR_ARGS
        );
        SinyalClose(h);
    }

    #[test]
    fn pop_cmd_reports_empty_then_delivers() {
        let name = unique("cmd");
        let h = open_small(&name);
        let core = Session::open(&name).unwrap();

        let mut out = Cmd::default();
        assert_eq!(
            unsafe { SinyalPopCmd(h, &mut out) },
            SINYAL_WOULD_BLOCK,
            "boş halka hata değil"
        );

        let cmd = Cmd {
            client_id: 7,
            volume: 0.5,
            price: 1.234,
            action: sinyal_proto::action::PENDING,
            order_type: sinyal_proto::order_type::SELL_LIMIT,
            ..Default::default()
        };
        assert!(unsafe { core.push_cmd(&cmd) });

        assert_eq!(unsafe { SinyalPopCmd(h, &mut out) }, SINYAL_OK);
        assert_eq!(out.client_id, 7);
        assert_eq!(out.order_type, sinyal_proto::order_type::SELL_LIMIT);
        SinyalClose(h);
    }

    #[test]
    fn pop_cmd_rejects_null_out() {
        let name = unique("cmdnull");
        let h = open_small(&name);
        assert_eq!(unsafe { SinyalPopCmd(h, core::ptr::null_mut()) }, SINYAL_ERR_ARGS);
        SinyalClose(h);
    }

    #[test]
    fn tick_loss_visible_through_ffi() {
        let name = unique("lossffi");
        let h = open_small(&name); // tick kapasitesi 64
        for _ in 0..64 {
            assert_eq!(
                unsafe { SinyalPushTick(h, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
                SINYAL_OK
            );
        }
        // 65'inci: halka dolu → kayıp.
        assert_eq!(
            unsafe { SinyalPushTick(h, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_WOULD_BLOCK
        );
        assert_eq!(unsafe { SinyalTickPushFailures(h) }, 1);
        assert_eq!(unsafe { SinyalTickBacklog(h) }, 64);
        SinyalClose(h);
    }

    #[test]
    fn diagnostics_on_bad_handle_return_sentinel_not_crash() {
        assert_eq!(unsafe { SinyalTickPushFailures(0) }, u64::MAX);
        assert_eq!(unsafe { SinyalTickBacklog(12345) }, u64::MAX);
    }

    #[test]
    fn last_error_respects_buffer_capacity() {
        set_error("çok uzun bir hata mesajı ".repeat(50));
        let mut buf = [0u16; 16];
        let n = unsafe { SinyalLastError(buf.as_mut_ptr(), buf.len() as i32) };
        assert_eq!(n, 15, "kapasite-1 karakter yazılmalı");
        assert_eq!(buf[15], 0, "NUL sonlandırıcı yazılmalı");

        // Çok küçük tampon reddedilmeli (NUL'a bile yer yok).
        assert_eq!(unsafe { SinyalLastError(buf.as_mut_ptr(), 1) }, SINYAL_ERR_ARGS);
        assert_eq!(unsafe { SinyalLastError(core::ptr::null_mut(), 16) }, SINYAL_ERR_ARGS);
    }

    #[test]
    fn qpc_exports_work() {
        let a = SinyalQpc();
        let b = SinyalQpc();
        assert!(b >= a);
        assert!(SinyalQpcFrequency() > 0);
    }

    // --- Geçmiş kanalı ---

    fn sample_bar(req_id: u32, index: u32, total: u32) -> BarRec {
        BarRec {
            time_msc: 1_700_000_000_000 + index as i64 * 60_000,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            tick_volume: 7,
            req_id,
            symbol_id: 3,
            timeframe: sinyal_proto::timeframe::M1,
            index,
            total,
            ..Default::default()
        }
    }

    #[test]
    fn hist_request_and_bar_cross_the_ffi_boundary() {
        let name = unique("hist");
        let w = wide(&name);
        // Service segmentleri kurar; çekirdek sonradan bağlanır.
        let svc = unsafe { SinyalHistOpen(w.as_ptr()) };
        assert!(svc > 0, "service oturumu açılmalı, dönen: {svc}");
        let core = unsafe { SinyalHistAttach(w.as_ptr()) };
        assert!(core > 0, "çekirdek bağlanmalı, dönen: {core}");

        // Çekirdek → service: istek.
        let req = HistReq {
            req_id: 55,
            symbol_id: 3,
            timeframe: sinyal_proto::timeframe::H1,
            count: 500,
            ..Default::default()
        };
        assert_eq!(unsafe { SinyalHistPushReq(core, &req) }, SINYAL_OK as i64);

        let mut got = HistReq::default();
        assert_eq!(unsafe { SinyalHistPopReq(svc, &mut got) }, SINYAL_OK as i64);
        assert_eq!(got.req_id, 55);
        assert_eq!(got.timeframe, sinyal_proto::timeframe::H1);
        assert_eq!(got.count, 500);
        // İkinci okumada istek yok — bu HATA DEĞİL.
        assert_eq!(
            unsafe { SinyalHistPopReq(svc, &mut got) },
            SINYAL_WOULD_BLOCK as i64,
            "boş istek halkası hata değil"
        );

        // Service → çekirdek: barlar.
        for i in 0..3u32 {
            assert_eq!(
                unsafe { SinyalHistPushBar(svc, &sample_bar(55, i, 3)) },
                SINYAL_OK as i64
            );
        }
        let mut bar = BarRec::default();
        for i in 0..3u32 {
            assert_eq!(unsafe { SinyalHistPopBar(core, &mut bar) }, SINYAL_OK as i64);
            assert_eq!(bar.index, i, "fiziksel sıra eskiden yeniye olmalı");
            assert_eq!(bar.req_id, 55);
            assert_eq!(bar.total, 3);
        }
        assert_eq!(unsafe { SinyalHistPopBar(core, &mut bar) }, SINYAL_WOULD_BLOCK as i64);

        // Kapatma sözleşmesi: başarıda 0.
        assert_eq!(SinyalHistClose(core), 0);
        assert_eq!(SinyalHistClose(svc), 0);
        // Çift kapatma serbest bırakılmış belleğe dokunmamalı.
        assert_eq!(SinyalHistClose(svc), SINYAL_ERR_HANDLE as i64);
    }

    #[test]
    fn hist_push_bar_reports_would_block_when_ring_is_full_not_an_error() {
        // Halka dolduğunda `0` dönmeli — service kısa bir `Sleep` ile yeniden
        // dener. HATA dönseydi service isteği iptal eder ve barlar KALICI
        // kaybolurdu.
        let name = unique("histfull");
        let w = wide(&name);
        let svc = unsafe { SinyalHistOpen(w.as_ptr()) };
        assert!(svc > 0);

        let cap = sinyal_proto::capacity::BARS;
        for i in 0..cap {
            assert_eq!(
                unsafe { SinyalHistPushBar(svc, &sample_bar(1, i as u32, cap as u32)) },
                SINYAL_OK as i64,
                "kapasite dolana kadar kabul edilmeli"
            );
        }
        assert_eq!(
            unsafe { SinyalHistPushBar(svc, &sample_bar(1, 0, 1)) },
            SINYAL_WOULD_BLOCK as i64,
            "dolu halka 0 dönmeli, hata değil"
        );

        // Çekirdek okuyup yer açınca yeniden deneme BAŞARILI olmalı.
        let core = unsafe { SinyalHistAttach(w.as_ptr()) };
        let mut bar = BarRec::default();
        assert_eq!(unsafe { SinyalHistPopBar(core, &mut bar) }, SINYAL_OK as i64);
        assert_eq!(
            unsafe { SinyalHistPushBar(svc, &sample_bar(1, 0, 1)) },
            SINYAL_OK as i64,
            "yer açılınca yeniden deneme yazmalı"
        );

        SinyalHistClose(core);
        SinyalHistClose(svc);
    }

    #[test]
    fn hist_bogus_handle_is_rejected_not_dereferenced() {
        // Daha önce GERÇEK bir çökme sebebiydi: MQL5 tarafından gelen rastgele
        // bir sayı adres olarak kullanılırsa terminal düşer.
        let mut req = HistReq::default();
        let mut bar = BarRec::default();
        for h in [0i64, 1, -1, 0x1234_5678, i64::MIN, i64::MAX, 0x4000_0000] {
            assert_eq!(
                unsafe { SinyalHistPopReq(h, &mut req) },
                SINYAL_ERR_HANDLE as i64,
                "tanıtıcı {h} reddedilmeli"
            );
            assert_eq!(
                unsafe { SinyalHistPushBar(h, &sample_bar(1, 0, 1)) },
                SINYAL_ERR_HANDLE as i64,
                "tanıtıcı {h} reddedilmeli"
            );
            assert_eq!(
                unsafe { SinyalHistPushReq(h, &req) },
                SINYAL_ERR_HANDLE as i64,
                "tanıtıcı {h} reddedilmeli"
            );
            assert_eq!(
                unsafe { SinyalHistPopBar(h, &mut bar) },
                SINYAL_ERR_HANDLE as i64,
                "tanıtıcı {h} reddedilmeli"
            );
        }
        // 0 dışındaki her tanıtıcı için kapatma da reddedilmeli (0 "zaten
        // kapalı" sayılır ve sessizce başarı döner).
        assert_eq!(SinyalHistClose(0x1234_5678), SINYAL_ERR_HANDLE as i64);
    }

    #[test]
    fn hist_and_tick_handles_are_not_interchangeable() {
        // İki tablo aynı indeks alanını kullanıyor; tür etiketi olmasaydı bir
        // tick tanıtıcısı geçerli bir geçmiş tanıtıcısına eşit olabilir ve
        // barlar YANLIŞ oturuma yazılabilirdi.
        let tick_name = unique("mix-tick");
        let hist_name = unique("mix-hist");
        let th = open_small(&tick_name);
        let hw = wide(&hist_name);
        let hh = unsafe { SinyalHistOpen(hw.as_ptr()) };
        assert!(th != 0 && hh > 0);
        assert_ne!(th, hh);

        // Tick tanıtıcısı geçmiş export'unda reddedilmeli.
        assert_eq!(
            unsafe { SinyalHistPushBar(th, &sample_bar(1, 0, 1)) },
            SINYAL_ERR_HANDLE as i64
        );
        assert_eq!(SinyalHistClose(th), SINYAL_ERR_HANDLE as i64);
        // Geçmiş tanıtıcısı tick export'unda reddedilmeli.
        assert_eq!(
            unsafe { SinyalPushTick(hh, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_ERR_HANDLE
        );
        assert_eq!(SinyalClose(hh), SINYAL_ERR_HANDLE);

        // Her ikisi de kendi export'unda hâlâ çalışıyor olmalı.
        assert_eq!(
            unsafe { SinyalHistPushBar(hh, &sample_bar(1, 0, 1)) },
            SINYAL_OK as i64
        );
        assert_eq!(
            unsafe { SinyalPushTick(th, 0, 0, 1.0, 1.0, 0.0, 0.0, 0, 1) },
            SINYAL_OK
        );
        assert_eq!(SinyalHistClose(hh), 0);
        assert_eq!(SinyalClose(th), SINYAL_OK);
    }

    #[test]
    fn hist_rejects_null_pointers_and_bad_instance() {
        let name = unique("histnull");
        let w = wide(&name);
        let h = unsafe { SinyalHistOpen(w.as_ptr()) };
        assert!(h > 0);
        assert_eq!(
            unsafe { SinyalHistPopReq(h, core::ptr::null_mut()) },
            SINYAL_ERR_ARGS as i64
        );
        assert_eq!(
            unsafe { SinyalHistPopBar(h, core::ptr::null_mut()) },
            SINYAL_ERR_ARGS as i64
        );
        assert_eq!(unsafe { SinyalHistPushBar(h, core::ptr::null()) }, SINYAL_ERR_ARGS as i64);
        assert_eq!(unsafe { SinyalHistPushReq(h, core::ptr::null()) }, SINYAL_ERR_ARGS as i64);
        SinyalHistClose(h);

        // Açılış hataları NEGATİF dönmeli (tick tarafındaki 0 sözleşmesi
        // değil) — MQL5 tarafı `<= 0` ile kontrol ediyor.
        assert!(unsafe { SinyalHistOpen(core::ptr::null()) } < 0);
        let empty = wide("   ");
        assert!(unsafe { SinyalHistOpen(empty.as_ptr()) } < 0);
        // Service başlamadan bağlanmak hata, ama yeniden denenebilir bir hata.
        let missing = wide(&unique("histmissing"));
        assert!(unsafe { SinyalHistAttach(missing.as_ptr()) } < 0);
    }

    #[test]
    fn hist_sizeof_matches_rust_layout() {
        // Service açılışta bunları MQL5 `sizeof()` ile karşılaştırır.
        assert_eq!(SinyalSizeof(sizeof_what::HIST_REQ), 64);
        assert_eq!(SinyalSizeof(sizeof_what::BAR_REC), 120);
    }
}
