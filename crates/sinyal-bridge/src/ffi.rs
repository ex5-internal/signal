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

use sinyal_proto::{Book, BookLevel, Cmd, Res, SymbolEntry, Tick, MAX_BOOK_DEPTH};

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
struct Slot {
    /// Slot kullanımda mı (tahsis yarışını çözer).
    claimed: AtomicBool,
    /// Her tahsiste artar; eski tanıtıcıları anında geçersizleştirir.
    generation: AtomicU32,
    ptr: AtomicPtr<Handle>,
}

impl Slot {
    const fn new() -> Self {
        Self {
            claimed: AtomicBool::new(false),
            generation: AtomicU32::new(0),
            ptr: AtomicPtr::new(core::ptr::null_mut()),
        }
    }
}

static SLOTS: [Slot; MAX_SESSIONS] = [const { Slot::new() }; MAX_SESSIONS];

struct Handle {
    session: Session,
}

/// Tanıtıcı = (kuşak << 32) | indeks. Kuşak 1'den başladığı için geçerli bir
/// tanıtıcı asla 0 olmaz — 0 güvenle "geçersiz" anlamına gelir.
#[inline]
fn encode(index: usize, generation: u32) -> i64 {
    (((generation as u64) << 32) | index as u64) as i64
}

#[inline]
fn decode(h: i64) -> (usize, u32) {
    let v = h as u64;
    ((v & 0xFFFF_FFFF) as usize, (v >> 32) as u32)
}

static LAST_ERROR: Mutex<String> = Mutex::new(String::new());

fn set_error(msg: impl Into<String>) {
    if let Ok(mut g) = LAST_ERROR.lock() {
        *g = msg.into();
    }
}

/// Yeni bir oturumu tabloya yerleştir; tanıtıcı döner. Yer yoksa 0.
fn alloc_slot(session: Session) -> i64 {
    let raw = Box::into_raw(Box::new(Handle { session }));
    for (i, slot) in SLOTS.iter().enumerate() {
        if slot
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // Kuşağı işaretçiden ÖNCE artır: böylece bu slota ait eski bir
            // tanıtıcı, yeni işaretçi görünür olmadan geçersizleşir.
            let generation = slot.generation.fetch_add(1, Ordering::AcqRel) + 1;
            slot.ptr.store(raw, Ordering::Release);
            return encode(i, generation);
        }
    }
    // Yer yok — sızdırmamak için kutuyu geri al.
    drop(unsafe { Box::from_raw(raw) });
    set_error(format!("açık oturum sayısı sınırına ulaşıldı ({MAX_SESSIONS})"));
    0
}

/// Tanıtıcıyı çöz. Geçersizse `None` — hiçbir durumda çağırandan gelen sayı
/// adres olarak kullanılmaz.
fn resolve<'a>(h: i64) -> Option<&'a Handle> {
    if h == 0 {
        return None;
    }
    let (index, generation) = decode(h);
    let slot = SLOTS.get(index)?;
    // Önce kuşak: eşleşmiyorsa tanıtıcı bayat, işaretçiye hiç bakmayız.
    if slot.generation.load(Ordering::Acquire) != generation {
        return None;
    }
    let p = slot.ptr.load(Ordering::Acquire);
    if p.is_null() {
        return None;
    }
    // Güvenlik: `p` bizim sakladığımız, `alloc_slot`'ta kutulanmış geçerli bir
    // işaretçi. Kuşak denetimi onun hâlâ bu tanıtıcıya ait olduğunu gösteriyor.
    Some(unsafe { &*p })
}

/// Slotu boşalt ve oturumu serbest bırak. Tanıtıcı geçersizse `false`.
///
/// # Eşzamanlılık
/// Aynı tanıtıcı için `SinyalClose` ile diğer çağrılar EŞZAMANLI OLMAMALIDIR.
/// MQL5 modelinde bu doğal olarak sağlanır: `OnDeinit` ile `OnTick`/`OnTimer`
/// aynı EA thread'inde sıralı çalışır.
fn free_slot(h: i64) -> bool {
    if h == 0 {
        return false;
    }
    let (index, generation) = decode(h);
    let Some(slot) = SLOTS.get(index) else {
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
        // Güvenlik: `alloc_slot`'ta kutulanan işaretçi, yalnızca burada ve bir
        // kez geri alınıyor (swap onu tablodan çıkardı).
        drop(unsafe { Box::from_raw(p) });
    }
    slot.claimed.store(false, Ordering::Release);
    true
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
        assert_eq!(SinyalSizeof(sizeof_what::RES), 120);
        assert_eq!(SinyalSizeof(sizeof_what::SYMBOL_ENTRY), 192);
        assert_eq!(SinyalSizeof(sizeof_what::PROTO_VERSION), 2);
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
}
