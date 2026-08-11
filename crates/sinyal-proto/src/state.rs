//! Hesap durumu, açık pozisyonlar ve bekleyen emirler.
//!
//! Halkalar **olay** taşır; bu segment **durum** taşır. Ayrımı korumak önemli:
//! yeniden bağlanan bir istemci olay geçmişini göremez ama "şu an ne açık"
//! sorusunun cevabını anında almalıdır.
//!
//! Sembol tablosuyla aynı seqlock desenini kullanır: EA yazmaya başlarken
//! kuşağı tek sayıya, bitirince çift sayıya çeker; okuyucu tek sayı görürse
//! veya değer değişirse yeniden dener. Böylece yarı yazılmış bir pozisyon
//! listesi asla kullanılmaz.
//!
//! # Neden tek bir seqlock
//!
//! Hesap, pozisyonlar ve emirler **birlikte** yayımlanır. Ayrı kilitler
//! kullanmak, equity'nin yeni ama pozisyon listesinin eski olduğu tutarsız bir
//! görüntüye izin verirdi — marjin hesabı yapan bir istemci için bu yanlış
//! karar demektir.

use core::sync::atomic::{AtomicU64, Ordering};

use crate::msg::{read_fixed_str, SYMBOL_NAME_LEN};

/// Durum segmentinin imzası.
pub const STATE_MAGIC: u64 = 0x5359_4E59_414C_5354; // "SYNYALST"

/// Yerleşim sürümü — [`crate::ring::RING_VERSION`] ile birlikte artırılır.
pub const STATE_VERSION: u32 = 1;

/// Başlık boyutu; hesap bloğu bu ofsetten başlar.
pub const STATE_HEADER_SIZE: usize = 128;

/// Azami pozisyon sayısı.
///
/// `ACCOUNT_LIMIT_ORDERS` brokerın gerçek bekleyen-emir tavanını verir; EA bunu
/// okuyup `min(capacity, limit)` ile karşılaştırır. 512 pratikte fazlasıyla
/// yeterli ve taşma **sessiz değildir**: `pos_total` terminaldeki gerçek sayıyı
/// taşır, `pos_count` yazılanı; ikisi farklıysa `flags` içinde
/// [`state_flag::TRUNCATED`] işaretlenir.
pub const MAX_POSITIONS: usize = 512;

/// Azami bekleyen emir sayısı.
pub const MAX_ORDERS: usize = 512;

/// Yorum alanı uzunluğu (pozisyon/emir kayıtları için).
pub const REC_COMMENT_LEN: usize = 32;

// ---------------------------------------------------------------------------
// Sabitler
// ---------------------------------------------------------------------------

/// `AccountSnapshot::trade_mode`.
///
/// MT5'in ham `int` değeri **tele yazılmaz**; EA `switch` ile bu değerlere
/// çevirir. Böylece MetaQuotes ileride enum'a değer eklerse protokol kaymaz.
pub mod trade_mode {
    pub const DEMO: u8 = 0;
    pub const CONTEST: u8 = 1;
    pub const REAL: u8 = 2;
    /// Okunamadı. **GERÇEK HESAP GİBİ** ele alınmalıdır (fail-safe).
    pub const UNKNOWN: u8 = 255;
}

/// `AccountSnapshot::margin_mode`.
pub mod margin_mode {
    pub const NETTING: u8 = 0;
    pub const EXCHANGE: u8 = 1;
    pub const HEDGING: u8 = 2;
    pub const UNKNOWN: u8 = 255;
}

/// `AccountSnapshot::so_mode` — stop-out seviyesinin birimi.
///
/// `margin_so_call`/`margin_so_so` alanlarının yüzde mi para birimi mi
/// olduğunu bu belirler; karıştırmak marjin hesabını tamamen yanlış yapar.
pub mod so_mode {
    pub const PERCENT: u8 = 0;
    pub const MONEY: u8 = 1;
    pub const UNKNOWN: u8 = 255;
}

/// [`StateHeader::flags`] bitleri.
pub mod state_flag {
    /// Terminalde kapasiteden fazla pozisyon/emir vardı, liste kesildi.
    pub const TRUNCATED: u32 = 1 << 0;
    /// Numaralandırma sırasında liste değişti; görüntü tutarsız olabilir.
    pub const UNSTABLE: u32 = 1 << 1;
}

/// Pozisyon/emir kayıtlarındaki `flags` bitleri.
pub mod rec_flag {
    pub const SYMBOL_TRUNCATED: u8 = 1 << 0;
    pub const COMMENT_TRUNCATED: u8 = 1 << 1;
}

// ---------------------------------------------------------------------------
// Hesap — 512 bayt
// ---------------------------------------------------------------------------

/// Hesap durumu.
///
/// String alanları EA'da **yalnızca `OnInit`'te bir kez** doldurulur; sıcak
/// yolda string dönüşümü yapılmaz. Sayısal alanlar saniyede bir tazelenir.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AccountSnapshot {
    /// Sunucu saati, epoch ms.
    pub time_msc: i64,

    pub balance: f64,
    pub credit: f64,
    pub profit: f64,
    pub equity: f64,
    pub margin: f64,
    pub margin_free: f64,
    pub margin_level: f64,
    /// Birimi [`so_mode`] belirler — yüzde mi para mı.
    pub margin_so_call: f64,
    pub margin_so_so: f64,
    pub margin_initial: f64,
    pub margin_maintenance: f64,
    pub assets: f64,
    pub liabilities: f64,
    pub commission_blocked: f64,

    pub login: i64,
    pub leverage: i64,

    /// `ACCOUNT_LIMIT_ORDERS` — brokerın bekleyen emir tavanı (0 = sınırsız).
    pub limit_orders: i32,
    pub currency_digits: i32,

    /// [`trade_mode`] — **canlı para kilidinin dayanağı**.
    pub trade_mode: u8,
    /// [`margin_mode`] — hedging'de pozisyon kapatma `ticket` ister.
    pub margin_mode: u8,
    /// [`so_mode`].
    pub so_mode: u8,
    /// `ACCOUNT_TRADE_ALLOWED` — sunucu bu hesapta ticarete izin veriyor mu.
    pub trade_allowed: u8,
    /// `ACCOUNT_TRADE_EXPERT` — sunucu EA ile ticarete izin veriyor mu.
    pub trade_expert: u8,
    /// `TERMINAL_TRADE_ALLOWED` — AutoTrading düğmesi.
    pub terminal_trade_allowed: u8,
    /// `MQL_TRADE_ALLOWED` — bu EA için ticaret izni.
    pub mql_trade_allowed: u8,
    /// `TERMINAL_CONNECTED`.
    pub connected: u8,
    pub fifo_close: u8,
    pub hedge_allowed: u8,
    /// Hangi string alanların kesildiğini gösteren maske.
    pub str_truncated: u8,
    pub _pad: [u8; 5],

    pub currency: [u8; 16],
    pub server: [u8; 64],
    pub company: [u8; 96],
    pub name: [u8; 96],
    pub _reserved: [u8; 80],
}

impl Default for AccountSnapshot {
    fn default() -> Self {
        Self {
            time_msc: 0,
            balance: 0.0,
            credit: 0.0,
            profit: 0.0,
            equity: 0.0,
            margin: 0.0,
            margin_free: 0.0,
            margin_level: 0.0,
            margin_so_call: 0.0,
            margin_so_so: 0.0,
            margin_initial: 0.0,
            margin_maintenance: 0.0,
            assets: 0.0,
            liabilities: 0.0,
            commission_blocked: 0.0,
            login: 0,
            leverage: 0,
            limit_orders: 0,
            currency_digits: 2,
            // Okunamamış hesap GERÇEK sayılır — emniyetli taraf.
            trade_mode: trade_mode::UNKNOWN,
            margin_mode: margin_mode::UNKNOWN,
            so_mode: so_mode::UNKNOWN,
            trade_allowed: 0,
            trade_expert: 0,
            terminal_trade_allowed: 0,
            mql_trade_allowed: 0,
            connected: 0,
            fifo_close: 0,
            hedge_allowed: 0,
            str_truncated: 0,
            _pad: [0; 5],
            currency: [0; 16],
            server: [0; 64],
            company: [0; 96],
            name: [0; 96],
            _reserved: [0; 80],
        }
    }
}

impl AccountSnapshot {
    pub fn currency_str(&self) -> &str {
        read_fixed_str(&self.currency)
    }
    pub fn server_str(&self) -> &str {
        read_fixed_str(&self.server)
    }
    pub fn company_str(&self) -> &str {
        read_fixed_str(&self.company)
    }
    pub fn name_str(&self) -> &str {
        read_fixed_str(&self.name)
    }

    /// Emir gönderilebilmesi için gereken TÜM izinler açık mı.
    ///
    /// Tek bir "izin yok" hatası hata ayıklanamaz; hangi bayrağın düştüğünü
    /// [`Self::permission_problem`] söyler.
    pub fn can_trade(&self) -> bool {
        self.connected != 0
            && self.terminal_trade_allowed != 0
            && self.mql_trade_allowed != 0
            && self.trade_allowed != 0
            && self.trade_expert != 0
    }

    /// Ticaret engelliyse hangi koşulun düştüğünü açıkla.
    pub fn permission_problem(&self) -> Option<&'static str> {
        if self.connected == 0 {
            return Some("terminal sunucuya bağlı değil");
        }
        if self.terminal_trade_allowed == 0 {
            return Some("terminalde AutoTrading kapalı (Algo Trading düğmesi)");
        }
        if self.mql_trade_allowed == 0 {
            return Some("bu EA için ticaret izni kapalı");
        }
        if self.trade_allowed == 0 {
            return Some("sunucu bu hesapta ticarete izin vermiyor");
        }
        if self.trade_expert == 0 {
            return Some("sunucu EA ile ticarete izin vermiyor");
        }
        None
    }

    /// Hesap gerçek parayla mı çalışıyor.
    ///
    /// `UNKNOWN` **gerçek sayılır** — okunamayan bir hesabı demo varsaymak,
    /// canlı hesapta kazara emir göndermek demektir.
    pub fn is_real_money(&self) -> bool {
        self.trade_mode != trade_mode::DEMO
    }
}

// ---------------------------------------------------------------------------
// Pozisyon — 192 bayt
// ---------------------------------------------------------------------------

/// Açık pozisyon.
///
/// `POSITION_COMMISSION` alanı **yoktur**: MT5'te kullanımdan kalktı ve daima
/// 0 döner. Komisyon yalnızca kapanışta `DEAL_COMMISSION` ile gelir.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PositionRec {
    /// `POSITION_TICKET` — **emir gönderiminde kullanılacak anahtar**.
    pub ticket: u64,
    /// `POSITION_IDENTIFIER` — geçmiş/deal eşleştirme anahtarı.
    ///
    /// Hedging'de kapatma komutunda **bu değil `ticket` kullanılmalıdır**;
    /// identifier→ticket eşlemesi tek yönlü garanti değildir.
    pub identifier: u64,
    /// `POSITION_MAGIC` — bizim gönderdiğimiz emirlerde `client_id` taşır.
    pub magic: u64,
    pub time_msc: i64,
    pub time_update_msc: i64,
    pub volume: f64,
    pub price_open: f64,
    pub price_current: f64,
    pub sl: f64,
    pub tp: f64,
    pub profit: f64,
    pub swap: f64,
    pub symbol: [u8; SYMBOL_NAME_LEN],
    pub comment: [u8; REC_COMMENT_LEN],
    /// `ENUM_POSITION_TYPE`: 0 = BUY, 1 = SELL.
    pub kind: u8,
    /// `ENUM_POSITION_REASON`.
    pub reason: u8,
    pub digits: u8,
    /// [`rec_flag`] bitleri.
    pub flags: u8,
    pub _reserved: [u8; 28],
}

impl Default for PositionRec {
    fn default() -> Self {
        Self {
            ticket: 0,
            identifier: 0,
            magic: 0,
            time_msc: 0,
            time_update_msc: 0,
            volume: 0.0,
            price_open: 0.0,
            price_current: 0.0,
            sl: 0.0,
            tp: 0.0,
            profit: 0.0,
            swap: 0.0,
            symbol: [0; SYMBOL_NAME_LEN],
            comment: [0; REC_COMMENT_LEN],
            kind: 0,
            reason: 0,
            digits: 0,
            flags: 0,
            _reserved: [0; 28],
        }
    }
}

impl PositionRec {
    pub fn symbol_str(&self) -> &str {
        read_fixed_str(&self.symbol)
    }
    pub fn comment_str(&self) -> &str {
        read_fixed_str(&self.comment)
    }
    pub fn is_buy(&self) -> bool {
        self.kind == 0
    }
}

// ---------------------------------------------------------------------------
// Bekleyen emir — 192 bayt
// ---------------------------------------------------------------------------

/// Bekleyen emir.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OrderRec {
    pub ticket: u64,
    /// `ORDER_MAGIC` — `client_id` taşır.
    pub magic: u64,
    /// `ORDER_POSITION_ID` — bekleyen emirde 0, tetiklenince dolar.
    pub position_id: u64,
    pub time_setup_msc: i64,
    /// `ORDER_TIME_EXPIRATION` — **saniye** (ms varyantı yok). 0 = süresiz.
    pub time_expiration: i64,
    pub volume_initial: f64,
    /// `ORDER_VOLUME_CURRENT` — **kalan** hacim. Dolan = initial − current.
    pub volume_current: f64,
    pub price_open: f64,
    pub price_stoplimit: f64,
    pub sl: f64,
    pub tp: f64,
    pub price_current: f64,
    pub symbol: [u8; SYMBOL_NAME_LEN],
    pub comment: [u8; REC_COMMENT_LEN],
    /// `ENUM_ORDER_TYPE`.
    pub kind: u8,
    /// `ENUM_ORDER_STATE`.
    pub state: u8,
    pub type_time: u8,
    pub type_filling: u8,
    /// `ENUM_ORDER_REASON`.
    pub reason: u8,
    pub digits: u8,
    pub flags: u8,
    pub _reserved: [u8; 25],
}

impl Default for OrderRec {
    fn default() -> Self {
        Self {
            ticket: 0,
            magic: 0,
            position_id: 0,
            time_setup_msc: 0,
            time_expiration: 0,
            volume_initial: 0.0,
            volume_current: 0.0,
            price_open: 0.0,
            price_stoplimit: 0.0,
            sl: 0.0,
            tp: 0.0,
            price_current: 0.0,
            symbol: [0; SYMBOL_NAME_LEN],
            comment: [0; REC_COMMENT_LEN],
            kind: 0,
            state: 0,
            type_time: 0,
            type_filling: 0,
            reason: 0,
            digits: 0,
            flags: 0,
            _reserved: [0; 25],
        }
    }
}

impl OrderRec {
    pub fn symbol_str(&self) -> &str {
        read_fixed_str(&self.symbol)
    }
    pub fn comment_str(&self) -> &str {
        read_fixed_str(&self.comment)
    }
}

// ---------------------------------------------------------------------------
// Segment
// ---------------------------------------------------------------------------

/// Durum segmenti başlığı.
#[repr(C, align(64))]
pub struct StateHeader {
    pub magic: u64,
    pub version: u32,
    pub _pad0: u32,
    /// Seqlock: TEK = yazılıyor, ÇİFT = tutarlı.
    pub generation: AtomicU64,
    /// Anlık görüntünün alındığı sunucu saati (epoch ms).
    pub built_at_msc: i64,
    /// Anlık görüntünün alındığı yerel QPC — bayatlık ölçümü için.
    pub built_at_qpc: u64,
    /// Yazılan pozisyon sayısı.
    pub pos_count: u32,
    /// Terminaldeki GERÇEK pozisyon sayısı. `pos_count`'tan büyükse liste
    /// kesilmiştir ve bu sessiz kalmamalıdır.
    pub pos_total: u32,
    pub ord_count: u32,
    pub ord_total: u32,
    pub pos_capacity: u32,
    pub ord_capacity: u32,
    /// [`state_flag`] bitleri.
    pub flags: u32,
    pub _pad1: u32,
    pub _reserved: [u8; 56],
}

/// Bir durum segmenti için gereken toplam bayt.
pub const fn bytes_needed(pos_cap: u32, ord_cap: u32) -> usize {
    STATE_HEADER_SIZE
        + core::mem::size_of::<AccountSnapshot>()
        + core::mem::size_of::<PositionRec>() * pos_cap as usize
        + core::mem::size_of::<OrderRec>() * ord_cap as usize
}

/// Tutarlı bir anlık görüntü.
#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub account: AccountSnapshot,
    pub positions: Vec<PositionRec>,
    pub orders: Vec<OrderRec>,
    pub built_at_msc: i64,
    pub built_at_qpc: u64,
    pub truncated: bool,
    pub unstable: bool,
    /// Terminaldeki gerçek sayılar (kesilme olduysa `positions.len()`'ten büyük).
    pub pos_total: u32,
    pub ord_total: u32,
}

/// Durum segmentine ham görünüm.
///
/// # Güvenlik
/// `base` en az [`bytes_needed`] bayt geçerli ve 64'e hizalı eşlenmiş belleğe
/// işaret etmelidir. Yalnızca TEK yazar (EA) olabilir.
pub struct StateTable {
    hdr: *mut StateHeader,
    account: *mut AccountSnapshot,
    positions: *mut PositionRec,
    orders: *mut OrderRec,
    pos_cap: u32,
    ord_cap: u32,
}

unsafe impl Send for StateTable {}
unsafe impl Sync for StateTable {}

/// Durum segmenti hataları.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    BadMagic(u64),
    VersionMismatch { found: u32, expected: u32 },
    BadCapacity { pos: u32, ord: u32 },
    TooMany { kind: &'static str, found: u32, capacity: u32 },
}

impl core::fmt::Display for StateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic(m) => write!(
                f,
                "durum segmenti imzası geçersiz (0x{m:016X}) — segment kurulmamış"
            ),
            Self::VersionMismatch { found, expected } => {
                write!(f, "durum segmenti sürümü uyumsuz: bulunan {found}, beklenen {expected}")
            }
            Self::BadCapacity { pos, ord } => {
                write!(f, "durum segmenti kapasitesi geçersiz (pos={pos}, ord={ord})")
            }
            Self::TooMany { kind, found, capacity } => {
                write!(f, "{kind} sayısı kapasiteyi aşıyor: {found} > {capacity}")
            }
        }
    }
}

impl std::error::Error for StateError {}

impl StateTable {
    /// Segmenti sıfırdan kur. Yalnızca EA tarafı çağırır.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn init(base: *mut u8, pos_cap: u32, ord_cap: u32) -> Self {
        assert!(pos_cap as usize <= MAX_POSITIONS && ord_cap as usize <= MAX_ORDERS);
        let hdr = base.cast::<StateHeader>();
        unsafe {
            core::ptr::write_bytes(base, 0, STATE_HEADER_SIZE);
            (*hdr).version = STATE_VERSION;
            (*hdr).pos_capacity = pos_cap;
            (*hdr).ord_capacity = ord_cap;
            core::sync::atomic::fence(Ordering::Release);
            (*hdr).magic = STATE_MAGIC;
        }
        unsafe { Self::wire(base, pos_cap, ord_cap) }
    }

    /// Var olan segmente bağlan.
    ///
    /// # Güvenlik
    /// Yapı düzeyindeki güvenlik notlarına bak.
    pub unsafe fn attach(base: *mut u8) -> Result<Self, StateError> {
        let hdr = base.cast::<StateHeader>();
        let (magic, version, pos_cap, ord_cap) = unsafe {
            core::sync::atomic::fence(Ordering::Acquire);
            ((*hdr).magic, (*hdr).version, (*hdr).pos_capacity, (*hdr).ord_capacity)
        };
        if magic != STATE_MAGIC {
            return Err(StateError::BadMagic(magic));
        }
        if version != STATE_VERSION {
            return Err(StateError::VersionMismatch { found: version, expected: STATE_VERSION });
        }
        if pos_cap as usize > MAX_POSITIONS || ord_cap as usize > MAX_ORDERS {
            return Err(StateError::BadCapacity { pos: pos_cap, ord: ord_cap });
        }
        Ok(unsafe { Self::wire(base, pos_cap, ord_cap) })
    }

    /// # Güvenlik
    /// `base` yeterli büyüklükte eşlenmiş bellek olmalıdır.
    unsafe fn wire(base: *mut u8, pos_cap: u32, ord_cap: u32) -> Self {
        let account = unsafe { base.add(STATE_HEADER_SIZE).cast::<AccountSnapshot>() };
        let pos_off = STATE_HEADER_SIZE + core::mem::size_of::<AccountSnapshot>();
        let positions = unsafe { base.add(pos_off).cast::<PositionRec>() };
        let ord_off = pos_off + core::mem::size_of::<PositionRec>() * pos_cap as usize;
        let orders = unsafe { base.add(ord_off).cast::<OrderRec>() };
        Self { hdr: base.cast::<StateHeader>(), account, positions, orders, pos_cap, ord_cap }
    }

    #[inline]
    fn hdr(&self) -> &StateHeader {
        unsafe { &*self.hdr }
    }

    /// Yalnızca başlığı okuyup kapasiteleri öğren (tüketici tarafı, eşleme
    /// boyutunu doğru seçebilmek için).
    ///
    /// # Güvenlik
    /// `base` en az [`STATE_HEADER_SIZE`] bayt geçerli belleğe işaret etmeli.
    pub unsafe fn peek_capacity(base: *const u8) -> Result<(u32, u32), StateError> {
        let hdr = base.cast::<StateHeader>();
        let (magic, version, pos_cap, ord_cap) = unsafe {
            core::sync::atomic::fence(Ordering::Acquire);
            ((*hdr).magic, (*hdr).version, (*hdr).pos_capacity, (*hdr).ord_capacity)
        };
        if magic != STATE_MAGIC {
            return Err(StateError::BadMagic(magic));
        }
        if version != STATE_VERSION {
            return Err(StateError::VersionMismatch { found: version, expected: STATE_VERSION });
        }
        if pos_cap as usize > MAX_POSITIONS || ord_cap as usize > MAX_ORDERS {
            return Err(StateError::BadCapacity { pos: pos_cap, ord: ord_cap });
        }
        Ok((pos_cap, ord_cap))
    }

    /// Tüm durumu topluca yayımla (seqlock'u doğru sırayla sürer).
    ///
    /// Hesap, pozisyonlar ve emirler **birlikte** yayımlanır: ayrı yayımlamak,
    /// equity'nin yeni ama pozisyon listesinin eski olduğu tutarsız bir
    /// görüntüye izin verirdi.
    ///
    /// # Güvenlik
    /// Yalnızca TEK yazar thread'i çağırabilir.
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn publish(
        &self,
        account: &AccountSnapshot,
        positions: &[PositionRec],
        orders: &[OrderRec],
        pos_total: u32,
        ord_total: u32,
        built_at_msc: i64,
        built_at_qpc: u64,
        unstable: bool,
    ) -> Result<(), StateError> {
        if positions.len() > self.pos_cap as usize {
            return Err(StateError::TooMany {
                kind: "pozisyon",
                found: positions.len() as u32,
                capacity: self.pos_cap,
            });
        }
        if orders.len() > self.ord_cap as usize {
            return Err(StateError::TooMany {
                kind: "emir",
                found: orders.len() as u32,
                capacity: self.ord_cap,
            });
        }

        let hdr = self.hdr();
        let gen = hdr.generation.load(Ordering::Relaxed);
        hdr.generation.store(gen.wrapping_add(1), Ordering::Relaxed);
        core::sync::atomic::fence(Ordering::Release);

        unsafe {
            core::ptr::write(self.account, *account);
            core::ptr::copy_nonoverlapping(positions.as_ptr(), self.positions, positions.len());
            core::ptr::copy_nonoverlapping(orders.as_ptr(), self.orders, orders.len());

            let h = &mut *self.hdr;
            h.built_at_msc = built_at_msc;
            h.built_at_qpc = built_at_qpc;
            h.pos_count = positions.len() as u32;
            h.pos_total = pos_total;
            h.ord_count = orders.len() as u32;
            h.ord_total = ord_total;
            let mut flags = 0u32;
            if pos_total as usize > positions.len() || ord_total as usize > orders.len() {
                flags |= state_flag::TRUNCATED;
            }
            if unstable {
                flags |= state_flag::UNSTABLE;
            }
            h.flags = flags;
        }

        hdr.generation.store(gen.wrapping_add(2), Ordering::Release);
        Ok(())
    }

    /// Tutarlı bir anlık görüntü al. Yazar tam o sırada güncelliyorsa
    /// `attempts` kez yeniden dener.
    pub fn snapshot(&self, attempts: usize) -> Option<StateSnapshot> {
        let hdr = self.hdr();
        for _ in 0..attempts.max(1) {
            let g1 = hdr.generation.load(Ordering::Acquire);
            if g1 & 1 != 0 {
                core::hint::spin_loop();
                continue;
            }
            let pos_n = (hdr.pos_count.min(self.pos_cap)) as usize;
            let ord_n = (hdr.ord_count.min(self.ord_cap)) as usize;
            let account = unsafe { core::ptr::read(self.account) };
            let mut positions = Vec::with_capacity(pos_n);
            let mut orders = Vec::with_capacity(ord_n);
            unsafe {
                positions.extend_from_slice(core::slice::from_raw_parts(self.positions, pos_n));
                orders.extend_from_slice(core::slice::from_raw_parts(self.orders, ord_n));
            }
            let built_at_msc = hdr.built_at_msc;
            let built_at_qpc = hdr.built_at_qpc;
            let flags = hdr.flags;
            let pos_total = hdr.pos_total;
            let ord_total = hdr.ord_total;

            core::sync::atomic::fence(Ordering::Acquire);
            if hdr.generation.load(Ordering::Relaxed) == g1 {
                return Some(StateSnapshot {
                    account,
                    positions,
                    orders,
                    built_at_msc,
                    built_at_qpc,
                    truncated: flags & state_flag::TRUNCATED != 0,
                    unstable: flags & state_flag::UNSTABLE != 0,
                    pos_total,
                    ord_total,
                });
            }
        }
        None
    }

    /// Hiç yayın yapılmış mı (kuşak > 0).
    pub fn has_data(&self) -> bool {
        self.hdr().generation.load(Ordering::Acquire) > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::write_fixed_str;

    struct Buf {
        ptr: *mut u8,
        layout: std::alloc::Layout,
    }
    impl Buf {
        fn new(bytes: usize) -> Self {
            let layout = std::alloc::Layout::from_size_align(bytes, 64).unwrap();
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null());
            Self { ptr, layout }
        }
    }
    impl Drop for Buf {
        fn drop(&mut self) {
            unsafe { std::alloc::dealloc(self.ptr, self.layout) };
        }
    }

    fn pos(ticket: u64, sym: &str, vol: f64) -> PositionRec {
        let mut p = PositionRec { ticket, identifier: ticket, volume: vol, ..Default::default() };
        write_fixed_str(&mut p.symbol, sym);
        p
    }

    fn ord(ticket: u64, sym: &str) -> OrderRec {
        let mut o = OrderRec { ticket, ..Default::default() };
        write_fixed_str(&mut o.symbol, sym);
        o
    }

    fn demo_account() -> AccountSnapshot {
        let mut a = AccountSnapshot {
            balance: 10_000.0,
            equity: 10_050.0,
            margin_free: 9_800.0,
            login: 318262494,
            leverage: 500,
            trade_mode: trade_mode::DEMO,
            margin_mode: margin_mode::HEDGING,
            so_mode: so_mode::PERCENT,
            trade_allowed: 1,
            trade_expert: 1,
            terminal_trade_allowed: 1,
            mql_trade_allowed: 1,
            connected: 1,
            ..Default::default()
        };
        write_fixed_str(&mut a.currency, "USD");
        write_fixed_str(&mut a.server, "XMGlobal-MT5 7");
        a
    }

    #[test]
    fn layout_sizes_are_fixed() {
        assert_eq!(core::mem::size_of::<AccountSnapshot>(), 512);
        assert_eq!(core::mem::size_of::<PositionRec>(), 192);
        assert_eq!(core::mem::size_of::<OrderRec>(), 192);
        assert_eq!(core::mem::size_of::<StateHeader>(), STATE_HEADER_SIZE);
        assert_eq!(core::mem::align_of::<StateHeader>(), 64);
    }

    #[test]
    fn publish_and_snapshot_roundtrip() {
        let buf = Buf::new(bytes_needed(8, 8));
        let t = unsafe { StateTable::init(buf.ptr, 8, 8) };

        assert!(!t.has_data(), "yayın öncesi veri olmamalı");

        let a = demo_account();
        let ps = vec![pos(1, "EURUSD", 0.1), pos(2, "GOLD", 0.05)];
        let os = vec![ord(10, "GBPUSD")];
        unsafe { t.publish(&a, &ps, &os, 2, 1, 123, 456, false) }.unwrap();

        let s = t.snapshot(8).unwrap();
        assert_eq!(s.positions.len(), 2);
        assert_eq!(s.orders.len(), 1);
        assert_eq!(s.positions[1].symbol_str(), "GOLD");
        assert_eq!(s.orders[0].ticket, 10);
        assert_eq!(s.account.login, 318262494);
        assert_eq!(s.account.server_str(), "XMGlobal-MT5 7");
        assert_eq!(s.built_at_msc, 123);
        assert!(!s.truncated && !s.unstable);
        assert!(t.has_data());
    }

    #[test]
    fn truncation_is_reported_not_hidden() {
        // Terminalde kapasiteden fazla pozisyon varsa istemci bunu BİLMELİ;
        // sessizce kısa liste vermek "başka pozisyon yok" gibi okunurdu.
        let buf = Buf::new(bytes_needed(2, 2));
        let t = unsafe { StateTable::init(buf.ptr, 2, 2) };
        let ps = vec![pos(1, "EURUSD", 0.1), pos(2, "GOLD", 0.1)];
        // Terminalde aslında 7 pozisyon var, biz 2 yazabildik.
        unsafe { t.publish(&demo_account(), &ps, &[], 7, 0, 1, 1, false) }.unwrap();

        let s = t.snapshot(8).unwrap();
        assert!(s.truncated, "kesilme işaretlenmeli");
        assert_eq!(s.pos_total, 7, "gerçek sayı korunmalı");
        assert_eq!(s.positions.len(), 2);
    }

    #[test]
    fn publish_rejects_more_than_capacity() {
        let buf = Buf::new(bytes_needed(2, 2));
        let t = unsafe { StateTable::init(buf.ptr, 2, 2) };
        let ps = vec![pos(1, "A", 0.1), pos(2, "B", 0.1), pos(3, "C", 0.1)];
        assert!(matches!(
            unsafe { t.publish(&demo_account(), &ps, &[], 3, 0, 0, 0, false) },
            Err(StateError::TooMany { kind: "pozisyon", .. })
        ));
    }

    #[test]
    fn unknown_trade_mode_counts_as_real_money() {
        // EN KRİTİK GÜVENLİK VARSAYIMI: okunamayan hesabı demo saymak,
        // canlı hesapta kazara emir göndermek demektir.
        let a = AccountSnapshot::default();
        assert_eq!(a.trade_mode, trade_mode::UNKNOWN);
        assert!(a.is_real_money(), "bilinmeyen hesap GERÇEK sayılmalı");

        let real = AccountSnapshot { trade_mode: trade_mode::REAL, ..Default::default() };
        assert!(real.is_real_money());
        let contest = AccountSnapshot { trade_mode: trade_mode::CONTEST, ..Default::default() };
        assert!(contest.is_real_money(), "yarışma hesabı da demo değildir");
        let demo = AccountSnapshot { trade_mode: trade_mode::DEMO, ..Default::default() };
        assert!(!demo.is_real_money());
    }

    #[test]
    fn permission_problem_names_the_specific_blocker() {
        // Tek bir "izin yok" hatası hata ayıklanamaz.
        let base = demo_account();
        assert!(base.can_trade());
        assert!(base.permission_problem().is_none());

        let cases: [(AccountSnapshot, &str); 5] = [
            (AccountSnapshot { connected: 0, ..base }, "bağlı değil"),
            (AccountSnapshot { terminal_trade_allowed: 0, ..base }, "AutoTrading"),
            (AccountSnapshot { mql_trade_allowed: 0, ..base }, "bu EA için"),
            (AccountSnapshot { trade_allowed: 0, ..base }, "sunucu bu hesapta"),
            (AccountSnapshot { trade_expert: 0, ..base }, "EA ile ticarete"),
        ];
        for (acc, needle) in cases {
            assert!(!acc.can_trade());
            let p = acc.permission_problem().expect("sebep belirtilmeli");
            assert!(p.contains(needle), "'{needle}' beklendi, bulunan: {p}");
        }
    }

    #[test]
    fn attach_validates_and_reads_writer_data() {
        let buf = Buf::new(bytes_needed(4, 4));
        let w = unsafe { StateTable::init(buf.ptr, 4, 4) };
        unsafe { w.publish(&demo_account(), &[pos(9, "USDJPY", 1.0)], &[], 1, 0, 5, 5, false) }
            .unwrap();

        let r = unsafe { StateTable::attach(buf.ptr) }.unwrap();
        let s = r.snapshot(8).unwrap();
        assert_eq!(s.positions[0].ticket, 9);
        assert_eq!(s.positions[0].symbol_str(), "USDJPY");
    }

    #[test]
    fn attach_rejects_uninitialised_memory() {
        let buf = Buf::new(bytes_needed(4, 4));
        assert!(matches!(
            unsafe { StateTable::attach(buf.ptr) },
            Err(StateError::BadMagic(0))
        ));
    }

    #[test]
    fn peek_capacity_lets_consumer_size_its_mapping() {
        let buf = Buf::new(bytes_needed(16, 32));
        let _w = unsafe { StateTable::init(buf.ptr, 16, 32) };
        assert_eq!(unsafe { StateTable::peek_capacity(buf.ptr) }.unwrap(), (16, 32));
    }

    #[test]
    fn snapshot_never_returns_torn_state_under_concurrent_writes() {
        use std::sync::atomic::{AtomicBool, Ordering as O};
        use std::sync::Arc;

        let buf = Buf::new(bytes_needed(64, 64));
        let t = Arc::new(unsafe { StateTable::init(buf.ptr, 64, 64) });
        let stop = Arc::new(AtomicBool::new(false));

        // İki tutarlı durum: ya hepsi "AAA" ya hepsi "BBB".
        let a: Vec<PositionRec> = (0..64).map(|i| pos(i, "AAA", 1.0)).collect();
        let b: Vec<PositionRec> = (0..64).map(|i| pos(i, "BBB", 2.0)).collect();
        unsafe { t.publish(&demo_account(), &a, &[], 64, 0, 0, 0, false) }.unwrap();

        let writer = {
            let t = t.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut flip = false;
                while !stop.load(O::Relaxed) {
                    let s = if flip { &b } else { &a };
                    unsafe { t.publish(&demo_account(), s, &[], 64, 0, 0, 0, false) }.unwrap();
                    flip = !flip;
                }
            })
        };

        for _ in 0..20_000 {
            if let Some(s) = t.snapshot(64) {
                let first = s.positions[0].symbol_str().to_owned();
                assert!(
                    s.positions.iter().all(|p| p.symbol_str() == first),
                    "yırtık okuma: pozisyon listesinde karışık durum"
                );
            }
        }
        stop.store(true, O::Relaxed);
        writer.join().unwrap();
    }
}
