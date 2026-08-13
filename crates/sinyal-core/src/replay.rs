//! Replay kaynağı: diske yazılmış tick kaydını **canlıymış gibi** oynatır.
//!
//! Canlı yolda [`crate::source::spawn_reader`] paylaşımlı bellekten okuyup
//! [`FeedEvent`] üretir; burada aynı olaylar bir dosyadan üretilir. Sinyal
//! sisteminin gördüğü mesaj biçimleri, alan adları ve kanal adları **birebir
//! aynıdır**. Tek fark `hello.mode` alanının `"replay"` olmasıdır ([`MODE`]).
//!
//! # Neden tek fark `hello.mode`
//!
//! Amaç sinyal sisteminin **kod yolunun değişmemesi** — sistemin
//! kandırılması değil. Replay'i canlı sanıp gerçek emir göndermek kabul
//! edilemez bir kaza olurdu; bu yüzden ayrım tel üzerinde AÇIKÇA duruyor ama
//! akışın kendisi hiç değişmiyor.
//!
//! # Kayıt biçimi
//!
//! ```text
//! <data-dir>/<instance>/ticks-YYYYMMDD.bin      sabit 48 baytlık kayıtlar
//! <data-dir>/<instance>/symbols-YYYYMMDD.jsonl  sembol tablosu anlık görüntüleri
//! <data-dir>/.lock                              tek yazar kilidi
//! ```
//!
//! Gün sınırı **UTC** ve tarih kaydın **yerel alım** zamanından
//! ([`TickRec::recv_ms`]) türetilir. Dosya başlığı YOK: dosya uzunluğu tek
//! doğruluk kaynağıdır, yükleme sırasında `len - (len % 48)` bayta kırpılır.
//! Çökme anında yarım kalmış kayıt böylece sessizce atılır — başlıkta kayıt
//! sayısı tutulsaydı çökmeden sonra başlık gerçekle çelişirdi.
//!
//! # İki saat
//!
//! [`TickRec::recv_ms`] yerel alım zamanıdır ve **replay temposu bundan**
//! üretilir. [`TickRec::time_msc`] broker sunucu saatidir ve tüketiciye giden
//! zamandır; aynı değeri taşıyan ardışık tick'ler olabildiği için gerçek
//! varış temposu yalnızca `recv_ms` ile yeniden üretilebilir.
//!
//! # Belirlenimcilik
//!
//! Aynı kayıt + aynı bayraklar → aynı çıktı. Oynatım kayıt sırasını KORUR;
//! `--replay-speed 0` ile de sıra aynı kalır, yalnızca bekleme yoktur. Birden
//! çok örnek varsa dosyalar `recv_ms`'e göre k-yollu **birleştirilir** (sıralı
//! değil): her dosyanın kendi iç sırası korunur, eşitlikte örnek adı
//! belirleyicidir.
//!
//! # Bu kipte MT5 geçmişi YOKTUR
//!
//! Mumlar yalnızca kayıttaki tick'lerden üretilir. `candles` cevabı bu yüzden
//! `src_kind: "tick"`, `hist: "off"` der ve `hist_note` sebebi açıkça söyler
//! ([`HIST_NOTE`]).
//!
//! # Gün aralığı
//!
//! `--replay-date` üç biçim kabul eder ([`parse_date_spec`]):
//!
//! ```text
//! 20260513              tek gün
//! 20260513-20260812     aralık, İKİSİ DE DAHİL
//! 20260513-             o günden kayıtta ne varsa sonuna kadar
//! ```
//!
//! Aralık **tek bağlantıda, kesintisiz** akar: günler kronolojik sırayla
//! oynatılır ve [`crate::server::ReplayEnd`] (`replay_done`) yalnızca
//! ARALIĞIN SONUNDA duyurulur. İstemci gün değiştiğini fark etmemelidir —
//! amacın kendisi bu. 66 ayrı süreç başlatıp 66 kez bağlanmak sadece zahmet
//! değil, **doğruluk** sorunuydu: gün sınırında bağlantı koptuğunda sinyal
//! sisteminin durumu sıfırlanır ve gerçekte TAŞINACAK bir pozisyon
//! taşınmamış olurdu. Gün-aşırı davranış ancak böyle sınanabilir.
//!
//! Kayıtta olmayan günler (hafta sonu, tatil) **sessizce atlanır**; 0 baytlık
//! gün dosyaları da öyle. Ama aralıkta HİÇ gün bulunamazsa hata AÇIKTIR
//! ([`ReplayError::NoRecordingInRange`]) — sessizce boş akışa dönüşmek,
//! "sistem çalışıyor ama piyasa hareketsiz" yanılsaması üretirdi.
//!
//! `--replay-from` / `--replay-to` aralıkla birlikte verildiğinde **HER GÜNE
//! ayrı ayrı** uygulanır (ör. her günün 09:00-17:00'si).
//!
//! # Günler arası boşluk KIRPILIR
//!
//! Tempo `recv_ms` farkından üretilir. Cuma 23:58'den Pazartesi 01:00'e
//! geçerken bu fark ~49 SAATtir ve `--replay-speed 1` ile oynatım 49 saat
//! uyurdu. Günler arası bekleme bu yüzden en fazla [`REPLAY_GAP_MAX_MS`]
//! olur (hız çarpanına da bölünür). Gün İÇİNDEKİ boşluklar **kırpılmaz** —
//! onlar gerçek piyasa sessizliğidir ve sadakatin parçasıdır.
//!
//! # Bellek
//!
//! Aralığın tamamı belleğe ALINMAZ: [`Recording`] yalnızca gün listesini ve
//! kapsamı tutar, tick'ler gün gün [`Recording::load_day`] ile yüklenip o gün
//! bitince bırakılır. 1,15 GB'lik 25,8 milyon kaydı RAM'e almak kabul
//! edilemezdi.
//!
//! Sembol tablosu da **her gün için yeniden** yüklenir: sembol kimlikleri
//! günler arasında DEĞİŞEBİLİR ve tick'i eski günün tablosuyla isimlendirmek
//! yanlış sembol adı yaymak olurdu.
//!
//! # Kullanım kısıtı
//!
//! `--replay` ile `--instance` **aynı anda kullanılamaz**: örnek listesi
//! kayıt dizinindeki alt dizinlerden keşfedilir ([`Recording::instances`]).

// Bu modül CLI tarafından bağlanana kadar bazı yüzeyleri kullanılmıyor
// görünür; yüzey kasten eksiksiz bırakıldı (hello alanları, teşhis).
#![allow(dead_code)]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinyal_proto::{sym_flag, write_fixed_str, SymbolEntry};
use tokio::sync::{broadcast, watch};

use crate::candles::CandleStore;
use crate::server::ReplayEnd;
use crate::source::FeedEvent;
use crate::state::{LastTick, Registry};

/// Replay'de `candles` cevabının `hist_note` alanı.
///
/// `hist` daima `"off"` ve `src_kind` daima `"tick"`tir; sebebi söylemeden
/// bırakmak, istemcinin "Service'i açmayı unuttum" sanmasına yol açardı.
pub const HIST_NOTE: &str =
    "replay kipi: MT5 gecmisi YOK — barlar yalnizca kayittaki tick'lerden uretildi";

/// Bir günün milisaniyesi. Gün sınırı UTC.
const DAY_MS: i64 = 86_400_000;

/// İki GÜN arasında beklenecek en uzun süre (ms, hız çarpanına bölünmeden).
///
/// Gün içi boşluklar gerçek piyasa sessizliğidir ve aynen beklenir. Günler
/// ARASI boşluk ise kaydın kendisinden değil, kaydın gün dosyalarına
/// bölünmesinden gelir: Cuma kapanışı ile Pazartesi açılışı arasında ~49 saat
/// vardır ve `--replay-speed 1` ile oynatım gerçekten 49 saat uyurdu. Bu
/// kırpma olmadan çok günlü replay pratikte KULLANILAMAZ.
pub const REPLAY_GAP_MAX_MS: i64 = 1_000;

// ---------------------------------------------------------------------------
// TickRec — dosyadaki sabit 48 baytlık kayıt
// ---------------------------------------------------------------------------

/// Diskteki kayıt boyu ve kaydın kendisi — **tek tanım**, `record.rs`'ten.
///
/// Bir zamanlar bu dosyada ikinci, bağımsız bir `TickRec` vardı: yazıcı
/// (`record.rs`) ve okuyucu (burası) ayrı ajanlar tarafından aynı şartnameden
/// yazılmıştı ve ikisi de "48 bayt, şu ofsetler" diyordu. İkisi de kendi
/// birim testini geçiyordu, çünkü her biri KENDİ yazdığını KENDİ okuyordu.
///
/// Bu, sessiz veri bozulmasının klasik kaynağıdır: biri bir alan eklediğinde
/// veya bir ofset kaydırdığında derleyici hiçbir şey söylemez, testler yeşil
/// kalır ve bozukluk ancak aylar sonra "grafik saçmalıyor" diye ortaya çıkar.
/// Tek bir on-disk biçimin tek bir tanımı olmalı; yerleşim değişirse iki
/// taraf da AYNI anda değişsin diye tip buradan yeniden dışa veriliyor.
pub use crate::record::{TickRec, REC_SIZE as TICK_REC_SIZE};

// ---------------------------------------------------------------------------
// symbols-YYYYMMDD.jsonl
// ---------------------------------------------------------------------------

/// Sembol tablosundaki tek satır — alanlar tel protokolündeki `SymbolInfo`
/// ile **aynı**, ek olarak wire `symbol_id`'yi taşıyan `id`.
///
/// Eksik alanlar varsayılana düşer: kayıt biçimi ileride alan kazanabilir ve
/// eski bir kaydı okuyamamak, kaydın tümünü çöpe atmak demek olurdu.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolItem {
    /// Wire `symbol_id` — tick kayıtlarındaki `symbol_id` ile eşleşir.
    pub id: u32,
    /// Broker'daki gerçek ad.
    pub s: String,
    #[serde(default)]
    pub digits: u32,
    #[serde(default)]
    pub point: f64,
    #[serde(default)]
    pub tick_size: f64,
    #[serde(default)]
    pub volume_min: f64,
    #[serde(default)]
    pub volume_max: f64,
    #[serde(default)]
    pub volume_step: f64,
    /// `SYMBOL_TRADE_CONTRACT_SIZE`.
    ///
    /// **Simülasyonun para birimine geçişi bu alandadır.** Marjin
    /// (`hacim × contract_size × fiyat / kaldıraç`) ve kâr
    /// (`fiyat farkı × hacim × contract_size`) ikisi de buna dayanır; alan
    /// yoksa forex'te 100 000 kat küçük bir kâr ve pratikte sonsuz kaldıraç
    /// çıkar — yani marjin denetimi (10019) sessizce ölür ve kâr eğrisi
    /// anlamsızlaşır. Kayıtta bu alan YOKKEN 0 gelir ve simülatör emri
    /// açıkça reddeder; sessizce 1 varsaymak tam da bu işin kapatmaya
    /// çalıştığı yanılgıyı üretirdi.
    #[serde(default)]
    pub contract_size: f64,
    #[serde(default)]
    pub exec_mode: u32,
    #[serde(default)]
    pub filling_mask: u32,
    #[serde(default)]
    pub stops_level: u32,
    #[serde(default)]
    pub book_depth: u32,
    #[serde(default)]
    pub polled_only: bool,
    #[serde(default)]
    pub chart: bool,
    #[serde(default)]
    pub ready: bool,
    /// Kaynak örnek adı. Dosya zaten örnek başına ayrıldığı için **bilgi
    /// amaçlı**; replay dizin adını kullanır.
    #[serde(default)]
    pub src: String,
}

impl SymbolItem {
    /// Kayıttan `Registry`'nin beklediği girdiyi kur.
    pub fn to_entry(&self) -> SymbolEntry {
        let mut flags = 0u32;
        if self.ready {
            flags |= sym_flag::READY;
        }
        if self.polled_only {
            flags |= sym_flag::POLLED_ONLY;
        }
        if self.chart {
            flags |= sym_flag::CHART;
        }
        if self.book_depth > 0 {
            flags |= sym_flag::BOOK_SUBSCRIBED;
        }
        // Kayıt yalnızca `contract_size`ı taşıyor (swap/marjin bloğunun geri
        // kalanını değil), bu yüzden bayrağı ANCAK o alan doluysa kuruyoruz.
        // Bayrağı taşımadığımız veriye dayandırmak, "sözleşme verisi var"
        // diyip swap alanlarını 0 bırakmak olurdu.
        if self.contract_size > 0.0 {
            flags |= sym_flag::CONTRACT_DATA;
        }
        let mut e = SymbolEntry {
            point: self.point,
            tick_size: self.tick_size,
            volume_min: self.volume_min,
            volume_max: self.volume_max,
            volume_step: self.volume_step,
            contract_size: self.contract_size,
            symbol_id: self.id,
            digits: self.digits,
            filling_mode: self.filling_mask,
            trade_exemode: self.exec_mode,
            stops_level: self.stops_level,
            ticks_bookdepth: self.book_depth,
            flags,
            ..Default::default()
        };
        write_fixed_str(&mut e.name, &self.s);
        e
    }
}

/// Sembol tablosunun bir anlık görüntüsü — tablo her DEĞİŞTİĞİNDE bir satır.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolSnapshot {
    /// Bu görüntünün alındığı yerel zaman, epoch ms.
    pub at_ms: i64,
    pub items: Vec<SymbolItem>,
}

// ---------------------------------------------------------------------------
// Hatalar
// ---------------------------------------------------------------------------

/// Replay yükleme hatası.
///
/// Her durum AÇIKÇA bildirilir; bozuk veya eksik bir kaydın sessizce boş
/// akışa dönüşmesi, "sistem çalışıyor ama piyasa hareketsiz" yanılsaması
/// üretirdi.
#[derive(Debug)]
pub enum ReplayError {
    /// `--data-dir` yok ya da dizin değil.
    NoDataDir(PathBuf),
    /// Dizinde bu tarihe ait hiçbir örnek kaydı yok.
    NoRecording { dir: PathBuf, date: String },
    /// İstenen gün ARALIĞINDA tek bir gün kaydı bile yok.
    ///
    /// Tek tek eksik günler (hafta sonu, tatil) sessizce atlanır; aralığın
    /// TAMAMEN boş olması ise yazım hatası ya da yanlış dizin demektir ve
    /// sessizce boş akışa dönüşmemelidir.
    NoRecordingInRange { dir: PathBuf, start: String, end: Option<String> },
    /// Tick dosyası okunamadı.
    Io { path: PathBuf, err: io::Error },
    /// Tick dosyası var ama tek bir tam kayıt bile içermiyor.
    EmptyTicks { path: PathBuf, len: u64 },
    /// Sembol tablosu dosyası yok — tick'ler isimlendirilemez.
    MissingSymbols { path: PathBuf },
    /// Sembol dosyası var ama tek bir geçerli satır bile yok.
    EmptySymbols { path: PathBuf },
    /// Sembol dosyasında bozuk satır.
    BadSymbolsLine { path: PathBuf, line: usize, err: String },
    /// `--replay-date` biçimi YYYYMMDD değil.
    BadDate(String),
    /// `--replay-date` ne tek gün ne de tanınan bir aralık biçimi.
    BadDateSpec(String),
    /// Aralığın başlangıcı bitişinden sonra.
    BadDateRange { start: String, end: String },
    /// `--replay-from` / `--replay-to` biçimi HH:MM değil.
    BadTime(String),
    /// `--replay-speed` negatif veya sonlu değil.
    BadSpeed(f64),
    /// `from >= to`.
    BadWindow { from: String, to: String },
    /// İstenen aralıkta tek bir tick bile yok.
    EmptyWindow { first_ms: i64, last_ms: i64, from_ms: i64, to_ms: i64 },
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoDataDir(p) => {
                write!(f, "kayit dizini yok ya da dizin degil: {}", p.display())
            }
            Self::NoRecording { dir, date } => write!(
                f,
                "{} altinda {date} tarihli kayit bulunamadi \
                 (beklenen: <ornek>/ticks-{date}.bin)",
                dir.display()
            ),
            Self::NoRecordingInRange { dir, start, end } => write!(
                f,
                "{} altinda {}..{} araliginda TEK BIR gun kaydi bile yok \
                 (beklenen: <ornek>/ticks-YYYYMMDD.bin)",
                dir.display(),
                start,
                end.as_deref().unwrap_or("(kayittaki son gun)"),
            ),
            Self::Io { path, err } => write!(f, "{} okunamadi: {err}", path.display()),
            Self::EmptyTicks { path, len } => write!(
                f,
                "{} tek bir tam kayit bile icermiyor ({len} bayt, kayit boyu {TICK_REC_SIZE})",
                path.display()
            ),
            Self::MissingSymbols { path } => write!(
                f,
                "sembol tablosu yok: {} — tick'ler isimlendirilemez, \
                 replay bos akisa donusurdu",
                path.display()
            ),
            Self::EmptySymbols { path } => {
                write!(f, "{} bos: tek bir sembol tablosu goruntusu yok", path.display())
            }
            Self::BadSymbolsLine { path, line, err } => {
                write!(f, "{}:{line} bozuk sembol satiri: {err}", path.display())
            }
            Self::BadDate(s) => write!(f, "--replay-date YYYYMMDD olmali, verilen: {s:?}"),
            Self::BadDateSpec(s) => write!(
                f,
                "--replay-date bicimi anlasilamadi: {s:?}. Kabul edilenler: \
                 20260513 (tek gun), 20260513-20260812 (aralik, IKISI DE DAHIL), \
                 20260513- (o gunden kayitta ne varsa sonuna kadar)"
            ),
            Self::BadDateRange { start, end } => write!(
                f,
                "--replay-date araliginin baslangici bitisinden SONRA: {start}-{end}. \
                 Gunler kronolojik oynatilir, ters aralik oynatilamaz."
            ),
            Self::BadTime(s) => write!(f, "saat HH:MM olmali (UTC), verilen: {s:?}"),
            Self::BadSpeed(v) => write!(
                f,
                "--replay-speed negatif olamaz (0 = beklemeden en hizli), verilen: {v}"
            ),
            Self::BadWindow { from, to } => {
                write!(f, "--replay-from {from} ile --replay-to {to} arasinda zaman yok")
            }
            Self::EmptyWindow { first_ms, last_ms, from_ms, to_ms } => write!(
                f,
                "istenen aralikta ({}..{}) tek bir tick yok; kayit {}..{} araligini kapsiyor",
                hhmmss(*from_ms),
                hhmmss(*to_ms),
                hhmmss(*first_ms),
                hhmmss(*last_ms),
            ),
        }
    }
}

impl std::error::Error for ReplayError {}

// ---------------------------------------------------------------------------
// Tarih / saat (UTC) — dış bağımlılık olmadan
// ---------------------------------------------------------------------------

/// `YYYYMMDD` → o günün UTC gece yarısı, epoch ms.
pub fn parse_yyyymmdd(s: &str) -> Result<i64, ReplayError> {
    let bad = || ReplayError::BadDate(s.to_owned());
    if s.len() != 8 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let y: i64 = s[0..4].parse().map_err(|_| bad())?;
    let m: u32 = s[4..6].parse().map_err(|_| bad())?;
    let d: u32 = s[6..8].parse().map_err(|_| bad())?;
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return Err(bad());
    }
    Ok(days_from_civil(y, m, d) * DAY_MS)
}

/// `--replay-date` değerinin çözülmüş hâli.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateSpec {
    /// `20260513` — tek gün.
    Day(String),
    /// `20260513-20260812` (ikisi de dahil) ya da `20260513-` (sona kadar).
    Range { start: String, end: Option<String> },
}

impl DateSpec {
    /// Aralığın (ya da tek günün) başlangıç damgası.
    pub fn start(&self) -> &str {
        match self {
            Self::Day(d) => d,
            Self::Range { start, .. } => start,
        }
    }

    pub fn is_range(&self) -> bool {
        matches!(self, Self::Range { .. })
    }
}

/// `--replay-date` değerini çöz.
///
/// ```text
/// 20260513              tek gün
/// 20260513-20260812     aralık, İKİSİ DE DAHİL
/// 20260513-             o günden kayıtta ne varsa sonuna kadar
/// ```
///
/// Tek gün biçimi [`DateSpec::Day`] verir ve davranışı **aynen korunur**:
/// aralık desteği eklemek, tek günlük replay'i tek satır bile değiştirmemeli.
///
/// Ayırıcı `-` seçildi çünkü `YYYYMMDD` içinde `-` yoktur; `2026-08-11` yazan
/// biri üç parça üretir ve AÇIK bir hata alır. Sessizce ilk parçayı gün
/// saymak, istenmeyen bir günü oynatmak olurdu.
pub fn parse_date_spec(s: &str) -> Result<DateSpec, ReplayError> {
    let bad = || ReplayError::BadDateSpec(s.to_owned());
    let Some((a, b)) = s.split_once('-') else {
        // Tek gün: hatanın kendisi `BadDate` kalmalı — mevcut mesaj ve
        // mevcut testler bunu bekliyor.
        parse_yyyymmdd(s)?;
        return Ok(DateSpec::Day(s.to_owned()));
    };
    if b.contains('-') {
        return Err(bad());
    }
    let start_ms = parse_yyyymmdd(a).map_err(|_| bad())?;
    if b.is_empty() {
        return Ok(DateSpec::Range { start: a.to_owned(), end: None });
    }
    let end_ms = parse_yyyymmdd(b).map_err(|_| bad())?;
    if end_ms < start_ms {
        return Err(ReplayError::BadDateRange { start: a.to_owned(), end: b.to_owned() });
    }
    Ok(DateSpec::Range { start: a.to_owned(), end: Some(b.to_owned()) })
}

/// `HH:MM` → gece yarısından itibaren milisaniye.
///
/// `24:00` kabul edilir: gün sonunu kapsayan bir üst sınır yazabilmek için.
pub fn parse_hhmm(s: &str) -> Result<i64, ReplayError> {
    let bad = || ReplayError::BadTime(s.to_owned());
    let (h, m) = s.split_once(':').ok_or_else(bad)?;
    if h.len() != 2 || m.len() != 2 {
        return Err(bad());
    }
    let h: i64 = h.parse().map_err(|_| bad())?;
    let m: i64 = m.parse().map_err(|_| bad())?;
    if !(0..=24).contains(&h) || !(0..=59).contains(&m) || (h == 24 && m != 0) {
        return Err(bad());
    }
    Ok(h * 3_600_000 + m * 60_000)
}

/// Epoch ms → `HH:MM:SS.mmm` (UTC). Yalnızca hata/teşhis metinleri için.
fn hhmmss(ms: i64) -> String {
    let of = ms.rem_euclid(DAY_MS);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        of / 3_600_000,
        (of / 60_000) % 60,
        (of / 1_000) % 60,
        of % 1_000
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Howard Hinnant'ın `days_from_civil` algoritması: takvim → 1970-01-01'den
/// itibaren gün sayısı. Tarih kütüphanesi eklemek yerine 15 satır: bu ikilinin
/// bağımlılık yüzeyi bilinçli olarak dar.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// Yükleme
// ---------------------------------------------------------------------------

/// Replay bayrakları.
#[derive(Debug, Clone)]
pub struct ReplayOpts {
    /// `--replay <data-dir>`
    pub data_dir: PathBuf,
    /// `--replay-date` HAM değeri (zorunlu); [`parse_date_spec`] çözer.
    ///
    /// `20260513` | `20260513-20260812` | `20260513-`
    pub date: String,
    /// `--replay-from HH:MM` (UTC).
    ///
    /// Aralıkla birlikte verilirse **HER GÜNE ayrı ayrı** uygulanır.
    pub from: Option<String>,
    /// `--replay-to HH:MM` (UTC) — aralıkta HER GÜNE uygulanır.
    pub to: Option<String>,
    /// `--replay-speed N`; 1.0 = gerçek zamanlı, 0 = beklemeden en hızlı.
    pub speed: f64,
}

impl ReplayOpts {
    pub fn new(data_dir: impl Into<PathBuf>, date: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            date: date.into(),
            from: None,
            to: None,
            speed: 1.0,
        }
    }
}

/// Oynatma kuyruğundaki tek öğe.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlayItem {
    /// [`Recording::instances`] içindeki indeks — **birleşik listede**, o
    /// günün kendi listesinde değil. Örnek kümesi günden güne değişebilir ve
    /// yerel indeks kullanmak, ikinci günde tick'i YANLIŞ örneğe yazardı.
    pub inst: usize,
    pub rec: TickRec,
}

/// Tek bir günün belleğe alınmış kaydı.
///
/// Bir seferde **yalnızca bir tanesi** yaşar: 66 günlük bir aralık 1,15 GB
/// ve 25,8 milyon kayıttır, hepsini birden RAM'e almak kabul edilemez. Gün
/// bitince bu yapı düşer ve belleği geri verir.
#[derive(Debug)]
pub struct DayData {
    /// `YYYYMMDD`.
    pub date: String,
    /// O günün oynatılacak kayıtları — **kayıt sırası korunmuş**.
    pub items: Vec<PlayItem>,
    /// [`Recording::instances`] ile HİZALI sembol tablosu görüntüleri; o gün
    /// kaydı olmayan örnek için boş.
    ///
    /// Tablo her gün için yeniden yüklenir: sembol kimlikleri günler arasında
    /// DEĞİŞEBİLİR ve tick'i eski günün tablosuyla isimlendirmek yanlış sembol
    /// adı yaymak olurdu.
    pub symbols: Vec<Vec<SymbolSnapshot>>,
    /// Bu güne uygulanan pencere (yarı açık: `[from, to)`), epoch ms.
    pub window_from_ms: i64,
    pub window_to_ms: i64,
    /// Pencereye KIRPILMADAN önceki gerçek kapsam; boş pencere hatasında
    /// kullanıcıya "kayıt aslında şu aralığı içeriyor" diyebilmek için.
    /// Kayıt boşsa sırasıyla `i64::MAX` / `i64::MIN`.
    pub raw_first_ms: i64,
    pub raw_last_ms: i64,
}

impl DayData {
    fn new(
        date: String,
        items: Vec<PlayItem>,
        symbols: Vec<Vec<SymbolSnapshot>>,
        window_from_ms: i64,
        window_to_ms: i64,
        raw_first_ms: i64,
        raw_last_ms: i64,
    ) -> Self {
        #[cfg(test)]
        probe::enter(items.len());
        Self { date, items, symbols, window_from_ms, window_to_ms, raw_first_ms, raw_last_ms }
    }
}

/// Aynı anda kaç günün belleğe alındığını ölçen sayaç — **yalnızca test**.
///
/// "66 günü birden yüklemiyoruz" bir yorum değil, sınanması gereken bir
/// özellik: `Recording`e bir `items` alanı eklemek tüm testleri yeşil
/// bırakırdı ve bellek patlaması ancak üretimde görülürdü.
///
/// Sayaçlar THREAD YEREL: testler paralel koşar ve süreç geneli bir sayaç
/// komşu testin yüklediği günleri sayardı. Oynatım senkrondur, yani bir
/// günün yüklenmesi ve düşmesi hep aynı thread'de olur.
#[cfg(test)]
pub(crate) mod probe {
    use std::cell::Cell;

    thread_local! {
        static DAYS_LIVE: Cell<usize> = const { Cell::new(0) };
        static DAYS_PEAK: Cell<usize> = const { Cell::new(0) };
        static TICKS_LIVE: Cell<usize> = const { Cell::new(0) };
        static TICKS_PEAK: Cell<usize> = const { Cell::new(0) };
    }

    pub(crate) fn enter(ticks: usize) {
        DAYS_LIVE.with(|c| {
            c.set(c.get() + 1);
            DAYS_PEAK.with(|p| p.set(p.get().max(c.get())));
        });
        TICKS_LIVE.with(|c| {
            c.set(c.get() + ticks);
            TICKS_PEAK.with(|p| p.set(p.get().max(c.get())));
        });
    }

    pub(crate) fn leave(ticks: usize) {
        DAYS_LIVE.with(|c| c.set(c.get().saturating_sub(1)));
        TICKS_LIVE.with(|c| c.set(c.get().saturating_sub(ticks)));
    }

    pub(crate) fn reset() {
        DAYS_LIVE.with(|c| c.set(0));
        DAYS_PEAK.with(|c| c.set(0));
        TICKS_LIVE.with(|c| c.set(0));
        TICKS_PEAK.with(|c| c.set(0));
    }

    /// `(aynı anda en çok kaç gün, aynı anda en çok kaç tick)`.
    pub(crate) fn peak() -> (usize, usize) {
        (DAYS_PEAK.with(Cell::get), TICKS_PEAK.with(Cell::get))
    }
}

#[cfg(test)]
impl Drop for DayData {
    fn drop(&mut self) {
        probe::leave(self.items.len());
    }
}

/// Oynatılacak GÜNLERİN planı — tick'ler burada DEĞİL.
///
/// Adı hâlâ `Recording`: tüketicinin gördüğü şey bir kaydın oynatılması ve
/// tek gün ile aralık arasında tel üzerinde hiçbir fark yok. Değişen tek şey,
/// bu yapının artık tick'lerin KENDİSİNİ değil nerede olduklarını tutması —
/// aralığın tamamını belleğe almamak için (bkz. [`DayData`]).
#[derive(Debug)]
pub struct Recording {
    /// Aralıktaki TÜM günlerde görülen örnek adları — birleşik ve sıralı.
    ///
    /// Birleşik olmak zorunda: bir örnek aralığın ortasında kayda başlamış
    /// olabilir ve `hello.instances` ile [`PlayItem::inst`] oynatım boyunca
    /// SABİT bir listeye bakmalı.
    pub instances: Vec<String>,
    /// Oynatılacak günler (`YYYYMMDD`), **kronolojik**. Kayıtta olmayan
    /// günler burada zaten yoktur.
    pub days: Vec<String>,
    /// Gerçekten kapsanan aralık — `hello.replay_from_ms` / `replay_to_ms`.
    /// TÜM aralığı kapsar: ilk dolu günün ilk tick'i, son dolu günün sonuncusu.
    pub covered_from_ms: i64,
    pub covered_to_ms: i64,
    /// `--replay-speed`.
    pub speed: f64,
    /// `--replay-date`in çözülmüş hâli (tanılama ve açılış çıktısı için).
    pub spec: DateSpec,
    data_dir: PathBuf,
    /// `--replay-from` / `--replay-to`, gece yarısından itibaren ms. HER GÜNE
    /// ayrı ayrı uygulanır.
    from_off: i64,
    to_off: i64,
    /// Gün başına, o gün kaydı olan örneklerin [`Self::instances`] indeksleri.
    day_insts: Vec<Vec<usize>>,
}

impl Recording {
    /// `hello`ya konacak kapsam bilgisi: `(replay_from_ms, replay_to_ms)`.
    pub fn span(&self) -> (i64, i64) {
        (self.covered_from_ms, self.covered_to_ms)
    }

    /// Oynatılacak gün sayısı.
    pub fn day_count(&self) -> usize {
        self.days.len()
    }

    /// Gün içi pencere, gece yarısından itibaren ms `(from, to)`.
    pub fn day_window(&self) -> (i64, i64) {
        (self.from_off, self.to_off)
    }

    /// `days[i]` gününü belleğe al.
    ///
    /// **Her çağrı diske gider ve dönen değer düştüğünde bellek geri verilir.**
    /// Oynatım günleri tek tek böyle yükler; sonucu saklamak, kaçınmaya
    /// çalıştığımız şeyin ta kendisi olurdu.
    pub fn load_day(&self, i: usize) -> Result<DayData, ReplayError> {
        let date = &self.days[i];
        let day_ms = parse_yyyymmdd(date)?;
        let window_from_ms = day_ms + self.from_off;
        let window_to_ms = day_ms + self.to_off;

        let n = self.instances.len();
        let mut per_inst: Vec<Vec<TickRec>> = vec![Vec::new(); n];
        let mut symbols: Vec<Vec<SymbolSnapshot>> = vec![Vec::new(); n];
        let mut raw_first_ms = i64::MAX;
        let mut raw_last_ms = i64::MIN;

        for &g in &self.day_insts[i] {
            let inst = &self.instances[g];
            let recs = load_ticks(&ticks_path(&self.data_dir, inst, date))?;
            // Sembol tablosu O GÜNÜN dosyasından: kimlikler günler arasında
            // değişebilir.
            symbols[g] = load_symbols(&symbols_path(&self.data_dir, inst, date))?;
            for r in &recs {
                raw_first_ms = raw_first_ms.min(r.recv_ms);
                raw_last_ms = raw_last_ms.max(r.recv_ms);
            }
            // Pencereye kırp. Yarı açık `[from, to)`: iki bitişik pencere
            // böylece örtüşmeden döşenir.
            per_inst[g] = recs
                .into_iter()
                .filter(|r| r.recv_ms >= window_from_ms && r.recv_ms < window_to_ms)
                .collect();
        }

        let items = merge_by_recv_ms(&per_inst);
        Ok(DayData::new(
            date.clone(),
            items,
            symbols,
            window_from_ms,
            window_to_ms,
            raw_first_ms,
            raw_last_ms,
        ))
    }

    /// SON günün sembol tabloları — açılıştaki `contract_size` uyarısı için.
    ///
    /// Tüm aralık taranmıyor: 66 günün tablosunu açılışta okumak, kaydın
    /// tamamına dokunmak olurdu. Uyarı zaten "kaydın son hâli" hakkında.
    pub fn symbols_of_last_day(&self) -> Result<Vec<Vec<SymbolSnapshot>>, ReplayError> {
        let Some(i) = self.days.len().checked_sub(1) else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        for &g in &self.day_insts[i] {
            out.push(load_symbols(&symbols_path(
                &self.data_dir,
                &self.instances[g],
                &self.days[i],
            ))?);
        }
        Ok(out)
    }

    /// Kapsanan aralığı bul: ilk DOLU günün ilk tick'i, son DOLU günün son
    /// tick'i.
    ///
    /// Baştan ve sondan yürünüyor, hepsi yüklenmiyor: tipik durumda iki gün
    /// okunur ve ikisi de hemen bırakılır. Pencere yüzünden boş kalan günler
    /// atlanır — `--replay-from 09:00` verilen bir aralıkta ilk günün 09:00
    /// öncesi bitmiş olması, kapsamın o günden başladığını söylemeyi
    /// gerektirmez.
    fn compute_span(&self) -> Result<(i64, i64), ReplayError> {
        let mut raw_first_ms = i64::MAX;
        let mut raw_last_ms = i64::MIN;
        let mut front: Option<(usize, i64, i64)> = None; // (gun, ilk, son)
        for i in 0..self.days.len() {
            let d = self.load_day(i)?;
            raw_first_ms = raw_first_ms.min(d.raw_first_ms);
            raw_last_ms = raw_last_ms.max(d.raw_last_ms);
            if let (Some(f), Some(l)) = (d.items.first(), d.items.last()) {
                front = Some((i, f.rec.recv_ms, l.rec.recv_ms));
                break;
            }
        }
        let Some((fi, from_ms, mut to_ms)) = front else {
            // Buraya gelindiyse TÜM günler yüklendi; kaydın gerçek kapsamı
            // elimizde ve kullanıcıya söylenmeli.
            let day0 = parse_yyyymmdd(&self.days[0])?;
            return Err(ReplayError::EmptyWindow {
                first_ms: raw_first_ms,
                last_ms: raw_last_ms,
                from_ms: day0 + self.from_off,
                to_ms: day0 + self.to_off,
            });
        };
        for i in (fi + 1..self.days.len()).rev() {
            let d = self.load_day(i)?;
            if let Some(l) = d.items.last() {
                to_ms = l.rec.recv_ms;
                break;
            }
        }
        Ok((from_ms, to_ms))
    }
}

// Ad deseni burada TEKRARLANMAZ — `record` modulu tek kaynaktir. Yazan ve
// okuyan taraf ayni fonksiyonu cagirmazsa, biri degisip digeri unutuldugunda
// yazici bir dosyaya yazip okuyucu baskasini arar ve bu hicbir derleme hatasi
// vermeden "kayit bos" olarak gorunur.
pub use crate::record::{symbols_path_str as symbols_path, tick_path_str as ticks_path};

/// Kayıt dizinini, verilen tarihe ait tick dosyası olan örnekler için tara.
///
/// Sonuç **sıralı**: örnek sırası belirlenimciliğin parçası (eşit `recv_ms`
/// taşıyan kayıtlarda birleştirme sırasını bu belirler).
pub fn discover_instances(data_dir: &Path, date: &str) -> Result<Vec<String>, ReplayError> {
    if !data_dir.is_dir() {
        return Err(ReplayError::NoDataDir(data_dir.to_path_buf()));
    }
    let mut out = Vec::new();
    for name in subdirs(data_dir)? {
        if ticks_path(data_dir, &name, date).is_file() {
            out.push(name);
        }
    }
    out.sort();
    if out.is_empty() {
        return Err(ReplayError::NoRecording {
            dir: data_dir.to_path_buf(),
            date: date.to_owned(),
        });
    }
    Ok(out)
}

/// Kayıt kökündeki alt dizinler (= örnek adayları), keşif sırasında.
fn subdirs(data_dir: &Path) -> Result<Vec<String>, ReplayError> {
    let rd = fs::read_dir(data_dir)
        .map_err(|err| ReplayError::Io { path: data_dir.to_path_buf(), err })?;
    let mut out = Vec::new();
    for ent in rd {
        let ent = ent.map_err(|err| ReplayError::Io { path: data_dir.to_path_buf(), err })?;
        if !ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let Some(name) = ent.file_name().to_str().map(str::to_owned) else { continue };
        out.push(name);
    }
    Ok(out)
}

/// Bu güne ait, EN AZ bir tam kayıt içeren tick dosyası olan örnekler.
///
/// **Yalnızca aralık kipinde** kullanılır. Hafta sonu ve tatillerde dosya ya
/// hiç yoktur ya da 0 bayttır; bunlar sessizce atlanmalı, yoksa 3 aylık bir
/// backtest ilk cumartesinde hata verip dururdu.
///
/// Tek gün kipinde bu süzgeç YOKTUR: orada boş bir dosya hâlâ AÇIK bir
/// hatadır, çünkü kullanıcı tam olarak o günü istemiştir ve sessizce boş
/// akış almak istediği son şeydir.
fn instances_with_data(data_dir: &Path, date: &str) -> Result<Vec<String>, ReplayError> {
    let mut out = Vec::new();
    for name in subdirs(data_dir)? {
        let p = ticks_path(data_dir, &name, date);
        let Ok(md) = fs::metadata(&p) else { continue };
        if !md.is_file() || md.len() == 0 {
            continue;
        }
        if (md.len() as usize) < TICK_REC_SIZE {
            // 0 bayt "kayıt yok" demek; 0 ile 48 arası bayt ise BOZUK bir
            // dosya demek. Atlıyoruz ama sessizce değil.
            eprintln!(
                "[replay] {}: tek bir tam kayit bile yok ({} bayt) — bu gun atlandi",
                p.display(),
                md.len()
            );
            continue;
        }
        out.push(name);
    }
    out.sort();
    Ok(out)
}

/// Kayıt dizinindeki TÜM günler (`YYYYMMDD`), kronolojik ve tekilleştirilmiş.
///
/// Takvimi gün gün yürümüyoruz, DİSKTE ne varsa onu listeliyoruz: hafta sonu
/// ve tatil böylece kendiliğinden düşer ve "kayıtta ne varsa sonuna kadar"
/// (`20260513-`) biçimi de aynı yoldan cevaplanır.
pub fn discover_days(data_dir: &Path) -> Result<Vec<String>, ReplayError> {
    if !data_dir.is_dir() {
        return Err(ReplayError::NoDataDir(data_dir.to_path_buf()));
    }
    let mut stamps: Vec<u32> = Vec::new();
    for name in subdirs(data_dir)? {
        let days = crate::record::list_days(data_dir, &name)
            .map_err(|err| ReplayError::Io { path: data_dir.join(&name), err })?;
        stamps.extend(days);
    }
    stamps.sort_unstable();
    stamps.dedup();
    Ok(stamps.into_iter().map(crate::record::date_str).collect())
}

/// Bir tick dosyasını oku.
///
/// Dosya uzunluğu **tek doğruluk kaynağıdır**: `len - (len % 48)` bayta
/// kırpılır, yani çökme anında yarım yazılmış kayıt sessizce atılır. Kırpma
/// gerçekleştiğinde `stderr`'e not düşülür — sessiz veri kaybı olmasın.
pub fn load_ticks(path: &Path) -> Result<Vec<TickRec>, ReplayError> {
    let bytes =
        fs::read(path).map_err(|err| ReplayError::Io { path: path.to_path_buf(), err })?;
    let full = bytes.len() - (bytes.len() % TICK_REC_SIZE);
    if full < TICK_REC_SIZE {
        return Err(ReplayError::EmptyTicks {
            path: path.to_path_buf(),
            len: bytes.len() as u64,
        });
    }
    if full != bytes.len() {
        eprintln!(
            "[replay] {}: son {} bayt yarim kayit — kirpildi (dosya uzunlugu tek dogruluk kaynagi)",
            path.display(),
            bytes.len() - full
        );
    }
    let mut out = Vec::with_capacity(full / TICK_REC_SIZE);
    for c in bytes[..full].chunks_exact(TICK_REC_SIZE) {
        // `chunks_exact` tam 48 bayt garanti eder; dönüşüm başarısız olamaz.
        out.push(TickRec::from_bytes(c.try_into().expect("chunks_exact 48 bayt verir")));
    }
    Ok(out)
}

/// Sembol tablosu görüntülerini oku.
///
/// Dosyanın YOKLUĞU hatadır: sembol tablosu olmadan tick'ler
/// isimlendirilemez ve replay sessizce boş bir akışa dönüşürdü.
pub fn load_symbols(path: &Path) -> Result<Vec<SymbolSnapshot>, ReplayError> {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(ReplayError::MissingSymbols { path: path.to_path_buf() })
        }
        Err(err) => return Err(ReplayError::Io { path: path.to_path_buf(), err }),
    };
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<SymbolSnapshot>(line) {
            Ok(s) => out.push(s),
            Err(err) => {
                return Err(ReplayError::BadSymbolsLine {
                    path: path.to_path_buf(),
                    line: i + 1,
                    err: err.to_string(),
                })
            }
        }
    }
    if out.is_empty() {
        return Err(ReplayError::EmptySymbols { path: path.to_path_buf() });
    }
    // Kayıt sırası zaten artan olmalı; garanti etmek ucuz ve "o ana kadarki
    // SON satır" kuralını sağlamlaştırıyor.
    out.sort_by_key(|s| s.at_ms);
    Ok(out)
}

/// Oynatılacak günleri çöz, doğrula ve kapsamı ölç.
///
/// Bu fonksiyon **tick yüklemeyi bitirmez**: yalnızca gün listesini kurar ve
/// kapsamı ölçmek için gereken kadarını (ilk ve son dolu gün) okuyup bırakır.
/// Aralığın tamamı belleğe ancak oynatım sırasında, gün gün girer.
///
/// Diskle konuşmadan önce yakalanan hatalar (biçim, ters pencere, negatif
/// hız) burada da yakalanır: operatörün yazım hatasını dosya sisteminden
/// dönmesini beklemeden görmesi gerekir.
pub fn load(opts: &ReplayOpts) -> Result<Recording, ReplayError> {
    if !opts.speed.is_finite() || opts.speed < 0.0 {
        return Err(ReplayError::BadSpeed(opts.speed));
    }
    let spec = parse_date_spec(&opts.date)?;
    let from_off = match &opts.from {
        Some(s) => parse_hhmm(s)?,
        None => 0,
    };
    let to_off = match &opts.to {
        Some(s) => parse_hhmm(s)?,
        None => DAY_MS,
    };
    if from_off >= to_off {
        return Err(ReplayError::BadWindow {
            from: opts.from.clone().unwrap_or_else(|| "00:00".into()),
            to: opts.to.clone().unwrap_or_else(|| "24:00".into()),
        });
    }

    // --- hangi günler, her günde hangi örnekler ---
    let (days, per_day_names) = match &spec {
        // Tek gün: davranış AYNEN korunuyor. Eksik kayıt, boş dosya ve eksik
        // sembol tablosu burada hâlâ AÇIK hatadır — kullanıcı tam olarak bu
        // günü istedi.
        DateSpec::Day(d) => (vec![d.clone()], vec![discover_instances(&opts.data_dir, d)?]),
        DateSpec::Range { start, end } => {
            let lo = parse_yyyymmdd(start)?;
            let hi = match end {
                Some(e) => Some(parse_yyyymmdd(e)?),
                None => None,
            };
            let mut days = Vec::new();
            let mut names = Vec::new();
            for d in discover_days(&opts.data_dir)? {
                let Ok(ms) = parse_yyyymmdd(&d) else { continue };
                if ms < lo || hi.is_some_and(|h| ms > h) {
                    continue;
                }
                let insts = instances_with_data(&opts.data_dir, &d)?;
                // Hafta sonu / tatil / 0 baytlık gün: SESSİZCE atlanır.
                if insts.is_empty() {
                    continue;
                }
                days.push(d);
                names.push(insts);
            }
            if days.is_empty() {
                return Err(ReplayError::NoRecordingInRange {
                    dir: opts.data_dir.clone(),
                    start: start.clone(),
                    end: end.clone(),
                });
            }
            (days, names)
        }
    };

    // Örnek listesi TÜM günlerin birleşimi: bir örnek aralığın ortasında
    // kayda başlamış olabilir ve `PlayItem::inst` oynatım boyunca sabit bir
    // listeye bakmalı.
    let mut instances: Vec<String> = per_day_names.iter().flatten().cloned().collect();
    instances.sort();
    instances.dedup();
    let day_insts: Vec<Vec<usize>> = per_day_names
        .iter()
        .map(|ns| {
            ns.iter()
                .map(|n| {
                    instances.iter().position(|k| k == n).expect("birlesim listesi tum adlari icerir")
                })
                .collect()
        })
        .collect();

    let mut rec = Recording {
        instances,
        days,
        covered_from_ms: 0,
        covered_to_ms: 0,
        speed: opts.speed,
        spec,
        data_dir: opts.data_dir.clone(),
        from_off,
        to_off,
        day_insts,
    };

    // Sembol tabloları ŞİMDİ doğrulanıyor, oynatımın ortasında değil.
    // Dosyalar küçüktür (jsonl) ve 66 tanesini okumak ucuzdur; buna karşılık
    // 40. günde patlayan bir replay, saatler süren bir testi çöpe atardı.
    // Tick'ler burada OKUNMAZ: asıl hacim onlarda.
    for (i, date) in rec.days.iter().enumerate() {
        for &g in &rec.day_insts[i] {
            load_symbols(&symbols_path(&rec.data_dir, &rec.instances[g], date))?;
        }
    }

    let (from_ms, to_ms) = rec.compute_span()?;
    rec.covered_from_ms = from_ms;
    rec.covered_to_ms = to_ms;
    Ok(rec)
}

/// Örnek dosyalarını `recv_ms`'e göre k-yollu birleştir.
///
/// **Sıralama değil, birleştirme**: her dosyanın kendi iç sırası aynen
/// korunur (kayıt sırası oynatımın sözleşmesi). Eşit `recv_ms`'te düşük
/// örnek indeksi önce gelir — örnek listesi sıralı olduğu için bu da
/// belirlenimci.
///
/// Tek örnek varsa sonuç dosyanın kendisidir; saat geriye sıçramış olsa bile
/// kayıtlar YENİDEN SIRALANMAZ.
fn merge_by_recv_ms(per_inst: &[Vec<TickRec>]) -> Vec<PlayItem> {
    let total: usize = per_inst.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(total);
    if per_inst.len() == 1 {
        out.extend(per_inst[0].iter().map(|r| PlayItem { inst: 0, rec: *r }));
        return out;
    }
    let mut cur = vec![0usize; per_inst.len()];
    for _ in 0..total {
        let mut best: Option<usize> = None;
        for (i, recs) in per_inst.iter().enumerate() {
            let Some(r) = recs.get(cur[i]) else { continue };
            match best {
                Some(b) if per_inst[b][cur[b]].recv_ms <= r.recv_ms => {}
                _ => best = Some(i),
            }
        }
        let Some(i) = best else { break };
        out.push(PlayItem { inst: i, rec: per_inst[i][cur[i]] });
        cur[i] += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Oynatım
// ---------------------------------------------------------------------------

/// Çalışan (veya bitmiş) bir replay.
pub struct ReplayHandle {
    /// Gönderen taraf burada da tutuluyor: oynatım thread'i bitince kanal
    /// KAPANMASIN. Kapanmış bir `watch` üzerinde `changed()` hemen hata
    /// döner ve bunu bir `select!` kolunda bekleyen sunucu boş dönerdi.
    done_tx: Arc<watch::Sender<Option<ReplayEnd>>>,
    done: watch::Receiver<Option<ReplayEnd>>,
    covered: (i64, i64),
    gate: Arc<StartGate>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl ReplayHandle {
    /// Bitiş bildirimi kanalı — `server::Replay::done` bunu alır.
    ///
    /// `watch` seçildi, yayın kanalı değil: oynatım bir istemci bağlanmadan
    /// önce bitmiş olabilir ve o istemci de `replay_done` görmelidir. Yayın
    /// kanalı geçmişi tutmadığı için bu bilgiyi KAYBEDERDİ.
    pub fn done(&self) -> watch::Receiver<Option<ReplayEnd>> {
        self.done.clone()
    }

    /// `hello.replay_from_ms` / `replay_to_ms`.
    pub fn covered_span(&self) -> (i64, i64) {
        self.covered
    }

    /// Oynatımı başlatan kapı — sunucu ilk istemcide açar.
    pub fn gate(&self) -> Arc<StartGate> {
        self.gate.clone()
    }

    /// Oynatım bitti mi.
    pub fn finished(&self) -> bool {
        self.done.borrow().is_some()
    }

    /// Oynatımın bitmesini bekle (testler ve `--replay-speed 0` için).
    ///
    /// Kapı henüz açılmadıysa BURADA açılır. "Bitmesini bekle" diyen bir
    /// çağıranın oynatımın hiç başlamamış olmasını kastetmesi mümkün değil;
    /// kapıyı açmadan beklemek sonsuza kadar asılı kalmak olurdu ve bu,
    /// bulunması pahalı bir kilitlenme sınıfıdır.
    pub fn join(mut self) {
        self.gate.open();
        if let Some(h) = self.join.take() {
            let _ = h.join();
        }
    }
}

/// Oynatımı **ilk istemci bağlanana kadar** bekleten kapı.
///
/// Bu kapı olmadan oynatım süreç başlar başlamaz akardı ve yayın kanalının o
/// anda hiç abonesi olmadığı için tick'ler BOŞLUĞA yayılırdı. Sonuç, modülün
/// kendi kullanım örneğinde (`--replay-speed 0`) görülebiliyordu: istemci
/// bağlandığında oynatım çoktan bitmiş oluyor, `replay_done` geliyor ve
/// **hiç tick gelmiyordu**. Kaydı aynı protokolden yeniden oynatmanın tüm
/// amacı tüketiciye o tick'leri vermek olduğu için bu, özelliği sessizce
/// işlevsiz bırakan bir kusurdu.
///
/// Kapı `Condvar` ile kurulu: açan taraf (sunucunun bağlantı yolu) asla
/// bloklanmaz, bekleyen taraf (oynatım thread'i) meşgul döngü kurmaz.
#[derive(Debug)]
pub struct StartGate {
    opened: Mutex<bool>,
    cv: std::sync::Condvar,
}

impl Default for StartGate {
    fn default() -> Self {
        Self::new()
    }
}

impl StartGate {
    pub fn new() -> Self {
        Self { opened: Mutex::new(false), cv: std::sync::Condvar::new() }
    }

    /// Kapıyı aç. Tekrar çağrılması zararsızdır (her istemci çağırır).
    pub fn open(&self) {
        let mut g = self.opened.lock().unwrap_or_else(|e| e.into_inner());
        if !*g {
            *g = true;
            self.cv.notify_all();
        }
    }

    pub fn is_open(&self) -> bool {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Kapı açılana kadar blokla.
    fn wait(&self) {
        let mut g = self.opened.lock().unwrap_or_else(|e| e.into_inner());
        while !*g {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Replay'i kendi thread'inde başlat.
///
/// Canlı okuyucu gibi ayrı bir OS thread'i kullanıyor: tempo `sleep` ile
/// tutuluyor ve bu uykuların tokio çalışma zamanının görev havuzunu meşgul
/// etmesi için hiçbir sebep yok.
///
/// Thread hemen başlar ama **oynatım [`StartGate`] açılana kadar beklemez**:
/// ilk istemci bağlanmadan tek bir tick bile yayılmaz.
pub fn spawn(
    rec: Recording,
    registry: Arc<Registry>,
    candles: Arc<Mutex<CandleStore>>,
    tx: broadcast::Sender<FeedEvent>,
    done: watch::Sender<Option<ReplayEnd>>,
    sim: Option<Arc<crate::server::SimExec>>,
) -> ReplayHandle {
    let covered = rec.span();
    let done_tx = Arc::new(done);
    let rx = done_tx.subscribe();
    let engine = done_tx.clone();
    let gate = Arc::new(StartGate::new());
    let thread_gate = gate.clone();
    let join = std::thread::Builder::new()
        .name("sinyal-replay".into())
        .spawn(move || {
            thread_gate.wait();
            play(&rec, &registry, &candles, &tx, &engine, sim.as_deref());
        })
        .expect("replay thread'i baslatilamadi");
    ReplayHandle { done_tx, done: rx, covered, gate, join: Some(join) }
}

/// Kaydı oynat. Bu fonksiyon **senkron**dur ve çağıran thread'i tutar.
///
/// Yayılan olaylar canlı okuyucununkiyle birebir aynıdır (bkz.
/// `source::reader_loop`): önce sembol tablosu güncellenir, sonra son fiyat
/// yazılır, sonra kapanan barlar, en sonda tick.
///
/// Bitince `done` kanalına **tam bir kez** özet yazılır; bağlantı DÜŞÜRÜLMEZ,
/// akış son durumda kalır.
///
/// `sim` verilirse motor **her tick'le** beslenir; bekleyen emirler ve SL/TP
/// ancak böyle tetiklenir. Besleme yayın kanalına abone bir görevle DEĞİL,
/// tam burada yapılıyor: `broadcast` geride kalınca mesaj DÜŞÜRÜR ve düşen
/// bir tick, tetiklenmeyen bir stop demektir. Buradan beslemek aynı zamanda
/// belirlenimciliği koruyor — motorun gördüğü tick dizisi kaydın kendisidir,
/// sunucunun o anki yüküne bağlı değildir.
pub fn play(
    rec: &Recording,
    registry: &Registry,
    candles: &Mutex<CandleStore>,
    tx: &broadcast::Sender<FeedEvent>,
    done: &watch::Sender<Option<ReplayEnd>>,
    sim: Option<&crate::server::SimExec>,
) -> ReplayEnd {
    let names: Vec<Arc<str>> = rec.instances.iter().map(|s| Arc::from(s.as_str())).collect();
    let anchor = Instant::now();
    // Plan tel üzerine çıkıyor: aralık yarıda kesilirse istemci `days_played
    // < days` görüp bu çalıştırmanın EKSİK olduğunu anlayabilmeli.
    let mut end = ReplayEnd { days: rec.days.len() as u32, ..Default::default() };

    // Oynatımın o ana kadar BORÇLANDIĞI sanal süre (ms, hız uygulanmadan).
    // Tek bir sayaç: gün değişince sıfırlanmaz, yoksa ikinci gün baştan
    // beklemeye başlar ve tempo her gün sınırında sıçrardı.
    let mut budget_ms: f64 = 0.0;
    let mut prev_last_recv: Option<i64> = None;

    for i in 0..rec.days.len() {
        // Gün gün: dönen `DayData` bu turun sonunda düşer ve belleği bırakır.
        let day = match rec.load_day(i) {
            Ok(d) => d,
            Err(e) => {
                // Aralığın ortasında bozulan bir kayıt SESSİZCE atlanamaz:
                // eksik günlerle devam etmek, backtest sonucunu sessizce
                // yanlış yapardı. Kalan günler oynatılmaz ve sebep yazılır.
                eprintln!("[replay] {} yuklenemedi: {e}", rec.days[i]);
                eprintln!(
                    "[replay] ARALIK YARIDA KESILDI: {} gununden sonrasi OYNATILMADI. \
                     Bu calistirmanin sonuclari EKSIK bir kaydin sonucudur.",
                    rec.days[i]
                );
                break;
            }
        };
        // Gün DİSKTEN OKUNDU: bu andan sonra oynatım o günü görmüş sayılır.
        // Sayaç burada artıyor, tick yayıldığında değil — penceresi boş kalan
        // bir gün atlanmış değil, İŞLENMİŞTİR ve `--replay-from 09:00` verilen
        // her aralığı yanlışlıkla "yarıda kesildi" diye damgalamak, gerçek
        // kesintiyi fark edilmez hâle getirirdi.
        end.days_played += 1;
        // Kayıtta olmayan ya da pencerenin dışında kalan gün: sessizce atlanır
        // (hafta sonu, tatil, `--replay-from` penceresine düşmeyen gün).
        if day.items.is_empty() {
            continue;
        }

        // Sembol imleçleri ve tablosu HER GÜN yeniden kurulur: sembol
        // kimlikleri günler arasında değişebilir.
        let mut cursor = vec![0usize; rec.instances.len()];
        seed_symbols(&day, rec, registry, &mut cursor);

        let base_ms = day.items[0].rec.recv_ms;
        // GÜNLER ARASI boşluğu kırp. Cuma 23:58 → Pazartesi 01:00 farkı ~49
        // saattir ve kırpılmazsa oynatım gerçekten 49 saat uyurdu. Gün
        // İÇİNDEKİ boşluklara dokunulmaz: onlar gerçek piyasa sessizliğidir.
        if let Some(prev) = prev_last_recv {
            budget_ms += (base_ms - prev).clamp(0, REPLAY_GAP_MAX_MS) as f64;
        }
        let day_start_ms = budget_ms;

        for item in &day.items {
            let g = item.inst;
            let inst_name = &rec.instances[g];
            let t = &item.rec;

            // Sembol tablosu bu ana kadar değiştiyse uygula — "o ana kadarki
            // SON satır" kuralı.
            apply_symbols_until(&day, rec, registry, &mut cursor, g, t.recv_ms);

            // Tempo: yerel ALIM zamanına göre. `time_msc` aynı değeri taşıyan
            // ardışık tick'ler olabilir; gerçek varış temposu yalnızca
            // `recv_ms` ile yeniden üretilebilir.
            if rec.speed > 0.0 {
                let target = (day_start_ms + (t.recv_ms - base_ms).max(0) as f64) / rec.speed;
                let due = Duration::from_secs_f64((target / 1000.0).max(0.0));
                let spent = anchor.elapsed();
                if due > spent {
                    std::thread::sleep(due - spent);
                }
            }

            // Canlıdaki kural: tablo henüz sembolü tanımıyorsa tick ATLANIR —
            // isimsiz göndermek istemciyi yanıltırdı.
            let Some(name) = registry.name_of(inst_name, t.symbol_id) else { continue };

            registry.update_last(
                inst_name,
                t.symbol_id,
                LastTick { bid: t.bid, ask: t.ask, last: t.last, time_msc: t.time_msc },
            );

            let closed = {
                let mut cs = candles.lock().unwrap_or_else(|e| e.into_inner());
                cs.on_tick(&name, t.bid, t.ask, t.time_msc)
            };
            for cb in closed {
                let _ = tx.send(FeedEvent::Candle {
                    symbol: Arc::from(cb.symbol.as_str()),
                    tf: cb.tf,
                    bar: cb.bar,
                });
            }

            let _ = tx.send(FeedEvent::Tick {
                instance: names[g].clone(),
                symbol: Arc::from(name.as_str()),
                bid: t.bid,
                ask: t.ask,
                last: t.last,
                time_msc: t.time_msc,
                // Gecikme replay'de ÖLÇÜLEMEZ: kayıtta yakalama damgası yok ve
                // uydurulmuş bir değer, gecikme ölçen bir tüketiciye yalan
                // söylerdi. 0 = "ölçüm yok".
                lat_us: 0,
            });

            // Tetiklenmeler tick'ten SONRA yayılır: istemci önce fiyatı, sonra
            // o fiyatın doğurduğu dolumu görür. Ters sıra, henüz görmediği bir
            // fiyattan dolum almış gibi görünürdü.
            if let Some(s) = sim {
                crate::server::pump_tick(s, tx, inst_name, &name, t.bid, t.ask, t.time_msc);
            }

            end.ticks += 1;
            // Yayılan SON tick'in broker saati. İsimlendirilemediği için
            // atlanan kayıtlar sayılmaz: istemciye "şuraya kadar oynattım"
            // derken göndermediğimiz bir tick'i saymak yanlış olurdu.
            end.last_ms = t.time_msc;
        }

        // Günün sonunda kalan sembol görüntülerini de uygula: gün bitiminde
        // yapılan bir tablo değişikliği `symbols` isteğine yansımalı.
        for g in 0..rec.instances.len() {
            apply_symbols_until(&day, rec, registry, &mut cursor, g, i64::MAX);
        }

        let last_recv = day.items[day.items.len() - 1].rec.recv_ms;
        budget_ms = day_start_ms + (last_recv - base_ms).max(0) as f64;
        prev_last_recv = Some(last_recv);
    }

    // TAM BİR KEZ ve ARALIĞIN SONUNDA — her gün sonunda DEĞİL. İstemci gün
    // değiştiğini fark etmemeli; `replay_done` gün başına gitseydi 66 günlük
    // bir backtest istemciye 66 kez "bitti" derdi.
    let _ = done.send(Some(end));
    end
}

/// Bir günün oynatımı başlamadan önceki tabloyu kur.
///
/// Kural "o ana kadarki SON satır"dır. Pencerenin başlangıcından önce hiç
/// satır yoksa **en eski** satır kullanılır: kaydedilmiş bir tick'in kayıt
/// anında bir adı vardı; yalnızca tablo satırı sonradan yazılmış olabilir ve
/// bu yüzden tick'leri isimsiz bırakmak veriyi çöpe atmak olurdu.
///
/// Her gün için yeniden çağrılır ve tabloyu **tamamen değiştirir**: sembol
/// kimlikleri günler arasında değişebilir ve dünün tablosunu taşımak, bugünün
/// tick'lerine yanlış sembol adı yapıştırmak olurdu.
fn seed_symbols(day: &DayData, rec: &Recording, registry: &Registry, cursor: &mut [usize]) {
    for (i, inst) in rec.instances.iter().enumerate() {
        let snaps = &day.symbols[i];
        // Bu örneğin bu gün kaydı yok: tablosuna dokunma.
        if snaps.is_empty() {
            cursor[i] = 0;
            continue;
        }
        let start = day
            .items
            .iter()
            .find(|it| it.inst == i)
            .map(|it| it.rec.recv_ms)
            .unwrap_or(day.window_from_ms);
        // Pencereden önce satır yoksa EN ESKİ tablo kullanılır (bkz. yukarıda).
        let idx = snaps.iter().rposition(|s| s.at_ms <= start).unwrap_or(0);
        registry.set_symbols(inst, snaps[idx].items.iter().map(SymbolItem::to_entry).collect());
        cursor[i] = idx + 1;
    }
}

/// `now_ms`'e kadar yayımlanmış sembol tablosu değişikliklerini uygula.
fn apply_symbols_until(
    day: &DayData,
    rec: &Recording,
    registry: &Registry,
    cursor: &mut [usize],
    inst: usize,
    now_ms: i64,
) {
    let snaps = &day.symbols[inst];
    let mut applied = None;
    while cursor[inst] < snaps.len() && snaps[cursor[inst]].at_ms <= now_ms {
        applied = Some(cursor[inst]);
        cursor[inst] += 1;
    }
    // Aynı anda birden çok değişiklik varsa yalnızca SONUNCUSU anlamlı;
    // ara tabloları `Registry`'ye basmak boş yere kilit tutardı.
    if let Some(k) = applied {
        registry.set_symbols(
            &rec.instances[inst],
            snaps[k].items.iter().map(SymbolItem::to_entry).collect(),
        );
    }
}

// ---------------------------------------------------------------------------
// Yazma yardımcıları (test ve teşhis)
// ---------------------------------------------------------------------------

/// Tick kaydını dosyaya yaz (başlık YOK, saf ardışık kayıtlar).
///
/// Üretimdeki kaydedici `record.rs`'tir; bu yardımcı okuyucunun kendi
/// biçimini doğrulayabilmesi (round-trip) ve teşhis amaçlıdır.
pub fn write_ticks(path: &Path, recs: &[TickRec]) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut buf = Vec::with_capacity(recs.len() * TICK_REC_SIZE);
    for r in recs {
        buf.extend_from_slice(&r.to_bytes());
    }
    fs::write(path, buf)
}

/// Sembol tablosu görüntülerini `jsonl` olarak yaz.
pub fn write_symbols(path: &Path, snaps: &[SymbolSnapshot]) -> io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut s = String::new();
    for snap in snaps {
        s.push_str(&serde_json::to_string(snap).map_err(io::Error::other)?);
        s.push('\n');
    }
    fs::write(path, s)
}

// ---------------------------------------------------------------------------
// Testler
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Kendini silen geçici dizin — `tempfile` bağımlılığı eklemeye değmez.
    struct Tmp(PathBuf);

    impl Tmp {
        fn new(tag: &str) -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let p = std::env::temp_dir().join(format!(
                "sinyal-replay-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).expect("gecici dizin");
            Self(p)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const DATE: &str = "20240315";
    /// 2024-03-15T00:00:00Z
    const DAY: i64 = 1_710_460_800_000;

    fn rec(recv_ms: i64, time_msc: i64, bid: f64, id: u32) -> TickRec {
        TickRec {
            recv_ms,
            time_msc,
            bid,
            ask: bid + 0.0002,
            last: 0.0,
            symbol_id: id,
            flags: sinyal_proto::tick_flag::BID | sinyal_proto::tick_flag::ASK,
            kind: sinyal_proto::kind::TICK as u16,
        }
    }

    fn item(id: u32, s: &str) -> SymbolItem {
        SymbolItem {
            id,
            s: s.to_owned(),
            digits: 5,
            point: 0.00001,
            tick_size: 0.00001,
            volume_min: 0.01,
            volume_max: 100.0,
            volume_step: 0.01,
            ready: true,
            ..Default::default()
        }
    }

    /// Tam bir günlük kayıt yaz: tick dosyası + sembol tablosu.
    fn lay_out(dir: &Path, inst: &str, ticks: &[TickRec], snaps: &[SymbolSnapshot]) {
        lay_out_on(dir, inst, DATE, ticks, snaps);
    }

    /// Aynısı ama gün seçilebilir — aralık testleri için.
    fn lay_out_on(
        dir: &Path,
        inst: &str,
        date: &str,
        ticks: &[TickRec],
        snaps: &[SymbolSnapshot],
    ) {
        write_ticks(&ticks_path(dir, inst, date), ticks).expect("tick yazilmali");
        write_symbols(&symbols_path(dir, inst, date), snaps).expect("sembol yazilmali");
    }

    /// `days[i]` gününün kayıtları — tick'ler artık plana değil güne ait.
    fn day_items(r: &Recording, i: usize) -> Vec<PlayItem> {
        r.load_day(i).expect("gun yuklenebilmeli").items.clone()
    }

    fn one_symbol_table(at_ms: i64) -> Vec<SymbolSnapshot> {
        vec![SymbolSnapshot { at_ms, items: vec![item(0, "EURUSD"), item(1, "GOLD")] }]
    }

    /// Kaydı oynat ve yalnızca `Tick` olaylarını topla.
    fn play_ticks(r: &Recording) -> (Vec<FeedEvent>, Arc<Registry>) {
        let (evs, reg, _) = play_all(r);
        (
            evs.into_iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect(),
            reg,
        )
    }

    /// Kaydı oynat; TÜM olayları, kaydı ve bitiş özetini döndür.
    fn play_all(r: &Recording) -> (Vec<FeedEvent>, Arc<Registry>, ReplayEnd) {
        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(8192);
        let (dtx, drx) = watch::channel(None);

        let end = play(r, &registry, &candles, &tx, &dtx, None);
        assert_eq!(*drx.borrow(), Some(end), "bitis kanala TAM olarak yazilmali");

        let mut evs = Vec::new();
        while let Ok(e) = rx.try_recv() {
            evs.push(e);
        }
        (evs, registry, end)
    }

    fn tick_fields(e: &FeedEvent) -> (String, String, f64, f64, i64) {
        match e {
            FeedEvent::Tick { instance, symbol, bid, ask, time_msc, .. } => {
                (instance.to_string(), symbol.to_string(), *bid, *ask, *time_msc)
            }
            other => panic!("tick beklenirdi: {other:?}"),
        }
    }

    // -- biçim ------------------------------------------------------------

    #[test]
    fn tick_record_is_exactly_48_bytes_at_the_agreed_offsets() {
        // Yerleşim dosya biçiminin KENDİSİ: kaydedici ile okuyucu ayrı ayrı
        // yazıldığı için sözleşme burada da sabitlenmeli.
        assert_eq!(std::mem::size_of::<TickRec>(), 48);
        let r = TickRec {
            recv_ms: 1,
            time_msc: 2,
            bid: 3.0,
            ask: 4.0,
            last: 5.0,
            symbol_id: 6,
            flags: 7,
            kind: 8,
        };
        let b = r.to_bytes();
        assert_eq!(i64::from_le_bytes(b[0..8].try_into().unwrap()), 1);
        assert_eq!(i64::from_le_bytes(b[8..16].try_into().unwrap()), 2);
        assert_eq!(f64::from_le_bytes(b[16..24].try_into().unwrap()), 3.0);
        assert_eq!(f64::from_le_bytes(b[24..32].try_into().unwrap()), 4.0);
        assert_eq!(f64::from_le_bytes(b[32..40].try_into().unwrap()), 5.0);
        assert_eq!(u32::from_le_bytes(b[40..44].try_into().unwrap()), 6);
        assert_eq!(u16::from_le_bytes(b[44..46].try_into().unwrap()), 7);
        assert_eq!(u16::from_le_bytes(b[46..48].try_into().unwrap()), 8);
        assert_eq!(TickRec::from_bytes(&b), r);
    }

    // -- round-trip -------------------------------------------------------

    #[test]
    fn round_trip_replays_the_exact_same_tick_sequence() {
        // ASIL NOKTA: yazılan ne ise okunan ve yayılan o olmalı — sıra, sembol
        // adı, fiyat ve BROKER zamanı dahil.
        let tmp = Tmp::new("rt");
        let written: Vec<TickRec> = (0..200i64)
            .map(|i| {
                let id = (i % 2) as u32;
                rec(DAY + 9 * 3_600_000 + i * 250, DAY + 9 * 3_600_000 + i * 250 - 37, 1.1 + i as f64 * 1e-5, id)
            })
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).expect("yuklenmeli");
        assert_eq!(day_items(&r, 0).len(), written.len(), "her kayit oynatilmali");

        let (evs, _reg) = play_ticks(&r);
        assert_eq!(evs.len(), written.len());
        for (e, w) in evs.iter().zip(&written) {
            let (inst, sym, bid, ask, ms) = tick_fields(e);
            assert_eq!(inst, "mt5-1");
            assert_eq!(sym, if w.symbol_id == 0 { "EURUSD" } else { "GOLD" });
            assert_eq!(bid, w.bid, "bid birebir donmeli");
            assert_eq!(ask, w.ask);
            // Tüketiciye giden zaman BROKER saatidir, alım zamanı değil.
            assert_eq!(ms, w.time_msc, "tuketiciye time_msc gitmeli");
        }
    }

    #[test]
    fn replayed_ticks_report_no_latency_instead_of_a_made_up_one() {
        // Kayıtta yakalama damgası yok; uydurulmuş bir gecikme, gecikme ölçen
        // tüketiciye yalan söylerdi.
        let tmp = Tmp::new("lat");
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1000, DAY + 900, 1.1, 0)], &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        match &evs[0] {
            FeedEvent::Tick { lat_us, .. } => assert_eq!(*lat_us, 0),
            other => panic!("{other:?}"),
        }
    }

    // -- sıra --------------------------------------------------------------

    #[test]
    fn speed_zero_preserves_the_recorded_order() {
        // Belirlenimcilik sözleşmesi: `--replay-speed 0` yalnızca beklemeyi
        // kaldırır, SIRAYI DEĞİŞTİRMEZ.
        let tmp = Tmp::new("order");
        // Aynı `time_msc`'i taşıyan ardışık kayıtlar bilerek var: sıra
        // broker saatinden ÇIKARILAMAZ, yalnızca kayıt sırasından bilinir.
        let written: Vec<TickRec> = (0..500i64)
            .map(|i| rec(DAY + 3_600_000 + i, DAY + 3_600_000 + (i / 5), 1.0 + i as f64, 0))
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());

        let got: Vec<f64> = evs.iter().map(|e| tick_fields(e).2).collect();
        let want: Vec<f64> = written.iter().map(|r| r.bid).collect();
        assert_eq!(got, want, "kayit sirasi birebir korunmali");
    }

    #[test]
    fn a_backwards_clock_jump_does_not_reorder_records() {
        // Kayıt sırası SÖZLEŞME. `recv_ms` geri sıçrasa bile (yaz saati,
        // NTP düzeltmesi) kayıtları yeniden sıralamak, gerçekte olan sırayı
        // tahrif etmek olurdu.
        let tmp = Tmp::new("jump");
        let written = vec![
            rec(DAY + 5_000, DAY + 5_000, 1.0, 0),
            rec(DAY + 4_000, DAY + 4_100, 2.0, 0), // saat geri gitti
            rec(DAY + 6_000, DAY + 6_000, 3.0, 0),
        ];
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        let got: Vec<f64> = evs.iter().map(|e| tick_fields(e).2).collect();
        assert_eq!(got, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn multiple_instances_merge_by_arrival_without_losing_file_order() {
        let tmp = Tmp::new("multi");
        lay_out(
            tmp.path(),
            "brokerA",
            &[rec(DAY + 100, DAY + 100, 1.0, 0), rec(DAY + 300, DAY + 300, 3.0, 0)],
            &one_symbol_table(DAY),
        );
        lay_out(
            tmp.path(),
            "brokerB",
            &[rec(DAY + 200, DAY + 200, 2.0, 0), rec(DAY + 400, DAY + 400, 4.0, 0)],
            &one_symbol_table(DAY),
        );

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(r.instances, vec!["brokerA".to_string(), "brokerB".to_string()]);

        let (evs, _) = play_ticks(&r);
        let got: Vec<(String, f64)> =
            evs.iter().map(|e| (tick_fields(e).0, tick_fields(e).2)).collect();
        assert_eq!(
            got,
            vec![
                ("brokerA".to_string(), 1.0),
                ("brokerB".to_string(), 2.0),
                ("brokerA".to_string(), 3.0),
                ("brokerB".to_string(), 4.0),
            ]
        );
    }

    // -- pencere -----------------------------------------------------------

    #[test]
    fn from_and_to_trim_the_window_on_arrival_time() {
        // Pencere yerel ALIM zamanına göre kırpılır — dosyanın gün sınırı da
        // oradan türetildiği için tek tutarlı seçim bu.
        let tmp = Tmp::new("window");
        // 08:00'den 12:00'ye, saat başı bir tick.
        let written: Vec<TickRec> = (8..=12i64)
            .map(|h| rec(DAY + h * 3_600_000, DAY + h * 3_600_000, h as f64, 0))
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        opts.from = Some("09:00".into());
        opts.to = Some("11:00".into());
        let r = load(&opts).unwrap();

        // Yarı açık [09:00, 11:00): 09 ve 10 içeride, 11 DIŞARIDA.
        let (evs, _) = play_ticks(&r);
        let got: Vec<f64> = evs.iter().map(|e| tick_fields(e).2).collect();
        assert_eq!(got, vec![9.0, 10.0], "ust sinir haric olmali");
        assert_eq!(r.span(), (DAY + 9 * 3_600_000, DAY + 10 * 3_600_000));
    }

    #[test]
    fn only_from_or_only_to_is_enough() {
        let tmp = Tmp::new("halfwindow");
        let written: Vec<TickRec> = (8..=12i64)
            .map(|h| rec(DAY + h * 3_600_000, DAY + h * 3_600_000, h as f64, 0))
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        opts.from = Some("11:00".into());
        assert_eq!(day_items(&load(&opts).unwrap(), 0).len(), 2, "11 ve 12");

        opts.from = None;
        opts.to = Some("10:00".into());
        assert_eq!(day_items(&load(&opts).unwrap(), 0).len(), 2, "8 ve 9");
    }

    #[test]
    fn an_empty_window_is_an_error_not_a_silent_empty_stream() {
        let tmp = Tmp::new("emptywin");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 8 * 3_600_000, DAY, 1.0, 0)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.from = Some("20:00".into());
        let e = load(&opts).expect_err("bos pencere hata olmali");
        let msg = e.to_string();
        assert!(matches!(e, ReplayError::EmptyWindow { .. }), "{e:?}");
        // Kullanıcı ne yapacağını bilmeli: kaydın gerçek kapsamı yazılmalı.
        assert!(msg.contains("08:00:00"), "gercek kapsam soylenmeli: {msg}");
    }

    #[test]
    fn a_reversed_window_is_rejected() {
        let tmp = Tmp::new("revwin");
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1, DAY + 1, 1.0, 0)], &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.from = Some("12:00".into());
        opts.to = Some("09:00".into());
        assert!(matches!(load(&opts), Err(ReplayError::BadWindow { .. })));
    }

    // -- bozuk / eksik dosya ----------------------------------------------

    #[test]
    fn a_missing_symbols_file_is_a_loud_error() {
        // ASIL NOKTA: sembol tablosu olmadan hiçbir tick isimlendirilemez ve
        // replay sessizce BOŞ bir akışa dönüşürdü — "sistem çalışıyor ama
        // piyasa hareketsiz" yanılsaması.
        let tmp = Tmp::new("nosym");
        write_ticks(&ticks_path(tmp.path(), "mt5-1", DATE), &[rec(DAY + 1, DAY + 1, 1.0, 0)])
            .unwrap();

        let opts = ReplayOpts::new(tmp.path(), DATE);
        let e = load(&opts).expect_err("sembol dosyasi yoksa hata gerek");
        assert!(matches!(e, ReplayError::MissingSymbols { .. }), "{e:?}");
        let msg = e.to_string();
        assert!(msg.contains("symbols-20240315.jsonl"), "hangi dosya soylenmeli: {msg}");
    }

    #[test]
    fn an_empty_symbols_file_is_an_error_too() {
        let tmp = Tmp::new("emptysym");
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1, DAY + 1, 1.0, 0)], &[]);
        assert!(matches!(
            load(&ReplayOpts::new(tmp.path(), DATE)),
            Err(ReplayError::EmptySymbols { .. })
        ));
    }

    #[test]
    fn a_corrupt_symbols_line_names_the_line() {
        let tmp = Tmp::new("badsym");
        write_ticks(&ticks_path(tmp.path(), "mt5-1", DATE), &[rec(DAY + 1, DAY + 1, 1.0, 0)])
            .unwrap();
        let p = symbols_path(tmp.path(), "mt5-1", DATE);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(
            &p,
            "{\"at_ms\":1,\"items\":[]}\n{bu json degil}\n",
        )
        .unwrap();

        let e = load(&ReplayOpts::new(tmp.path(), DATE)).expect_err("bozuk satir hata olmali");
        match &e {
            ReplayError::BadSymbolsLine { line, .. } => assert_eq!(*line, 2),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_torn_last_record_is_trimmed_and_the_rest_still_loads() {
        // ASIL NOKTA: dosya başlığı YOK; uzunluk tek doğruluk kaynağı. Çökme
        // anında yarım yazılmış kayıt sessizce atılmalı, dosyanın tamamı
        // reddedilmemeli.
        let tmp = Tmp::new("torn");
        let written: Vec<TickRec> =
            (0..7i64).map(|i| rec(DAY + i * 10, DAY + i * 10, i as f64, 0)).collect();
        let p = ticks_path(tmp.path(), "mt5-1", DATE);
        write_ticks(&p, &written).unwrap();
        // Son kaydın 17 baytını ekle: dosya 48'in katı DEĞİL.
        let mut raw = fs::read(&p).unwrap();
        raw.extend_from_slice(&[0xAB; 17]);
        fs::write(&p, &raw).unwrap();
        assert_ne!(raw.len() % TICK_REC_SIZE, 0);
        write_symbols(&symbols_path(tmp.path(), "mt5-1", DATE), &one_symbol_table(DAY)).unwrap();

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).expect("kirpilip yuklenmeli");
        assert_eq!(day_items(&r, 0).len(), 7, "tam kayitlar korunmali, yarim kayit atilmali");
        let (evs, _) = play_ticks(&r);
        let got: Vec<f64> = evs.iter().map(|e| tick_fields(e).2).collect();
        assert_eq!(got, vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    #[test]
    fn a_file_shorter_than_one_record_is_an_error() {
        // 48 bayttan kısa bir dosya kırpıldığında geriye HİÇBİR ŞEY kalmaz;
        // bunu "kayıt boş" diye sessizce geçmek, bozuk kaydı gizlemek olurdu.
        let tmp = Tmp::new("stub");
        let p = ticks_path(tmp.path(), "mt5-1", DATE);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, [0u8; 20]).unwrap();
        write_symbols(&symbols_path(tmp.path(), "mt5-1", DATE), &one_symbol_table(DAY)).unwrap();
        assert!(matches!(
            load(&ReplayOpts::new(tmp.path(), DATE)),
            Err(ReplayError::EmptyTicks { .. })
        ));
    }

    #[test]
    fn a_missing_recording_is_reported_with_the_expected_path() {
        let tmp = Tmp::new("norec");
        let e = load(&ReplayOpts::new(tmp.path(), DATE)).expect_err("kayit yok");
        assert!(matches!(e, ReplayError::NoRecording { .. }), "{e:?}");
        assert!(e.to_string().contains("ticks-20240315.bin"), "{e}");
    }

    #[test]
    fn a_missing_data_dir_is_reported() {
        let tmp = Tmp::new("nodir");
        let missing = tmp.path().join("yok");
        assert!(matches!(
            load(&ReplayOpts::new(&missing, DATE)),
            Err(ReplayError::NoDataDir(_))
        ));
    }

    #[test]
    fn bad_flags_are_rejected_before_any_file_is_touched() {
        let tmp = Tmp::new("badflags");
        let mut opts = ReplayOpts::new(tmp.path(), "2024-03-15");
        // `-` artık aralık ayırıcısı: `2024-03-15` üç parça verir ve hata
        // ARALIK hatası olur. Sessizce ilk parçayı ("2024") gün saymak,
        // istenmeyen bir günü oynatmak olurdu.
        let e = load(&opts).expect_err("ISO tarih kabul edilmemeli");
        assert!(matches!(e, ReplayError::BadDateSpec(_)), "{e:?}");
        assert!(e.to_string().contains("20260513-20260812"), "kabul edilenler yazilmali: {e}");
        opts.date = "20240230".into();
        assert!(matches!(load(&opts), Err(ReplayError::BadDate(_))), "31 subat yok");
        opts.date = DATE.into();
        opts.speed = -1.0;
        assert!(matches!(load(&opts), Err(ReplayError::BadSpeed(_))));
        opts.speed = 1.0;
        opts.from = Some("9:00".into());
        assert!(matches!(load(&opts), Err(ReplayError::BadTime(_))));
        opts.from = Some("25:00".into());
        assert!(matches!(load(&opts), Err(ReplayError::BadTime(_))));
    }

    // -- sembol tablosu ----------------------------------------------------

    #[test]
    fn the_symbol_table_reaches_the_registry_so_symbols_answers_like_live() {
        let tmp = Tmp::new("symreg");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 1000, DAY + 1000, 1.1, 1)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (_evs, reg) = play_ticks(&load(&opts).unwrap());

        // `symbols` isteğinin okuduğu yüzey.
        let all = reg.all_symbols();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, "mt5-1");
        assert_eq!(all[0].1.name_str(), "EURUSD");
        assert_eq!(all[0].1.digits, 5);
        assert!((all[0].1.point - 0.00001).abs() < 1e-12);
        // `snapshot` isteğinin okuduğu yüzey — son fiyat yazılmış olmalı.
        let snap = reg.snapshot(&[]);
        assert_eq!(snap.len(), 1, "yalniz tick goren sembol");
        assert_eq!(snap[0].1, "GOLD");
        assert!((snap[0].2.bid - 1.1).abs() < 1e-12);
    }

    #[test]
    fn the_table_in_effect_at_each_moment_is_the_last_line_before_it() {
        // "O ana kadarki SON satır" kuralı: kayıt sırasında yeniden
        // adlandırılan bir sembol, replay'de de o andan itibaren yeni adıyla
        // görünmeli.
        let tmp = Tmp::new("symchange");
        let snaps = vec![
            SymbolSnapshot { at_ms: DAY, items: vec![item(0, "EURUSD")] },
            SymbolSnapshot { at_ms: DAY + 2_000, items: vec![item(0, "EURUSD.pro")] },
        ];
        lay_out(
            tmp.path(),
            "mt5-1",
            &[
                rec(DAY + 1_000, DAY + 1_000, 1.0, 0),
                rec(DAY + 3_000, DAY + 3_000, 2.0, 0),
            ],
            &snaps,
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        let got: Vec<String> = evs.iter().map(|e| tick_fields(e).1).collect();
        assert_eq!(got, vec!["EURUSD".to_string(), "EURUSD.pro".to_string()]);
    }

    #[test]
    fn replaying_a_late_window_starts_from_the_table_in_force_then() {
        // Pencere 10:00'de başlıyorsa 09:00'da yayımlanmış tablo geçerlidir —
        // dosyanın başındaki tablo değil.
        let tmp = Tmp::new("latewin");
        let snaps = vec![
            SymbolSnapshot { at_ms: DAY, items: vec![item(0, "ESKI")] },
            SymbolSnapshot { at_ms: DAY + 9 * 3_600_000, items: vec![item(0, "YENI")] },
        ];
        lay_out(
            tmp.path(),
            "mt5-1",
            &[
                rec(DAY + 1_000, DAY + 1_000, 1.0, 0),
                rec(DAY + 10 * 3_600_000, DAY + 10 * 3_600_000, 2.0, 0),
            ],
            &snaps,
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        opts.from = Some("10:00".into());
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        assert_eq!(evs.len(), 1);
        assert_eq!(tick_fields(&evs[0]).1, "YENI");
    }

    #[test]
    fn ticks_for_unknown_symbol_ids_are_skipped_exactly_like_live() {
        // Canlı okuyucu isimsiz tick GÖNDERMEZ; replay de göndermemeli.
        let tmp = Tmp::new("unknown");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 1, DAY + 1, 1.0, 0), rec(DAY + 2, DAY + 2, 2.0, 99)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        assert_eq!(evs.len(), 1, "tablodaki olmayan symbol_id atlanmali");
    }

    // -- mumlar ------------------------------------------------------------

    #[test]
    fn candles_are_built_from_replayed_ticks_so_the_candles_request_works() {
        // Replay'de MT5 geçmişi YOK; `candles` isteği yalnızca burada üretilen
        // seriden cevaplanabilir.
        let tmp = Tmp::new("candles");
        let base = DAY + 10 * 3_600_000;
        // Üç dakikaya yayılmış tick'ler → iki bar kapanır.
        let written: Vec<TickRec> = (0..180i64)
            .map(|i| rec(base + i * 1_000, base + i * 1_000, 1.10 + i as f64 * 1e-4, 0))
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(8192);
        let (dtx, _drx) = watch::channel(None);
        play(&r, &registry, &candles, &tx, &dtx, None);

        let mut closed = 0;
        while let Ok(e) = rx.try_recv() {
            if matches!(e, FeedEvent::Candle { .. }) {
                closed += 1;
            }
        }
        assert!(closed > 0, "kapanan bar olayi yayilmali");

        let cs = candles.lock().unwrap();
        let view = cs.get("EURUSD", "M1", 10);
        assert_eq!(view.src, crate::candles::BarSource::Tick, "replay'de kaynak daima tick");
        assert!(view.age_ms.is_none(), "tick serisi canli, yas alani olmamali");
        assert_eq!(view.bars.len(), 3, "3 dakikalik tick 3 M1 bari uretmeli");
        // MT5 tarafına HİÇ dokunulmamalı: replay'de broker geçmişi yoktur.
        assert_eq!(cs.bar_counts().1, 0);
        assert!(cs.mt5_status("EURUSD", "M1").is_none());
    }

    #[test]
    fn playback_feeds_the_simulator_so_pending_orders_actually_trigger() {
        // UÇTAN UCA: kayıt → oynatım → simülatör.
        //
        // Beslemeyi yayın kanalına abone bir görev yapsaydı bu test yeşil
        // kalır ama üretimde `--replay-speed 0`da kanal taşar ve tick'ler
        // motora HİÇ ulaşmazdı. Bu yüzden besleme oynatım döngüsünün
        // içindedir ve test onu oradan sınıyor.
        let tmp = Tmp::new("replay-sim-pending");
        let base = DAY + 9 * 3_600_000;
        // Fiyat düşüyor: 1.1000 → 1.0940 (ask = bid + 0.0002).
        let written = vec![
            rec(base, base, 1.1000, 0),
            rec(base + 1_000, base + 1_000, 1.0970, 0),
            rec(base + 2_000, base + 2_000, 1.0940, 0),
        ];
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();

        // Kaymasız: dolum fiyatı beklentisi sade kalsın.
        let sim = crate::server::SimExec::new(
            crate::server::MODE_REPLAY,
            &["mt5-1".to_string()],
            crate::sim::SimConfig { balance: 10_000.0, leverage: 100, slippage_points: 0.0 },
        );
        let sym = crate::sim::SimSymbol {
            digits: 5,
            point: 0.00001,
            tick_size: 0.00001,
            volume_step: 0.01,
            volume_min: 0.01,
            volume_max: 100.0,
            stops_level: 0,
            contract_size: 100_000.0,
        };
        // ASK 1.1002 iken 1.0950'ye buy_limit.
        let req = crate::sim::SimOrderReq {
            id: "p1".into(),
            client_id: 7,
            symbol: "EURUSD".into(),
            kind: crate::sim::SimOrderKind::BuyLimit,
            volume: 0.10,
            price: 1.09500,
            time_msc: base,
            ..Default::default()
        };
        let placed = sim
            .engine
            .lock()
            .unwrap()
            .place(&req, &sym, 1.10000, 1.10020);
        assert!(placed.is_accepted(), "bekleyen emir kurulmali: {placed:?}");

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(8192);
        let (dtx, _drx) = watch::channel(None);
        play(&r, &registry, &candles, &tx, &dtx, Some(&sim));

        // Yayında TAM BİR dolum olayı olmalı, kaynağı da kayıttaki örnek.
        let mut fills = Vec::new();
        while let Ok(e) = rx.try_recv() {
            if let FeedEvent::Order { kind, retcode, price, position, comment, instance, .. } = e {
                fills.push((kind, retcode, price, position, comment, instance.to_string()));
            }
        }
        assert_eq!(fills.len(), 1, "tetiklenen emir tam bir dolum uretmeli: {fills:?}");
        let (kind, retcode, price, position, comment, instance) = &fills[0];
        assert_eq!(*kind, "txn");
        assert_eq!(*retcode, 10009);
        assert_eq!(*comment, "pending_triggered", "sebep soylenmeli");
        assert_eq!(instance, "mt5-1", "src kayittaki ornek olmali");
        // Limit emri fiyatından KÖTÜ dolamaz.
        assert!((price - 1.09500).abs() < 1e-12, "limit fiyatindan dolmali: {price}");

        let eng = sim.engine.lock().unwrap();
        assert!(eng.orders().is_empty(), "tetiklenen emir bekleyenlerde kalmamali");
        let book = crate::sim::PriceBook::new();
        let pos = eng.positions(&book);
        assert_eq!(pos.len(), 1, "emir POZISYONA donmeli");
        assert_eq!(pos[0].ticket, *position, "olay ile durum ayni bileti gostermeli");
        assert_eq!(pos[0].client_id, 7, "pozisyon emri KOYAN istemciye ait kalmali");
    }

    #[test]
    fn the_replay_hist_note_says_why_there_is_no_broker_history() {
        // Sebebi söylemeden `hist: "off"` demek, istemcinin "Service'i açmayı
        // unuttum" sanmasına yol açardı.
        assert!(HIST_NOTE.contains("replay"));
        assert!(HIST_NOTE.to_lowercase().contains("gecmis"));
    }

    // -- bitiş bildirimi ---------------------------------------------------

    #[test]
    fn finishing_announces_the_end_exactly_once_with_a_real_summary() {
        let tmp = Tmp::new("done");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 1, DAY + 11, 1.0, 0), rec(DAY + 2, DAY + 22, 1.0, 0)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;

        let r = load(&opts).unwrap();
        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, _rx) = broadcast::channel(64);
        let (dtx, mut drx) = watch::channel(None);
        assert!(drx.borrow().is_none(), "baslamadan bitmis gorunmemeli");

        let end = play(&r, &registry, &candles, &tx, &dtx, None);
        assert_eq!(end.ticks, 2, "oynatilan tick sayisi bildirilmeli");
        // Son tick'in BROKER saati — alım zamanı değil.
        assert_eq!(end.last_ms, DAY + 22);

        // Kanala tam bir kez yazıldı: ikinci bir `changed()` uyanışı olmamalı.
        assert!(drx.has_changed().unwrap(), "bir kez degisti");
        assert_eq!(*drx.borrow_and_update(), Some(end));
        assert!(!drx.has_changed().unwrap(), "bitis TAM BIR KEZ duyurulmali");
    }

    #[test]
    fn ticks_that_were_never_emitted_are_not_counted_as_played() {
        // İsimlendirilemediği için gönderilmeyen bir tick'i "oynattım" diye
        // saymak, istemciye kapsam hakkında yalan söylemek olurdu.
        let tmp = Tmp::new("count");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 1, DAY + 1, 1.0, 0), rec(DAY + 2, DAY + 2, 2.0, 99)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (_evs, _reg, end) = play_all(&load(&opts).unwrap());
        assert_eq!(end.ticks, 1);
        assert_eq!(end.last_ms, DAY + 1);
    }

    #[test]
    fn a_client_that_connects_after_the_end_still_learns_it_finished() {
        // `watch` seçilmesinin sebebi: yayın kanalı geçmişi tutmaz ve
        // oynatım bittikten sonra bağlanan istemci `replay_done` görmezdi.
        let tmp = Tmp::new("late");
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1, DAY + 5, 1.0, 0)], &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, _rx) = broadcast::channel(64);
        let (dtx, _drx) = watch::channel(None);
        play(&load(&opts).unwrap(), &registry, &candles, &tx, &dtx, None);

        // Bitiş yazıldıktan SONRA abone ol.
        let late = dtx.subscribe();
        assert_eq!(late.borrow().map(|e| e.ticks), Some(1));
    }

    #[test]
    fn the_spawned_replay_finishes_and_reports_its_span() {
        let tmp = Tmp::new("spawn");
        lay_out(
            tmp.path(),
            "mt5-1",
            &[rec(DAY + 10, DAY + 10, 1.0, 0), rec(DAY + 90, DAY + 90, 1.0, 0)],
            &one_symbol_table(DAY),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(r.span(), (DAY + 10, DAY + 90));

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(64);
        let (dtx, _drx) = watch::channel(None);
        let h = spawn(r, registry, candles, tx, dtx, None);
        assert_eq!(h.covered_span(), (DAY + 10, DAY + 90));
        let done = h.done();
        // Oynatım başlatma kapısının arkasında bekliyor; sunucuda bunu ilk
        // istemci açar, burada testin kendisi açmalı.
        h.gate().open();
        h.join();

        assert!(done.borrow().is_some(), "bitis bildirilmeli");
        assert_eq!(done.borrow().unwrap().ticks, 2);
        let mut ticks = 0;
        while let Ok(e) = rx.try_recv() {
            if matches!(e, FeedEvent::Tick { .. }) {
                ticks += 1;
            }
        }
        assert_eq!(ticks, 2);
    }

    // -- tempo -------------------------------------------------------------

    #[test]
    fn speed_zero_does_not_sleep_at_all() {
        // Bir günlük aralığa yayılmış kayıt: `speed = 0` bunu anında
        // bitirmeli, yoksa "en hızlı" kipi diye bir şey yok demektir.
        let tmp = Tmp::new("fast");
        let written: Vec<TickRec> = (0..50i64)
            .map(|i| rec(DAY + i * 600_000, DAY + i * 600_000, 1.0, 0))
            .collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();

        let t0 = Instant::now();
        let (evs, _) = play_ticks(&r);
        assert_eq!(evs.len(), 50);
        assert!(t0.elapsed() < Duration::from_secs(2), "beklememeliydi: {:?}", t0.elapsed());
    }

    #[test]
    fn speed_divides_the_recorded_gaps() {
        // 300 ms'lik gerçek aralık, 100x hızda ~3 ms olmalı. Ölçüm gevşek
        // tutuldu (uyku çözünürlüğü); test edilen şey "bekleme gerçekten
        // recv_ms farkından ve hız böleninden geliyor".
        let tmp = Tmp::new("speed");
        let written = vec![
            rec(DAY + 1_000, DAY + 1_000, 1.0, 0),
            rec(DAY + 1_300, DAY + 1_300, 2.0, 0),
        ];
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 100.0;
        let r = load(&opts).unwrap();
        let t0 = Instant::now();
        let (evs, _) = play_ticks(&r);
        let took = t0.elapsed();
        assert_eq!(evs.len(), 2);
        assert!(took < Duration::from_millis(250), "100x hizda beklenmemeli: {took:?}");

        // Aynı kayıt gerçek zamanda en az 300 ms sürmeli.
        opts.speed = 1.0;
        let r = load(&opts).unwrap();
        let t0 = Instant::now();
        let _ = play_ticks(&r);
        assert!(t0.elapsed() >= Duration::from_millis(280), "gercek zamanli tempo tutmali");
    }

    // -- tarih/saat --------------------------------------------------------

    #[test]
    fn dates_parse_to_utc_midnight() {
        assert_eq!(parse_yyyymmdd("19700101").unwrap(), 0);
        assert_eq!(parse_yyyymmdd("20240315").unwrap(), DAY);
        // Artık gün gerçekten var.
        assert!(parse_yyyymmdd("20240229").is_ok());
        assert!(parse_yyyymmdd("20230229").is_err(), "2023 artik yil degil");
        assert!(parse_yyyymmdd("20241301").is_err());
        assert!(parse_yyyymmdd("2024031").is_err());
    }

    #[test]
    fn times_parse_as_offsets_from_midnight() {
        assert_eq!(parse_hhmm("00:00").unwrap(), 0);
        assert_eq!(parse_hhmm("09:30").unwrap(), 34_200_000);
        assert_eq!(parse_hhmm("23:59").unwrap(), 86_340_000);
        // Gün sonunu kapsayan üst sınır yazılabilmeli.
        assert_eq!(parse_hhmm("24:00").unwrap(), DAY_MS);
        assert!(parse_hhmm("24:01").is_err());
        assert!(parse_hhmm("12:60").is_err());
        assert!(parse_hhmm("1200").is_err());
    }

    #[test]
    fn the_day_boundary_is_utc_not_local() {
        // Gün sınırı UTC: yerel saate göre bölmek, aynı kaydın makineden
        // makineye farklı dosyalara düşmesi demekti.
        let tmp = Tmp::new("utc");
        // 23:59:59.999Z içeride, ertesi günün 00:00:00.000Z'si dışarıda.
        let written = vec![
            rec(DAY + DAY_MS - 1, DAY + DAY_MS - 1, 1.0, 0),
            rec(DAY + DAY_MS, DAY + DAY_MS, 2.0, 0),
        ];
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        let items = day_items(&r, 0);
        assert_eq!(items.len(), 1, "ertesi gune tasan kayit bu gune ait degil");
        assert_eq!(items[0].rec.bid, 1.0);
    }

    // -- başlatma kapısı ---------------------------------------------------

    #[test]
    fn playback_does_not_start_until_a_client_opens_the_gate() {
        // GERÇEK KUSURUN TESTİ: kapı olmadan oynatım süreç başlar başlamaz
        // akıyordu. `--replay-speed 0` ile kayıt, hiçbir istemci bağlanmaya
        // fırsat bulamadan bitiyor ve istemci yalnızca `replay_done` görüp
        // TEK BİR TICK almıyordu — özelliğin tüm amacı buydu.
        let tmp = Tmp::new("gate");
        let written: Vec<TickRec> =
            (0..25i64).map(|i| rec(DAY + i * 10, DAY + i * 10, 1.1 + i as f64, 0)).collect();
        lay_out(tmp.path(), "mt5-1", &written, &one_symbol_table(DAY));

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0; // en hızlı: kapı yoksa anında biter
        let r = load(&opts).unwrap();

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(4096);
        let (dtx, _drx) = watch::channel(None);

        let handle = spawn(r, registry, candles, tx, dtx, None);

        // Kapı KAPALI: oynatımın başlamadığını görmek için gerçekten bekle.
        // (Sadece "hemen bakıp boş" demek, yarışı kaybetmiş bir testi de
        // geçirirdi.)
        std::thread::sleep(Duration::from_millis(150));
        assert!(!handle.gate().is_open(), "kapi kendiliginden acilmamali");
        assert!(!handle.finished(), "kapi kapaliyken oynatim BITMEMELI");
        assert!(
            rx.try_recv().is_err(),
            "kapi kapaliyken TEK BIR olay bile yayilmamali"
        );

        // İstemci bağlandı: kapıyı aç.
        handle.gate().open();
        handle.join();

        // Artık kaydın TAMAMI, baştan itibaren gelmiş olmalı.
        let mut ticks = Vec::new();
        while let Ok(e) = rx.try_recv() {
            if let FeedEvent::Tick { bid, .. } = e {
                ticks.push(bid);
            }
        }
        assert_eq!(ticks.len(), written.len(), "abone kaydin TAMAMINI gormeli");
        assert_eq!(ticks[0], written[0].bid, "ilk tick kacirilmamali");
        assert_eq!(ticks[ticks.len() - 1], written[written.len() - 1].bid);
    }

    #[test]
    fn opening_the_gate_twice_is_harmless() {
        // Her bağlanan istemci kapıyı açmaya çalışır; ikincisi bir şeyi
        // yeniden başlatmamalı.
        let g = StartGate::new();
        assert!(!g.is_open());
        g.open();
        g.open();
        assert!(g.is_open());
    }

    // -- diğer -------------------------------------------------------------

    #[test]
    fn symbol_items_survive_a_json_round_trip_with_their_flags() {
        let mut it = item(7, "GOLD");
        it.digits = 2;
        it.point = 0.01;
        it.chart = true;
        it.polled_only = false;
        it.book_depth = 10;
        let j = serde_json::to_string(&it).unwrap();
        assert!(j.contains(r#""id":7"#), "{j}");
        assert!(j.contains(r#""s":"GOLD""#), "{j}");
        let back: SymbolItem = serde_json::from_str(&j).unwrap();
        let e = back.to_entry();
        assert_eq!(e.symbol_id, 7);
        assert_eq!(e.name_str(), "GOLD");
        assert_eq!(e.digits, 2);
        assert_ne!(e.flags & sym_flag::READY, 0);
        assert_ne!(e.flags & sym_flag::CHART, 0);
        assert_eq!(e.flags & sym_flag::POLLED_ONLY, 0);
        assert_eq!(e.ticks_bookdepth, 10);
    }

    #[test]
    fn unknown_fields_in_a_symbols_line_do_not_break_loading() {
        // Kayıt biçimi ileride alan kazanabilir; eski okuyucunun tüm kaydı
        // reddetmesi, kaydı çöpe atmak olurdu.
        let tmp = Tmp::new("fwd");
        write_ticks(&ticks_path(tmp.path(), "mt5-1", DATE), &[rec(DAY + 1, DAY + 1, 1.0, 0)])
            .unwrap();
        let p = symbols_path(tmp.path(), "mt5-1", DATE);
        fs::write(
            &p,
            format!(
                "{{\"at_ms\":{DAY},\"items\":[{{\"id\":0,\"s\":\"EURUSD\",\"ready\":true,\
                 \"gelecek_alan\":42}}],\"gelecek_ust_alan\":true}}\n"
            ),
        )
        .unwrap();
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        assert_eq!(evs.len(), 1);
        assert_eq!(tick_fields(&evs[0]).1, "EURUSD");
    }

    #[test]
    fn a_lock_file_next_to_the_instance_dirs_is_not_mistaken_for_an_instance() {
        let tmp = Tmp::new("lock");
        fs::write(tmp.path().join(".lock"), b"pid").unwrap();
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1, DAY + 1, 1.0, 0)], &one_symbol_table(DAY));
        let insts = discover_instances(tmp.path(), DATE).unwrap();
        assert_eq!(insts, vec!["mt5-1".to_string()]);
    }

    #[test]
    fn instance_dirs_without_this_date_are_ignored() {
        let tmp = Tmp::new("otherday");
        lay_out(tmp.path(), "mt5-1", &[rec(DAY + 1, DAY + 1, 1.0, 0)], &one_symbol_table(DAY));
        write_ticks(&ticks_path(tmp.path(), "mt5-2", "20240316"), &[rec(1, 1, 1.0, 0)]).unwrap();
        assert_eq!(discover_instances(tmp.path(), DATE).unwrap(), vec!["mt5-1".to_string()]);
    }

    // -- gün aralığı -------------------------------------------------------
    //
    // 2024-03-15 Cuma, 16-17 hafta sonu, 18 Pazartesi. Gerçek takvimin
    // kullanılması kasıtlı: hafta sonu boşluğu ve gün-aşırı tempo bu işin
    // asıl konusu.

    /// `(YYYYMMDD, o günün UTC gece yarısı)` — 2024 Mart.
    fn march(dom: u32) -> (String, i64) {
        (format!("202403{dom:02}"), DAY + (dom as i64 - 15) * DAY_MS)
    }

    /// Aralık kipi için hazır kurulum: her gün için verilen `bid` değerlerini
    /// o günün 12:00'sinden itibaren saniyede bir yaz.
    fn lay_out_days(dir: &Path, inst: &str, days: &[(u32, &[f64])]) {
        for (dom, bids) in days {
            let (date, day_ms) = march(*dom);
            let base = day_ms + 12 * 3_600_000;
            let ticks: Vec<TickRec> = bids
                .iter()
                .enumerate()
                .map(|(i, b)| rec(base + i as i64 * 1_000, base + i as i64 * 1_000, *b, 0))
                .collect();
            lay_out_on(dir, inst, &date, &ticks, &[SymbolSnapshot {
                at_ms: day_ms,
                items: vec![item(0, "EURUSD")],
            }]);
        }
    }

    fn bids_of(evs: &[FeedEvent]) -> Vec<f64> {
        evs.iter().map(|e| tick_fields(e).2).collect()
    }

    #[test]
    fn a_single_day_spec_still_means_exactly_one_day() {
        // GERİLEME KORUMASI: aralık desteği eklemek, tek günlük replay'in
        // davranışını bir satır bile değiştirmemeli.
        assert_eq!(parse_date_spec("20260513").unwrap(), DateSpec::Day("20260513".into()));
        assert!(!parse_date_spec("20260513").unwrap().is_range());

        let tmp = Tmp::new("onedayspec");
        // Aralıkta olsaydı komşu gün de gelirdi; tek gün istendiğinde GELMEMELİ.
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0, 2.0]), (18, &[3.0])]);

        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(r.days, vec![DATE.to_string()], "tek gun istendi, tek gun oynatilmali");
        assert_eq!(r.day_count(), 1);

        let (evs, _, end) = play_all(&r);
        let ticks: Vec<&FeedEvent> =
            evs.iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect();
        assert_eq!(ticks.len(), 2);
        assert_eq!(end.ticks, 2);
    }

    #[test]
    fn a_range_plays_its_days_in_order_over_one_uninterrupted_stream() {
        // ASIL NOKTA: 66 ayrı süreç yerine tek akış. Günler kronolojik gelir
        // ve istemci gün değiştiğini fark etmez.
        let tmp = Tmp::new("range2");
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0, 2.0]), (18, &[3.0, 4.0])]);

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(r.days, vec!["20240315".to_string(), "20240318".to_string()]);

        let (evs, _, end) = play_all(&r);
        let ticks: Vec<FeedEvent> =
            evs.into_iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect();
        assert_eq!(bids_of(&ticks), vec![1.0, 2.0, 3.0, 4.0], "gunler KRONOLOJIK akmali");
        assert_eq!(end.ticks, 4, "sayac aralik boyunca birikmeli, gun basina sifirlanmamali");
    }

    #[test]
    fn a_missing_or_empty_day_inside_the_range_is_skipped_silently() {
        // Hafta sonu ve tatiller kayıtta YOKTUR. Bunların her biri için hata
        // vermek, 3 aylık bir backtest'i ilk cumartesinde durdururdu.
        let tmp = Tmp::new("weekend");
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0]), (18, &[2.0])]);
        // Cumartesi: dosya VAR ama 0 bayt (kaydedici açıp hiç yazmamış).
        let sat = ticks_path(tmp.path(), "mt5-1", &march(16).0);
        fs::write(&sat, []).unwrap();
        // Pazar: dosya hiç yok.

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        let r = load(&opts).expect("hafta sonu bir hata degildir");
        assert_eq!(
            r.days,
            vec!["20240315".to_string(), "20240318".to_string()],
            "bos ve eksik gunler atlanmali"
        );
        let (evs, _, end) = play_all(&r);
        let ticks: Vec<FeedEvent> =
            evs.into_iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect();
        assert_eq!(bids_of(&ticks), vec![1.0, 2.0]);
        assert_eq!(end.ticks, 2);
        // Hafta sonu ATLANAN gün değil, planda HİÇ OLMAYAN gündür: plan iki
        // gündür ve ikisi de oynatılmıştır. Aksi hâlde her normal aralık
        // "yarıda kesildi" damgası yerdi.
        assert_eq!((end.days, end.days_played), (2, 2), "hafta sonu kesinti degildir");
    }

    #[test]
    fn a_day_that_cannot_be_read_mid_range_is_reported_not_silently_swallowed() {
        // Aralığın ortasındaki bir gün okunamazsa oynatım duruyordu ama bunu
        // TEL ÜZERİNDE söyleyemiyordu: istemci normal bir `replay_done` görür
        // ve 3 aylık sandığı bir backtest'i tek günün sonucuyla raporlardı.
        // Sessiz kesinti, yanlış sonucu doğru sanmak demektir.
        let tmp = Tmp::new("truncated");
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0]), (18, &[2.0]), (19, &[3.0])]);

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240319");
        opts.speed = 0.0;
        let r = load(&opts).expect("uc gun de yerinde");
        assert_eq!(r.days.len(), 3);

        // Açılış doğrulamasından SONRA, oynatım başlamadan önce ortadaki gün
        // okunamaz hâle geliyor (silinme, kilitlenme, disk hatası). Kapı
        // sayesinde bu pencere gerçekte de vardır.
        fs::remove_file(ticks_path(tmp.path(), "mt5-1", "20240318")).unwrap();

        let (evs, _, end) = play_all(&r);
        let ticks: Vec<FeedEvent> =
            evs.into_iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect();
        assert_eq!(bids_of(&ticks), vec![1.0], "kesintiden sonrasi oynatilmaz");
        assert_eq!(end.days, 3, "PLAN uc gundu");
        assert_eq!(end.days_played, 1, "yalnizca ilk gun oynatildi");
        assert!(end.days_played < end.days, "kesinti sayilardan okunabilmeli");
    }

    #[test]
    fn a_range_that_matches_no_recorded_day_is_a_loud_error() {
        // Tek tek eksik günler sessizce atlanır; aralığın TAMAMEN boş olması
        // yazım hatası ya da yanlış dizin demektir ve sessizce boş akışa
        // dönüşmesi "sistem calisiyor ama piyasa hareketsiz" yanılsaması
        // üretirdi.
        let tmp = Tmp::new("emptyrange");
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0])]);

        let opts = ReplayOpts::new(tmp.path(), "20240401-20240405");
        let e = load(&opts).expect_err("bos aralik hata olmali");
        assert!(matches!(e, ReplayError::NoRecordingInRange { .. }), "{e:?}");
        let msg = e.to_string();
        assert!(msg.contains("20240401") && msg.contains("20240405"), "aralik yazilmali: {msg}");
        assert!(msg.contains("ticks-YYYYMMDD.bin"), "beklenen dosya adi yazilmali: {msg}");

        // Açık uçlu aralıkta da aynı: sondaki gün yoksa "sonuna kadar" boştur.
        let e = load(&ReplayOpts::new(tmp.path(), "20240401-")).expect_err("bos");
        assert!(matches!(e, ReplayError::NoRecordingInRange { .. }), "{e:?}");
        assert!(e.to_string().contains("kayittaki son gun"), "{e}");

        // Dizin hiç yoksa hata dizinin kendisi hakkında olmalı.
        let missing = tmp.path().join("yok");
        assert!(matches!(
            load(&ReplayOpts::new(&missing, "20240315-20240318")),
            Err(ReplayError::NoDataDir(_))
        ));
    }

    #[test]
    fn an_open_ended_range_starts_where_asked_and_takes_everything_after() {
        let tmp = Tmp::new("openrange");
        lay_out_days(tmp.path(), "mt5-1", &[(14, &[9.0]), (15, &[1.0]), (18, &[2.0])]);

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-");
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(
            r.days,
            vec!["20240315".to_string(), "20240318".to_string()],
            "baslangictan ONCEKI gun alinmamali"
        );
        let (evs, _, _) = play_all(&r);
        let ticks: Vec<FeedEvent> =
            evs.into_iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).collect();
        assert_eq!(bids_of(&ticks), vec![1.0, 2.0]);
    }

    #[test]
    fn the_hello_span_covers_the_whole_range_not_just_one_day() {
        // `hello.replay_from_ms` / `replay_to_ms` istemcinin "ne kadarlık bir
        // kayıt dinliyorum" sorusunun tek cevabı; tek günü söylemek yalan olurdu.
        let tmp = Tmp::new("rangespan");
        lay_out_days(tmp.path(), "mt5-1", &[(15, &[1.0, 2.0]), (18, &[3.0, 4.0])]);

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        let r = load(&opts).unwrap();

        let (_, fri) = march(15);
        let (_, mon) = march(18);
        assert_eq!(
            r.span(),
            (fri + 12 * 3_600_000, mon + 12 * 3_600_000 + 1_000),
            "kapsam ILK gunun ilk tick'inden SON gunun son tick'ine olmali"
        );
    }

    #[test]
    fn the_symbol_table_is_reloaded_for_every_day_because_ids_can_change() {
        // ASIL TUZAK: `symbol_id` günler arasında AYNI KALMAK ZORUNDA DEĞİL.
        // Dünün tablosunu taşımak, bugünün tick'lerine yanlış sembol adı
        // yapıştırmak olurdu — ve bu hiçbir hata vermeden, sessizce.
        let tmp = Tmp::new("symperday");
        let (d1, ms1) = march(15);
        let (d2, ms2) = march(18);
        // İki günde kimlikler TAKAS EDİLMİŞ.
        lay_out_on(
            tmp.path(),
            "mt5-1",
            &d1,
            &[rec(ms1 + 1_000, ms1 + 1_000, 1.0, 0)],
            &[SymbolSnapshot { at_ms: ms1, items: vec![item(0, "EURUSD"), item(1, "GOLD")] }],
        );
        lay_out_on(
            tmp.path(),
            "mt5-1",
            &d2,
            &[rec(ms2 + 1_000, ms2 + 1_000, 2.0, 0)],
            &[SymbolSnapshot { at_ms: ms2, items: vec![item(0, "GOLD"), item(1, "EURUSD")] }],
        );

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        let names: Vec<String> = evs.iter().map(|e| tick_fields(e).1).collect();
        assert_eq!(
            names,
            vec!["EURUSD".to_string(), "GOLD".to_string()],
            "her gun KENDI tablosuyla isimlendirilmeli"
        );
    }

    #[test]
    fn the_day_window_is_applied_to_every_day_of_the_range() {
        // `--replay-from 09:00 --replay-to 11:00` bir aralıkta "her günün
        // 09:00-11:00'i" demektir; aralığın tamamına tek pencere uygulamak
        // ikinci günü baştan sona düşürürdü.
        let tmp = Tmp::new("rangewindow");
        for dom in [15u32, 18] {
            let (date, day_ms) = march(dom);
            let ticks: Vec<TickRec> = [8i64, 10, 12]
                .iter()
                .map(|h| {
                    rec(day_ms + h * 3_600_000, day_ms + h * 3_600_000, (dom as f64) + *h as f64, 0)
                })
                .collect();
            lay_out_on(tmp.path(), "mt5-1", &date, &ticks, &[SymbolSnapshot {
                at_ms: day_ms,
                items: vec![item(0, "EURUSD")],
            }]);
        }

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        opts.from = Some("09:00".into());
        opts.to = Some("11:00".into());
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        assert_eq!(bids_of(&evs), vec![25.0, 28.0], "her gunden yalniz 10:00 tick'i");
    }

    #[test]
    fn the_end_is_announced_only_after_the_last_day_never_between_days() {
        // ASIL NOKTA: `replay_done` gün başına gitseydi, 66 günlük bir
        // backtest istemciye 66 kez "bitti" derdi ve istemci her seferinde
        // durumunu sıfırlardı — yani gün-aşırı pozisyon yine test edilemezdi.
        let tmp = Tmp::new("doneonce");
        let (d1, ms1) = march(15);
        let (d2, ms2) = march(18);
        let table = |at: i64| vec![SymbolSnapshot { at_ms: at, items: vec![item(0, "EURUSD")] }];
        // 1. gün 100 ms sürer, 2. gün 800 ms.
        lay_out_on(
            tmp.path(),
            "mt5-1",
            &d1,
            &[rec(ms1, ms1, 1.0, 0), rec(ms1 + 100, ms1 + 100, 2.0, 0)],
            &table(ms1),
        );
        lay_out_on(
            tmp.path(),
            "mt5-1",
            &d2,
            &[rec(ms2, ms2, 3.0, 0), rec(ms2 + 800, ms2 + 800, 4.0, 0)],
            &table(ms2),
        );

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        // Gerçek zamana yakın: gün 2'nin oynatımı ölçülebilir bir süre sürsün.
        opts.speed = 2.0;
        let r = load(&opts).unwrap();

        let registry = Arc::new(Registry::new());
        let candles = Arc::new(Mutex::new(CandleStore::new()));
        let (tx, mut rx) = broadcast::channel(64);
        // Gönderen `Arc` içinde: oynatım thread'i bitince kanal KAPANMASIN,
        // yoksa `has_changed()` "gönderen gitti" diye hata döner ve testin
        // asıl sorusu (kaç kez duyuruldu) cevapsız kalır.
        let dtx = Arc::new(watch::channel(None).0);
        let mut drx = dtx.subscribe();
        let probe_rx = dtx.subscribe();
        let thread_tx = dtx.clone();

        let h = std::thread::spawn(move || play(&r, &registry, &candles, &tx, &thread_tx, None));

        // 1. günün İKİ tick'ini bekle — "gün 1 bitti" anını böyle biliyoruz.
        let mut seen = 0;
        while seen < 2 {
            if let Ok(FeedEvent::Tick { .. }) = rx.blocking_recv() {
                seen += 1;
            }
        }
        // Gün 2 daha ~400 ms sürecek (gunler arasi kirpilmis bekleme dahil);
        // bu pencerede bitiş duyurulmuş OLMAMALI.
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            probe_rx.borrow().is_none(),
            "bitis gun 1'in sonunda DUYURULMAMALI — yalnizca araligin sonunda"
        );

        let end = h.join().unwrap();
        assert_eq!(end.ticks, 4, "aralik boyunca 4 tick");
        assert!(drx.has_changed().unwrap(), "bitis duyurulmali");
        assert_eq!(*drx.borrow_and_update(), Some(end));
        assert!(!drx.has_changed().unwrap(), "bitis TAM BIR KEZ duyurulmali");
    }

    #[test]
    fn the_gap_between_days_is_clipped_but_a_gap_inside_a_day_is_not() {
        // Cuma 23:58 → Pazartesi 01:00 farkı ~49 SAAT. Kırpılmasaydı
        // `--replay-speed 1` ile oynatım 49 saat uyurdu ve çok günlü replay
        // pratikte kullanılamazdı.
        let tmp = Tmp::new("gapclip");
        let (d1, ms1) = march(15);
        let (d2, ms2) = march(18);
        let table = |at: i64| vec![SymbolSnapshot { at_ms: at, items: vec![item(0, "EURUSD")] }];
        let fri = ms1 + 23 * 3_600_000 + 58 * 60_000;
        let mon = ms2 + 3_600_000;
        lay_out_on(tmp.path(), "mt5-1", &d1, &[rec(fri, fri, 1.0, 0)], &table(ms1));
        lay_out_on(tmp.path(), "mt5-1", &d2, &[rec(mon, mon, 2.0, 0)], &table(ms2));
        assert!(mon - fri > 48 * 3_600_000, "test kurulumu 48 saatten uzun bir bosluk kurmali");

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 1.0;
        let r = load(&opts).unwrap();
        let t0 = Instant::now();
        let (evs, _) = play_ticks(&r);
        let took = t0.elapsed();
        assert_eq!(bids_of(&evs), vec![1.0, 2.0]);
        assert!(took < Duration::from_secs(4), "49 saatlik bosluk kirpilmali: {took:?}");
        assert!(
            took >= Duration::from_millis(800),
            "kirpma tavani {REPLAY_GAP_MAX_MS} ms; bekleme tamamen kaldirilmis: {took:?}"
        );

        // Gün İÇİNDEKİ boşluk GERÇEK piyasa sessizliğidir ve kırpılmaz.
        let tmp = Tmp::new("gapinside");
        let inside = ms1 + 9 * 3_600_000;
        lay_out_on(
            tmp.path(),
            "mt5-1",
            &d1,
            &[rec(inside, inside, 1.0, 0), rec(inside + 1_200, inside + 1_200, 2.0, 0)],
            &table(ms1),
        );
        let mut opts = ReplayOpts::new(tmp.path(), DATE);
        opts.speed = 1.0;
        let t0 = Instant::now();
        let (evs, _) = play_ticks(&load(&opts).unwrap());
        let took = t0.elapsed();
        assert_eq!(evs.len(), 2);
        assert!(
            took >= Duration::from_millis(1_100),
            "gun ICINDEKI 1200 ms bosluk KIRPILMAMALI: {took:?}"
        );
    }

    #[test]
    fn the_whole_range_is_never_resident_in_memory_at_once() {
        // ASIL NOKTA: 66 gün = 1,15 GB ve 25,8 milyon kayıt. Hepsini birden
        // yüklemek kabul edilemez. Bunu bir yorumla değil ÖLÇEREK sabitliyoruz:
        // `Recording`e bir `items` alanı eklemek diğer tüm testleri yeşil
        // bırakır, bellek patlaması ancak üretimde görülürdü.
        let tmp = Tmp::new("mem");
        let per_day: Vec<f64> = (0..100).map(|i| 1.0 + i as f64).collect();
        let days: Vec<(u32, &[f64])> =
            [11u32, 12, 13, 14, 15, 18, 19, 20].iter().map(|d| (*d, &per_day[..])).collect();
        lay_out_days(tmp.path(), "mt5-1", &days);

        let mut opts = ReplayOpts::new(tmp.path(), "20240311-20240320");
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        assert_eq!(r.day_count(), 8);

        // Kapsam ölçümü de gün yükler; sayacı oynatımdan HEMEN önce sıfırla.
        probe::reset();
        let (evs, _, end) = play_all(&r);
        let (peak_days, peak_ticks) = probe::peak();

        assert_eq!(end.ticks, 800, "8 gun x 100 tick oynatilmali");
        assert_eq!(
            evs.iter().filter(|e| matches!(e, FeedEvent::Tick { .. })).count(),
            800
        );
        assert_eq!(peak_days, 1, "ayni anda YALNIZCA bir gun bellekte olmali");
        assert_eq!(peak_ticks, 100, "ayni anda yalnizca bir gunun tick'leri bellekte olmali");
    }

    #[test]
    fn date_specs_parse_into_a_day_or_a_range() {
        assert_eq!(parse_date_spec("20260513").unwrap(), DateSpec::Day("20260513".into()));
        assert_eq!(
            parse_date_spec("20260513-20260812").unwrap(),
            DateSpec::Range { start: "20260513".into(), end: Some("20260812".into()) }
        );
        assert_eq!(
            parse_date_spec("20260513-").unwrap(),
            DateSpec::Range { start: "20260513".into(), end: None }
        );
        // Tek günlük aralık geçerli: `20260513-20260513`.
        assert!(parse_date_spec("20260513-20260513").is_ok());
        assert_eq!(parse_date_spec("20260513-20260812").unwrap().start(), "20260513");
    }

    #[test]
    fn malformed_range_specs_are_rejected_loudly() {
        // Sessizce ilk parçayı gün saymak, İSTENMEYEN bir günü oynatmak
        // olurdu ve sonuç doğru görünürdü.
        for s in [
            "20260513-20260812-20260901", // fazladan ayirici
            "-20260812",                  // baslangic yok
            "2026051-20260812",           // kisa baslangic
            "20260513-2026081",           // kisa bitis
            "20260513-20261332",          // takvimde olmayan bitis
            "-",                          // ikisi de yok
        ] {
            let e = parse_date_spec(s).expect_err("kabul edilmemeliydi: {s}");
            assert!(matches!(e, ReplayError::BadDateSpec(_)), "{s}: {e:?}");
            let msg = e.to_string();
            assert!(msg.contains("--replay-date"), "{s}: hangi bayrak oldugu soylenmeli: {msg}");
            assert!(msg.contains("20260513-20260812"), "{s}: kabul edilenler yazilmali: {msg}");
        }

        // Ters aralık AYRI bir hata: biçim doğru, anlam yanlış.
        let e = parse_date_spec("20260812-20260513").expect_err("ters aralik");
        assert!(matches!(e, ReplayError::BadDateRange { .. }), "{e:?}");
        let msg = e.to_string();
        assert!(msg.contains("--replay-date"), "{msg}");
        assert!(msg.contains("20260812-20260513"), "verilen deger yazilmali: {msg}");

        // Ayırıcısız bozuk tarih ESKİ hatasını korumalı.
        assert!(matches!(parse_date_spec("2024031"), Err(ReplayError::BadDate(_))));
        assert!(matches!(parse_date_spec(""), Err(ReplayError::BadDate(_))));
    }

    #[test]
    fn recorded_days_are_discovered_across_every_instance_in_order() {
        let tmp = Tmp::new("days");
        lay_out_days(tmp.path(), "brokerA", &[(15, &[1.0]), (18, &[2.0])]);
        lay_out_days(tmp.path(), "brokerB", &[(14, &[3.0]), (18, &[4.0])]);
        assert_eq!(
            discover_days(tmp.path()).unwrap(),
            vec!["20240314".to_string(), "20240315".to_string(), "20240318".to_string()],
            "gunler birlestirilmis, tekillestirilmis ve KRONOLOJIK olmali"
        );
    }

    #[test]
    fn several_instances_still_merge_by_arrival_on_every_day_of_the_range() {
        // Çoklu örnek desteği aralıkta da GÜN BAŞINA çalışmalı; bir örnek
        // aralığın ortasında kayda başlamış olabilir.
        let tmp = Tmp::new("rangemulti");
        let (d1, ms1) = march(15);
        let (d2, ms2) = march(18);
        let table = |at: i64| vec![SymbolSnapshot { at_ms: at, items: vec![item(0, "EURUSD")] }];
        // 1. günde yalnız brokerA var; 2. günde ikisi de.
        lay_out_on(tmp.path(), "mt5-a", &d1, &[rec(ms1 + 100, ms1 + 100, 1.0, 0)], &table(ms1));
        lay_out_on(
            tmp.path(),
            "mt5-a",
            &d2,
            &[rec(ms2 + 100, ms2 + 100, 2.0, 0), rec(ms2 + 300, ms2 + 300, 4.0, 0)],
            &table(ms2),
        );
        lay_out_on(
            tmp.path(),
            "mt5-b",
            &d2,
            &[rec(ms2 + 200, ms2 + 200, 3.0, 0)],
            &table(ms2),
        );

        let mut opts = ReplayOpts::new(tmp.path(), "20240315-20240318");
        opts.speed = 0.0;
        let r = load(&opts).unwrap();
        // Örnek listesi TÜM günlerin birleşimi: `hello.instances` oynatım
        // boyunca sabit kalmalı.
        assert_eq!(r.instances, vec!["mt5-a".to_string(), "mt5-b".to_string()]);

        let (evs, _) = play_ticks(&r);
        let got: Vec<(String, f64)> =
            evs.iter().map(|e| (tick_fields(e).0, tick_fields(e).2)).collect();
        assert_eq!(
            got,
            vec![
                ("mt5-a".to_string(), 1.0),
                ("mt5-a".to_string(), 2.0),
                ("mt5-b".to_string(), 3.0),
                ("mt5-a".to_string(), 4.0),
            ],
            "gun basina varis sirasina gore birlestirilmeli"
        );
    }
}
