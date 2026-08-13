//! Geri-doldurulmuş tick dosyalarını **kanonik kayıt biçimine** taşır.
//!
//! Canlı kayıt (`--record`) 11 Ağustos'ta başladı; ondan öncesi yok ve
//! beklemekle gelmiyor. Geçmiş tick'ler MT5'in kendi deposundan ancak AÇIK
//! istekle indirilir; indiren taraf bir MQL5 Service'tir ve MQL5 dosya G/Ç'si
//! `MQL5\Files\` altına sınırlı olduğu için oraya yazar. Bu modül o dosyaları
//! okuyup `veri/<instance>/` altındaki biçime taşır.
//!
//! ```text
//! MQL5\Files\Sinyal\backfill\<Sembol>-YYYYMMDD.bin   48 baytlık kayıtlar
//!                    ↓  sinyald --import-backfill
//! <data-dir>/<instance>/ticks-YYYYMMDD.bin           aynı 48 baytlık biçim
//! <data-dir>/<instance>/symbols-YYYYMMDD.jsonl       sembol tablosu
//! ```
//!
//! # KRİTİK — `recv_ms` bu dosyalarda YEREL ALIM ZAMANI DEĞİL
//!
//! Geçmiş tick'lerde yerel alım zamanı **yoktur**: o tick buraya hiç
//! ulaşmadı, bir arşivden kopyalandı. Service `recv_ms` alanına broker
//! saatini yazar. Replay temposu ([`crate::replay::play`]) `recv_ms`
//! farklarından üretildiği için geri-doldurulan günlerde tempo **broker
//! saatinden** gelir.
//!
//! Bu kabul edilebilir ama sessiz kalamaz, çünkü iki saat arasında ÖLÇÜLMÜŞ
//! bir fark var: bu makinedeki canlı kayıtta `recv_ms - time_msc` medyanı
//! **-10 798 428 ms (≈ -3 saat)**. Yani broker saati UTC+3. Canlı bir günün
//! üstüne geri-doldurma birleştirilirse aynı dosyada iki farklı saat
//! yaşar ve o günün replay temposu iki saat arasında gidip gelir. Özet bunu
//! ölçüp AÇIKÇA yazar ([`ImportSummary`]).
//!
//! # Gün sınırı
//!
//! Kayıt her zaman `recv_ms`'in UTC gününe göre dosyalanır — kaydedici de
//! öyle yapıyor ([`crate::record`]) ve replay o alana göre pencereliyor. Bir
//! kaydın hangi güne gideceğini **kaydın kendi damgası** söyler, dosya adı
//! değil: dosya adı yalnızca hangi günün istendiğini gösterir ve broker
//! saatiyle UTC arasındaki kayma yüzünden kayıtların bir kısmı komşu güne
//! düşebilir. Uyuşmazlık sayılır ve raporlanır.
//!
//! # Birleştirme
//!
//! Hedef gün dosyası varsa **üzerine yazılmaz, birleştirilir**: iki kaynak
//! `recv_ms` ile sıralanır (replay dosyayı o alana göre pencereler, birleştirir
//! ve TEMPOYU ondan üretir) ve aynı `(symbol_id, time_msc, bid, ask)` dörtlüsü
//! bir kez yazılır. Çift kayıt replay'de sahte hareket üretir.
//!
//! Denetim **komşu gün dosyalarına da bakar**: broker saati UTC'den kayık
//! olduğu için aynı piyasa tick'inin canlı kopyası bir gün dosyasında,
//! geri-doldurma kopyası KOMŞU gün dosyasında olabilir. Tek gün içinde çalışan
//! bir denetim o çifti hiç göremezdi.
//!
//! Aynı içe aktarma iki kez çalıştırılabilir: ikinci çalıştırma çıktıyı
//! DEĞİŞTİRMEZ (bkz. testler).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::record::{self, TickRec, REC_SIZE};
use crate::replay::{SymbolItem, SymbolSnapshot};

/// İçe aktarma bayrakları.
#[derive(Debug, Clone)]
pub struct ImportOpts {
    /// `--import-backfill <dir>` — Service'in yazdığı dizin.
    pub src_dir: PathBuf,
    /// `--data-dir <dir>` — kayıt kökü (kaydedicinin `--record` dizini).
    pub data_dir: PathBuf,
    /// `--instance <ad>` — hangi örneğin kaydına yazılacak.
    pub instance: String,
}

// ---------------------------------------------------------------------------
// Hatalar
// ---------------------------------------------------------------------------

/// İçe aktarma hatası. Her durum AÇIKÇA bildirilir ve ne yapılacağını söyler:
/// sessizce eksik aktarılmış bir geçmiş, aylar sonra "strateji neden bu günü
/// görmüyor" olarak geri gelirdi.
#[derive(Debug)]
pub enum ImportError {
    BadInstance(String),
    NoSrcDir(PathBuf),
    NoFiles(PathBuf),
    /// Kayıt dizinini bir yazar (çalışan `sinyald --record`) tutuyor.
    Locked(record::RecError),
    Io { what: &'static str, path: PathBuf, source: io::Error },
    /// Hedef dizinde tek bir sembol tablosu bile yok.
    NoSymbolTable { dir: PathBuf, wanted: Vec<String> },
    /// Sembol tablosu okunamadı/bozuk.
    BadSymbols { path: PathBuf, err: String },
    /// Referans tabloda bu sembol yok — kimliği uydurulamaz.
    UnknownSymbol { symbol: String, table: PathBuf, known: Vec<String> },
    /// Günün tablosunda sembol var ama her görüntüde değil.
    SymbolMissingInDay { symbol: String, path: PathBuf, snapshot: usize },
    /// Günün tablosunda sembol iki farklı kimlikle geçiyor.
    SymbolIdConflict { symbol: String, path: PathBuf, ids: Vec<u32> },
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadInstance(s) => write!(
                f,
                "ornek adi ({s:?}) dosya yolunda kullanilamaz — hedef \
                 <data-dir>/<instance> olarak aciliyor"
            ),
            Self::NoSrcDir(p) => write!(
                f,
                "kaynak dizin yok ya da dizin degil: {} — Service'in yazdigi \
                 MQL5\\Files\\Sinyal\\backfill dizinini ver",
                p.display()
            ),
            Self::NoFiles(p) => write!(
                f,
                "{} altinda <Sembol>-YYYYMMDD.bin desenine uyan dosya YOK. \
                 Bos bir ice aktarmayi basarili saymiyoruz: yol yanlissa bu \
                 sessizce 'bitti' derdi.",
                p.display()
            ),
            Self::Locked(e) => write!(
                f,
                "{e}\nIce aktarma gun dosyalarini YENIDEN YAZAR; ayni anda \
                 kayit yapan bir daemon varsa onun ekledigi tick'ler kaybolurdu."
            ),
            Self::Io { what, path, source } => {
                write!(f, "{what} basarisiz ({}): {source}", path.display())
            }
            Self::NoSymbolTable { dir, wanted } => write!(
                f,
                "{} altinda hic sembol tablosu (symbols-YYYYMMDD.jsonl) yok; \
                 {} icin symbol_id ve sembol ozellikleri (digits, point, \
                 contract_size) bilinmiyor.\n\
                 Geri-doldurma yalnizca FIYAT tasir, sembol ozelliklerini \
                 TASIMAZ. Kimlik uydurmak replay'in tick'i isimlendirmesini \
                 saglardi ama contract_size 0 kalirdi ve simulatorun marjini \
                 ile kari SESSIZCE yanlis cikardi.\n\
                 Once bir kez `sinyald --instance <ad> --record <dizin>` \
                 calistir (tablo saniyeler icinde yazilir), sonra ice aktar.",
                dir.display(),
                wanted.join(", ")
            ),
            Self::BadSymbols { path, err } => write!(
                f,
                "sembol tablosu okunamadi ({}): {err}\n\
                 Bozuk bir tabloyla ice aktarma, tick'leri cozulemeyen bir \
                 kimlikle damgalardi.",
                path.display()
            ),
            Self::UnknownSymbol { symbol, table, known } => write!(
                f,
                "{symbol:?} sembol tablosunda yok ({}); bilinenler: {}.\n\
                 Kimlik uydurmuyoruz: uydurulan kimlik replay'de baska bir \
                 sembolun adiyla cozulebilirdi. Once bu sembolu canli kayitta \
                 gorunur yap (EA'nin sembol listesine ekle), sonra ice aktar.",
                table.display(),
                if known.is_empty() { "<yok>".to_string() } else { known.join(", ") }
            ),
            Self::SymbolMissingInDay { symbol, path, snapshot } => write!(
                f,
                "{}: {snapshot}. sembol tablosu goruntusunde {symbol:?} YOK.\n\
                 O gunun bir bolumunde bu sembol tanimli degil; ice aktarilan \
                 tick'ler orada isimlendirilemez ve replay onlari SESSIZCE \
                 atlardi. Bu gunu AYRI bir --data-dir'e aktar.",
                path.display()
            ),
            Self::SymbolIdConflict { symbol, path, ids } => write!(
                f,
                "{}: {symbol:?} gun icinde iki farkli symbol_id ile geciyor \
                 ({}). Ice aktarilan kayitlar tek bir kimlikle damgalanmak \
                 zorunda; hangisinin dogru oldugunu tahmin etmiyoruz. Bu gunu \
                 AYRI bir --data-dir'e aktar.",
                path.display(),
                ids.iter().map(u32::to_string).collect::<Vec<_>>().join(", ")
            ),
        }
    }
}

impl std::error::Error for ImportError {}

fn io_err<'a>(what: &'static str, path: &'a Path) -> impl FnOnce(io::Error) -> ImportError + 'a {
    move |source| ImportError::Io { what, path: path.to_path_buf(), source }
}

// ---------------------------------------------------------------------------
// Özet
// ---------------------------------------------------------------------------

/// Tek bir hedef günün sonucu.
#[derive(Debug, Default, Clone)]
pub struct DayReport {
    pub day: u32,
    /// İçe aktarmadan ÖNCE dosyada olan kayıt sayısı.
    pub existing: usize,
    /// Kaynak dosyalardan bu güne düşen kayıt sayısı.
    pub imported: usize,
    /// Atılan çift kayıt (kaynak dosyalardan gelen).
    pub dup_imported: usize,
    /// **KOMŞU GÜN** dosyasında zaten bulunduğu için atılan gelen kayıt.
    ///
    /// Bu, geri-doldurmanın en sinsi çift kaydıydı: broker saati UTC'den
    /// kayık (bu makinede ölçüldü: UTC+3) ve geri-doldurmada `recv_ms`
    /// broker saatidir. Aynı piyasa tick'i canlı tarafta bir gün dosyasına,
    /// geri-doldurma tarafında KOMŞU gün dosyasına düşer. Tek gün içinde
    /// çalışan bir denetim onları hiç karşılaştıramaz ve replay o anı İKİ
    /// KEZ oynatır — kullanıcının tüm amacı canlıyla AYNI veriyi görmekken.
    pub dup_neighbour: usize,
    /// Atılan çift kayıt — **mevcut dosyadan**. Sıfır olmalı; değilse canlı
    /// kayıt değişmiş demektir ve bu ayrıca uyarılır.
    pub dup_existing: usize,
    /// Dosyaya yazılan toplam kayıt.
    pub written: usize,
    /// `symbols-YYYYMMDD.jsonl` bu çalıştırmada üretildi mi.
    pub symbols_written: bool,
    /// Gün dosyasında ZATEN tick vardı ama sembol tablosu yoktu.
    pub symbols_written_over_ticks: bool,
    /// Mevcut kayıtların `recv_ms - time_msc` medyanı (saat farkı).
    pub existing_skew_ms: Option<i64>,
    /// İçe aktarılan kayıtların `recv_ms - time_msc` medyanı.
    pub imported_skew_ms: Option<i64>,
    /// Mevcut dosyada `time_msc` geriye sıçrayan kayıt sayısı — birleştirme
    /// bunları YENİDEN SIRALAR.
    pub existing_out_of_order: usize,
    /// Sembol → kullanılan `symbol_id`.
    pub names: BTreeMap<String, u32>,
}

impl DayReport {
    pub fn merged(&self) -> bool {
        self.existing > 0
    }

    /// İki saat aynı dosyada mı (canlı + geri-doldurma).
    pub fn mixed_clocks(&self) -> bool {
        match (self.existing_skew_ms, self.imported_skew_ms) {
            (Some(a), Some(b)) => (a - b).abs() > 60_000,
            _ => false,
        }
    }
}

/// İçe aktarmanın tam raporu.
#[derive(Debug, Default)]
pub struct ImportSummary {
    pub src_dir: PathBuf,
    pub dst_dir: PathBuf,
    /// Okunan kaynak dosya sayısı.
    pub files: usize,
    /// Desene uymadığı için ATLANAN dosya adları.
    pub skipped: Vec<String>,
    /// Tek bir tam kayıt bile içermeyen dosyalar.
    pub empty_files: Vec<PathBuf>,
    /// 48'in katı olmadığı için kırpılan dosyalar ve kırpılan bayt sayısı.
    pub trimmed: Vec<(PathBuf, usize)>,
    /// Kaydın kendi damgası dosya adındaki günden farklı olan kayıt sayısı.
    pub day_mismatch: usize,
    /// Okunan toplam kayıt (kırpma sonrası).
    pub read: usize,
    pub days: BTreeMap<u32, DayReport>,
    /// Sembol tablosunun alındığı dosya.
    pub reference_table: Option<PathBuf>,
    /// İçe aktarmadan ÖNCE hedef dizinde bulunan gün dosyaları — yani canlı
    /// kaydın kapsadığı günler.
    pub preexisting_days: Vec<u32>,
}

impl ImportSummary {
    pub fn duplicates(&self) -> usize {
        self.days
            .values()
            .map(|d| d.dup_imported + d.dup_existing + d.dup_neighbour)
            .sum()
    }

    /// Komşu gün dosyalarında yakalanan çift kayıt toplamı.
    pub fn neighbour_duplicates(&self) -> usize {
        self.days.values().map(|d| d.dup_neighbour).sum()
    }

    pub fn written(&self) -> usize {
        self.days.values().map(|d| d.written).sum()
    }

    pub fn merged_days(&self) -> Vec<u32> {
        self.days.values().filter(|d| d.merged()).map(|d| d.day).collect()
    }

    /// Günler arası bir örtüşme yaşandı mı — yani çift kayıt denetiminin
    /// **komşu güne genişletilmesi** gereken durum oluştu mu.
    ///
    /// Bu makinede ÖLÇÜLDÜ: broker saati UTC+3 ve geri-doldurmada `recv_ms`
    /// broker saatidir. Yani canlı tarafta 11 Ağustos'un akşamına düşen bir
    /// tick, geri-doldurma tarafında 12 Ağustos dosyasına düşer. İki kopya
    /// AYRI dosyalarda kalır — gerçek bir içe aktarmada bunun 28 038 kayıtta
    /// olduğu görüldü.
    ///
    /// Tek gün içinde çalışan bir `(symbol_id,time_msc,bid,ask)` denetimi
    /// onları hiç karşılaştıramazdı; bu yüzden denetim [`neighbour_keys`] ile
    /// KOMŞU günlere genişletildi. Bayrak artık "göremediğimiz bir şey var"
    /// değil, "bu koşuda günler arası eleme DEVREYE GİRDİ" demektir ve kaç
    /// kaydın orada yakalandığı özette yazılıdır.
    ///
    /// Ölçüt DOKUNULAN günler değil, dizinde ZATEN bulunan günler: örtüşen
    /// kopya tam da bizim hiç dokunmadığımız komşu günde durabilir.
    pub fn cross_day_overlap_risk(&self) -> bool {
        self.day_mismatch > 0 && !self.preexisting_days.is_empty()
    }
}

impl std::fmt::Display for ImportSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let merged = self.merged_days();
        writeln!(f, "geri-doldurma ice aktarildi")?;
        writeln!(f, "  kaynak   : {}", self.src_dir.display())?;
        writeln!(f, "  hedef    : {}", self.dst_dir.display())?;
        writeln!(
            f,
            "  dosya    : {} okundu, {} atlandi (desene uymuyor)",
            self.files,
            self.skipped.len()
        )?;
        writeln!(
            f,
            "  tick     : {} okundu, {} cift kayit atildi, {} yazildi",
            self.read,
            self.duplicates(),
            self.written()
        )?;
        writeln!(
            f,
            "  gun      : {} ({} birlestirildi, {} yeni)",
            self.days.len(),
            merged.len(),
            self.days.len() - merged.len()
        )?;
        if let Some(p) = &self.reference_table {
            writeln!(f, "  tablo    : {}", p.display())?;
        }

        for d in self.days.values() {
            if d.merged() {
                writeln!(
                    f,
                    "    {} BIRLESIK  {} mevcut + {} gelen - {} cift = {} kayit",
                    d.day,
                    d.existing,
                    d.imported,
                    d.dup_imported + d.dup_existing + d.dup_neighbour,
                    d.written
                )?;
            } else {
                writeln!(f, "    {} yeni      {} kayit", d.day, d.written)?;
            }
            if d.symbols_written {
                writeln!(
                    f,
                    "      sembol tablosu URETILDI (en son bilinen tablodan kopyalandi)"
                )?;
            }
            if !d.names.is_empty() {
                let s: Vec<String> =
                    d.names.iter().map(|(n, id)| format!("{n}={id}")).collect();
                writeln!(f, "      symbol_id: {}", s.join(", "))?;
            }
        }

        // --- Sessiz kalmaması gereken her şey ---

        if !self.skipped.is_empty() {
            writeln!(f)?;
            writeln!(f, "  ATLANAN dosyalar (<Sembol>-YYYYMMDD.bin degil):")?;
            for n in self.skipped.iter().take(10) {
                writeln!(f, "    {n}")?;
            }
            if self.skipped.len() > 10 {
                writeln!(f, "    ... ve {} tane daha", self.skipped.len() - 10)?;
            }
        }
        for (p, extra) in &self.trimmed {
            writeln!(
                f,
                "  KIRPILDI: {} son {extra} bayt yarim kayit (dosya uzunlugu tek \
                 dogruluk kaynagi)",
                p.display()
            )?;
        }
        for p in &self.empty_files {
            writeln!(f, "  BOS: {} tek bir tam kayit bile icermiyor", p.display())?;
        }
        if self.day_mismatch > 0 {
            writeln!(
                f,
                "  NOT: {} kaydin damgasi dosya adindaki gunden farkli bir UTC \
                 gunune dustu ve O gunun dosyasina yazildi (broker saati ile UTC \
                 arasindaki kayma).",
                self.day_mismatch
            )?;
        }
        for d in self.days.values() {
            if d.dup_neighbour > 0 {
                writeln!(
                    f,
                    "  KOMSU GUN: {} gununde gelen {} kayit, KOMSU gun dosyasinda \
                     zaten bulundugu icin atildi (broker saati ile UTC arasindaki \
                     kayma). Bunlar tek-gun denetiminin goremedigi ciftlerdi.",
                    d.day, d.dup_neighbour
                )?;
            }
            if d.dup_existing > 0 {
                writeln!(
                    f,
                    "  DIKKAT: {} gununde MEVCUT dosyadaki {} kayit cift oldugu \
                     icin atildi — canli kayit degisti.",
                    d.day, d.dup_existing
                )?;
            }
            if d.existing_out_of_order > 0 {
                writeln!(
                    f,
                    "  DIKKAT: {} gununde mevcut {} kaydin time_msc damgasi geriye \
                     siciyordu; birlestirme onlari YENIDEN SIRALADI (kayit sirasi \
                     degisti).",
                    d.day, d.existing_out_of_order
                )?;
            }
            if d.symbols_written_over_ticks {
                writeln!(
                    f,
                    "  DIKKAT: {} gununde tick vardi ama sembol tablosu YOKTU; \
                     tablo en son bilinen tablodan uretildi. O gunun canli \
                     tick'lerindeki symbol_id degerleri bu tabloyla ayni olmalidir.",
                    d.day
                )?;
            }
            if d.mixed_clocks() {
                writeln!(f)?;
                writeln!(f, "  UYARI: {} gununde IKI SAAT bir arada:", d.day)?;
                writeln!(
                    f,
                    "         mevcut kayitlar recv_ms - time_msc = {} ms (yerel alim)",
                    d.existing_skew_ms.unwrap_or(0)
                )?;
                writeln!(
                    f,
                    "         gelen  kayitlar recv_ms - time_msc = {} ms (BROKER saati)",
                    d.imported_skew_ms.unwrap_or(0)
                )?;
                writeln!(
                    f,
                    "         Replay temposu recv_ms farkindan uretilir; bu gunde \
                     tempo iki saat arasinda gidip gelir."
                )?;
            }
        }

        if self.cross_day_overlap_risk() {
            writeln!(f)?;
            writeln!(f, "  NOT: GUNLER ARASI ORTUSME denetlendi.")?;
            writeln!(
                f,
                "        Gelen kayitlarin bir kismi baska bir UTC gunune dustu ve bu \
                 dizinde"
            )?;
            writeln!(
                f,
                "        zaten canli kayit var. Ayni piyasa tick'inin canli kopyasi \
                 KOMSU gun"
            )?;
            writeln!(
                f,
                "        dosyasinda durabilecegi icin cift kayit denetimi komsu \
                 gunlere (+/-1)"
            )?;
            writeln!(
                f,
                "        GENISLETILDI: {} kayit orada yakalanip atildi.",
                self.neighbour_duplicates()
            )?;
            writeln!(
                f,
                "        Kalan tek acik: kopyasi bu dizinde HIC olmayan bir tick \
                 elbette"
            )?;
            writeln!(f, "        elenmez — o zaten cift degil, yeni veridir.")?;
        }

        writeln!(f)?;
        writeln!(f, "  ZAMAN ALANI — geri-doldurulan tick'lerde YEREL ALIM ZAMANI YOK.")?;
        writeln!(f, "  Service `recv_ms` alanina BROKER saatini yaziyor. Replay temposu")?;
        writeln!(f, "  `recv_ms` farkindan uretildigi icin bu gunlerde tempo BROKER")?;
        writeln!(f, "  saatinden gelir; gecikme (lat_us) zaten olculemez ve 0'dir.")?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Kaynak dosya adları
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SrcFile {
    path: PathBuf,
    symbol: String,
    /// Dosya adındaki gün — yalnızca gruplama ve uyuşmazlık raporu için.
    day: u32,
}

/// `<Sembol>-YYYYMMDD.bin` → `(sembol, gun)`.
///
/// `rsplit_once`: sembol adının kendisinde tire olabilir (`US30-cash`), gün
/// damgası ise HER ZAMAN sondadır.
fn parse_src_name(name: &str) -> Option<(String, u32)> {
    let stem = name.strip_suffix(".bin")?;
    let (sym, date) = stem.rsplit_once('-')?;
    if sym.is_empty() || date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let stamp: u32 = date.parse().ok()?;
    // Takvimde gerçekten olmayan bir gün (20260231) sessizce kabul edilirse
    // o dosyanın kayıtları başka bir güne dağılır ve kimse fark etmez.
    record::day_start_ms(stamp)?;
    Some((sym.to_owned(), stamp))
}

fn scan(src: &Path, sum: &mut ImportSummary) -> Result<Vec<SrcFile>, ImportError> {
    let rd = fs::read_dir(src).map_err(io_err("kaynak dizini okuma", src))?;
    let mut out = Vec::new();
    for ent in rd {
        let ent = ent.map_err(io_err("kaynak dizini okuma", src))?;
        if !ent.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let name = ent.file_name();
        let Some(name) = name.to_str() else {
            sum.skipped.push(ent.file_name().to_string_lossy().into_owned());
            continue;
        };
        match parse_src_name(name) {
            Some((symbol, day)) => out.push(SrcFile { path: ent.path(), symbol, day }),
            None => sum.skipped.push(name.to_owned()),
        }
    }
    // Belirlenimcilik: aynı dizin her zaman aynı sırayla işlenir.
    out.sort_by(|a, b| (a.day, &a.symbol).cmp(&(b.day, &b.symbol)));
    Ok(out)
}

// ---------------------------------------------------------------------------
// Sembol tablosu
// ---------------------------------------------------------------------------

/// Bir `symbols-*.jsonl` dosyasını oku.
///
/// Okuma **replay'in kendi okuyucusuyla** yapılıyor: biçimi ikinci kez
/// ayrıştırmak, iki okuyucunun zamanla ayrışması demekti.
fn read_snapshots(path: &Path) -> Result<Vec<SymbolSnapshot>, ImportError> {
    crate::replay::load_symbols(path)
        .map_err(|e| ImportError::BadSymbols { path: path.to_path_buf(), err: e.to_string() })
}

/// Hedef dizindeki EN YENİ sembol tablosunun son görüntüsü.
///
/// Geri-doldurulan günlerin tablosu buradan üretilir: elimizdeki tek gerçek
/// sembol tanımı budur (Service yalnızca fiyat indiriyor, sembol özelliklerini
/// indirmiyor).
fn reference_snapshot(dir: &Path) -> Result<Option<(PathBuf, SymbolSnapshot)>, ImportError> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ImportError::Io {
            what: "hedef dizini okuma",
            path: dir.to_path_buf(),
            source: e,
        }),
    };
    let mut best: Option<(u32, PathBuf)> = None;
    for ent in rd.flatten() {
        let name = ent.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("symbols-") else { continue };
        let Some(stamp) = rest.strip_suffix(".jsonl") else { continue };
        if stamp.len() != 8 {
            continue;
        }
        let Ok(v) = stamp.parse::<u32>() else { continue };
        // `map_or(true, ..)`, `is_none_or` DEĞİL: ikincisi 1.82'de kararlı
        // oldu ve bu çalışma alanının MSRV'si 1.77.
        if best.as_ref().map_or(true, |(b, _)| v > *b) {
            best = Some((v, ent.path()));
        }
    }
    let Some((_, path)) = best else { return Ok(None) };
    let mut snaps = read_snapshots(&path)?;
    // SON görüntü: tablo gün içinde değişmiş olabilir ve bizi ilgilendiren
    // en güncel hâli.
    let last = snaps.pop().expect("load_symbols bos liste dondurmez");
    Ok(Some((path, last)))
}

/// Bir günün sembol adı → `symbol_id` eşlemesini çöz; gerekiyorsa tabloyu üret.
fn resolve_ids(
    opts: &ImportOpts,
    day: u32,
    names: &BTreeSet<String>,
    reference: &Option<(PathBuf, SymbolSnapshot)>,
    rep: &mut DayReport,
) -> Result<BTreeMap<String, u32>, ImportError> {
    let date = record::date_str(day);
    let path = record::symbols_path_str(&opts.data_dir, &opts.instance, &date);
    let mut out = BTreeMap::new();

    if path.is_file() {
        // Gün ZATEN kayıtlı: kimlikler oradan gelir. Kendi kimliğimizi
        // dayatmak, aynı dosyadaki canlı tick'lerin kimliğiyle çakışırdı.
        let snaps = read_snapshots(&path)?;
        for name in names {
            let mut ids: Vec<u32> = Vec::new();
            for (k, s) in snaps.iter().enumerate() {
                match s.items.iter().find(|i| i.s == *name) {
                    Some(it) => {
                        if !ids.contains(&it.id) {
                            ids.push(it.id);
                        }
                    }
                    None => {
                        return Err(ImportError::SymbolMissingInDay {
                            symbol: name.clone(),
                            path,
                            snapshot: k + 1,
                        })
                    }
                }
            }
            if ids.len() > 1 {
                return Err(ImportError::SymbolIdConflict {
                    symbol: name.clone(),
                    path,
                    ids,
                });
            }
            out.insert(name.clone(), ids[0]);
        }
        return Ok(out);
    }

    // Gün yeni: tabloyu en son bilinen tablodan üret.
    let Some((table, snap)) = reference else {
        return Err(ImportError::NoSymbolTable {
            dir: opts.data_dir.join(&opts.instance),
            wanted: names.iter().cloned().collect(),
        });
    };
    for name in names {
        match snap.items.iter().find(|i| i.s == *name) {
            Some(it) => {
                out.insert(name.clone(), it.id);
            }
            None => {
                return Err(ImportError::UnknownSymbol {
                    symbol: name.clone(),
                    table: table.clone(),
                    known: snap.items.iter().map(|i| i.s.clone()).collect(),
                })
            }
        }
    }

    // Tablonun TAMAMI kopyalanıyor, yalnızca içe aktarılan semboller değil:
    // eksik bırakılan bir sembol, o güne sonradan eklenecek bir içe
    // aktarmada kimliksiz kalırdı.
    let items: Vec<SymbolItem> = snap.items.clone();
    // `at_ms` günün başlangıcı: replay "o ana kadarki SON satır" kuralıyla
    // seçiyor, tablonun ilk tick'ten ÖNCE yürürlükte olması gerekiyor. Sabit
    // bir değer olması aynı zamanda içe aktarmayı idempotent yapıyor.
    let at_ms = record::day_start_ms(day).expect("gun damgasi takvimde var");
    let line = serde_json::to_string(&SymbolSnapshot { at_ms, items })
        .expect("SymbolSnapshot serilestirilemez olamaz");
    write_atomic(&path, format!("{line}\n").as_bytes())?;
    rep.symbols_written = true;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Dosya yazma
// ---------------------------------------------------------------------------

/// Önce `.tmp`, sonra `rename`.
///
/// Yarıda kalan bir yazım hedef dosyaya HİÇ dokunmaz: birleştirmenin ortasında
/// kesilen bir içe aktarma, günün canlı kaydını yarım bırakamaz.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ImportError> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = fs::File::create(&tmp).map_err(io_err("gecici dosya olusturma", &tmp))?;
        f.write_all(bytes).map_err(io_err("gecici dosyaya yazma", &tmp))?;
        // Veri diskte OLMADAN rename yapmak, elektrik kesintisinde boş ama
        // "yeni" bir gün dosyası bırakırdı.
        f.sync_all().map_err(io_err("gecici dosyayi diske isleme", &tmp))?;
    }
    fs::rename(&tmp, path).map_err(io_err("gecici dosyayi yerine tasima", path))
}

// ---------------------------------------------------------------------------
// Saat farkı ölçümü
// ---------------------------------------------------------------------------

/// `recv_ms - time_msc` medyanı. Canlı kayıtta bu, yerel saat ile broker saati
/// arasındaki farktır; geri-doldurmada 0'dır (ikisi de broker saati).
fn median_skew(recs: &[TickRec]) -> Option<i64> {
    if recs.is_empty() {
        return None;
    }
    // Örnekleme: bir gün 350 bin kayıt olabiliyor ve medyan için hepsini
    // sıralamanın bir faydası yok.
    let stride = (recs.len() / 512).max(1);
    let mut v: Vec<i64> = recs.iter().step_by(stride).map(|r| r.recv_ms - r.time_msc).collect();
    v.sort_unstable();
    Some(v[v.len() / 2])
}

/// `recv_ms` geriye sıçrayan kayıt sayısı.
///
/// **`recv_ms`, `time_msc` DEĞİL.** Birleştirme dosyayı `recv_ms`'e göre
/// sıralıyor çünkü replay dosyayı o alana göre pencereliyor, k-yollu
/// birleştiriyor ve TEMPOYU o alandan üretiyor ([`crate::replay`]). Canlı
/// kayıtta `recv_ms` kusursuz artan olduğu için bu sayaç normalde 0'dır;
/// 0 olmayan bir değer, sıralamanın mevcut kayıt sırasını gerçekten
/// değiştirdiği anlamına gelir ve bu SÖYLENMELİDİR.
fn out_of_order(recs: &[TickRec]) -> usize {
    recs.windows(2).filter(|w| w[1].recv_ms < w[0].recv_ms).count()
}

// ---------------------------------------------------------------------------
// İçe aktarma
// ---------------------------------------------------------------------------

/// Çift kayıt anahtarı — `(symbol_id, time_msc, bid, ask)`.
///
/// Fiyatlar BİT düzeyinde karşılaştırılıyor: `f64` eşitliği yuvarlamayla
/// oynanacak bir şey değil, bayt bayt aynı kayıt aranıyor.
///
/// `symbol_id` anahtara DAHİL: iki farklı sembolün aynı milisaniyede aynı
/// bid/ask taşıması olağandışı ama imkânsız değil ve o durumda gerçek bir
/// tick'i silmek, önlemeye çalıştığımız sessiz veri kaybının ta kendisi olurdu.
type DupKey = (u32, i64, u64, u64);

fn dup_key(r: &TickRec) -> DupKey {
    (r.symbol_id, r.time_msc, r.bid.to_bits(), r.ask.to_bits())
}

/// Bir günün KOMŞULARINDAKİ (±1) kayıtların çift kayıt anahtarları.
///
/// # Neden komşuya bakmak ZORUNDA
///
/// Anahtarın alanları (`symbol_id`, `time_msc`, `bid`, `ask`) iki tarafta da
/// AYNI anlamı taşıyor: `time_msc` her iki kayıtta da broker damgasıdır. Ama
/// kaydın hangi DOSYAYA gideceğini `recv_ms` belirliyor ve `recv_ms` iki
/// tarafta AYNI ŞEY DEĞİL — canlıda yerel alım, geri-doldurmada broker saati.
/// Broker saati UTC'den kayık olduğu için (bu makinede ölçüldü: UTC+3) aynı
/// piyasa tick'inin iki kopyası KOMŞU gün dosyalarına düşer. Tek gün içinde
/// çalışan bir denetim onları hiç karşılaştıramaz; sonuç, replay'de aynı anın
/// iki kez oynaması, yani SAHTE FİYAT HAREKETİDİR.
///
/// ±1 gün yeterli: kayma broker sunucu saati farkıdır ve dünyada kullanılan
/// hiçbir sunucu saati UTC'den 24 saatten fazla sapmaz.
///
/// Komşu dosyalar YALNIZCA OKUNUR; içeriğine dokunulmaz.
/// Yalnızca **içe aktarmadan ÖNCE var olan** komşu günlere bakılır
/// (`preexisting`). Kendi bu koşuda yazdığımız komşu güne bakmak gereksizdi:
/// iki geri-doldurma dosyasının `time_msc` aralıkları tanım gereği AYRIKTIR
/// (kayıt kendi `recv_ms = time_msc` damgasına göre dosyalanıyor), yani
/// aralarında çift kayıt OLUŞAMAZ. Çift kayıt ancak bir kopyanın canlı
/// kayıttan gelmesiyle doğar. 92 günlük bir içe aktarmada bu, her gün için
/// iki dosyayı boşuna okuyup ~350 bin anahtar sayfalamak demekti.
fn neighbour_keys(
    opts: &ImportOpts,
    day: u32,
    preexisting: &BTreeSet<u32>,
) -> Result<HashSet<DupKey>, ImportError> {
    const MS_PER_DAY: i64 = 86_400_000;
    let mut keys = HashSet::new();
    let Some(start) = record::day_start_ms(day) else { return Ok(keys) };
    for delta in [-MS_PER_DAY, MS_PER_DAY] {
        let n = record::day_stamp(start + delta);
        if !preexisting.contains(&n) {
            continue;
        }
        let path = record::tick_path(&opts.data_dir, &opts.instance, n);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path).map_err(io_err("komsu gun dosyasi okuma", &path))?;
        for r in record::decode_all(&bytes) {
            keys.insert(dup_key(&r));
        }
    }
    Ok(keys)
}

/// Geri-doldurulan dosyaları kanonik kayda taşı.
pub fn import(opts: &ImportOpts) -> Result<ImportSummary, ImportError> {
    if !record::instance_ok(&opts.instance) {
        return Err(ImportError::BadInstance(opts.instance.clone()));
    }
    if !opts.src_dir.is_dir() {
        return Err(ImportError::NoSrcDir(opts.src_dir.clone()));
    }

    let dst_dir = opts.data_dir.join(&opts.instance);
    let mut sum = ImportSummary {
        src_dir: opts.src_dir.clone(),
        dst_dir: dst_dir.clone(),
        ..Default::default()
    };

    let files = scan(&opts.src_dir, &mut sum)?;
    if files.is_empty() {
        return Err(ImportError::NoFiles(opts.src_dir.clone()));
    }

    fs::create_dir_all(&opts.data_dir).map_err(io_err("dizin olusturma", &opts.data_dir))?;
    // Kayıt dizininin TEK YAZAR kilidi: çalışan bir `--record` daemon'u varken
    // gün dosyasını yeniden yazmak, onun bu arada eklediği tick'leri silerdi.
    let _lock = record::lock_dir(&opts.data_dir).map_err(ImportError::Locked)?;
    fs::create_dir_all(&dst_dir).map_err(io_err("dizin olusturma", &dst_dir))?;

    let reference = reference_snapshot(&dst_dir)?;
    sum.reference_table = reference.as_ref().map(|(p, _)| p.clone());
    // Dokunmadan ÖNCEKİ hâl: hangi günlerde zaten canlı kayıt var. Görülemez
    // örtüşme uyarısı buna dayanıyor (bkz. `cross_day_overlap_risk`).
    sum.preexisting_days = record::list_days(&opts.data_dir, &opts.instance)
        .map_err(io_err("gun listesi okuma", &dst_dir))?;
    let preexisting: BTreeSet<u32> = sum.preexisting_days.iter().copied().collect();

    // Dosya adındaki güne göre GRUP GRUP işleniyor: 3 aylık bir içe aktarma
    // tek diziye alınsa ~350 MB bellek isterdi. Bir gün ~17 MB.
    let mut by_name_day: BTreeMap<u32, Vec<&SrcFile>> = BTreeMap::new();
    for f in &files {
        by_name_day.entry(f.day).or_default().push(f);
    }

    for (name_day, group) in by_name_day {
        // gun -> sembol -> kayitlar
        let mut buckets: BTreeMap<u32, BTreeMap<String, Vec<TickRec>>> = BTreeMap::new();
        for f in group {
            let bytes = fs::read(&f.path).map_err(io_err("kaynak dosya okuma", &f.path))?;
            let extra = bytes.len() % REC_SIZE;
            if extra != 0 {
                // Kırpma HATA DEĞİL: yarım kayıt, indirmenin yarıda kesilmesinden
                // başka bir anlama gelmez ve tek doğru davranış onu atmaktır.
                sum.trimmed.push((f.path.clone(), extra));
            }
            let recs = record::decode_all(&bytes);
            sum.files += 1;
            sum.read += recs.len();
            if recs.is_empty() {
                sum.empty_files.push(f.path.clone());
                continue;
            }
            for r in recs {
                // Kaydın hangi güne gideceğini KENDİ damgası söyler.
                let day = record::day_stamp(r.recv_ms);
                if day != name_day {
                    sum.day_mismatch += 1;
                }
                buckets.entry(day).or_default().entry(f.symbol.clone()).or_default().push(r);
            }
        }

        for (day, per_symbol) in buckets {
            let names: BTreeSet<String> = per_symbol.keys().cloned().collect();
            let rep = sum.days.entry(day).or_insert(DayReport { day, ..Default::default() });

            let ids = resolve_ids(opts, day, &names, &reference, rep)?;

            // symbol_id GERÇEK kimlikle dolduruluyor: Service bu alanı
            // bilemez (bizim tablomuz onda yok) ve doldurulmazsa replay
            // tick'i isimlendiremez.
            let mut imported: Vec<TickRec> = Vec::new();
            for (name, recs) in per_symbol {
                let id = ids[&name];
                imported.extend(recs.into_iter().map(|mut r| {
                    r.symbol_id = id;
                    r
                }));
                rep.names.insert(name, id);
            }
            rep.imported += imported.len();
            rep.imported_skew_ms = median_skew(&imported);

            let path = record::tick_path(&opts.data_dir, &opts.instance, day);
            let existing: Vec<TickRec> = if path.is_file() {
                let bytes = fs::read(&path).map_err(io_err("gun dosyasi okuma", &path))?;
                let extra = bytes.len() % REC_SIZE;
                if extra != 0 {
                    sum.trimmed.push((path.clone(), extra));
                }
                record::decode_all(&bytes)
            } else {
                Vec::new()
            };
            // Gün ilk kez dokunuluyorsa "önceki hâl" budur; ikinci kez
            // dokunuluyorsa (komşu günden taşan kayıtlar) ilk ölçüm korunur.
            if rep.written == 0 {
                rep.existing = existing.len();
                rep.existing_skew_ms = median_skew(&existing);
                rep.existing_out_of_order = out_of_order(&existing);
                rep.symbols_written_over_ticks = rep.symbols_written && !existing.is_empty();
            }

            // KOMŞU GÜNLERDEKİ kopyalar. Yalnızca GELEN kayıtlar buna karşı
            // eleniyor: diskteki bir kaydı komşusuna bakarak silmek, canlı
            // kaydı bozmak olurdu.
            let neighbours = neighbour_keys(opts, day, &preexisting)?;

            // BİRLEŞTİR — üzerine YAZMA. Mevcut kayıtlar önce geliyor: aynı
            // dörtlü iki kaynakta da varsa DİSKTEKİ kayıt korunur, gelen
            // atılır.
            let mut seen: HashSet<DupKey> =
                HashSet::with_capacity(existing.len() + imported.len());
            let mut out: Vec<TickRec> = Vec::with_capacity(existing.len() + imported.len());
            for (r, from_disk) in existing
                .iter()
                .map(|r| (r, true))
                .chain(imported.iter().map(|r| (r, false)))
            {
                let key = dup_key(r);
                if !from_disk && neighbours.contains(&key) {
                    rep.dup_neighbour += 1;
                    continue;
                }
                if seen.insert(key) {
                    out.push(*r);
                } else if from_disk {
                    rep.dup_existing += 1;
                } else {
                    rep.dup_imported += 1;
                }
            }
            // SIRALAMA ALANI `recv_ms` — `time_msc` DEĞİL.
            //
            // Replay gün dosyasını `recv_ms` ile pencereliyor, örnekler arası
            // k-yollu birleştirmeyi `recv_ms` ile yapıyor ve TEMPOYU
            // `recv_ms` farklarından üretiyor (bkz. `replay::merge_by_recv_ms`
            // ve oynatım döngüsü). `time_msc` ile sıralamak iki ayrı zararı
            // birden veriyordu:
            //   1. Canlı kayıtta `recv_ms` kusursuz artandır ama `time_msc`
            //      semboller arası damga gecikmesi yüzünden geriye sıçrar
            //      (bu makinede ölçüldü: 352 471 kayıtta 33 587 sıçrama).
            //      `time_msc` ile sıralamak, replay'in "kayıt sırası
            //      sözleşmedir" kuralını çiğneyerek canlı kaydı YENİDEN
            //      DİZİYORDU.
            //   2. Birleşik bir günde iki saat yaşıyor (canlı `recv_ms`
            //      yerel, gelen `recv_ms` broker = ~3 saat kayık).
            //      `time_msc` ile sıralamak `recv_ms` dizisini zikzak
            //      yapıyordu; oynatım döngüsü `(recv_ms - base).max(0)`
            //      kullandığı için tempo önce SIFIRA çöküp sonra saatlerce
            //      uyuyacaktı.
            // KARARLI sıralama: eşit `recv_ms` taşıyan kayıtlarda disk
            // sırası korunur, yani aynı girdi her zaman aynı çıktıyı verir.
            out.sort_by_key(|r| r.recv_ms);

            // GERİYE HİÇBİR ŞEY KALMADIYSA o günü hiç YARATMA.
            //
            // Komşu-gün elemesinden sonra bir gün tamamen boşalabilir: gelen
            // kayıtların TAMAMI zaten komşu gün dosyasında canlı olarak
            // duruyordur. O gün için 0 baytlık bir tick dosyası ve yanına bir
            // sembol tablosu bırakmak, kayıtta VAR OLMAYAN bir günü var
            // göstermek olurdu — replay onu listeler, açar ve boş bulur.
            if out.is_empty() && !path.is_file() {
                if rep.symbols_written {
                    let sym =
                        record::symbols_path_str(&opts.data_dir, &opts.instance, &record::date_str(day));
                    let _ = fs::remove_file(&sym);
                    rep.symbols_written = false;
                }
                continue;
            }

            let mut bytes = Vec::with_capacity(out.len() * REC_SIZE);
            for r in &out {
                bytes.extend_from_slice(&r.to_bytes());
            }
            write_atomic(&path, &bytes)?;
            rep.written = out.len();
        }
    }

    Ok(sum)
}

// ---------------------------------------------------------------------------
// Testler
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new(tag: &str) -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let p = std::env::temp_dir().join(format!(
                "sinyal-bf-{}-{}-{}",
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

    const INST: &str = "mt5-1";
    const DAY: u32 = 2026_05_04;

    /// Kurulum: kaynak dizin + hedef kayıt kökü.
    struct Fixture {
        tmp: TmpDir,
        src: PathBuf,
        data: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> Self {
            let tmp = TmpDir::new(tag);
            let src = tmp.path().join("backfill");
            let data = tmp.path().join("veri");
            fs::create_dir_all(&src).unwrap();
            fs::create_dir_all(data.join(INST)).unwrap();
            Self { tmp, src, data }
        }

        fn opts(&self) -> ImportOpts {
            ImportOpts {
                src_dir: self.src.clone(),
                data_dir: self.data.clone(),
                instance: INST.to_owned(),
            }
        }

        /// Referans sembol tablosu — canlı kaydın bıraktığı dosyanın eşi.
        fn write_table(&self, day: u32, items: &[(u32, &str)]) {
            let items: Vec<SymbolItem> = items
                .iter()
                .map(|(id, s)| SymbolItem {
                    id: *id,
                    s: (*s).to_owned(),
                    digits: 2,
                    point: 0.01,
                    contract_size: 100.0,
                    ready: true,
                    src: INST.to_owned(),
                    ..Default::default()
                })
                .collect();
            let line = serde_json::to_string(&SymbolSnapshot {
                at_ms: record::day_start_ms(day).unwrap() + 3_600_000,
                items,
            })
            .unwrap();
            let p = record::symbols_path_str(&self.data, INST, &record::date_str(day));
            fs::write(p, format!("{line}\n")).unwrap();
        }

        fn write_src(&self, symbol: &str, day: u32, recs: &[TickRec]) -> PathBuf {
            let mut bytes = Vec::new();
            for r in recs {
                bytes.extend_from_slice(&r.to_bytes());
            }
            let p = self.src.join(format!("{symbol}-{}.bin", record::date_str(day)));
            fs::write(&p, &bytes).unwrap();
            p
        }

        fn day_path(&self, day: u32) -> PathBuf {
            record::tick_path(&self.data, INST, day)
        }

        fn tmp(&self) -> &Path {
            self.tmp.path()
        }
    }

    /// Geri-doldurulmuş bir kayıt: `recv_ms` = broker saati (yerel alım YOK).
    fn back(i: i64) -> TickRec {
        let t = record::day_start_ms(DAY).unwrap() + 9 * 3_600_000 + i * 1000;
        TickRec {
            recv_ms: t,
            time_msc: t,
            bid: 2300.0 + i as f64 * 0.01,
            ask: 2300.2 + i as f64 * 0.01,
            last: 0.0,
            symbol_id: 0,
            flags: 6,
            kind: 1,
        }
    }

    #[test]
    fn an_imported_file_reads_back_bit_for_bit() {
        // İçe aktarılan dosya ÜRETİMDEKİ yükleyiciyle geri okunduğunda
        // birebir aynı olmalı: yuvarlama ya da alan kaybı, stratejinin
        // gördüğü seriyi sessizce değiştirirdi.
        let fx = Fixture::new("roundtrip");
        fx.write_table(2026_08_11, &[(0, "EURUSD"), (3, "GOLD")]);
        let recs: Vec<TickRec> = (0..500).map(back).collect();
        fx.write_src("GOLD", DAY, &recs);

        let sum = import(&fx.opts()).expect("ice aktarma basarili olmali");
        assert_eq!(sum.files, 1);
        assert_eq!(sum.read, 500);
        assert_eq!(sum.written(), 500);
        assert_eq!(sum.duplicates(), 0);

        let back_recs = record::load_ticks(&fx.day_path(DAY)).unwrap();
        assert_eq!(back_recs.len(), recs.len());
        for (i, (got, want)) in back_recs.iter().zip(&recs).enumerate() {
            assert_eq!(got.recv_ms, want.recv_ms, "kayit {i}");
            assert_eq!(got.time_msc, want.time_msc, "kayit {i}");
            // BİT düzeyinde: fiyat yuvarlaması kabul edilemez.
            assert_eq!(got.bid.to_bits(), want.bid.to_bits(), "kayit {i}");
            assert_eq!(got.ask.to_bits(), want.ask.to_bits(), "kayit {i}");
            assert_eq!(got.last.to_bits(), want.last.to_bits(), "kayit {i}");
            assert_eq!(got.flags, want.flags, "kayit {i}");
            assert_eq!(got.kind, want.kind, "kayit {i}");
            // TEK DEĞİŞEN ALAN: kimlik dosya adından çözülüp dolduruldu.
            assert_eq!(got.symbol_id, 3, "GOLD'un gercek kimligi yazilmali");
        }
        assert_eq!(sum.days[&DAY].names["GOLD"], 3);
    }

    #[test]
    fn an_imported_day_is_loadable_by_the_real_replay_loader() {
        // DİKİŞ TESTİ: içe aktarıcı ile replay ayrı ayrı yeşil kalabilir ve
        // yine de birbirini hiç anlamayabilir. Burada ÜRETİMDEKİ replay
        // yükleyicisi, içe aktarıcının ürettiği dizini okuyor.
        let fx = Fixture::new("seam");
        fx.write_table(2026_08_11, &[(0, "EURUSD"), (3, "GOLD")]);
        fx.write_src("GOLD", DAY, &(0..64).map(back).collect::<Vec<_>>());
        import(&fx.opts()).unwrap();

        let opts = crate::replay::ReplayOpts {
            speed: 0.0,
            ..crate::replay::ReplayOpts::new(&fx.data, record::date_str(DAY))
        };
        let rec = crate::replay::load(&opts)
            .expect("ice aktarilan gun replay tarafindan YUKLENEBILMELI");
        assert_eq!(rec.instances, vec![INST.to_string()]);
        assert_eq!(rec.len(), 64);

        // ASIL TUZAK: symbol_id → ad. Kopmuşsa replay her tick'i atlar ve
        // akış SESSİZCE boşalır.
        let items = &rec.symbols[0].last().unwrap().items;
        let name = items.iter().find(|i| i.id == 3).map(|i| i.s.as_str());
        assert_eq!(name, Some("GOLD"), "kimlik cozulemedi: {items:?}");
        // Sembol özellikleri de taşınmalı: contract_size 0 kalsaydı
        // simülatörün marjini ve kârı sessizce yanlış çıkardı.
        assert_eq!(items.iter().find(|i| i.id == 3).unwrap().contract_size, 100.0);
    }

    #[test]
    fn a_file_that_is_not_a_multiple_of_48_is_trimmed_not_refused() {
        // Yarıda kesilmiş bir indirme HATA DEĞİL: dosya uzunluğu tek doğruluk
        // kaynağı, yarım kayıt atılır ve bu BİLDİRİLİR.
        let fx = Fixture::new("trim");
        fx.write_table(2026_08_11, &[(3, "GOLD")]);
        let recs: Vec<TickRec> = (0..3).map(back).collect();
        let p = fx.write_src("GOLD", DAY, &recs);
        let mut bytes = fs::read(&p).unwrap();
        bytes.extend_from_slice(&[0xAB; 17]); // yarım kayıt
        fs::write(&p, &bytes).unwrap();

        let sum = import(&fx.opts()).expect("kirpilmali, HATA VERMEMELI");
        assert_eq!(sum.read, 3, "yarim kayit atilmali");
        assert_eq!(sum.written(), 3);
        assert_eq!(sum.trimmed.len(), 1, "kirpma sessiz kalmamali");
        assert_eq!(sum.trimmed[0].1, 17);
        assert_eq!(record::load_ticks(&fx.day_path(DAY)).unwrap().len(), 3);
        assert!(format!("{sum}").contains("KIRPILDI"), "ozet kirpmayi yazmali");
    }

    #[test]
    fn merging_with_an_existing_day_never_duplicates_a_tick() {
        // Canlı kayıt o günü kısmen kapsıyorsa üzerine yazmak veriyi çöpe
        // atardı; körlemesine eklemek ise her örtüşen tick'i İKİ KEZ yazardı
        // ve replay'de sahte hareket üretirdi.
        let fx = Fixture::new("merge");
        fx.write_table(DAY, &[(3, "GOLD")]);

        // Diskte 0..40 arası kayıtlar (canlı kayıt gibi: recv_ms broker
        // saatinden 3 saat geride).
        let live: Vec<TickRec> = (0..40)
            .map(back)
            .map(|mut r| {
                r.symbol_id = 3;
                r.recv_ms -= 3 * 3_600_000;
                r
            })
            .collect();
        let mut bytes = Vec::new();
        for r in &live {
            bytes.extend_from_slice(&r.to_bytes());
        }
        fs::write(fx.day_path(DAY), &bytes).unwrap();

        // Geri-doldurma 0..60'ı kapsıyor: 40 kayıt ÖRTÜŞÜYOR.
        fx.write_src("GOLD", DAY, &(0..60).map(back).collect::<Vec<_>>());

        let sum = import(&fx.opts()).unwrap();
        let rep = &sum.days[&DAY];
        assert!(rep.merged(), "gun birlestirilmis sayilmali");
        assert_eq!(rep.existing, 40);
        assert_eq!(rep.imported, 60);
        assert_eq!(rep.dup_imported, 40, "ortusen her kayit bir kez atilmali");
        assert_eq!(rep.dup_existing, 0, "canli kayittan hicbir sey atilmamali");
        assert_eq!(rep.written, 60);

        let out = record::load_ticks(&fx.day_path(DAY)).unwrap();
        assert_eq!(out.len(), 60);
        // Sıra `time_msc` ile artan olmalı.
        // SIRALAMA ALANI recv_ms: replay pencereyi, birleştirmeyi ve TEMPOYU
        // o alandan üretiyor.
        assert!(out.windows(2).all(|w| w[0].recv_ms <= w[1].recv_ms), "recv_ms sirali degil");
        // Tek bir (symbol_id,time_msc,bid,ask) dörtlüsü iki kez olmamalı.
        let uniq: HashSet<DupKey> = out.iter().map(dup_key).collect();
        assert_eq!(uniq.len(), out.len(), "cift kayit kalmis");
        // Örtüşen bölgede DİSKTEKİ kayıt korunmalı: yerel alım zamanı taşıyan
        // kayıt, broker saatiyle damgalanmış kopyasından daha değerli.
        assert_eq!(out[0].recv_ms, live[0].recv_ms, "diskteki kayit korunmali");

        // İki saat bir arada: bu SESSİZ KALMAMALI.
        assert!(rep.mixed_clocks(), "karisik saat tespit edilmeliydi: {rep:?}");
        let text = format!("{sum}");
        assert!(text.contains("IKI SAAT"), "ozet karisik saati yazmali:\n{text}");
        assert!(text.contains("BIRLESIK"), "ozet birlesen gunu yazmali:\n{text}");
    }

    #[test]
    fn running_the_same_import_twice_changes_nothing() {
        // İdempotenslik: operatör aynı komutu ikinci kez çalıştırdığında
        // (yarıda kesilen bir indirmeden sonra bu KAÇINILMAZ) kayıt
        // büyümemeli.
        let fx = Fixture::new("idem");
        fx.write_table(2026_08_11, &[(3, "GOLD"), (0, "EURUSD")]);
        fx.write_src("GOLD", DAY, &(0..120).map(back).collect::<Vec<_>>());
        fx.write_src(
            "EURUSD",
            DAY,
            &(0..30)
                .map(|i| {
                    let mut r = back(i);
                    r.bid = 1.1 + i as f64 * 1e-5;
                    r.ask = 1.2 + i as f64 * 1e-5;
                    r
                })
                .collect::<Vec<_>>(),
        );

        let first = import(&fx.opts()).unwrap();
        let bytes1 = fs::read(fx.day_path(DAY)).unwrap();
        let sym1 = fs::read(record::symbols_path_str(&fx.data, INST, &record::date_str(DAY)))
            .unwrap();
        assert_eq!(first.written(), 150);
        assert_eq!(first.duplicates(), 0);
        assert!(first.days[&DAY].symbols_written, "gun icin tablo uretilmeli");

        let second = import(&fx.opts()).unwrap();
        let bytes2 = fs::read(fx.day_path(DAY)).unwrap();
        let sym2 = fs::read(record::symbols_path_str(&fx.data, INST, &record::date_str(DAY)))
            .unwrap();

        assert_eq!(bytes1, bytes2, "ikinci calistirma gun dosyasini DEGISTIRMEMELI");
        assert_eq!(sym1, sym2, "ikinci calistirma sembol tablosunu DEGISTIRMEMELI");
        assert_eq!(second.written(), 150, "kayit sayisi buyumemeli");
        assert_eq!(second.duplicates(), 150, "ikinci turda her kayit cift olmali");
        assert!(!second.days[&DAY].symbols_written, "tablo ikinci kez yazilmamali");
        // Geçici dosya ARDINDA BIRAKILMAMALI.
        let leftovers: Vec<PathBuf> = fs::read_dir(fx.data.join(INST))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "tmp"))
            .collect();
        assert!(leftovers.is_empty(), "gecici dosya kalmis: {leftovers:?}");
    }

    #[test]
    fn a_corrupt_or_missing_symbol_table_is_a_clear_error() {
        // Kimliksiz bir tick replay'de İSİMLENDİRİLEMEZ ve sessizce atlanır:
        // "sistem calisiyor ama piyasa hareketsiz" yanılsaması.
        let fx = Fixture::new("nosym");
        fx.write_src("GOLD", DAY, &(0..10).map(back).collect::<Vec<_>>());

        // (a) Hiç tablo yok.
        let err = import(&fx.opts()).expect_err("tablosuz ice aktarma REDDEDILMELI");
        let msg = err.to_string();
        assert!(matches!(err, ImportError::NoSymbolTable { .. }), "{err:?}");
        assert!(msg.contains("GOLD"), "hangi sembol oldugu soylenmeli: {msg}");
        assert!(msg.contains("--record"), "ne yapilacagi soylenmeli: {msg}");
        assert!(!fx.day_path(DAY).exists(), "hata sonrasi gun dosyasi YAZILMAMALI");

        // (b) Tablo var ama bozuk.
        let p = record::symbols_path_str(&fx.data, INST, "20260811");
        fs::write(&p, "{\"at_ms\":1,\"items\":[{\"id\":3,\n").unwrap();
        let err = import(&fx.opts()).expect_err("bozuk tablo REDDEDILMELI");
        assert!(matches!(err, ImportError::BadSymbols { .. }), "{err:?}");
        assert!(err.to_string().contains("symbols-20260811.jsonl"), "{err}");

        // (c) Tablo geçerli ama sembol içinde yok.
        fx.write_table(2026_08_11, &[(0, "EURUSD")]);
        let err = import(&fx.opts()).expect_err("bilinmeyen sembol REDDEDILMELI");
        assert!(matches!(err, ImportError::UnknownSymbol { .. }), "{err:?}");
        assert!(err.to_string().contains("EURUSD"), "bilinenler listelenmeli: {err}");

        // (d) Tablo tamam: artık geçmeli.
        fx.write_table(2026_08_11, &[(0, "EURUSD"), (3, "GOLD")]);
        assert_eq!(import(&fx.opts()).unwrap().written(), 10);
    }

    #[test]
    fn a_day_whose_table_lacks_the_symbol_is_refused_not_guessed() {
        // Gün ZATEN kayıtlı ve tablosunda bu sembol yok: kimlik uydurmak, o
        // günün canlı tick'lerindeki bir kimliğe çarpabilirdi.
        let fx = Fixture::new("dayconflict");
        fx.write_table(DAY, &[(0, "EURUSD")]);
        fx.write_src("GOLD", DAY, &(0..5).map(back).collect::<Vec<_>>());
        let err = import(&fx.opts()).expect_err("REDDEDILMELI");
        assert!(matches!(err, ImportError::SymbolMissingInDay { .. }), "{err:?}");
        assert!(err.to_string().contains("--data-dir"), "cikis yolu soylenmeli: {err}");
    }

    #[test]
    fn a_record_whose_stamp_falls_on_another_day_goes_to_that_day() {
        // Broker saati UTC'den kaymış olabilir; dosya adı hangi günün
        // İSTENDİĞİNİ söyler, kaydın damgası hangi güne AİT olduğunu.
        let fx = Fixture::new("spill");
        fx.write_table(2026_08_11, &[(3, "GOLD")]);
        let mut recs: Vec<TickRec> = (0..4).map(back).collect();
        // Son iki kayıt ertesi güne taşıyor.
        for r in recs.iter_mut().skip(2) {
            r.recv_ms += 20 * 3_600_000;
            r.time_msc += 20 * 3_600_000;
        }
        fx.write_src("GOLD", DAY, &recs);

        let sum = import(&fx.opts()).unwrap();
        assert_eq!(sum.day_mismatch, 2, "uyusmazlik sayilmali");
        assert_eq!(sum.days.len(), 2, "iki ayri gun dosyasi olmali");
        assert_eq!(record::load_ticks(&fx.day_path(DAY)).unwrap().len(), 2);
        assert_eq!(record::load_ticks(&fx.day_path(DAY + 1)).unwrap().len(), 2);
        assert!(format!("{sum}").contains("dosya adindaki gunden farkli"));
        // Ortada canlı kayıt yokken bu bir RİSK DEĞİL: her açılışta bağıran
        // bir uyarı, gerçekten önemli olduğunda okunmaz.
        assert!(!sum.cross_day_overlap_risk(), "yanlis alarm: {sum}");
    }

    #[test]
    fn an_overlap_that_lands_on_the_neighbour_day_is_warned_about() {
        // Çift kayıt denetimi TEK GÜN dosyası içinde çalışır. Broker saati
        // UTC'den kaymış olduğu için (bu makinede ölçülen: +3 saat) aynı
        // piyasa tick'i canlı tarafta bir güne, geri-doldurmada KOMŞU güne
        // düşebilir; iki kopya ayrı dosyalarda olduğu için denetim onları hiç
        // karşılaştıramaz. Sessiz kalması, aynı anı iki kez içeren bir kayıt
        // bırakırdı.
        let fx = Fixture::new("crossday");
        fx.write_table(DAY, &[(3, "GOLD")]);
        let live: Vec<TickRec> = (0..5)
            .map(back)
            .map(|mut r| {
                r.symbol_id = 3;
                r
            })
            .collect();
        let mut bytes = Vec::new();
        for r in &live {
            bytes.extend_from_slice(&r.to_bytes());
        }
        fs::write(fx.day_path(DAY), &bytes).unwrap();

        // Gelen dosyanın kayıtları ertesi güne taşıyor.
        let spill: Vec<TickRec> = (0..4)
            .map(back)
            .map(|mut r| {
                r.recv_ms += 20 * 3_600_000;
                r.time_msc += 20 * 3_600_000;
                r
            })
            .collect();
        fx.write_src("GOLD", DAY, &spill);

        let sum = import(&fx.opts()).unwrap();
        assert!(sum.cross_day_overlap_risk(), "gunler arasi ortusme bildirilmeli");
        let text = format!("{sum}");
        assert!(text.contains("GUNLER ARASI ORTUSME"), "{text}");
        // Bu kurgudaki taşan kayıtlar canlı kayıtla AYNI tick DEĞİL (damgaları
        // farklı), yani atılacak bir şey yok: uyarı var, eleme yok.
        assert_eq!(sum.neighbour_duplicates(), 0, "yanlis eleme: {text}");
        assert_eq!(record::load_ticks(&fx.day_path(DAY + 1)).unwrap().len(), 4);
    }

    #[test]
    fn a_live_day_keeps_its_record_order_even_when_time_msc_jumps_back() {
        // ÖLÇÜLDÜ: canlı kayıtta `recv_ms` kusursuz artan ama `time_msc`
        // geriye sıçrıyor (352 471 kayıtta 33 587 sıçrama — semboller arası
        // damga gecikmesi farklı). Replay "kayıt sırası SÖZLEŞMEDİR" diyor ve
        // tempoyu `recv_ms`'ten üretiyor; birleştirme `time_msc` ile
        // sıralasaydı canlı kaydı yeniden dizerdi.
        let fx = Fixture::new("order");
        fx.write_table(DAY, &[(3, "GOLD")]);
        let live: Vec<TickRec> = (0..8)
            .map(back)
            .enumerate()
            .map(|(i, mut r)| {
                r.symbol_id = 3;
                // Tek indekslerde damga geriye sıçrıyor.
                r.time_msc = r.recv_ms - if i % 2 == 1 { 2_000 } else { 0 };
                r
            })
            .collect();
        let mut bytes = Vec::new();
        for r in &live {
            bytes.extend_from_slice(&r.to_bytes());
        }
        fs::write(fx.day_path(DAY), &bytes).unwrap();

        // Aynı güne, damgaları farklı bir tek kayıt geliyor.
        let mut extra = back(100);
        extra.recv_ms += 60_000;
        extra.time_msc = extra.recv_ms;
        fx.write_src("GOLD", DAY, &[extra]);

        let sum = import(&fx.opts()).unwrap();
        assert_eq!(sum.days[&DAY].existing_out_of_order, 0, "canli kayit recv_ms'te sirali");
        let out = record::load_ticks(&fx.day_path(DAY)).unwrap();
        assert_eq!(out.len(), live.len() + 1);
        // Canlı kayıtların GÖRECELİ SIRASI aynen korunmuş olmalı.
        let live_back: Vec<i64> =
            out.iter().filter(|r| r.recv_ms != extra.recv_ms).map(|r| r.recv_ms).collect();
        assert_eq!(
            live_back,
            live.iter().map(|r| r.recv_ms).collect::<Vec<_>>(),
            "canli kayit YENIDEN DIZILMIS"
        );
        assert!(out.windows(2).all(|w| w[0].recv_ms <= w[1].recv_ms), "recv_ms sirali degil");
    }

    #[test]
    fn a_copy_that_landed_on_the_neighbour_day_is_dropped_not_replayed_twice() {
        // GERÇEK SENARYO. Broker saati UTC+3: canlı kayıtta bir tick'in
        // `recv_ms`'i yerel alım zamanıdır ve o tick 4 Mayıs dosyasına düşer;
        // AYNI tick geri-doldurmada `recv_ms = time_msc` (broker saati) ile
        // gelir ve 5 Mayıs dosyasına düşer. Tek gün içinde çalışan bir çift
        // kayıt denetimi ikisini HİÇ karşılaştıramaz ve replay o piyasa anını
        // İKİ KEZ oynatır — sahte fiyat hareketi.
        let fx = Fixture::new("neighbourdup");
        fx.write_table(DAY, &[(3, "GOLD")]);

        // Canlı kayıt: gün DAY, `recv_ms` yerel (broker damgasından 3 saat geri).
        // Tick'ler günün SONUNDA (22:00 UTC civarı): +3 saat onları ertesi
        // güne taşır. Örtüşme tam da burada, gün sınırında yaşanıyor.
        let live: Vec<TickRec> = (0..6)
            .map(back)
            .map(|mut r| {
                r.symbol_id = 3;
                r.recv_ms += 22 * 3_600_000;
                // time_msc 3 saat İLERİ: broker saati UTC+3.
                r.time_msc = r.recv_ms + 3 * 3_600_000;
                r
            })
            .collect();
        let mut bytes = Vec::new();
        for r in &live {
            bytes.extend_from_slice(&r.to_bytes());
        }
        fs::write(fx.day_path(DAY), &bytes).unwrap();

        // Geri-doldurma: AYNI tick'ler, `recv_ms = time_msc`. Damga 3 saat
        // ileri olduğu için kayıtların bir kısmı ERTESİ güne düşüyor.
        let backfilled: Vec<TickRec> = live
            .iter()
            .map(|r| {
                let mut c = *r;
                c.recv_ms = c.time_msc;
                c.symbol_id = 0; // Service kimlik yazmaz
                c
            })
            .collect();
        fx.write_src("GOLD", DAY, &backfilled);

        let sum = import(&fx.opts()).unwrap();
        let text = format!("{sum}");
        assert!(sum.day_mismatch > 0, "kayitlar komsu gune dusmeliydi:\n{text}");
        assert_eq!(
            sum.neighbour_duplicates(),
            backfilled.len(),
            "komsu gune dusen her kopya elenmeliydi:\n{text}"
        );
        // Ertesi günün dosyası HİÇ OLUŞMAMALI: oraya düşen her kayıt zaten
        // DAY dosyasında canlı olarak duruyor.
        assert!(!fx.day_path(DAY + 1).is_file(), "ayni tick ikinci bir gune yazilmis:\n{text}");
        assert!(
            !record::symbols_path_str(&fx.data, INST, &record::date_str(DAY + 1)).is_file(),
            "bos gun icin sembol tablosu birakilmis:\n{text}"
        );
        // Canlı kayıt aynen duruyor.
        assert_eq!(record::load_ticks(&fx.day_path(DAY)).unwrap().len(), live.len());
        assert!(text.contains("KOMSU GUN"), "eleme sessiz kalmamali:\n{text}");

        // İKİNCİ KOŞU hiçbir şeyi değiştirmemeli.
        let before = fs::read(fx.day_path(DAY)).unwrap();
        let sum2 = import(&fx.opts()).unwrap();
        assert_eq!(fs::read(fx.day_path(DAY)).unwrap(), before, "idempotent degil");
        assert_eq!(sum2.written(), sum.written());
    }

    #[test]
    fn files_that_do_not_match_the_pattern_are_named_not_swallowed() {
        let fx = Fixture::new("skip");
        fx.write_table(2026_08_11, &[(3, "GOLD")]);
        fx.write_src("GOLD", DAY, &(0..2).map(back).collect::<Vec<_>>());
        fs::write(fx.src.join("GOLD-2026050.bin"), [0u8; 48]).unwrap();
        fs::write(fx.src.join("GOLD-20260231.bin"), [0u8; 48]).unwrap(); // takvimde yok
        fs::write(fx.src.join("notlar.txt"), b"x").unwrap();

        let sum = import(&fx.opts()).unwrap();
        assert_eq!(sum.files, 1);
        assert_eq!(sum.skipped.len(), 3, "atlananlar: {:?}", sum.skipped);
        let text = format!("{sum}");
        assert!(text.contains("ATLANAN"), "{text}");
        assert!(text.contains("GOLD-20260231.bin"), "{text}");
    }

    #[test]
    fn the_symbol_name_may_contain_a_dash() {
        // `US30-cash-20260504.bin` — gün damgası HER ZAMAN sonda.
        assert_eq!(
            parse_src_name("US30-cash-20260504.bin"),
            Some(("US30-cash".to_string(), 2026_05_04))
        );
        assert_eq!(parse_src_name("GOLD-20260504.bin"), Some(("GOLD".to_string(), 2026_05_04)));
        assert_eq!(parse_src_name("GOLD-20260504.tkc"), None);
        assert_eq!(parse_src_name("-20260504.bin"), None);
        assert_eq!(parse_src_name("GOLD20260504.bin"), None);
        assert_eq!(parse_src_name("GOLD-2026050a.bin"), None);
    }

    #[test]
    fn a_running_recorder_blocks_the_import() {
        // İçe aktarma gün dosyasını YENİDEN YAZAR. Aynı anda kayıt yapan bir
        // daemon varsa onun bu arada eklediği tick'ler `rename` ile silinirdi.
        let fx = Fixture::new("lock");
        fx.write_table(2026_08_11, &[(3, "GOLD")]);
        fx.write_src("GOLD", DAY, &(0..2).map(back).collect::<Vec<_>>());

        let rec = record::Recorder::start(&fx.data, "mt5-2").unwrap();
        let err = import(&fx.opts()).expect_err("kilitli dizinde ice aktarma REDDEDILMELI");
        assert!(matches!(err, ImportError::Locked(_)), "{err:?}");
        assert!(err.to_string().contains("kayit"), "{err}");

        assert!(rec.stop());
        drop(rec);
        // Kilit bırakılınca geçmeli.
        assert_eq!(import(&fx.opts()).unwrap().written(), 2);
    }

    #[test]
    fn an_empty_source_directory_is_an_error_not_a_silent_success() {
        let fx = Fixture::new("empty");
        let err = import(&fx.opts()).expect_err("bos ice aktarma basarili sayilmamali");
        assert!(matches!(err, ImportError::NoFiles(_)), "{err:?}");

        let missing = ImportOpts { src_dir: fx.tmp().join("yok"), ..fx.opts() };
        assert!(matches!(import(&missing), Err(ImportError::NoSrcDir(_))));

        let bad = ImportOpts { instance: "../kacis".into(), ..fx.opts() };
        assert!(matches!(import(&bad), Err(ImportError::BadInstance(_))));
    }

    #[test]
    fn the_summary_always_states_that_recv_ms_is_broker_time() {
        // Bu not KAYBOLMAMALI: replay temposu `recv_ms` farkindan uretiliyor
        // ve geri-doldurulan gunlerde o alan broker saatidir.
        let fx = Fixture::new("note");
        fx.write_table(2026_08_11, &[(3, "GOLD")]);
        fx.write_src("GOLD", DAY, &(0..2).map(back).collect::<Vec<_>>());
        let text = format!("{}", import(&fx.opts()).unwrap());
        assert!(text.contains("ZAMAN ALANI"), "{text}");
        assert!(text.contains("BROKER"), "{text}");
        assert!(text.contains("recv_ms"), "{text}");
    }
}
