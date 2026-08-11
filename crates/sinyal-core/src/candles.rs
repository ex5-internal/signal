//! Mum (OHLC) deposu: **iki ayrı kaynak**, karıştırılmadan.
//!
//! # Neden iki kaynak
//!
//! - **tick**: akan tick'lerden burada üretilen mum. Fiyat olarak **mid**
//!   `(bid+ask)/2` kullanılır. Geriye dönük geçmiş YOKTUR; depo daemon
//!   başladığı andan itibaren dolar.
//! - **mt5**: broker'ın kendi bar serisi, MQL5 Service'in `CopyRates`'inden.
//!   Geriye dönük geçmiş vardır.
//!
//! # KRİTİK: bunlar aynı seri DEĞİLDİR
//!
//! Forex/CFD'de MT5'in bar serisi neredeyse her zaman **BID** üzerinden
//! kuruludur; bizim ürettiğimiz bar ise **MID**. Aradaki fark spread'in
//! yarısı kadardır ve spread sabit değildir — seans açılışında, haber anında
//! ve hafta sonu boşluğunda büyür. İki seriyi tek dizide birleştirmek şu
//! yalanları üretirdi:
//!
//! - Birleşim noktasında **sahte bir boşluk (gap)**: MID barından BID barına
//!   geçerken fiyat spread'in yarısı kadar "sıçrar". Grafiği okuyan da,
//!   üzerinde gösterge hesaplayan da bunu gerçek bir hareket sanar.
//! - Aynı dakikanın barı iki kaynakta iki farklı OHLC verir; hangisinin
//!   doğru olduğu **kayıtta hiçbir yerde yazmaz**.
//!
//! ## Verilen karar
//!
//! 1. İki seri **ayrı** saklanır; birleştirme, ekleme, uç uca dikme YOK.
//! 2. Bir cevap **tek kaynaktan** gelir. Aynı (sembol, dilim) için MT5 barı
//!    varsa **o** döner; yoksa tick'ten üretilene düşülür.
//! 3. Hangi kaynağın döndüğü istemciye **söylenir** ([`BarSource`], tel
//!    protokolünde `src_kind`). Söylemeden karar veremez.
//! 4. Canlı kapanan bar olayı (`candle`) daima **tick** kaynaklıdır ve öyle
//!    işaretlenir — istemci onu bir MT5 dizisinin sonuna eklememelidir.
//!
//! Alternatif olarak "MT5 geçmişi + canlı tick kuyruğu" dikilebilirdi ve daha
//! zengin görünürdü; tam da yukarıdaki sahte gap'i üretirdiği için
//! reddedildi. Eksik veri, yanlış veriden iyidir.
//!
//! # Zaman ekseni
//!
//! Her iki kaynakta da bar sınırları **broker sunucu saatindendir** ve `t`
//! alanı epoch **milisaniye**dir (`MqlRates.time` saniyedir; 1000 ile çarpma
//! service tarafında yapılır).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use serde::Serialize;

/// Tick'ten **üretilen** zaman dilimleri.
///
/// D1 bilinçli olarak YOK: MT5'in günlük barı broker sunucusunun gün
/// başlangıcında açılır (tipik olarak UTC+2/+3), bizim `rem_euclid` ile
/// bulduğumuz sınır ise UTC gece yarısıdır. İkisi tutmaz ve tick'ten
/// üretilmiş bir D1 barı MT5'in D1'iyle **hiçbir zaman** eşleşmezdi. D1
/// yalnızca MT5 kaynağından servis edilir.
pub const TIMEFRAMES: [(&str, i64); 6] = [
    ("M1", 60_000),
    ("M5", 300_000),
    ("M15", 900_000),
    ("M30", 1_800_000),
    ("H1", 3_600_000),
    ("H4", 14_400_000),
];

/// Zaman dilimi adını milisaniyeye çevir. **Tick'ten üretilebilen** dilimler.
pub fn tf_millis(tf: &str) -> Option<i64> {
    let up = tf.to_ascii_uppercase();
    TIMEFRAMES.iter().find(|(n, _)| *n == up).map(|(_, ms)| *ms)
}

/// İstemcinin verdiği dilim adını kanonik biçime çevir (`m5` → `M5`).
///
/// MT5 enum'unu tek doğruluk kaynağı alıyoruz; böylece tick tarafında
/// olmayan ama MT5'in verebildiği dilimler (D1) de tanınır.
pub fn canon_tf(tf: &str) -> Option<&'static str> {
    let up = tf.to_ascii_uppercase();
    sinyal_proto::timeframe::from_name(&up).and_then(sinyal_proto::timeframe::to_name)
}

/// Sembol başına saklanan azami bar sayısı (kaynak ve dilim başına).
const MAX_BARS: usize = 5_000;

/// Tek istekte dönebilecek azami bar sayısı.
pub const MAX_REQUEST: usize = MAX_BARS;

/// Bir bar serisinin kaynağı.
///
/// İki kaynak **aynı fiyat serisi değildir** (bkz. modül belgesi); bu yüzden
/// her cevapta bildirilir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarSource {
    /// Broker'ın kendi serisi (forex/CFD'de genellikle BID).
    Mt5,
    /// Tick'ten burada üretildi (MID).
    Tick,
}

impl BarSource {
    pub fn name(self) -> &'static str {
        match self {
            Self::Mt5 => "mt5",
            Self::Tick => "tick",
        }
    }
}

/// Tek bir mum.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Bar {
    /// Barın AÇILIŞ zamanı, broker saati, epoch ms.
    pub t: i64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    /// Bu barı oluşturan tick sayısı. Gerçek hacim DEĞİL — forex'te broker
    /// çoğu zaman hacim vermez; bunu hacim sanmak yanıltıcı olurdu.
    pub ticks: u32,
    /// Bu bar tamamlanmamış.
    ///
    /// tick kaynağında: daemon barın ORTASINDA başladı, açılış gerçek açılış
    /// değil. MT5 kaynağında: bar **hâlâ oluşuyor** (`CopyRates`'in döndürdüğü
    /// son bar her zaman budur), `c` değişmeye devam eder.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
    /// Gerçek işlem hacmi. `None` = **broker yayınlamıyor**.
    ///
    /// 0 ile `None` ayrımı kasıtlı: çoğu forex broker'ı gerçek hacim vermez ve
    /// 0'ı "hacim sıfır" diye okumak yanlış olurdu.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub real_volume: Option<i64>,
    /// Barın spread'i (point). Yalnızca MT5 kaynaklı barlarda bilinir.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread: Option<i32>,
}

/// Bir sembol+dilim için tick'ten üretilen yuvarlanan pencere.
#[derive(Debug, Default)]
struct Series {
    bars: VecDeque<Bar>,
}

impl Series {
    /// Tick'i uygula; bar kapandıysa kapanan barı döndür.
    fn apply(&mut self, bucket: i64, price: f64, first_ever: bool) -> Option<Bar> {
        match self.bars.back_mut() {
            Some(b) if b.t == bucket => {
                if price > b.h {
                    b.h = price;
                }
                if price < b.l {
                    b.l = price;
                }
                b.c = price;
                b.ticks = b.ticks.saturating_add(1);
                None
            }
            _ => {
                let closed = self.bars.back().copied();
                self.bars.push_back(Bar {
                    t: bucket,
                    o: price,
                    h: price,
                    l: price,
                    c: price,
                    ticks: 1,
                    // Yalnızca depodaki İLK bar eksik olabilir; sonrakiler
                    // gerçek bar sınırında açılır.
                    partial: first_ever,
                    // Tick'ten üretilen barda ne gerçek hacim ne de spread
                    // bilinir; uydurmuyoruz.
                    real_volume: None,
                    spread: None,
                });
                while self.bars.len() > MAX_BARS {
                    self.bars.pop_front();
                }
                closed
            }
        }
    }

    fn last(&self, count: usize) -> Vec<Bar> {
        let n = count.min(self.bars.len());
        self.bars.iter().skip(self.bars.len() - n).copied().collect()
    }
}

/// MT5'ten çekilmiş bar penceresi.
///
/// `BTreeMap`: aynı seri birden çok kez çekilebilir (yenileme, farklı `count`)
/// ve **oluşmakta olan bar** ikinci çekimde tamamlanmış olarak gelir. Zamana
/// göre anahtarlamak, o barın üzerine yazılmasını ve sıranın kendiliğinden
/// korunmasını sağlıyor; listeye eklemek çift bar üretirdi.
#[derive(Debug)]
struct Mt5Series {
    bars: BTreeMap<i64, Bar>,
    fetched: Instant,
}

/// Bir cevabın barları + hangi kaynaktan geldiği.
#[derive(Debug, Clone)]
pub struct CandleView {
    pub bars: Vec<Bar>,
    pub src: BarSource,
    /// MT5 görüntüsünün yaşı (ms). Tick kaynağında `None` — o seri canlıdır.
    ///
    /// MT5 serisi bir **anlık görüntüdür**: çekildikten sonra kendiliğinden
    /// güncellenmez. Yaşı söylemezsek istemci bayat bir seriyi canlı sanardı.
    pub age_ms: Option<i64>,
}

impl CandleView {
    pub fn is_empty(&self) -> bool {
        self.bars.is_empty()
    }
}

/// Tüm sembollerin mum deposu.
#[derive(Debug, Default)]
pub struct CandleStore {
    /// (sembol, TIMEFRAMES indeksi) → tick'ten üretilen seri
    tick: HashMap<(String, usize), Series>,
    /// (sembol, kanonik dilim adı) → MT5 serisi
    mt5: HashMap<(String, &'static str), Mt5Series>,
}

impl CandleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bir tick'i tüm zaman dilimlerine uygula.
    ///
    /// Fiyat olarak **mid** (bid+ask)/2 kullanılır: bid tabanlı mum, spread
    /// değiştiğinde gerçek olmayan hareket gösterir; mid daha temsili. (Bu
    /// seçim, MT5'in BID tabanlı serisiyle neden birleştirilemeyeceğinin de
    /// sebebi — bkz. modül belgesi.)
    ///
    /// Kapanan barları döndürür (canlı yayın için).
    pub fn on_tick(&mut self, symbol: &str, bid: f64, ask: f64, time_msc: i64) -> Vec<ClosedBar> {
        // Geçersiz fiyat veya zaman gelirse mum bozulmasın.
        if time_msc <= 0 || !bid.is_finite() || !ask.is_finite() {
            return Vec::new();
        }
        let price = if ask > 0.0 && bid > 0.0 {
            (bid + ask) / 2.0
        } else if bid > 0.0 {
            bid
        } else if ask > 0.0 {
            ask
        } else {
            return Vec::new();
        };

        let mut closed = Vec::new();
        for (idx, (name, ms)) in TIMEFRAMES.iter().enumerate() {
            let bucket = time_msc - time_msc.rem_euclid(*ms);
            let key = (symbol.to_owned(), idx);
            let entry = self.tick.entry(key).or_default();
            let first_ever = entry.bars.is_empty();
            if let Some(b) = entry.apply(bucket, price, first_ever) {
                closed.push(ClosedBar { symbol: symbol.to_owned(), tf: name, bar: b });
            }
        }
        closed
    }

    /// MT5 kaynaklı barları depoya al.
    ///
    /// `tf` kanonik ad olmalıdır ([`canon_tf`]). Aynı zamana denk gelen eski
    /// kayıt **üzerine yazılır**: bir önceki çekimde "oluşmakta" olarak gelen
    /// bar, ikinci çekimde tamamlanmış hâliyle gelir ve eskisinin kalması
    /// kapanış fiyatını kalıcı sanmamıza yol açardı.
    ///
    /// Boş liste de kaydedilir: "bu seri çekildi, broker bar vermedi" ile
    /// "hiç çekilmedi" farklı şeyler.
    pub fn ingest_mt5(&mut self, symbol: &str, tf: &'static str, bars: &[Bar]) {
        let entry = self
            .mt5
            .entry((symbol.to_owned(), tf))
            .or_insert_with(|| Mt5Series { bars: BTreeMap::new(), fetched: Instant::now() });
        for b in bars {
            if b.t <= 0 {
                continue;
            }
            entry.bars.insert(b.t, *b);
        }
        // Pencereyi sınırla — en eskiden at.
        while entry.bars.len() > MAX_BARS {
            let Some(oldest) = entry.bars.keys().next().copied() else { break };
            entry.bars.remove(&oldest);
        }
        entry.fetched = Instant::now();
    }

    /// Son `count` barı, **tek kaynaktan** ver.
    ///
    /// MT5 barı varsa o döner (broker'ın kendi serisi, geriye dönük geçmişi
    /// olan tek kaynak); yoksa tick'ten üretilene düşülür. İki kaynak ASLA
    /// aynı cevapta karışmaz.
    pub fn get(&self, symbol: &str, tf: &str, count: usize) -> CandleView {
        if let Some(canon) = canon_tf(tf) {
            if let Some(s) = self.mt5.get(&(symbol.to_owned(), canon)) {
                if !s.bars.is_empty() {
                    let n = count.min(s.bars.len());
                    let bars: Vec<Bar> =
                        s.bars.values().skip(s.bars.len() - n).copied().collect();
                    return CandleView {
                        bars,
                        src: BarSource::Mt5,
                        age_ms: Some(s.fetched.elapsed().as_millis() as i64),
                    };
                }
            }
        }
        CandleView { bars: self.tick_bars(symbol, tf, count), src: BarSource::Tick, age_ms: None }
    }

    /// Yalnızca tick'ten üretilen seriyi ver (sadakat karşılaştırması için).
    pub fn tick_bars(&self, symbol: &str, tf: &str, count: usize) -> Vec<Bar> {
        let up = tf.to_ascii_uppercase();
        let Some(idx) = TIMEFRAMES.iter().position(|(n, _)| *n == up) else {
            return Vec::new();
        };
        self.tick
            .get(&(symbol.to_owned(), idx))
            .map(|s| s.last(count))
            .unwrap_or_default()
    }

    /// MT5 görüntüsünün durumu: `(bar sayısı, yaş ms)`.
    ///
    /// `None` = bu seri hiç çekilmedi. Yenileme kararı buna bakar; her istekte
    /// yeniden çekmek `CopyRates`'i gereksiz yere sürekli meşgul ederdi.
    pub fn mt5_status(&self, symbol: &str, tf: &str) -> Option<(usize, i64)> {
        let canon = canon_tf(tf)?;
        self.mt5
            .get(&(symbol.to_owned(), canon))
            .map(|s| (s.bars.len(), s.fetched.elapsed().as_millis() as i64))
    }

    /// Depodaki bar sayısı (teşhis): `(tick, mt5)`.
    ///
    /// Ayrı sayılır — toplamak, iki kaynağın tek bir seri olduğu izlenimi
    /// verirdi.
    pub fn bar_counts(&self) -> (usize, usize) {
        (
            self.tick.values().map(|s| s.bars.len()).sum(),
            self.mt5.values().map(|s| s.bars.len()).sum(),
        )
    }
}

/// Kapanan bar bildirimi. **Daima tick kaynaklıdır.**
#[derive(Debug, Clone)]
pub struct ClosedBar {
    pub symbol: String,
    pub tf: &'static str,
    pub bar: Bar,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mt5_bar(t: i64, c: f64) -> Bar {
        Bar {
            t,
            o: c,
            h: c + 0.5,
            l: c - 0.5,
            c,
            ticks: 100,
            partial: false,
            real_volume: None,
            spread: Some(12),
        }
    }

    #[test]
    fn tf_names_resolve_case_insensitively() {
        assert_eq!(tf_millis("M1"), Some(60_000));
        assert_eq!(tf_millis("m5"), Some(300_000));
        assert_eq!(tf_millis("H1"), Some(3_600_000));
        assert_eq!(tf_millis("saçma"), None);
    }

    #[test]
    fn d1_is_mt5_only_because_our_day_boundary_would_not_match() {
        // MT5'in günlük barı broker sunucusunun gün başlangıcında açılır;
        // bizim UTC gece yarısı sınırımız onunla tutmaz. Tick'ten D1 üretmek
        // MT5'in grafiğiyle asla eşleşmeyen bir seri demekti.
        assert_eq!(tf_millis("D1"), None, "tick'ten D1 URETILMEMELI");
        assert_eq!(canon_tf("d1"), Some("D1"), "MT5 tarafinda D1 gecerli");

        let mut s = CandleStore::new();
        s.on_tick("EURUSD", 1.1, 1.1, 1_700_000_000_000);
        assert!(s.get("EURUSD", "D1", 10).is_empty(), "tick'ten D1 gelmemeli");

        s.ingest_mt5("EURUSD", "D1", &[mt5_bar(1_700_000_000_000, 1.1)]);
        let v = s.get("EURUSD", "D1", 10);
        assert_eq!(v.src, BarSource::Mt5);
        assert_eq!(v.bars.len(), 1);
    }

    #[test]
    fn builds_ohlc_from_ticks_within_one_bar() {
        let mut s = CandleStore::new();
        let t0: i64 = 1_700_000_000_000; // M1 sınırına oturtulmuş
        let base = t0 - t0.rem_euclid(60_000);

        // Aynı dakika içinde dört tick: 1.10 → 1.12 → 1.08 → 1.11
        for (off, bid) in [(0i64, 1.10), (1_000, 1.12), (2_000, 1.08), (3_000, 1.11)] {
            s.on_tick("EURUSD", bid, bid, base + off);
        }
        let bars = s.get("EURUSD", "M1", 10).bars;
        assert_eq!(bars.len(), 1, "hepsi tek bara girmeli");
        let b = bars[0];
        assert_eq!(b.t, base);
        assert!((b.o - 1.10).abs() < 1e-12, "açılış ilk tick");
        assert!((b.h - 1.12).abs() < 1e-12, "en yüksek");
        assert!((b.l - 1.08).abs() < 1e-12, "en düşük");
        assert!((b.c - 1.11).abs() < 1e-12, "kapanış son tick");
        assert_eq!(b.ticks, 4);
        assert_eq!(b.real_volume, None, "tick'ten gercek hacim uydurulmaz");
        assert_eq!(b.spread, None);
    }

    #[test]
    fn opens_new_bar_at_timeframe_boundary_and_reports_closed() {
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_040_000 - 1_700_000_040_000i64.rem_euclid(60_000);

        s.on_tick("EURUSD", 1.10, 1.10, base + 10_000);
        // Bir sonraki dakikaya geç.
        let closed = s.on_tick("EURUSD", 1.20, 1.20, base + 60_000 + 1_000);

        // M1 barı kapandı; daha büyük dilimler hâlâ açık.
        let m1: Vec<_> = closed.iter().filter(|c| c.tf == "M1").collect();
        assert_eq!(m1.len(), 1, "M1 barı kapanmalı");
        assert!((m1[0].bar.c - 1.10).abs() < 1e-12);

        let bars = s.get("EURUSD", "M1", 10).bars;
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[1].t, base + 60_000);
    }

    #[test]
    fn bar_boundaries_come_from_broker_time_not_local() {
        // Bar açılış zamanı daima dilime tam bölünebilir olmalı; yerel saat
        // kullansaydık sunucu saatiyle arasındaki fark kadar kayardı ve
        // MT5'in kendi grafiğiyle uyuşmazdı.
        let mut s = CandleStore::new();
        let odd: i64 = 1_700_000_037_123; // dilime oturmayan bir an
        s.on_tick("EURUSD", 1.1, 1.1, odd);
        for (tf, ms) in TIMEFRAMES {
            let bars = s.get("EURUSD", tf, 1).bars;
            assert_eq!(bars.len(), 1);
            assert_eq!(bars[0].t % ms, 0, "{tf} barı dilime oturmalı");
            assert!(bars[0].t <= odd && odd - bars[0].t < ms);
        }
    }

    #[test]
    fn first_bar_is_marked_partial_but_later_ones_are_not() {
        // Daemon barın ortasında başlarsa ilk barın açılışı gerçek açılış
        // DEĞİLDİR. Bunu gizlemek, istemcinin eksik barı tam sanması demek.
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);

        s.on_tick("EURUSD", 1.10, 1.10, base + 45_000); // barın ortası
        s.on_tick("EURUSD", 1.11, 1.11, base + 60_000 + 5_000);

        let bars = s.get("EURUSD", "M1", 10).bars;
        assert_eq!(bars.len(), 2);
        assert!(bars[0].partial, "ilk bar eksik işaretlenmeli");
        assert!(!bars[1].partial, "sonraki barlar tam");
    }

    #[test]
    fn uses_mid_price_not_bid() {
        // Bid tabanlı mum, spread değiştiğinde gerçek olmayan hareket gösterir.
        let mut s = CandleStore::new();
        s.on_tick("EURUSD", 1.1000, 1.1002, 1_700_000_000_000);
        let b = s.get("EURUSD", "M1", 1).bars[0];
        assert!((b.c - 1.1001).abs() < 1e-12, "mid kullanılmalı, bulunan {}", b.c);
    }

    #[test]
    fn falls_back_when_one_side_missing() {
        // CFD/borsa sembollerinde tek taraf 0 gelebilir.
        let mut s = CandleStore::new();
        s.on_tick("X", 1.5, 0.0, 1_700_000_000_000);
        assert!((s.get("X", "M1", 1).bars[0].c - 1.5).abs() < 1e-12);

        let mut s2 = CandleStore::new();
        s2.on_tick("Y", 0.0, 2.5, 1_700_000_000_000);
        assert!((s2.get("Y", "M1", 1).bars[0].c - 2.5).abs() < 1e-12);
    }

    #[test]
    fn ignores_garbage_input_instead_of_corrupting_bars() {
        let mut s = CandleStore::new();
        assert!(s.on_tick("X", f64::NAN, 1.0, 1_700_000_000_000).is_empty());
        assert!(s.on_tick("X", 1.0, 1.0, 0).is_empty(), "zaman 0 kabul edilmemeli");
        assert!(s.on_tick("X", 1.0, 1.0, -5).is_empty());
        assert!(s.on_tick("X", 0.0, 0.0, 1_700_000_000_000).is_empty());
        assert_eq!(s.bar_counts(), (0, 0), "hiçbiri bar üretmemeli");
    }

    #[test]
    fn window_is_bounded() {
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);
        // MAX_BARS'tan fazla dakika üret.
        for i in 0..(MAX_BARS as i64 + 500) {
            s.on_tick("EURUSD", 1.0 + i as f64 * 1e-6, 1.0 + i as f64 * 1e-6, base + i * 60_000);
        }
        let bars = s.get("EURUSD", "M1", 999_999).bars;
        assert_eq!(bars.len(), MAX_BARS, "pencere sınırlı kalmalı");
        // En yeni bar korunmalı, en eski atılmalı.
        assert_eq!(bars[bars.len() - 1].t, base + (MAX_BARS as i64 + 499) * 60_000);
    }

    #[test]
    fn unknown_symbol_or_timeframe_returns_empty_not_panic() {
        let s = CandleStore::new();
        assert!(s.get("YOK", "M1", 10).is_empty());
        assert!(s.get("EURUSD", "YOKTF", 10).is_empty());
        assert!(s.mt5_status("EURUSD", "YOKTF").is_none());
    }

    #[test]
    fn symbols_do_not_bleed_into_each_other() {
        let mut s = CandleStore::new();
        let t = 1_700_000_000_000;
        s.on_tick("EURUSD", 1.10, 1.10, t);
        s.on_tick("GOLD", 3300.0, 3300.0, t);
        assert!((s.get("EURUSD", "M1", 1).bars[0].c - 1.10).abs() < 1e-12);
        assert!((s.get("GOLD", "M1", 1).bars[0].c - 3300.0).abs() < 1e-9);
    }

    // --- iki kaynak ---

    #[test]
    fn mt5_bars_win_and_the_source_is_reported() {
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);
        s.on_tick("EURUSD", 1.1000, 1.1002, base + 1_000);

        let tick_view = s.get("EURUSD", "M1", 10);
        assert_eq!(tick_view.src, BarSource::Tick, "MT5 yokken tick'e dusulmeli");
        assert_eq!(tick_view.age_ms, None, "tick serisi canli");

        s.ingest_mt5("EURUSD", "M1", &[mt5_bar(base, 1.0950)]);
        let v = s.get("EURUSD", "M1", 10);
        assert_eq!(v.src, BarSource::Mt5);
        assert!(v.age_ms.is_some(), "MT5 goruntusunun yasi bildirilmeli");
        assert_eq!(v.bars.len(), 1);
        assert!((v.bars[0].c - 1.0950).abs() < 1e-12);
    }

    #[test]
    fn sources_are_never_mixed_in_one_answer() {
        // ASIL NOKTA: MT5 serisi BID, bizimki MID. Uc uca dikmek birlesim
        // noktasinda SAHTE BIR GAP uretirdi.
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);

        // Tick tarafında 3 dakika üret (mid ~1.1001).
        for i in 0..3 {
            s.on_tick("EURUSD", 1.1000, 1.1002, base + i * 60_000 + 1_000);
        }
        // MT5 tarafında bambaşka bir dakika (daha eski) ve farklı fiyat.
        s.ingest_mt5("EURUSD", "M1", &[mt5_bar(base - 60_000, 1.0900)]);

        let v = s.get("EURUSD", "M1", 100);
        assert_eq!(v.src, BarSource::Mt5);
        assert_eq!(v.bars.len(), 1, "tick barlari MT5 cevabina KARISMAMALI");
        assert!((v.bars[0].c - 1.0900).abs() < 1e-12);

        // Tick serisi kaybolmadı; ayrı yüzeyden hâlâ okunabiliyor.
        assert_eq!(s.tick_bars("EURUSD", "M1", 100).len(), 3);
        // İki depo AYRI sayılır: tick tarafı tüm dilimlerdeki barların
        // toplamı, MT5 tarafı yalnız çekilen tek bar.
        let tick_total: usize =
            TIMEFRAMES.iter().map(|(tf, _)| s.tick_bars("EURUSD", tf, 999_999).len()).sum();
        assert!(tick_total >= 3);
        assert_eq!(s.bar_counts(), (tick_total, 1), "iki depo ayri sayilmali");
    }

    #[test]
    fn refetching_overwrites_the_forming_bar_instead_of_duplicating_it() {
        // `CopyRates`'in son barı OLUŞMAKTA olandır; ikinci çekimde tamamlanmış
        // gelir. Listeye eklemek aynı dakikadan iki bar üretirdi.
        let mut s = CandleStore::new();
        let t = 1_700_000_000_000;

        let mut forming = mt5_bar(t, 1.1000);
        forming.partial = true;
        s.ingest_mt5("EURUSD", "M1", &[forming]);

        let done = mt5_bar(t, 1.1050);
        s.ingest_mt5("EURUSD", "M1", &[done]);

        let v = s.get("EURUSD", "M1", 10);
        assert_eq!(v.bars.len(), 1, "ayni zamandan iki bar olmamali");
        assert!(!v.bars[0].partial, "tamamlanmis hali gecerli olmali");
        assert!((v.bars[0].c - 1.1050).abs() < 1e-12);
    }

    #[test]
    fn mt5_bars_stay_sorted_and_bounded_across_fetches() {
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000;
        // Karışık sırada iki çekim.
        let newer: Vec<Bar> = (500..600).map(|i| mt5_bar(base + i * 60_000, 1.0)).collect();
        let older: Vec<Bar> = (0..500).map(|i| mt5_bar(base + i * 60_000, 1.0)).collect();
        s.ingest_mt5("EURUSD", "M1", &newer);
        s.ingest_mt5("EURUSD", "M1", &older);

        let v = s.get("EURUSD", "M1", 999_999);
        assert_eq!(v.bars.len(), 600);
        assert!(v.bars.windows(2).all(|w| w[0].t < w[1].t), "zaman artan olmali");

        // Pencere sınırı.
        let flood: Vec<Bar> =
            (0..(MAX_BARS as i64 + 100)).map(|i| mt5_bar(base + 1_000_000 + i * 60_000, 1.0)).collect();
        s.ingest_mt5("EURUSD", "M1", &flood);
        let v = s.get("EURUSD", "M1", 999_999);
        assert_eq!(v.bars.len(), MAX_BARS);
        // En yeni korunmalı.
        assert_eq!(v.bars[v.bars.len() - 1].t, base + 1_000_000 + (MAX_BARS as i64 + 99) * 60_000);
    }

    #[test]
    fn empty_mt5_fetch_is_recorded_but_does_not_hide_tick_bars() {
        // "cekildi, broker bar vermedi" ile "hic cekilmedi" farkli seyler;
        // ama bos bir MT5 serisi yuzunden istemciyi bos cevapla gondermek
        // elimizdeki tick barlarini saklamak olurdu.
        let mut s = CandleStore::new();
        s.on_tick("EURUSD", 1.10, 1.10, 1_700_000_000_000);
        s.ingest_mt5("EURUSD", "M1", &[]);

        let (n, _age) = s.mt5_status("EURUSD", "M1").expect("cekim KAYITLI olmali");
        assert_eq!(n, 0);
        let v = s.get("EURUSD", "M1", 10);
        assert_eq!(v.src, BarSource::Tick, "bos MT5 serisi tick'i gizlememeli");
        assert_eq!(v.bars.len(), 1);
    }

    #[test]
    fn ingest_ignores_bars_with_impossible_timestamps() {
        let mut s = CandleStore::new();
        s.ingest_mt5("EURUSD", "M1", &[mt5_bar(0, 1.0), mt5_bar(-5, 1.0), mt5_bar(1_700_000_000_000, 1.0)]);
        assert_eq!(s.get("EURUSD", "M1", 10).bars.len(), 1);
    }
}
