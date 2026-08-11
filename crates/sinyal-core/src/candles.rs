//! Tick akışından mum (OHLC) üretimi.
//!
//! # Neden çekirdekte
//!
//! Grafik çizen bir istemcinin bara ihtiyacı var; MT5'ten geçmiş bar almak
//! EA'ya istek-yanıt yolu eklemeyi gerektirir (protokol değişikliği + sıcak
//! yolda `CopyRates` riski). Buradaki üretim EA'ya ve protokole **hiç
//! dokunmaz**: zaten akan tick'lerden mum kurulur.
//!
//! # Kabul edilen sınır
//!
//! Geriye dönük geçmiş **yoktur**. Depo, daemon başladığı andan itibaren
//! dolar. Bunu gizlemiyoruz: yanıtta `partial` bayrağı, ilk barın gerçekten
//! o barın başlangıcından mı yoksa daemon açıldığı andan mı toplandığını
//! söyler — aksi halde istemci eksik bir barı tam sanıp yanlış gösterirdi.
//!
//! # Zaman ekseni
//!
//! Bar sınırları **broker sunucu saatinden** (`time_msc`) türetilir, yerel
//! saatten değil. Yerel saat kullanmak, sunucu saatiyle arasındaki fark
//! kadar kaymış barlar üretirdi ve MT5'in kendi grafiğiyle uyuşmazdı.

use std::collections::HashMap;

use serde::Serialize;

/// Desteklenen zaman dilimleri.
///
/// MT5'in tümünü değil, grafik için pratikte gerekli olanları taşıyoruz;
/// her ek dilim bellek ve tick başına iş demek.
pub const TIMEFRAMES: [(&str, i64); 6] = [
    ("M1", 60_000),
    ("M5", 300_000),
    ("M15", 900_000),
    ("M30", 1_800_000),
    ("H1", 3_600_000),
    ("H4", 14_400_000),
];

/// Zaman dilimi adını milisaniyeye çevir.
pub fn tf_millis(tf: &str) -> Option<i64> {
    let up = tf.to_ascii_uppercase();
    TIMEFRAMES.iter().find(|(n, _)| *n == up).map(|(_, ms)| *ms)
}

/// Sembol başına saklanan azami bar sayısı (zaman dilimi başına).
const MAX_BARS: usize = 5_000;

/// Tek istekte dönebilecek azami bar sayısı.
///
/// İstemcinin milyonlarca bar isteyip belleği şişirmesini engeller; depo
/// zaten [`MAX_BARS`] ile sınırlı olduğu için pratikte kısıtlayıcı değil.
pub const MAX_REQUEST: usize = MAX_BARS;

/// Tek bir mum.
#[derive(Debug, Clone, Copy, Serialize)]
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
    /// Bu bar daemon açıldığı anda ORTASINDAN başladı mı.
    ///
    /// `true` ise açılış fiyatı barın gerçek açılışı değildir. Gizlemiyoruz:
    /// istemci ilk barı eksik olarak işaretleyebilsin.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

/// Bir sembol+dilim için yuvarlanan bar penceresi.
#[derive(Debug, Default)]
struct Series {
    bars: std::collections::VecDeque<Bar>,
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

/// Kapanan bar bildirimi.
#[derive(Debug, Clone)]
pub struct ClosedBar {
    pub symbol: String,
    pub tf: &'static str,
    pub bar: Bar,
}

/// Tüm sembollerin mum deposu.
#[derive(Debug, Default)]
pub struct CandleStore {
    /// (sembol, dilim indeksi) → seri
    series: HashMap<(String, usize), Series>,
}

impl CandleStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bir tick'i tüm zaman dilimlerine uygula.
    ///
    /// Fiyat olarak **mid** (bid+ask)/2 kullanılır: bid tabanlı mum, spread
    /// değiştiğinde gerçek olmayan hareket gösterir; mid daha temsili.
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
            let entry = self.series.entry(key).or_default();
            let first_ever = entry.bars.is_empty();
            if let Some(b) = entry.apply(bucket, price, first_ever) {
                closed.push(ClosedBar { symbol: symbol.to_owned(), tf: name, bar: b });
            }
        }
        closed
    }

    /// Son `count` barı ver. Sembol/dilim bilinmiyorsa boş liste.
    pub fn get(&self, symbol: &str, tf: &str, count: usize) -> Vec<Bar> {
        let up = tf.to_ascii_uppercase();
        let Some(idx) = TIMEFRAMES.iter().position(|(n, _)| *n == up) else {
            return Vec::new();
        };
        self.series
            .get(&(symbol.to_owned(), idx))
            .map(|s| s.last(count))
            .unwrap_or_default()
    }

    /// Depodaki toplam bar sayısı (teşhis).
    pub fn total_bars(&self) -> usize {
        self.series.values().map(|s| s.bars.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tf_names_resolve_case_insensitively() {
        assert_eq!(tf_millis("M1"), Some(60_000));
        assert_eq!(tf_millis("m5"), Some(300_000));
        assert_eq!(tf_millis("H1"), Some(3_600_000));
        assert_eq!(tf_millis("saçma"), None);
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
        let bars = s.get("EURUSD", "M1", 10);
        assert_eq!(bars.len(), 1, "hepsi tek bara girmeli");
        let b = bars[0];
        assert_eq!(b.t, base);
        assert!((b.o - 1.10).abs() < 1e-12, "açılış ilk tick");
        assert!((b.h - 1.12).abs() < 1e-12, "en yüksek");
        assert!((b.l - 1.08).abs() < 1e-12, "en düşük");
        assert!((b.c - 1.11).abs() < 1e-12, "kapanış son tick");
        assert_eq!(b.ticks, 4);
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

        let bars = s.get("EURUSD", "M1", 10);
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
            let bars = s.get("EURUSD", tf, 1);
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

        let bars = s.get("EURUSD", "M1", 10);
        assert_eq!(bars.len(), 2);
        assert!(bars[0].partial, "ilk bar eksik işaretlenmeli");
        assert!(!bars[1].partial, "sonraki barlar tam");
    }

    #[test]
    fn uses_mid_price_not_bid() {
        // Bid tabanlı mum, spread değiştiğinde gerçek olmayan hareket gösterir.
        let mut s = CandleStore::new();
        s.on_tick("EURUSD", 1.1000, 1.1002, 1_700_000_000_000);
        let b = s.get("EURUSD", "M1", 1)[0];
        assert!((b.c - 1.1001).abs() < 1e-12, "mid kullanılmalı, bulunan {}", b.c);
    }

    #[test]
    fn falls_back_when_one_side_missing() {
        // CFD/borsa sembollerinde tek taraf 0 gelebilir.
        let mut s = CandleStore::new();
        s.on_tick("X", 1.5, 0.0, 1_700_000_000_000);
        assert!((s.get("X", "M1", 1)[0].c - 1.5).abs() < 1e-12);

        let mut s2 = CandleStore::new();
        s2.on_tick("Y", 0.0, 2.5, 1_700_000_000_000);
        assert!((s2.get("Y", "M1", 1)[0].c - 2.5).abs() < 1e-12);
    }

    #[test]
    fn ignores_garbage_input_instead_of_corrupting_bars() {
        let mut s = CandleStore::new();
        assert!(s.on_tick("X", f64::NAN, 1.0, 1_700_000_000_000).is_empty());
        assert!(s.on_tick("X", 1.0, 1.0, 0).is_empty(), "zaman 0 kabul edilmemeli");
        assert!(s.on_tick("X", 1.0, 1.0, -5).is_empty());
        assert!(s.on_tick("X", 0.0, 0.0, 1_700_000_000_000).is_empty());
        assert_eq!(s.total_bars(), 0, "hiçbiri bar üretmemeli");
    }

    #[test]
    fn window_is_bounded() {
        let mut s = CandleStore::new();
        let base: i64 = 1_700_000_000_000 - 1_700_000_000_000i64.rem_euclid(60_000);
        // MAX_BARS'tan fazla dakika üret.
        for i in 0..(MAX_BARS as i64 + 500) {
            s.on_tick("EURUSD", 1.0 + i as f64 * 1e-6, 1.0 + i as f64 * 1e-6, base + i * 60_000);
        }
        let bars = s.get("EURUSD", "M1", 999_999);
        assert_eq!(bars.len(), MAX_BARS, "pencere sınırlı kalmalı");
        // En yeni bar korunmalı, en eski atılmalı.
        assert_eq!(bars[bars.len() - 1].t, base + (MAX_BARS as i64 + 499) * 60_000);
    }

    #[test]
    fn unknown_symbol_or_timeframe_returns_empty_not_panic() {
        let s = CandleStore::new();
        assert!(s.get("YOK", "M1", 10).is_empty());
        assert!(s.get("EURUSD", "YOKTF", 10).is_empty());
    }

    #[test]
    fn symbols_do_not_bleed_into_each_other() {
        let mut s = CandleStore::new();
        let t = 1_700_000_000_000;
        s.on_tick("EURUSD", 1.10, 1.10, t);
        s.on_tick("GOLD", 3300.0, 3300.0, t);
        assert!((s.get("EURUSD", "M1", 1)[0].c - 1.10).abs() < 1e-12);
        assert!((s.get("GOLD", "M1", 1)[0].c - 3300.0).abs() < 1e-9);
    }
}
