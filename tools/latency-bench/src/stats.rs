//! Gecikme örneklerinden yüzdelik dağılım.
//!
//! Ortalama bilinçli olarak ikincil planda: gecikme dağılımları ağır kuyrukludur
//! ve ortalama, bizi asıl ilgilendiren en kötü durumu gizler. Faz 1 kapısı
//! **p99** üzerinden tanımlıdır.

/// Sıralanmış örneklerden yüzdelik dağılım.
#[derive(Debug, Clone)]
pub struct Percentiles {
    pub count: usize,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub mean_ns: u64,
}

impl Percentiles {
    /// `samples` yerinde sıralanır.
    ///
    /// Boş girdide `None` döner — 0 uydurmak, "ölçüm yapılmadı" ile "gecikme
    /// sıfır" durumlarını karıştırırdı.
    pub fn from_samples(samples: &mut [u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let n = samples.len();
        // Toplam u64'ü aşabilir (çok sayıda örnek × büyük gecikme) → u128.
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        Some(Self {
            count: n,
            min_ns: samples[0],
            p50_ns: nearest_rank(samples, 50.0),
            p90_ns: nearest_rank(samples, 90.0),
            p99_ns: nearest_rank(samples, 99.0),
            p999_ns: nearest_rank(samples, 99.9),
            max_ns: samples[n - 1],
            mean_ns: (sum / n as u128) as u64,
        })
    }
}

/// "Nearest rank" yöntemi: p'inci yüzdelik = ceil(p/100 × N)'inci eleman.
///
/// İnterpolasyon YAPMIYORUZ — gecikme ölçümünde gerçekten gözlenmiş bir değeri
/// raporlamak, iki gözlem arasında var olmayan bir sayı üretmekten dürüsttür.
fn nearest_rank(sorted: &[u64], p: f64) -> u64 {
    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(n - 1)]
}

/// Nanosaniyeyi okunabilir biçime çevir.
pub fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_none_not_zero() {
        // "Ölçüm yok" ile "gecikme sıfır" karıştırılmamalı.
        assert!(Percentiles::from_samples(&mut []).is_none());
    }

    #[test]
    fn single_sample_is_every_percentile() {
        let p = Percentiles::from_samples(&mut [42]).unwrap();
        assert_eq!(p.count, 1);
        assert_eq!(p.min_ns, 42);
        assert_eq!(p.p50_ns, 42);
        assert_eq!(p.p99_ns, 42);
        assert_eq!(p.max_ns, 42);
        assert_eq!(p.mean_ns, 42);
    }

    #[test]
    fn percentiles_on_uniform_1_to_100() {
        let mut s: Vec<u64> = (1..=100).collect();
        let p = Percentiles::from_samples(&mut s).unwrap();
        assert_eq!(p.count, 100);
        assert_eq!(p.min_ns, 1);
        assert_eq!(p.max_ns, 100);
        assert_eq!(p.p50_ns, 50);
        assert_eq!(p.p90_ns, 90);
        assert_eq!(p.p99_ns, 99);
        assert_eq!(p.mean_ns, 50); // (1+..+100)/100 = 50.5 → tamsayı bölme 50
    }

    #[test]
    fn sorts_unsorted_input() {
        let mut s = vec![100, 1, 50, 3, 2];
        let p = Percentiles::from_samples(&mut s).unwrap();
        assert_eq!(p.min_ns, 1);
        assert_eq!(p.max_ns, 100);
    }

    #[test]
    fn tail_outlier_moves_p999_not_p50() {
        // Gecikme dağılımlarının ağır kuyruğu: ortalamaya bakmanın neden
        // yanıltıcı olduğunu gösteren durum.
        let mut s: Vec<u64> = std::iter::repeat_n(10u64, 999).chain([10_000_000]).collect();
        let p = Percentiles::from_samples(&mut s).unwrap();
        assert_eq!(p.p50_ns, 10, "medyan kuyruktan etkilenmemeli");
        assert_eq!(p.p99_ns, 10);
        assert_eq!(p.max_ns, 10_000_000, "en kötü durum görünür olmalı");
        assert!(p.mean_ns > 10, "ortalama kuyruk tarafından çekilir");
    }

    #[test]
    fn mean_does_not_overflow_on_many_large_samples() {
        // u64 toplamı taşabilirdi; u128 ara değer kullanıyoruz.
        let mut s = vec![u64::MAX / 2; 1000];
        let p = Percentiles::from_samples(&mut s).unwrap();
        assert_eq!(p.mean_ns, u64::MAX / 2);
    }

    #[test]
    fn formats_scale_appropriately() {
        assert_eq!(fmt_ns(999), "999 ns");
        assert_eq!(fmt_ns(1_500), "1.50 µs");
        assert_eq!(fmt_ns(2_500_000), "2.50 ms");
    }
}
