//! Emir doğrulama — saf fonksiyonlar.
//!
//! Bu modüldeki her şey yan etkisizdir ve yalnızca [`SymbolEntry`]'ye dayanır;
//! broker özellikleri **asla tahmin edilmez**. Eski adaptörün emirlerinin
//! reddedilmesinin iki sebebi buradaki iki fonksiyonun yokluğuydu: doldurma
//! modunu sabit kodlamak ve hacmi adıma oturtmamak.

use crate::msg::{action, exec_mode, filling, filling_mask, SymbolEntry};

/// Emir doğrulamasında oluşabilecek hatalar.
// `Eq` türetilemez: `BadVolumeStep` bir `f64` taşır ve NaN kendine eşit
// olmadığı için f64 `Eq` uygulamaz.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValidationError {
    /// Sembol için hiçbir doldurma modu geçerli değil.
    ///
    /// Tahmin etmek yerine reddediyoruz: yanlış mod 10030 (INVALID_FILL) ile
    /// broker tarafından reddedilir ve sebebi loglardan anlaşılmaz.
    NoValidFilling { exemode: u32, mask: u32 },
    /// Bilinmeyen eylem kodu.
    UnknownAction(u8),
    /// Hacim adımı sıfır veya negatif — sembol tablosu bozuk.
    ///
    /// `SymbolInfoDouble` başarısızlıkta sessizce 0 döndürdüğü için bu, EA'nın
    /// okunamayan bir sembolü tabloya yazdığı anlamına gelir.
    BadVolumeStep(f64),
}

impl core::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NoValidFilling { exemode, mask } => write!(
                f,
                "sembol için geçerli doldurma modu yok (exemode={exemode}, maske={mask:#b}) \
                 — tahmin etmek yerine emir reddedildi"
            ),
            Self::UnknownAction(a) => write!(f, "bilinmeyen eylem kodu: {a}"),
            Self::BadVolumeStep(s) => write!(
                f,
                "hacim adımı geçersiz ({s}) — sembol tablosu bozuk, \
                 SymbolInfoDouble başarısız olmuş olabilir"
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// `filling::AUTO` için doğru MT5 doldurma modunu çöz.
///
/// `requested` `AUTO` değilse olduğu gibi döner — çağıranın açık tercihi
/// geçersiz kılınmaz.
///
/// # Çözümleme kuralları
///
/// Bunlar keyfi değil, MT5'in emir kabul mantığından türer:
/// - **Bekleyen emir** → daima `RETURN`. Bekleyen emirlerde yürütme tipinden
///   bağımsız olarak yalnızca bu geçerlidir.
/// - **INSTANT / REQUEST** → `FOK`. Bu modlarda FOK maskeden **bağımsız**
///   izinlidir; maskeye bakmak yanlış sonuç verir.
/// - **MARKET** → maskeden `FOK`, yoksa `IOC`. `RETURN` bu modda **yasaktır**;
///   ikisi de yoksa tahmin etmek yerine reddedilir.
/// - **EXCHANGE** → maskeden `FOK` → `IOC`, hiçbiri yoksa `RETURN`.
///
/// `BOC` asla otomatik seçilmez; yalnızca çağıran açıkça isterse kullanılır.
pub fn resolve_filling(
    action_code: u8,
    requested: u8,
    entry: &SymbolEntry,
) -> Result<u8, ValidationError> {
    if requested != filling::AUTO {
        return Ok(requested);
    }
    let mask = entry.filling_mode;
    let has = |bit: u32| mask & bit != 0;

    match action_code {
        // Bekleyen emirler: yürütme tipine bakılmaz.
        action::PENDING | action::MODIFY | action::REMOVE => Ok(filling::RETURN),

        action::DEAL | action::CLOSE_POSITION | action::CLOSE_BY => match entry.trade_exemode {
            exec_mode::INSTANT | exec_mode::REQUEST => Ok(filling::FOK),
            exec_mode::MARKET => {
                if has(filling_mask::FOK) {
                    Ok(filling::FOK)
                } else if has(filling_mask::IOC) {
                    Ok(filling::IOC)
                } else {
                    // RETURN bu modda yasak; tahmin etmiyoruz.
                    Err(ValidationError::NoValidFilling { exemode: entry.trade_exemode, mask })
                }
            }
            exec_mode::EXCHANGE => {
                if has(filling_mask::FOK) {
                    Ok(filling::FOK)
                } else if has(filling_mask::IOC) {
                    Ok(filling::IOC)
                } else {
                    Ok(filling::RETURN)
                }
            }
            other => Err(ValidationError::NoValidFilling { exemode: other, mask }),
        },

        // SL/TP değişikliği emir doldurma modu taşımaz; MT5 alanı yok sayar.
        action::SLTP => Ok(filling::RETURN),

        other => Err(ValidationError::UnknownAction(other)),
    }
}

/// Hacmi brokerın adımına oturt ve sınırlara kıs.
///
/// # Kayan nokta tuzağı
///
/// `floor(v / step)` **kullanılamaz**: `0.07 / 0.01` çoğu makinede
/// `6.999999999999999` verir ve `floor` bunu 6'ya indirir — yani istenen
/// hacimden bir adım eksik emir gönderilir. Bu, gerçek parayla çalışan bir
/// sistemde sessiz ve sinsi bir hatadır. Bu yüzden yuvarlama `round` ile ve
/// küçük bir epsilon düzeltmesiyle yapılır.
///
/// Kıstıktan **sonra** tekrar adıma oturtulur: `volume_max` adımın katı
/// olmayabilir (aşağı yuvarlanır), `volume_min`'e çıkarken yukarı yuvarlanır.
pub fn normalize_volume(
    volume: f64,
    vmin: f64,
    vmax: f64,
    step: f64,
) -> Result<f64, ValidationError> {
    if !(step > 0.0) || !step.is_finite() {
        return Err(ValidationError::BadVolumeStep(step));
    }
    if !volume.is_finite() || volume <= 0.0 {
        // Geçersiz girdi asgari hacme çekilir; sıfır hacimli emir zaten
        // reddedilirdi.
        return Ok(snap_up(vmin.max(step), step));
    }

    let decimals = decimals_of(step);
    // Epsilon düzeltmesi: 0.07/0.01 = 6.999... → 7
    let k = (volume / step * (1.0 + f64::EPSILON)).round();
    let mut v = round_to(k * step, decimals);

    let lo = if vmin > 0.0 { vmin } else { step };
    let hi = if vmax > 0.0 { vmax } else { f64::INFINITY };

    if v < lo {
        v = snap_up(lo, step);
    }
    if v > hi {
        v = snap_down(hi, step);
    }
    // Aşağı yuvarlama asgarinin altına düşürdüyse emir gönderilemez; asgariye
    // çekiyoruz — çağıran zaten sınırları tabloda görebiliyor.
    if v < lo {
        v = snap_up(lo, step);
    }
    Ok(round_to(v, decimals))
}

/// Fiyatı brokerın tick boyutuna ve ondalık hassasiyetine oturt.
///
/// Yalnızca `digits`'e yuvarlamak **yetmez**: `tick_size > point` olan borsa ve
/// endeks sembollerinde (ör. tick_size = 0.25) `digits` doğru olsa bile fiyat
/// geçersiz olur ve emir reddedilir.
pub fn normalize_price(price: f64, tick_size: f64, digits: u32) -> f64 {
    if !price.is_finite() {
        return price;
    }
    let d = digits.min(15);
    if tick_size > 0.0 && tick_size.is_finite() {
        let k = (price / tick_size * (1.0 + f64::EPSILON)).round();
        round_to(k * tick_size, decimals_of(tick_size).max(d as usize))
    } else {
        round_to(price, d as usize)
    }
}

/// Bir SL/TP veya bekleyen emir fiyatının `stops_level` kısıtını sağlayıp
/// sağlamadığı.
///
/// `stops_level` 0 ise broker kısıt koymuyordur (dinamik olabilir; o durumda
/// gerçek doğrulama yalnızca sunucuda yapılır).
pub fn respects_stops_level(
    price: f64,
    reference: f64,
    point: f64,
    stops_level: u32,
) -> bool {
    if stops_level == 0 || !(point > 0.0) {
        return true;
    }
    let distance = (price - reference).abs() / point;
    // Tolerans ŞART: mesafe iki kayan nokta çıkarma/bölmesinden türüyor ve
    // tam sınırda ölçüm hatası taşıyor. Örnek: (1.10010-1.10000)/0.00001
    // matematiksel olarak 10 ama kayan noktada 9.999999999998899 — tam
    // eşitlik karşılaştırması geçerli bir emri reddederdi.
    const POINT_TOLERANCE: f64 = 1e-6;
    distance + POINT_TOLERANCE >= stops_level as f64
}

// --- yardımcılar -----------------------------------------------------------

/// Bir adım değerinin ondalık basamak sayısı (0.01 → 2, 0.5 → 1, 1.0 → 0).
fn decimals_of(step: f64) -> usize {
    let mut scaled = step;
    for d in 0..=8usize {
        // Adım bu basamakta tam sayıya oturuyorsa bulduk.
        if (scaled - scaled.round()).abs() < 1e-9 {
            return d;
        }
        scaled *= 10.0;
    }
    8
}

fn round_to(v: f64, decimals: usize) -> f64 {
    let f = 10f64.powi(decimals.min(15) as i32);
    (v * f).round() / f
}

fn snap_up(v: f64, step: f64) -> f64 {
    let d = decimals_of(step);
    round_to((v / step * (1.0 - f64::EPSILON)).ceil() * step, d)
}

fn snap_down(v: f64, step: f64) -> f64 {
    let d = decimals_of(step);
    round_to((v / step * (1.0 + f64::EPSILON)).floor() * step, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::order_type;

    fn sym(exemode: u32, mask: u32) -> SymbolEntry {
        SymbolEntry { trade_exemode: exemode, filling_mode: mask, ..Default::default() }
    }

    // --- doldurma modu -----------------------------------------------------

    #[test]
    fn explicit_filling_is_never_overridden() {
        let s = sym(exec_mode::MARKET, filling_mask::IOC);
        assert_eq!(resolve_filling(action::DEAL, filling::BOC, &s).unwrap(), filling::BOC);
        assert_eq!(resolve_filling(action::DEAL, filling::FOK, &s).unwrap(), filling::FOK);
    }

    #[test]
    fn pending_orders_always_use_return() {
        // Yürütme tipi ne olursa olsun.
        for exemode in [exec_mode::REQUEST, exec_mode::INSTANT, exec_mode::MARKET, exec_mode::EXCHANGE] {
            let s = sym(exemode, 0);
            assert_eq!(
                resolve_filling(action::PENDING, filling::AUTO, &s).unwrap(),
                filling::RETURN,
                "exemode={exemode}"
            );
        }
    }

    #[test]
    fn instant_and_request_pick_fok_regardless_of_mask() {
        // ASIL NOKTA: bu modlarda FOK maskeden BAĞIMSIZ izinli. Maskeye bakan
        // bir uygulama burada yanlış moda düşerdi.
        for exemode in [exec_mode::INSTANT, exec_mode::REQUEST] {
            let s = sym(exemode, 0); // maske BOŞ
            assert_eq!(
                resolve_filling(action::DEAL, filling::AUTO, &s).unwrap(),
                filling::FOK,
                "exemode={exemode}"
            );
        }
    }

    #[test]
    fn market_mode_prefers_fok_then_ioc() {
        let s = sym(exec_mode::MARKET, filling_mask::FOK | filling_mask::IOC);
        assert_eq!(resolve_filling(action::DEAL, filling::AUTO, &s).unwrap(), filling::FOK);

        let s = sym(exec_mode::MARKET, filling_mask::IOC);
        assert_eq!(resolve_filling(action::DEAL, filling::AUTO, &s).unwrap(), filling::IOC);
    }

    #[test]
    fn market_mode_refuses_rather_than_guessing_when_nothing_fits() {
        // RETURN bu modda YASAK. Tahmin etmek 10030 ile reddedilirdi ve sebebi
        // loglardan anlaşılmazdı — açıkça reddetmek daha iyi.
        let s = sym(exec_mode::MARKET, filling_mask::BOC);
        assert!(matches!(
            resolve_filling(action::DEAL, filling::AUTO, &s),
            Err(ValidationError::NoValidFilling { .. })
        ));
    }

    #[test]
    fn boc_only_symbol_does_not_silently_become_fok() {
        // Maskede yalnızca BOC varsa, sadece maskeye bakan hatalı bir
        // uygulama FOK'a düşerdi. EXCHANGE modunda RETURN'e düşmeli.
        let s = sym(exec_mode::EXCHANGE, filling_mask::BOC);
        assert_eq!(resolve_filling(action::DEAL, filling::AUTO, &s).unwrap(), filling::RETURN);
    }

    #[test]
    fn exchange_mode_falls_back_to_return() {
        let s = sym(exec_mode::EXCHANGE, 0);
        assert_eq!(resolve_filling(action::DEAL, filling::AUTO, &s).unwrap(), filling::RETURN);
    }

    #[test]
    fn auto_never_selects_boc() {
        // BOC yalnızca açık istekle kullanılmalı — otomatik seçilirse
        // doldurulmayan emirler sessizce iptal olur.
        for exemode in [exec_mode::REQUEST, exec_mode::INSTANT, exec_mode::MARKET, exec_mode::EXCHANGE] {
            let s = sym(exemode, filling_mask::FOK | filling_mask::IOC | filling_mask::BOC);
            let got = resolve_filling(action::DEAL, filling::AUTO, &s);
            assert_ne!(got.unwrap_or(filling::AUTO), filling::BOC, "exemode={exemode}");
        }
    }

    #[test]
    fn unknown_action_is_rejected() {
        let s = sym(exec_mode::MARKET, filling_mask::FOK);
        assert!(matches!(
            resolve_filling(200, filling::AUTO, &s),
            Err(ValidationError::UnknownAction(200))
        ));
    }

    // --- hacim -------------------------------------------------------------

    #[test]
    fn exact_step_multiples_survive_float_division() {
        // ASIL TUZAK: v/step bazı değerlerde tam sayının hemen ALTINA düşer
        // (ör. 0.29/0.01 = 28.999999999999996). floor kullanan bir uygulama
        // bir adım eksik hacim gönderirdi — gerçek parayla sessiz bir hata.
        //
        // Hangi değerlerde olduğu platforma/optimizasyona bağlı olduğu için
        // tek bir değeri test etmek kırılgan olurdu; adımın TÜM katlarını
        // tarayıp hiçbirinin kaymadığını doğruluyoruz.
        let step = 0.01;
        let mut trap_seen = false;
        for k in 1..=1000u32 {
            let want = round_to(k as f64 * step, 2);
            if (want / step).floor() < k as f64 {
                trap_seen = true; // floor kullanılsaydı burada hata olurdu
            }
            let got = normalize_volume(want, 0.01, 100.0, step).unwrap();
            assert!(
                (got - want).abs() < 1e-12,
                "k={k}: beklenen {want}, bulunan {got}"
            );
        }
        // Tuzağın en az bir yerde gerçekten oluştuğunu göster — yoksa test
        // hiçbir şey kanıtlamıyor demektir.
        assert!(trap_seen, "float tuzağı hiç gözlenmedi; test anlamsız kalıyor");
    }

    #[test]
    fn volume_snaps_to_step_grid() {
        assert!((normalize_volume(0.13, 0.01, 100.0, 0.01).unwrap() - 0.13).abs() < 1e-12);
        assert!((normalize_volume(0.3, 0.1, 100.0, 0.1).unwrap() - 0.3).abs() < 1e-12);
        // Adım arası değer en yakına yuvarlanır.
        assert!((normalize_volume(0.117, 0.01, 100.0, 0.01).unwrap() - 0.12).abs() < 1e-12);
        assert!((normalize_volume(0.114, 0.01, 100.0, 0.01).unwrap() - 0.11).abs() < 1e-12);
    }

    #[test]
    fn volume_clamps_to_min_and_max() {
        assert!((normalize_volume(0.001, 0.01, 100.0, 0.01).unwrap() - 0.01).abs() < 1e-12);
        assert!((normalize_volume(999.0, 0.01, 5.0, 0.01).unwrap() - 5.0).abs() < 1e-12);
    }

    #[test]
    fn volume_max_that_is_not_a_step_multiple_rounds_down() {
        // vmax adımın katı değilse yukarı yuvarlamak sınırı aşardı.
        let v = normalize_volume(999.0, 0.1, 5.55, 0.1).unwrap();
        assert!(v <= 5.55 + 1e-12, "sınır aşılmamalı: {v}");
        assert!((v - 5.5).abs() < 1e-12, "beklenen 5.5, bulunan {v}");
    }

    #[test]
    fn volume_with_whole_number_step() {
        assert!((normalize_volume(3.7, 1.0, 100.0, 1.0).unwrap() - 4.0).abs() < 1e-12);
        assert!((normalize_volume(3.2, 1.0, 100.0, 1.0).unwrap() - 3.0).abs() < 1e-12);
    }

    #[test]
    fn volume_rejects_broken_symbol_table() {
        // SymbolInfoDouble başarısızlıkta sessizce 0 döner; bu tabloya
        // sızarsa her emir reddedilirdi. Açıkça yakalıyoruz.
        assert!(matches!(
            normalize_volume(1.0, 0.01, 100.0, 0.0),
            Err(ValidationError::BadVolumeStep(_))
        ));
        assert!(normalize_volume(1.0, 0.01, 100.0, -0.01).is_err());
        assert!(normalize_volume(1.0, 0.01, 100.0, f64::NAN).is_err());
    }

    #[test]
    fn volume_handles_nonsense_input_without_panicking() {
        assert!(normalize_volume(f64::NAN, 0.01, 100.0, 0.01).is_ok());
        assert!(normalize_volume(-5.0, 0.01, 100.0, 0.01).unwrap() >= 0.01);
        assert!(normalize_volume(0.0, 0.01, 100.0, 0.01).unwrap() >= 0.01);
    }

    #[test]
    fn volume_result_is_always_on_the_step_grid() {
        // Özellik testi: hangi girdi verilirse verilsin çıktı adıma oturmalı.
        let step = 0.01;
        for i in 1..2000 {
            let v = normalize_volume(i as f64 * 0.0037, 0.01, 50.0, step).unwrap();
            let k = v / step;
            assert!(
                (k - k.round()).abs() < 1e-6,
                "{v} adıma oturmuyor (k={k})"
            );
        }
    }

    // --- fiyat -------------------------------------------------------------

    #[test]
    fn price_rounds_to_digits_when_no_tick_size() {
        let p = normalize_price(1.234567891, 0.0, 5);
        assert!((p - 1.23457).abs() < 1e-12, "bulunan {p}");
    }

    #[test]
    fn price_snaps_to_tick_size_grid() {
        // Borsa/endeks sembolleri: tick_size > point. Yalnızca digits'e
        // yuvarlamak geçersiz fiyat üretirdi.
        let p = normalize_price(1234.30, 0.25, 2);
        assert!((p - 1234.25).abs() < 1e-9, "beklenen 1234.25, bulunan {p}");
        let p = normalize_price(1234.40, 0.25, 2);
        assert!((p - 1234.50).abs() < 1e-9, "beklenen 1234.50, bulunan {p}");
    }

    #[test]
    fn price_passes_through_non_finite() {
        assert!(normalize_price(f64::NAN, 0.25, 2).is_nan());
        assert!(normalize_price(f64::INFINITY, 0.25, 2).is_infinite());
    }

    // --- stops level -------------------------------------------------------

    #[test]
    fn stops_level_zero_means_no_constraint() {
        assert!(respects_stops_level(1.1000, 1.1000, 0.00001, 0));
    }

    #[test]
    fn stops_level_enforced_symmetrically() {
        let point = 0.00001;
        // 10 point mesafe gerekiyor.
        assert!(!respects_stops_level(1.10005, 1.10000, point, 10), "5 point yetersiz");
        assert!(respects_stops_level(1.10010, 1.10000, point, 10), "10 point yeterli");
        assert!(respects_stops_level(1.09990, 1.10000, point, 10), "ters yön de geçerli");
    }

    // --- yardımcılar -------------------------------------------------------

    #[test]
    fn decimals_of_common_steps() {
        assert_eq!(decimals_of(1.0), 0);
        assert_eq!(decimals_of(0.5), 1);
        assert_eq!(decimals_of(0.1), 1);
        assert_eq!(decimals_of(0.01), 2);
        assert_eq!(decimals_of(0.001), 3);
        assert_eq!(decimals_of(0.25), 2);
    }

    #[test]
    fn order_type_constants_are_distinct() {
        // Wire değerlerinin çakışması sessizce yanlış emir tipi gönderirdi.
        let all = [
            order_type::BUY,
            order_type::SELL,
            order_type::BUY_LIMIT,
            order_type::SELL_LIMIT,
            order_type::BUY_STOP,
            order_type::SELL_STOP,
            order_type::BUY_STOP_LIMIT,
            order_type::SELL_STOP_LIMIT,
        ];
        let uniq: std::collections::HashSet<u8> = all.iter().copied().collect();
        assert_eq!(uniq.len(), all.len());
    }
}
