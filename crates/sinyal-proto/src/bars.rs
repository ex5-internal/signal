//! Geçmiş bar kanalı: MT5'in **kendi** mum serisini taşıyan yapılar.
//!
//! # Neden ayrı bir kanal, neden EA'da değil
//!
//! `CopyRates` MQL5 dokümanına göre EA/script içinde **çağıran thread'i
//! bloklar** ve zaman aşımı süresi **belgelenmemiştir**; canlı hesaplarda
//! 30-60 saniyelik donmalar raporlanmıştır. EA tek thread olduğu için böyle
//! bir çağrı tick toplamayı tamamen durdururdu.
//!
//! Bu yüzden geçmiş okuma ayrı bir **MQL5 Service**'e taşındı: kendi
//! thread'inde çalışır, blokladığında kimseyi etkilemez ve `Sleep()` serbest
//! olduğu için yeniden deneme döngüsü kurulabilir (EA'nın `OnTick`'inde
//! kurulamaz — doküman bunu açıkça önermiyor).
//!
//! Service **ayrı bir yazar** olduğu için EA'nın tek-yazar kilidine dokunmaz:
//! kendi segment çiftini kullanır (`hist` = çekirdek→service istek,
//! `bars` = service→çekirdek yanıt). EA bu özellikten hiç etkilenmez.
//!
//! # Neden bu alanlar
//!
//! `MqlRates` yapısı birebir taşınır, iki dönüşümle:
//!
//! - **`MqlRates.time` SANİYEDİR**, milisaniye değil (`datetime` = epoch
//!   saniye). Tick tarafındaki `time_msc` milisaniye olduğu için burada
//!   1000 ile çarpılıp normalize edilir; aksi halde iki kaynağın bar sınırları
//!   1000 kat kayar ve kesişimde çift/eksik bar oluşur.
//! - **`MqlRates.spread` `int` (32 bit)**, diğer alanlar gibi 64 bit değil.
//!   Hepsini 8 bayt varsaymak yanlış dolgu üretir.
//!
//! `real_volume = 0` **"hacim sıfır" değil, "veri yok"** anlamına gelir —
//! çoğu forex broker'ı gerçek hacim yayınlamaz. Bu ayrım [`bar_flag::HAS_REAL_VOLUME`]
//! ile taşınır; tüketici 0'ı hacim sanmamalıdır.

/// [`HistReq::symbol`] alanının bayt uzunluğu.
///
/// Bu sabit **tek doğruluk kaynağıdır**: MQL5 başlığındaki `symbol[...]` dizisi
/// de üreteç tarafından bundan türetilir. İki yerde elle tutulsaydı biri kayar
/// ve service isteğin ortasından okumaya başlardı.
pub const HIST_SYMBOL_LEN: usize = 32;

/// Bir geçmiş bar isteğinin taşıyıcısı (çekirdek → service).
///
/// İstek mevcut komut halkasından DEĞİL, ayrı bir `hist` halkasından geçer:
/// komut halkasını EA drenaj ediyor ve orada `EnableTrading` kapısı var; veri
/// toplayan bir kurulumda ticaret kapalıyken geçmiş isteği reddedilirdi.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistReq {
    /// Bar açılış zamanı üst sınırı (epoch ms, broker saati). 0 = en son
    /// barlardan geriye doğru.
    pub to_msc: i64,
    /// İsteği yanıtla eşleştiren kimlik. 0 GEÇERSİZDİR.
    pub req_id: u32,
    /// Sembol tablosundaki `symbol_id`. Yalnızca gelen barların doğru isteğe
    /// ait olduğunu doğrulamak için; service bunu ada ÇEVİREMEZ.
    pub symbol_id: u32,
    /// MT5 `ENUM_TIMEFRAMES` ham değeri (PERIOD_M1 = 1, PERIOD_H1 = 16385 …).
    ///
    /// Kendi kısaltmamızı değil MT5'in enum'unu taşıyoruz: service `CopyRates`
    /// çağrısına doğrudan verecek, aradaki her çeviri bir hata fırsatıdır.
    pub timeframe: u32,
    /// İstenen bar sayısı. Service bunu ayrıca `TERMINAL_MAXBARS` ile kırpar —
    /// sınırın dışına çıkan istek `CopyRates`'ten -1 döndürür.
    pub count: u32,
    /// Broker'daki GERÇEK sembol adı, NUL dolgulu (bkz. [`HIST_SYMBOL_LEN`]).
    ///
    /// **Doldurulması zorunludur.** Service bir grafiğe bağlı değildir
    /// (`_Symbol` yoktur) ve sembol tablosu segmentini okumaz; `symbol_id`'yi
    /// ada çevirmenin başka yolu yoktur. Boş gelen istek service tarafından
    /// 4301 ile reddedilir ve o istek için HİÇ bar dönmez.
    ///
    /// Yazmak için [`HistReq::set_symbol`] kullanın — elle kopyalamak, adın
    /// sığmadığı durumda sessizce BAŞKA bir sembolün geçmişini getirebilir.
    pub symbol: [u8; HIST_SYMBOL_LEN],
    pub _reserved: [u8; 8],
}

/// Tek bir geçmiş bar (service → çekirdek).
///
/// Boyut 120 bayt → `Cell<BarRec>` = 128 bayt. Bu, mevcut halkaların hiçbiriyle
/// AYNI DEĞİL (Tick 64, Cmd/Res 192, Book 832). Kasıtlı: `RING_MAGIC` tüm
/// halkalarda aynı olduğu için yanlış segment adına bağlanmayı yakalayan tek
/// çalışma-zamanı denetimi slot boyutudur.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarRec {
    /// Bar AÇILIŞ zamanı, epoch **milisaniye**, broker sunucu saati.
    ///
    /// `MqlRates.time` saniyedir; service 1000 ile çarpar.
    pub time_msc: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Barı oluşturan tick sayısı — gerçek hacim DEĞİL.
    pub tick_volume: i64,
    /// Gerçek işlem hacmi. Yalnızca [`bar_flag::HAS_REAL_VOLUME`] varsa
    /// anlamlıdır; yoksa broker yayınlamıyor demektir ve 0 "veri yok"tur.
    pub real_volume: i64,
    /// Hangi isteğe ait.
    pub req_id: u32,
    pub symbol_id: u32,
    /// MT5 `ENUM_TIMEFRAMES` ham değeri.
    pub timeframe: u32,
    /// Spread, **point** cinsinden. `MqlRates.spread` `int`'tir.
    pub spread: i32,
    pub flags: u32,
    /// İstek içindeki sıra (0'dan başlar, eskiden yeniye).
    ///
    /// `CopyRates` fiziksel dizide **her zaman** en eskiyi başa koyar;
    /// `ArraySetAsSeries` bunu değiştirmez. Sıra bu yüzden kronolojik artandır.
    pub index: u32,
    /// Bu isteğin toplam bar sayısı. Çekirdek eksik teslimatı bununla anlar:
    /// halka dolarsa barlar sessizce kaybolurdu, sayı tutmayınca fark edilir.
    pub total: u32,
    pub _pad: u32,
    /// 32 bayt — `BarRec`'i 120'ye tamamlar, böylece `Cell<BarRec>` tam 128
    /// (iki önbellek satırı) olur. Rezerve boyutu keyfi değildir.
    pub _reserved: [u8; 32],
}

/// [`BarRec::flags`] bitleri.
pub mod bar_flag {
    /// Bu bar **oluşmakta** — kapanmamıştır, `close` değişmeye devam eder.
    ///
    /// `CopyRates`'in döndürdüğü SON bar her zaman budur (`start_pos = 0`
    /// güncel tamamlanmamış bara karşılık gelir). Kapanmış sanıp seriye
    /// koymak `close` değerini kalıcı zannettirir.
    pub const PARTIAL: u32 = 1 << 0;
    /// Bu isteğin son barı — çekirdek akışın bittiğini buradan anlar.
    pub const LAST: u32 = 1 << 1;
    /// `real_volume` alanı ANLAMLI. Yoksa broker gerçek hacim yayınlamıyor
    /// demektir ve 0 değeri "hacim sıfır" olarak yorumlanmamalıdır.
    pub const HAS_REAL_VOLUME: u32 = 1 << 2;
    /// İstek BAŞARISIZ oldu; bu kayıt veri değil hata taşır.
    ///
    /// Hata kodu [`BarRec::spread`] alanında (MQL5 `GetLastError()` değeri),
    /// [`BarRec::total`] 0'dır. Hatayı bar gibi göstermek yerine ayrı
    /// işaretlemek şart: aksi halde istemci "geçmiş yok" ile "geçmiş
    /// alınamadı"yı ayırt edemez.
    pub const ERROR: u32 = 1 << 3;
}

/// MQL5 `ENUM_TIMEFRAMES` ham değerleri.
///
/// Değerler MetaQuotes tarafından tanımlanmıştır ve keyfi değildir: dakika
/// dilimleri 1..30, saatlik ve üstü 16385'ten başlar. Kendi sıralı enum'umuzu
/// uydurmak, service tarafında bir çeviri tablosu gerektirirdi.
pub mod timeframe {
    pub const M1: u32 = 1;
    pub const M5: u32 = 5;
    pub const M15: u32 = 15;
    pub const M30: u32 = 30;
    pub const H1: u32 = 16385;
    pub const H4: u32 = 16388;
    pub const D1: u32 = 16408;

    /// Dilimin milisaniye uzunluğu. Bilinmeyen değer için `None`.
    pub const fn millis(tf: u32) -> Option<i64> {
        match tf {
            M1 => Some(60_000),
            M5 => Some(300_000),
            M15 => Some(900_000),
            M30 => Some(1_800_000),
            H1 => Some(3_600_000),
            H4 => Some(14_400_000),
            D1 => Some(86_400_000),
            _ => None,
        }
    }

    /// İstemcinin kullandığı kısaltmayı MT5 enum değerine çevir.
    pub fn from_name(name: &str) -> Option<u32> {
        match name {
            "M1" => Some(M1),
            "M5" => Some(M5),
            "M15" => Some(M15),
            "M30" => Some(M30),
            "H1" => Some(H1),
            "H4" => Some(H4),
            "D1" => Some(D1),
            _ => None,
        }
    }

    /// MT5 enum değerini istemci kısaltmasına çevir.
    pub fn to_name(tf: u32) -> Option<&'static str> {
        match tf {
            M1 => Some("M1"),
            M5 => Some("M5"),
            M15 => Some("M15"),
            M30 => Some("M30"),
            H1 => Some("H1"),
            H4 => Some("H4"),
            D1 => Some("D1"),
            _ => None,
        }
    }
}

impl Default for HistReq {
    fn default() -> Self {
        Self {
            to_msc: 0,
            req_id: 0,
            symbol_id: 0,
            timeframe: 0,
            count: 0,
            symbol: [0; HIST_SYMBOL_LEN],
            _reserved: [0; 8],
        }
    }
}

impl HistReq {
    /// Sembol adını yaz. Ad sığmıyorsa **yazmaz** ve `false` döner.
    ///
    /// Kırpmıyoruz bilinçli olarak: broker sembolleri sonek taşır
    /// (`EURUSD.pro`, `XAUUSD.m`) ve kırpılmış bir ad BAŞKA, GERÇEK bir
    /// sembolün adına dönüşebilir. O durumda service hatasız çalışır ve
    /// yanlış enstrümanın geçmişini teslim eder — grafikte fark edilmeyen,
    /// modeli sessizce zehirleyen bir hata. Sığmayan istek reddedilmelidir.
    pub fn set_symbol(&mut self, name: &str) -> bool {
        if name.is_empty() || name.len() > HIST_SYMBOL_LEN {
            return false;
        }
        crate::msg::write_fixed_str(&mut self.symbol, name);
        true
    }

    /// İstekteki sembol adı (NUL dolgusu atılır). Boşsa `""`.
    pub fn symbol_str(&self) -> &str {
        crate::msg::read_fixed_str(&self.symbol)
    }
}

impl Default for BarRec {
    fn default() -> Self {
        Self {
            time_msc: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            tick_volume: 0,
            real_volume: 0,
            req_id: 0,
            symbol_id: 0,
            timeframe: 0,
            spread: 0,
            flags: 0,
            index: 0,
            total: 0,
            _pad: 0,
            _reserved: [0; 32],
        }
    }
}

impl BarRec {
    pub fn is_partial(&self) -> bool {
        self.flags & bar_flag::PARTIAL != 0
    }
    pub fn is_last(&self) -> bool {
        self.flags & bar_flag::LAST != 0
    }
    pub fn is_error(&self) -> bool {
        self.flags & bar_flag::ERROR != 0
    }
    /// Gerçek hacim anlamlıysa döndür. `None` = broker yayınlamıyor.
    pub fn real_volume_opt(&self) -> Option<i64> {
        (self.flags & bar_flag::HAS_REAL_VOLUME != 0).then_some(self.real_volume)
    }
    /// Hata kaydıysa MQL5 hata kodunu döndür.
    pub fn error_code(&self) -> Option<i32> {
        self.is_error().then_some(self.spread)
    }

    /// OHLC tutarlı mı — `high` en yüksek, `low` en düşük olmalı.
    ///
    /// Bozuk bir bar sessizce grafiğe girerse fiyat ekseni patlar; service ile
    /// çekirdek ayrı derleyicilerden geldiği için ucuz bir sağlık kontrolü.
    pub fn is_coherent(&self) -> bool {
        if self.is_error() {
            return true;
        }
        self.high >= self.low
            && self.high >= self.open
            && self.high >= self.close
            && self.low <= self.open
            && self.low <= self.close
            && self.open > 0.0
            && self.close > 0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeframe_names_round_trip() {
        for n in ["M1", "M5", "M15", "M30", "H1", "H4", "D1"] {
            let v = timeframe::from_name(n).expect("bilinen dilim");
            assert_eq!(timeframe::to_name(v), Some(n));
            assert!(timeframe::millis(v).is_some());
        }
        assert_eq!(timeframe::from_name("M2"), None);
        assert_eq!(timeframe::to_name(9999), None);
    }

    #[test]
    fn hourly_timeframes_use_mt5_encoding_not_minutes() {
        // MT5'te PERIOD_H1 = 16385, 60 DEĞİL. Dakika sanmak service tarafında
        // sessizce yanlış dilim çekilmesine yol açardı.
        assert_eq!(timeframe::H1, 16385);
        assert_eq!(timeframe::millis(timeframe::H1), Some(3_600_000));
        assert_eq!(timeframe::from_name("H1"), Some(16385));
        // 60 geçerli bir dilim DEĞİL.
        assert_eq!(timeframe::millis(60), None);
    }

    #[test]
    fn request_carries_the_symbol_name_because_the_service_cannot_resolve_ids() {
        // Service grafiğe bağlı DEĞİLDİR ve sembol tablosunu okumaz; adı
        // taşımazsak `SymbolSelect`'e verecek bir şey kalmaz ve HER istek
        // 4301 ile reddedilir.
        let mut r = HistReq::default();
        assert_eq!(r.symbol_str(), "", "doldurulmamis istek bos ad tasir");
        assert!(r.set_symbol("XAUUSD.m"));
        assert_eq!(r.symbol_str(), "XAUUSD.m");

        // Ad alanı `HistReq`'in İÇİNDE kalmalı ve yapı 64 bayt sürmeli;
        // taşarsa RING_VERSION kırılırdı.
        assert_eq!(core::mem::size_of::<HistReq>(), 64);
        assert_eq!(core::mem::offset_of!(HistReq, symbol), 24);
    }

    #[test]
    fn an_oversized_symbol_is_refused_not_truncated() {
        // Kırpılmış bir ad BAŞKA bir gerçek sembole dönüşebilir; service
        // hatasız çalışır ve YANLIŞ enstrümanın geçmişini teslim eder.
        let mut r = HistReq::default();
        let long = "A".repeat(HIST_SYMBOL_LEN + 1);
        assert!(!r.set_symbol(&long));
        assert_eq!(r.symbol_str(), "", "reddedilen ad kismen bile yazilmamali");
        assert!(!r.set_symbol(""), "bos ad da gecersiz");

        // Tam sığan ad kabul edilir (NUL sonlandırıcı YOKTUR — okuyucu
        // tampon sonunda da durmalı).
        let exact = "B".repeat(HIST_SYMBOL_LEN);
        assert!(r.set_symbol(&exact));
        assert_eq!(r.symbol_str(), exact);
    }

    #[test]
    fn zero_real_volume_is_not_reported_as_volume() {
        // Çoğu forex broker'ı gerçek hacim yayınlamaz; 0 "veri yok" demektir.
        let b = BarRec { real_volume: 0, ..Default::default() };
        assert_eq!(b.real_volume_opt(), None, "bayrak yoksa hacim raporlanmamali");

        let b = BarRec { real_volume: 0, flags: bar_flag::HAS_REAL_VOLUME, ..Default::default() };
        assert_eq!(b.real_volume_opt(), Some(0), "bayrak varsa 0 gercek bir olcumdur");
    }

    #[test]
    fn error_record_is_distinguishable_from_empty_history() {
        let e = BarRec { flags: bar_flag::ERROR | bar_flag::LAST, spread: 4401, ..Default::default() };
        assert!(e.is_error());
        assert_eq!(e.error_code(), Some(4401));
        assert_eq!(e.total, 0);
        // Hata kaydı OHLC tutarlılığından muaf — veri taşımıyor.
        assert!(e.is_coherent());
    }

    #[test]
    fn incoherent_bars_are_rejected() {
        let ok = BarRec {
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            ..Default::default()
        };
        assert!(ok.is_coherent());

        // high < low
        let bad = BarRec { open: 100.0, high: 99.0, low: 101.0, close: 100.0, ..Default::default() };
        assert!(!bad.is_coherent());

        // close high'in ustunde
        let bad = BarRec { open: 100.0, high: 101.0, low: 99.0, close: 102.0, ..Default::default() };
        assert!(!bad.is_coherent());

        // sifir fiyat — bos/doldurulmamis kayit
        assert!(!BarRec::default().is_coherent());
    }

    #[test]
    fn partial_and_last_flags_are_independent() {
        // CopyRates'in son bari HEM son HEM olusmakta olabilir.
        let b = BarRec {
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            flags: bar_flag::PARTIAL | bar_flag::LAST,
            ..Default::default()
        };
        assert!(b.is_partial() && b.is_last() && !b.is_error());
    }
}
