//! Geçmiş bar kanalının **çekirdek tarafı**: istek yaz, bar topla.
//!
//! # Yön
//!
//! ```text
//! çekirdek --(HistReq)--> hreq halkası --> MQL5 Service
//! çekirdek <--(BarRec)--- bars halkası <-- MQL5 Service
//! ```
//!
//! Service ayrı bir yazardır; EA'nın tek-yazar kilidine dokunmaz. Service hiç
//! çalışmıyorsa bağlanma başarısız olur — bu bir **hata değildir**, yalnızca
//! "geçmiş özelliği kapalı" demektir ve çekirdek tick akışını normal sürdürür.
//!
//! # Neden bir iz (`HistLink`) üzerinden
//!
//! Somut taşıyıcı paylaşımlı belleğe bağlanır ve tek thread'e kilitlidir; onu
//! doğrudan kullanmak bu modülü testten çıkarırdı. İz sayesinde eşleştirme,
//! eksik teslimat tespiti ve zaman aşımı mantığı paylaşımlı bellek olmadan
//! sınanabiliyor — burada yanlış olan bir şey sessizce **eksik grafik** demek
//! olduğu için bu mantığın test edilebilir olması şart.
//!
//! # Bu modülün savunduğu üç şey
//!
//! 1. **Sonsuza kadar bekleme yok.** Service donarsa (`CopyRates` canlıda
//!    30-60 sn bloklayabiliyor) istek zaman aşımına uğrar ve istemci "geçmiş
//!    alınamadı" cevabı alır. Cevapsız bırakmak istemciyi asardı.
//! 2. **Eksik teslimat sessiz kalmaz.** Bar halkası dolarsa yazar yeniden
//!    DENEMEZ ve barlar kalıcı kaybolur. Her bar kendi isteğinin `total`
//!    değerini taşıdığı için sayı tutmadığında bunu açıkça raporluyoruz —
//!    yoksa istemci delikli bir seriyi tam sanardı.
//! 3. **Bozuk bar seriye girmez.** Service ile çekirdek ayrı derleyicilerden
//!    geliyor; OHLC tutarsız bir kayıt grafiği patlatır. Reddedip sayıyoruz.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use sinyal_proto::{timeframe, BarRec, HistReq, HIST_SYMBOL_LEN};

use crate::candles::Bar;

/// Bir isteğin tamamlanması için tanınan süre.
///
/// `CopyRates` ilk çağrıda geçmişi sunucudan indirebilir ve bu saniyeler
/// sürer; fazla kısa bir süre sağlıklı bir indirmeyi hata sayardı. Fazla uzun
/// bir süre ise donmuş bir service'i saklardı.
///
/// # Bu süre service'in bütçesinden BÜYÜK olmak ZORUNDA
///
/// Service bir isteğe kendi bütçesi (`HistoryBudgetMs` + `RingWaitMs`) kadar
/// zaman ayırır ve barları ancak bütçe bitince yayınlar. O toplam bu süreyi
/// aşarsa şu döngü kurulur: biz zaman aşımıyla vazgeçeriz, service saniyeler
/// sonra barları yazar, barlar artık kimseye ait olmadığı için `orphan_bars`'a
/// düşer, depo hiç dolmaz ve bir sonraki istek aynı şeyi tekrarlar. Sonuç:
/// geçmişi istenenden KISA olan her sembol için sonsuza kadar `timeout` ve
/// SIFIR bar. `SinyalHistory.mq5` bu yüzden bütçesini burada yazan değere
/// bakarak kırpar (bkz. `SINYAL_HIST_CONSUMER_TIMEOUT_MS`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(8);

/// Tek turda halkadan çekilecek azami bar.
///
/// Geçmiş akışı patlamalıdır (tek istek binlerce bar). Sınırsız çekmek
/// okuyucu döngüsünü tick akışından koparırdı — fiyat gecikmesi geçmişten
/// daha önemli.
const DRAIN_BUDGET: usize = 4096;

/// Service'in "bar halkası doldu, teslimatı kestim" kodu.
///
/// MQL5'in hata uzayında (4000-5xxx) karşılığı YOK; 9000 bloğu bu projeye
/// aittir ve çakışmaz. Ayrı tutulması şart: bu kod terminalde değil BİZDE bir
/// sorun olduğunu söyler. (`HistBridge.mqh::SINYAL_HIST_ERR_RING_FULL`)
pub const ERR_RING_FULL: i32 = 9001;

/// Geçmiş kanalının taşıyıcısı.
///
/// Somut uygulama paylaşımlı belleğe bağlanır (bkz. [`crate::hist_bridge`]).
pub trait HistLink: Send {
    /// İsteği service'e ilet. `false` = istek halkası DOLU.
    fn push_req(&self, req: &HistReq) -> bool;
    /// Bekleyen bir bar al. `None` = şu an yok.
    fn pop_bar(&self) -> Option<BarRec>;
}

/// Bir isteğin sonucu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistStatus {
    /// Beklenen bar sayısı birebir geldi. `total = 0` ise broker'da bu aralık
    /// için geçmiş yok — bu da **başarılı** bir cevaptır, hatadan farklıdır.
    Complete,
    /// Akış bitti ama sayı tutmuyor. En olası sebep bar halkasının dolması;
    /// bozuk bar reddi de buraya düşer. Seri DELİKLİDİR.
    Incomplete { expected: u32, got: u32 },
    /// Service hata bildirdi; değer MQL5 `GetLastError()` kodudur.
    ///
    /// 4401/4403 geçicidir (yeniden denenebilir), 4404/4301/4302/4407
    /// kalıcıdır — istemciye kodu olduğu gibi veriyoruz ki ayırt edebilsin.
    Failed(i32),
    /// Süre doldu; akışın sonu hiç gelmedi.
    TimedOut { got: u32 },
}

impl HistStatus {
    /// Tel protokolünde kullanılan kısa ad.
    pub fn name(self) -> &'static str {
        match self {
            Self::Complete => "ok",
            Self::Incomplete { .. } => "incomplete",
            Self::Failed(_) => "failed",
            Self::TimedOut { .. } => "timeout",
        }
    }

    /// İnsan okunur ayrıntı (hata kodu, eksik sayı). Yoksa `None`.
    pub fn detail(self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::Incomplete { expected, got } => {
                Some(format!("{got}/{expected} bar geldi — halka dolmus olabilir"))
            }
            // 9001 bir MQL5 kodu DEĞİL, service'in bize "senin bar halkan
            // doldu, teslimatı kesmek zorunda kaldım" demesidir. MQL5 hatası
            // gibi göstermek operatörü terminalde sebep aramaya gönderirdi —
            // oysa sorun BİZDE: barları yeterince hızlı çekmiyoruz.
            Self::Failed(ERR_RING_FULL) => Some(format!(
                "bar halkasi doldu (kod {ERR_RING_FULL}) — cekirdek barlari yeterince \
                 hizli cekmiyor, terminal hatasi DEGIL"
            )),
            Self::Failed(code) => Some(format!("MQL5 hata {code}")),
            Self::TimedOut { got } => Some(format!("zaman asimi ({got} bar gelmisti)")),
        }
    }
}

/// Tamamlanmış (veya başarısız olmuş) bir istek.
#[derive(Debug, Clone)]
pub struct HistOutcome {
    pub req_id: u32,
    pub symbol: String,
    /// Kanonik dilim adı (`M1`, `H4`, `D1`).
    pub tf: &'static str,
    /// Kabul edilen barlar, eskiden yeniye.
    pub bars: Vec<Bar>,
    pub status: HistStatus,
}

/// İstek gönderilemedi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistError {
    /// Dilim MT5 `ENUM_TIMEFRAMES` karşılığı olmayan bir ad.
    UnknownTimeframe(String),
    /// İstek halkası dolu. Service tıkanmış demektir; yeniden denenebilir.
    RingFull,
    /// `count` 0 verildi — anlamsız istek.
    EmptyRequest,
    /// Sembol adı `HistReq`'in ad alanına sığmıyor (veya boş).
    ///
    /// Kırpıp göndermek YASAK: broker sembolleri sonek taşır ve kırpılmış bir
    /// ad BAŞKA, gerçek bir sembole dönüşebilir — service hatasız çalışır ve
    /// yanlış enstrümanın geçmişini teslim eder.
    SymbolDoesNotFit(String),
}

impl core::fmt::Display for HistError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownTimeframe(t) => write!(f, "gecersiz dilim '{t}'"),
            Self::RingFull => write!(f, "gecmis istek halkasi dolu — service tikanmis"),
            Self::EmptyRequest => write!(f, "count 0"),
            Self::SymbolDoesNotFit(s) => {
                write!(f, "sembol adi '{s}' istek alanina sigmiyor (en fazla {HIST_SYMBOL_LEN} bayt)")
            }
        }
    }
}

/// WebSocket görevinden okuyucu thread'ine giden geçmiş isteği.
///
/// Doğrudan yazmıyoruz: taşıyıcı, oturuma erişen ilk thread'e kilitlidir ve
/// başka thread'den gelen işlemi SESSİZCE reddeder. Mevcut `cmd_tx` kalıbının
/// aynısı.
pub struct HistCmd {
    pub symbol: String,
    pub symbol_id: u32,
    /// İstemcinin verdiği dilim adı; burada kanonikleştirilir.
    pub tf: String,
    pub count: u32,
    /// Üst sınır (epoch ms). 0 = en son barlardan geriye.
    pub to_msc: i64,
    pub reply: tokio::sync::oneshot::Sender<HistReply>,
}

/// Okuyucu thread'inin WebSocket görevine verdiği cevap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistReply {
    /// Akış bitti; barlar (varsa) depoya işlendi.
    Done { status: HistStatus, bars: usize },
    /// Geçmiş kanalı yok — service çalışmıyor. Hata DEĞİL, özellik kapalı.
    Unavailable,
    /// İstek hiç gönderilemedi.
    Refused(String),
}

/// Bekleyen tek bir istek.
struct Pending {
    symbol: String,
    symbol_id: u32,
    tf: &'static str,
    tf_raw: u32,
    requested: u32,
    started: Instant,
    /// Kabul edilen barlar.
    bars: Vec<Bar>,
    /// Service'in bildirdiği toplam. Bilinmiyorsa `None`.
    total: Option<u32>,
    error: Option<i32>,
    /// `LAST` bayrağı görüldü mü.
    done: bool,
}

/// Geçmiş istek istemcisi.
///
/// Okuyucu thread'i sahiplenir: `request` çağrılır, `drain` her turda bar
/// toplar, `expire` süresi dolanları kapatır.
pub struct HistClient {
    link: Box<dyn HistLink>,
    next_id: u32,
    pending: HashMap<u32, Pending>,
    timeout: Duration,
    /// Hiçbir bekleyen isteğe ait olmayan bar (geç gelen, süresi dolmuş
    /// isteğin artığı). Atılır ama SAYILIR.
    pub orphan_bars: u64,
    /// OHLC tutarsız olduğu için reddedilen bar.
    pub incoherent_bars: u64,
    /// İsteğin sembol/dilimiyle uyuşmayan bar — service tarafında karışıklık.
    pub mismatched_bars: u64,
}

impl HistClient {
    pub fn new(link: Box<dyn HistLink>, timeout: Duration) -> Self {
        Self {
            link,
            next_id: 1,
            pending: HashMap::new(),
            timeout,
            orphan_bars: 0,
            incoherent_bars: 0,
            mismatched_bars: 0,
        }
    }

    /// Bekleyen istek sayısı.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Yeni bir geçmiş isteği gönder. Başarılıysa `req_id`.
    ///
    /// `count`, deponun tek istekte taşıyabileceği azami barla kırpılır;
    /// service ayrıca `TERMINAL_MAXBARS` ile kırpar (sınır dışı `count`
    /// `CopyRates`'ten -1 döndürür).
    pub fn request(
        &mut self,
        symbol: &str,
        symbol_id: u32,
        tf: &str,
        count: u32,
        to_msc: i64,
    ) -> Result<u32, HistError> {
        let up = tf.to_ascii_uppercase();
        let (tf_raw, tf_name) = match timeframe::from_name(&up).and_then(|r| timeframe::to_name(r).map(|n| (r, n)))
        {
            Some(v) => v,
            None => return Err(HistError::UnknownTimeframe(tf.to_owned())),
        };
        if count == 0 {
            return Err(HistError::EmptyRequest);
        }
        let count = count.min(crate::candles::MAX_REQUEST as u32);

        let req_id = self.take_id();
        let mut req = HistReq {
            to_msc,
            req_id,
            symbol_id,
            timeframe: tf_raw,
            count,
            ..Default::default()
        };
        // SEMBOL ADI ZORUNLU. Service bir grafiğe bağlı değildir (`_Symbol`
        // yoktur) ve sembol tablosu segmentini okumaz; `symbol_id`'yi ada
        // çevirmesinin başka yolu yok. Ad boş giderse service isteği 4301 ile
        // reddeder ve o istek için HİÇ bar dönmez — kanal tamamen ölür.
        // `set_symbol` sığmayan adı kırpmak yerine reddediyor: kırpılmış bir
        // ad başka bir gerçek sembole dönüşüp YANLIŞ geçmişi getirebilirdi.
        if !req.set_symbol(symbol) {
            return Err(HistError::SymbolDoesNotFit(symbol.to_owned()));
        }

        // Halka doluysa isteği KAYDETME: kaydetmek, hiç gitmemiş bir isteği
        // zaman aşımına kadar bekletmek olurdu.
        if !self.link.push_req(&req) {
            return Err(HistError::RingFull);
        }

        self.pending.insert(
            req_id,
            Pending {
                symbol: symbol.to_owned(),
                symbol_id,
                tf: tf_name,
                tf_raw,
                requested: count,
                started: Instant::now(),
                bars: Vec::new(),
                total: None,
                error: None,
                done: false,
            },
        );
        Ok(req_id)
    }

    /// `req_id` üret. **0 asla verilmez** — protokolde 0 geçersizdir.
    fn take_id(&mut self) -> u32 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1;
            }
            if id != 0 && !self.pending.contains_key(&id) {
                return id;
            }
        }
    }

    /// Halkadan bar çek; tamamlanan istekleri döndür.
    pub fn drain(&mut self) -> Vec<HistOutcome> {
        let mut finished: Vec<u32> = Vec::new();
        let mut pulled = 0usize;

        while pulled < DRAIN_BUDGET {
            let Some(rec) = self.link.pop_bar() else { break };
            pulled += 1;

            let Some(p) = self.pending.get_mut(&rec.req_id) else {
                // Süresi dolmuş bir isteğin geç gelen barı olabilir. Atıyoruz
                // ama sayıyoruz: sürekli artıyorsa zaman aşımı çok kısa.
                self.orphan_bars += 1;
                continue;
            };

            if let Some(code) = rec.error_code() {
                // Hata kaydı veri taşımaz; `total` 0'dır. Buradan sonrası yok.
                p.error = Some(code);
                p.done = true;
            } else if rec.time_msc <= 0 {
                // Zamansız kayıt bar DEĞİL, akışı kapatan bir sonlandırıcıdır:
                // broker o aralık için hiç bar vermediğinde service'in
                // `LAST`'ı iliştirebileceği bir bar yoktur. Bunu "bozuk bar"
                // saymak, boş ama BAŞARILI bir cevabı bozulmuş gibi
                // gösterirdi.
            } else if rec.symbol_id != p.symbol_id || rec.timeframe != p.tf_raw {
                // Service karıştırmış: yanlış seriyi doğru isteğe yazmak
                // grafikte fark edilmeyen bir yalan üretirdi.
                self.mismatched_bars += 1;
            } else if !rec.is_coherent() || rec.time_msc <= 0 {
                self.incoherent_bars += 1;
            } else {
                p.bars.push(from_rec(&rec));
            }

            if rec.is_last() {
                // Son kayıt AKIŞIN ÖZETİDİR: `total`'ı olduğu gibi alıyoruz,
                // 0 olsa bile. "Broker bu aralık için bar vermedi" (total 0)
                // ile "beklediğimiz kadar gelmedi" ancak böyle ayrılabilir.
                p.total = Some(rec.total);
                p.done = true;
            } else if rec.total > 0 {
                // Ara kayıtlarda en büyüğünü tutuyoruz: bir kayıt 0 yazarsa
                // beklenti düşmesin.
                p.total = Some(p.total.map_or(rec.total, |t| t.max(rec.total)));
            }
            // Emniyet ağı: `LAST` bayrağı kaybolsa bile beklenen sayıya
            // ulaşıldığında isteği kapat, yoksa gereksiz yere zaman aşımına
            // uğrardı.
            if let Some(t) = p.total {
                if p.bars.len() as u32 >= t {
                    p.done = true;
                }
            }
            if p.done {
                finished.push(rec.req_id);
            }
        }

        finished
            .into_iter()
            .filter_map(|id| self.pending.remove(&id).map(|p| finish(id, p)))
            .collect()
    }

    /// Süresi dolan istekleri kapat.
    ///
    /// Cevapsız bırakmak istemciyi sonsuza kadar bekletirdi; "geçmiş
    /// alınamadı" demek daha dürüst.
    pub fn expire(&mut self) -> Vec<HistOutcome> {
        let timeout = self.timeout;
        let stale: Vec<u32> = self
            .pending
            .iter()
            .filter(|(_, p)| p.started.elapsed() >= timeout)
            .map(|(id, _)| *id)
            .collect();
        stale
            .into_iter()
            .filter_map(|id| self.pending.remove(&id).map(|p| (id, p)))
            .map(|(id, p)| HistOutcome {
                // `req_id` KORUNUR: çağıran bekleyen tarafı bununla buluyor.
                // 0 döndürmek, isteği yapanı kendi zaman aşımına kadar
                // bekletmek olurdu.
                req_id: id,
                status: HistStatus::TimedOut { got: p.bars.len() as u32 },
                symbol: p.symbol,
                tf: p.tf,
                bars: p.bars,
            })
            .collect()
    }
}

/// Bekleyen isteği sonuca çevir.
fn finish(req_id: u32, p: Pending) -> HistOutcome {
    let got = p.bars.len() as u32;
    let status = if let Some(code) = p.error {
        HistStatus::Failed(code)
    } else {
        // `total` bildirilmediyse istediğimiz sayıyı bekliyoruz. Service
        // 0 bar döndürdüyse (geçmiş yok) `total` da 0'dır ve `got == 0`
        // olduğu için bu BAŞARILI sayılır — "geçmiş yok" ile "geçmiş
        // alınamadı" ayrı şeyler.
        let expected = p.total.unwrap_or(p.requested);
        if got == expected {
            HistStatus::Complete
        } else {
            HistStatus::Incomplete { expected, got }
        }
    };
    HistOutcome { req_id, symbol: p.symbol, tf: p.tf, bars: p.bars, status }
}

/// Tel kaydını depo barına çevir.
///
/// `time_msc` zaten milisaniyedir — `MqlRates.time` saniye olduğu için 1000
/// ile çarpma **service tarafında** yapılır. Burada tekrar çarpmak bar
/// sınırlarını 1000 kat kaydırırdı.
fn from_rec(r: &BarRec) -> Bar {
    Bar {
        t: r.time_msc,
        o: r.open,
        h: r.high,
        l: r.low,
        c: r.close,
        // MT5'in `tick_volume`'u da hacim değil tick sayısıdır; tick'ten
        // ürettiğimiz barla aynı anlamı taşıdığı için aynı alana giriyor.
        ticks: r.tick_volume.clamp(0, u32::MAX as i64) as u32,
        partial: r.is_partial(),
        // 0 "hacim sıfır" değil "veri yok" demek; bayrak yoksa None.
        real_volume: r.real_volume_opt(),
        spread: Some(r.spread),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinyal_proto::bar_flag;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// Taşıyıcıyı taklit eden test bağlantısı.
    #[derive(Default)]
    struct FakeLink {
        reqs: Mutex<Vec<HistReq>>,
        bars: Mutex<VecDeque<BarRec>>,
        /// `true` ise istek halkası dolu davranır.
        full: Mutex<bool>,
    }

    impl HistLink for Arc<FakeLink> {
        fn push_req(&self, req: &HistReq) -> bool {
            if *self.full.lock().unwrap() {
                return false;
            }
            self.reqs.lock().unwrap().push(*req);
            true
        }
        fn pop_bar(&self) -> Option<BarRec> {
            self.bars.lock().unwrap().pop_front()
        }
    }

    fn setup() -> (Arc<FakeLink>, HistClient) {
        let link = Arc::new(FakeLink::default());
        let client = HistClient::new(Box::new(link.clone()), DEFAULT_TIMEOUT);
        (link, client)
    }

    fn bar(req_id: u32, index: u32, total: u32, price: f64) -> BarRec {
        BarRec {
            time_msc: 1_700_000_000_000 + index as i64 * 60_000,
            open: price,
            high: price + 0.001,
            low: price - 0.001,
            close: price,
            tick_volume: 42,
            req_id,
            symbol_id: 7,
            timeframe: timeframe::M1,
            index,
            total,
            flags: if index + 1 == total { bar_flag::LAST } else { 0 },
            ..Default::default()
        }
    }

    fn push(link: &FakeLink, b: BarRec) {
        link.bars.lock().unwrap().push_back(b);
    }

    #[test]
    fn request_carries_mt5_enum_not_our_shorthand() {
        // Service `CopyRates`'e doğrudan verecek; aradaki her çeviri bir hata
        // fırsatı. H1 = 16385, 60 DEĞİL.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "h1", 300, 0).unwrap();
        assert_ne!(id, 0, "req_id 0 protokolde gecersiz");

        let reqs = link.reqs.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].timeframe, 16385, "MT5 ham degeri gitmeli");
        assert_eq!(reqs[0].symbol_id, 7);
        assert_eq!(reqs[0].count, 300);
        assert_eq!(reqs[0].req_id, id);
        assert_eq!(reqs[0].to_msc, 0);
    }

    #[test]
    fn request_carries_the_symbol_name_not_just_the_id() {
        // GEÇMİŞTE OLAN HATA: istek yalnızca `symbol_id` taşıyordu. Service
        // grafiğe bağlı değildir ve id'yi ada çeviremez; boş ad gördüğü her
        // isteği 4301 ile reddeder. Kanal derlenir, testler yeşil kalır ve
        // canlıda TEK BİR BAR bile gelmez.
        let (link, mut c) = setup();
        c.request("XAUUSD.m", 7, "M1", 100, 0).unwrap();
        let reqs = link.reqs.lock().unwrap();
        assert_eq!(reqs[0].symbol_str(), "XAUUSD.m", "service adi ISTEKTEN okur");
    }

    #[test]
    fn a_symbol_that_would_be_truncated_is_refused_before_it_is_sent() {
        // Kırpılmış ad BAŞKA bir gerçek sembole dönüşebilir; service hatasız
        // çalışır ve yanlış enstrümanın geçmişini teslim eder. Sessiz ve
        // grafikte fark edilmeyen bir yalan — reddetmek tek doğru davranış.
        let (link, mut c) = setup();
        let too_long = "X".repeat(sinyal_proto::HIST_SYMBOL_LEN + 1);
        assert_eq!(
            c.request(&too_long, 7, "M1", 10, 0),
            Err(HistError::SymbolDoesNotFit(too_long))
        );
        assert!(link.reqs.lock().unwrap().is_empty(), "sigmayan istek gonderilmemeli");
        assert_eq!(c.pending_len(), 0, "gonderilmeyen istek asili kalmamali");
    }

    #[test]
    fn request_count_is_clamped_to_what_the_store_can_hold() {
        let (link, mut c) = setup();
        c.request("EURUSD", 7, "M1", 10_000_000, 0).unwrap();
        assert_eq!(
            link.reqs.lock().unwrap()[0].count,
            crate::candles::MAX_REQUEST as u32,
            "istemci bellegi sisirememeli"
        );
    }

    #[test]
    fn unknown_timeframe_is_refused_before_it_reaches_the_service() {
        let (link, mut c) = setup();
        assert_eq!(
            c.request("EURUSD", 7, "M2", 10, 0),
            Err(HistError::UnknownTimeframe("M2".into()))
        );
        assert_eq!(c.request("EURUSD", 7, "M1", 0, 0), Err(HistError::EmptyRequest));
        assert!(link.reqs.lock().unwrap().is_empty(), "gecersiz istek gonderilmemeli");
        assert_eq!(c.pending_len(), 0);
    }

    #[test]
    fn full_request_ring_is_reported_and_leaves_nothing_pending() {
        // Halka doluyken isteği kaydetmek, hiç gitmemiş bir isteği zaman
        // aşımına kadar bekletmek olurdu.
        let (link, mut c) = setup();
        *link.full.lock().unwrap() = true;
        assert_eq!(c.request("EURUSD", 7, "M1", 10, 0), Err(HistError::RingFull));
        assert_eq!(c.pending_len(), 0, "gonderilemeyen istek asili kalmamali");
    }

    #[test]
    fn collects_a_complete_series_in_chronological_order() {
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 3, 0).unwrap();
        for i in 0..3 {
            push(&link, bar(id, i, 3, 1.10 + i as f64 * 0.01));
        }

        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, HistStatus::Complete);
        assert_eq!(out[0].symbol, "EURUSD");
        assert_eq!(out[0].tf, "M1");
        assert_eq!(out[0].bars.len(), 3);
        // index 0 = EN ESKİ; `ArraySetAsSeries` fiziksel düzeni değiştirmez.
        assert!(out[0].bars[0].t < out[0].bars[2].t, "eskiden yeniye olmali");
        assert!((out[0].bars[0].o - 1.10).abs() < 1e-12);
        assert_eq!(c.pending_len(), 0, "biten istek temizlenmeli");
    }

    #[test]
    fn missing_delivery_is_reported_not_hidden() {
        // Bar halkası dolarsa yazar yeniden DENEMEZ; barlar kalıcı kaybolur.
        // `total` tutmadığında bunu söylemezsek istemci delikli seriyi tam
        // sanardı.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 5, 0).unwrap();
        push(&link, bar(id, 0, 5, 1.10));
        push(&link, bar(id, 1, 5, 1.11));
        // 2 ve 3 kayboldu; son bar geldi.
        let mut lastb = bar(id, 4, 5, 1.14);
        lastb.flags |= bar_flag::LAST;
        push(&link, lastb);

        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, HistStatus::Incomplete { expected: 5, got: 3 });
        assert!(out[0].status.detail().unwrap().contains("3/5"));
        assert_eq!(out[0].status.name(), "incomplete");
    }

    #[test]
    fn error_record_is_distinguishable_from_empty_history() {
        // "gecmis yok" ile "gecmis alinamadi" ayni sey degil.
        let (link, mut c) = setup();

        let id = c.request("EURUSD", 7, "M1", 10, 0).unwrap();
        push(
            &link,
            BarRec {
                req_id: id,
                symbol_id: 7,
                timeframe: timeframe::M1,
                flags: bar_flag::ERROR | bar_flag::LAST,
                spread: 4401,
                ..Default::default()
            },
        );
        let out = c.drain();
        assert_eq!(out[0].status, HistStatus::Failed(4401));
        assert!(out[0].bars.is_empty());

        // Boş ama BAŞARILI cevap: total 0, hata yok.
        let id2 = c.request("EURUSD", 7, "M1", 10, 0).unwrap();
        push(
            &link,
            BarRec {
                req_id: id2,
                symbol_id: 7,
                timeframe: timeframe::M1,
                flags: bar_flag::LAST,
                total: 0,
                ..Default::default()
            },
        );
        let out = c.drain();
        assert_eq!(out[0].status, HistStatus::Complete, "gecmis yok = basarili bos cevap");
        assert!(out[0].bars.is_empty());
    }

    #[test]
    fn a_ring_full_report_is_not_dressed_up_as_a_terminal_error() {
        // Service halka dolunca 9001 yazar. Bunu "MQL5 hata 9001" diye
        // sunmak operatörü terminalde sebep aramaya gönderirdi; oysa sorun
        // bizim tarafımızda (barları yeterince hızlı çekmiyoruz).
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 5, 0).unwrap();
        push(&link, bar(id, 0, 5, 1.10));
        push(
            &link,
            BarRec {
                req_id: id,
                symbol_id: 7,
                timeframe: timeframe::M1,
                flags: bar_flag::ERROR | bar_flag::LAST,
                spread: ERR_RING_FULL,
                ..Default::default()
            },
        );

        let out = c.drain();
        assert_eq!(out[0].status, HistStatus::Failed(ERR_RING_FULL));
        let detail = out[0].status.detail().unwrap();
        assert!(detail.contains("halkasi doldu"), "sebep acikca soylenmeli: {detail}");
        assert!(!detail.contains("MQL5 hata"), "terminal hatasi gibi gosterilmemeli");
        // Kesilmeden önce gelen barlar ATILMAZ.
        assert_eq!(out[0].bars.len(), 1);
    }

    #[test]
    fn request_times_out_instead_of_waiting_forever() {
        // `CopyRates` canlıda 30-60 sn bloklayabiliyor; service donarsa
        // istemciyi asmak yerine cevap veriyoruz.
        let link = Arc::new(FakeLink::default());
        let mut c = HistClient::new(Box::new(link.clone()), Duration::from_millis(1));
        let id = c.request("EURUSD", 7, "M1", 3, 0).unwrap();
        push(&link, bar(id, 0, 3, 1.10));
        assert!(c.drain().is_empty(), "henuz bitmedi");
        assert_eq!(c.pending_len(), 1);

        std::thread::sleep(Duration::from_millis(5));
        let out = c.expire();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, HistStatus::TimedOut { got: 1 });
        assert_eq!(c.pending_len(), 0);
        // Kısmi barlar atılmıyor: elde ne varsa istemci görsün.
        assert_eq!(out[0].bars.len(), 1);
    }

    #[test]
    fn late_bars_of_an_expired_request_are_counted_not_applied() {
        let link = Arc::new(FakeLink::default());
        let mut c = HistClient::new(Box::new(link.clone()), Duration::from_millis(1));
        let id = c.request("EURUSD", 7, "M1", 3, 0).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        c.expire();

        push(&link, bar(id, 0, 3, 1.10));
        assert!(c.drain().is_empty());
        assert_eq!(c.orphan_bars, 1, "gec gelen bar sayilmali");
    }

    #[test]
    fn incoherent_bars_are_rejected_and_surface_as_incomplete() {
        // Bozuk bar sessizce grafiğe girerse fiyat ekseni patlar.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 2, 0).unwrap();
        let mut bad = bar(id, 0, 2, 1.10);
        bad.high = 1.0;
        bad.low = 2.0; // high < low
        push(&link, bad);
        push(&link, bar(id, 1, 2, 1.11));

        let out = c.drain();
        assert_eq!(c.incoherent_bars, 1);
        assert_eq!(out[0].bars.len(), 1);
        assert_eq!(out[0].status, HistStatus::Incomplete { expected: 2, got: 1 });
    }

    #[test]
    fn bars_of_the_wrong_series_are_rejected() {
        // Yanlış seriyi doğru isteğe yazmak, grafikte fark edilmeyen bir yalan.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 2, 0).unwrap();
        let mut wrong = bar(id, 0, 2, 1.10);
        wrong.symbol_id = 9;
        push(&link, wrong);
        let mut wrong_tf = bar(id, 1, 2, 1.11);
        wrong_tf.timeframe = timeframe::H1;
        wrong_tf.flags |= bar_flag::LAST;
        push(&link, wrong_tf);

        let out = c.drain();
        assert_eq!(c.mismatched_bars, 2);
        assert!(out[0].bars.is_empty());
        assert!(matches!(out[0].status, HistStatus::Incomplete { .. }));
    }

    #[test]
    fn two_requests_interleave_without_mixing() {
        let (link, mut c) = setup();
        let a = c.request("EURUSD", 7, "M1", 2, 0).unwrap();
        let b = c.request("EURUSD", 7, "M1", 2, 0).unwrap();
        assert_ne!(a, b);

        push(&link, bar(a, 0, 2, 1.10));
        push(&link, bar(b, 0, 2, 2.10));
        push(&link, bar(a, 1, 2, 1.11));
        push(&link, bar(b, 1, 2, 2.11));

        let out = c.drain();
        assert_eq!(out.len(), 2);
        for o in &out {
            assert_eq!(o.status, HistStatus::Complete);
            assert_eq!(o.bars.len(), 2);
            let base = if o.req_id == a { 1.10 } else { 2.10 };
            assert!((o.bars[0].o - base).abs() < 1e-12, "istekler karismamali");
        }
    }

    #[test]
    fn partial_flag_and_volume_semantics_survive_the_crossing() {
        let (link, mut c) = setup();
        let id = c.request("XAUUSD", 7, "M1", 2, 0).unwrap();

        // Gerçek hacim yayınlayan bir bar.
        let mut with_vol = bar(id, 0, 2, 3300.0);
        with_vol.real_volume = 0;
        with_vol.flags |= bar_flag::HAS_REAL_VOLUME;
        push(&link, with_vol);

        // Son bar OLUŞMAKTA — `CopyRates`'in son barı her zaman budur.
        let mut forming = bar(id, 1, 2, 3301.0);
        forming.flags |= bar_flag::PARTIAL | bar_flag::LAST;
        forming.spread = 17;
        push(&link, forming);

        let out = c.drain();
        let bars = &out[0].bars;
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].real_volume, Some(0), "bayrak varsa 0 gercek bir olcum");
        assert!(!bars[0].partial);
        assert!(bars[1].partial, "olusmakta olan bar isaretlenmeli");
        assert_eq!(bars[1].real_volume, None, "bayraksiz 0 'veri yok' demek");
        assert_eq!(bars[1].spread, Some(17));
        assert_eq!(bars[0].ticks, 42, "tick_volume tick sayisi olarak tasinir");
    }

    #[test]
    fn stream_ends_on_count_even_if_the_last_flag_is_lost() {
        // Emniyet ağı: LAST kaybolursa istek gereksiz yere zaman aşımına
        // uğramamalı.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 2, 0).unwrap();
        let mut b0 = bar(id, 0, 2, 1.10);
        b0.flags = 0;
        let mut b1 = bar(id, 1, 2, 1.11);
        b1.flags = 0; // LAST yok
        push(&link, b0);
        push(&link, b1);

        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, HistStatus::Complete);
    }

    #[test]
    fn req_ids_never_reach_zero_even_after_wrapping() {
        let (_link, mut c) = setup();
        c.next_id = u32::MAX;
        let a = c.request("EURUSD", 7, "M1", 1, 0).unwrap();
        let b = c.request("EURUSD", 7, "M1", 1, 0).unwrap();
        assert_eq!(a, u32::MAX);
        assert_ne!(b, 0, "0 protokolde gecersiz");
        assert_eq!(b, 1);
    }

    #[test]
    fn drain_is_bounded_so_history_cannot_starve_the_tick_loop() {
        // Fiyat gecikmesi geçmişten önemli: tek turda sınırsız bar çekmek
        // okuyucu döngüsünü tick akışından koparırdı.
        let (link, mut c) = setup();
        let id = c.request("EURUSD", 7, "M1", 5000, 0).unwrap();
        for i in 0..(DRAIN_BUDGET + 100) as u32 {
            push(&link, bar(id, i, 5000, 1.10));
        }
        assert!(c.drain().is_empty(), "istek bitmedi");
        assert_eq!(
            link.bars.lock().unwrap().len(),
            100,
            "tur basina en fazla DRAIN_BUDGET bar cekilmeli"
        );
    }
}
