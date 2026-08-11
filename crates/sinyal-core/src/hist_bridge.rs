//! Geçmiş kanalının paylaşımlı bellek bağlantısı — **tek entegrasyon noktası**.
//!
//! [`crate::history`] taşıyıcıyı [`HistLink`] izi üzerinden kullanır; bu dosya
//! izin somut karşılığını `sinyal-bridge`'in `HistSession`'ına bağlar. Geçmiş
//! kanalının çekirdek tarafındaki TEK `sinyal-bridge` bağımlılığı burasıdır:
//! köprü tarafındaki imza değişirse yalnızca bu dosya güncellenir, eşleştirme
//! ve zaman aşımı mantığına dokunulmaz.
//!
//! # Neden `mt5-hist` özelliği arkasında
//!
//! Özellik **varsayılan olarak açıktır**; kapatmak bir kaçış kapısıdır. Köprü
//! tarafındaki imza değişirse veya geçmiş kanalı olmadan bir ikili gerekirse,
//! `--no-default-features` ile çekirdek **tam işlevsel** derlenir: [`attach`]
//! `None` döner, mumlar tick'ten üretilmeye devam eder ve `candles` cevabı
//! `hist: "off"` der. Bu bir hata değil, kapalı bir özelliktir.
//!
//! # Thread sahipliği
//!
//! [`attach`] **okuyucu thread'inden** çağrılmalıdır. Oturum, kendisine erişen
//! ilk thread'i sahip yapar ve başka thread'den gelen işlemi sessizce reddeder
//! (`false`/`None` döner) — main'de bağlanıp okuyucuya taşımak, hiç bar
//! gelmeyen ama hata da vermeyen bir kurulum üretirdi.

use crate::history::HistLink;

#[cfg(feature = "mt5-hist")]
mod imp {
    use super::HistLink;
    use sinyal_bridge::HistSession;
    use sinyal_proto::{BarRec, HistReq};

    /// `HistSession`'ı [`HistLink`] izine bağlayan sarmalayıcı.
    struct BridgeLink(HistSession);

    impl HistLink for BridgeLink {
        fn push_req(&self, req: &HistReq) -> bool {
            // Güvenlik: bu tip yalnızca `attach`'ı çağıran thread'e teslim
            // edilir ve okuyucu döngüsünden çıkmaz — SPSC sözleşmesi korunur.
            unsafe { self.0.push_req(req) }
        }
        fn pop_bar(&self) -> Option<BarRec> {
            // Güvenlik: yukarıdaki ile aynı.
            unsafe { self.0.pop_bar() }
        }
    }

    pub fn attach(instance: &str) -> Option<Box<dyn HistLink>> {
        match HistSession::attach(instance) {
            Ok(s) => Some(Box::new(BridgeLink(s))),
            Err(e) => {
                // Service henüz başlamamış olabilir; çağıran periyodik olarak
                // yeniden dener.
                eprintln!("[{instance}] gecmis kanali yok ({e}) — MT5 barlari kapali");
                None
            }
        }
    }
}

#[cfg(not(feature = "mt5-hist"))]
mod imp {
    use super::HistLink;

    pub fn attach(_instance: &str) -> Option<Box<dyn HistLink>> {
        None
    }
}

/// Bir örneğin geçmiş kanalına bağlan.
///
/// `None` = service çalışmıyor **veya** `mt5-hist` özelliği kapalı. İki durum
/// da hata değildir: geçmiş özelliği kapalı olur, tick'ten üretilen mumlar
/// çalışmaya devam eder.
pub fn attach(instance: &str) -> Option<Box<dyn HistLink>> {
    imp::attach(instance)
}

/// Geçmiş kanalı bu ikiliye derlenmiş mi.
///
/// Telemetride "service yok" ile "özellik kapalı" ayrımını yapabilmek için.
pub const COMPILED_IN: bool = cfg!(feature = "mt5-hist");
