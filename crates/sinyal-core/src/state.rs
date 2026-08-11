//! Paylaşılan durum: sembol kaydı ve sembol başına son fiyat.
//!
//! Halkalar akışı taşır; burası "şu an ne biliyoruz"u tutar. Yeni bağlanan bir
//! istemci akışın ortasına düşer — anlık görüntü olmadan ilk tick gelene kadar
//! (hareketsiz bir sembolde dakikalarca) kör kalırdı.

use std::collections::HashMap;
use std::sync::RwLock;

use sinyal_proto::{sym_flag, SymbolEntry};

/// Bir sembolün son bilinen fiyatı.
#[derive(Debug, Clone, Copy, Default)]
pub struct LastTick {
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    /// Broker sunucu saati, epoch ms.
    pub time_msc: i64,
}

/// Tek bir örneğin (broker/terminal) durumu.
#[derive(Debug, Default)]
pub struct InstanceState {
    /// wire `symbol_id` → sembol kaydı.
    pub symbols: Vec<SymbolEntry>,
    /// Broker sembol adı → `symbol_id`.
    pub by_name: HashMap<String, u32>,
    /// `symbol_id` → son fiyat.
    pub last: HashMap<u32, LastTick>,
}

impl InstanceState {
    /// Sembol tablosunu değiştir ve ad indeksini yeniden kur.
    ///
    /// EA sembol listesini değiştirdiğinde çağrılır. Son fiyatlar KORUNUR:
    /// aynı sembol yeni tabloda da varsa fiyatı sıfırlamak, istemcilere
    /// gereksiz bir "veri yok" penceresi yaşatırdı.
    pub fn set_symbols(&mut self, entries: Vec<SymbolEntry>) {
        self.by_name.clear();
        for e in &entries {
            self.by_name.insert(e.name_str().to_owned(), e.symbol_id);
        }
        self.symbols = entries;
    }

    pub fn symbol(&self, id: u32) -> Option<&SymbolEntry> {
        self.symbols.iter().find(|e| e.symbol_id == id)
    }

    pub fn id_of(&self, name: &str) -> Option<u32> {
        self.by_name.get(name).copied()
    }

    pub fn name_of(&self, id: u32) -> Option<&str> {
        self.symbol(id).map(|e| e.name_str())
    }
}

/// Tüm örneklerin durumu.
///
/// `RwLock`: okuma çok, yazma nadir (sembol tablosu değişimi). Son fiyat
/// güncellemesi de yazma ama tick başına bir kez ve çok kısa — okuyucu
/// tarafında tıkanma yaratmaz.
#[derive(Debug, Default)]
pub struct Registry {
    inner: RwLock<HashMap<String, InstanceState>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_symbols(&self, instance: &str, entries: Vec<SymbolEntry>) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        g.entry(instance.to_owned()).or_default().set_symbols(entries);
    }

    pub fn update_last(&self, instance: &str, id: u32, t: LastTick) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(st) = g.get_mut(instance) {
            st.last.insert(id, t);
        }
    }

    /// `symbol_id` → broker sembol adı.
    pub fn name_of(&self, instance: &str, id: u32) -> Option<String> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        g.get(instance)?.name_of(id).map(|s| s.to_owned())
    }

    /// Sembol adını bir örnekte çöz.
    pub fn resolve(&self, instance: &str, name: &str) -> Option<u32> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        g.get(instance)?.id_of(name)
    }

    /// Sembol adını TÜM örneklerde ara; ilk eşleşmeyi döndür.
    ///
    /// İstemci `EURUSD` derken hangi broker'ı kastettiğini belirtmek zorunda
    /// kalmasın diye. Birden fazla örnekte varsa ilk bulunan kullanılır —
    /// belirsizlik durumunda istemci `instance` alanını açıkça vermelidir.
    pub fn resolve_any(&self, name: &str) -> Option<(String, u32)> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        for (inst, st) in g.iter() {
            if let Some(id) = st.id_of(name) {
                return Some((inst.clone(), id));
            }
        }
        None
    }

    pub fn symbol(&self, instance: &str, id: u32) -> Option<SymbolEntry> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        g.get(instance)?.symbol(id).copied()
    }

    pub fn instances(&self) -> Vec<String> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut v: Vec<String> = g.keys().cloned().collect();
        v.sort();
        v
    }

    /// Tüm örneklerin sembollerini (örnek adıyla birlikte) döndür.
    pub fn all_symbols(&self) -> Vec<(String, SymbolEntry)> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        for (inst, st) in g.iter() {
            for e in &st.symbols {
                out.push((inst.clone(), *e));
            }
        }
        out.sort_by(|a, b| (a.0.as_str(), a.1.name_str()).cmp(&(b.0.as_str(), b.1.name_str())));
        out
    }

    /// Son fiyat anlık görüntüsü. `filter` boşsa tüm semboller.
    pub fn snapshot(&self, filter: &[String]) -> Vec<(String, String, LastTick)> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        for (inst, st) in g.iter() {
            for e in &st.symbols {
                let name = e.name_str();
                if !filter.is_empty() && !filter.iter().any(|f| f == name) {
                    continue;
                }
                // READY olmayan sembolün fiyatına güvenilmez; hiç göndermiyoruz.
                if e.flags & sym_flag::READY == 0 {
                    continue;
                }
                if let Some(t) = st.last.get(&e.symbol_id) {
                    out.push((inst.clone(), name.to_owned(), *t));
                }
            }
        }
        out.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinyal_proto::write_fixed_str;

    fn entry(id: u32, name: &str, flags: u32) -> SymbolEntry {
        let mut e = SymbolEntry { symbol_id: id, flags, digits: 5, ..Default::default() };
        write_fixed_str(&mut e.name, name);
        e
    }

    #[test]
    fn resolves_names_to_ids_and_back() {
        let r = Registry::new();
        r.set_symbols(
            "mt5-1",
            vec![entry(0, "EURUSD", sym_flag::READY), entry(1, "GOLD#", sym_flag::READY)],
        );
        assert_eq!(r.resolve("mt5-1", "EURUSD"), Some(0));
        assert_eq!(r.resolve("mt5-1", "GOLD#"), Some(1));
        assert_eq!(r.resolve("mt5-1", "YOKSUN"), None);
        assert_eq!(r.name_of("mt5-1", 1).as_deref(), Some("GOLD#"));
    }

    #[test]
    fn resolve_any_searches_every_instance() {
        // İstemci hangi broker olduğunu bilmek zorunda kalmasın.
        let r = Registry::new();
        r.set_symbols("brokerA", vec![entry(0, "EURUSD", sym_flag::READY)]);
        r.set_symbols("brokerB", vec![entry(0, "XAUUSD", sym_flag::READY)]);
        assert_eq!(r.resolve_any("XAUUSD"), Some(("brokerB".into(), 0)));
        assert!(r.resolve_any("YOKSUN").is_none());
    }

    #[test]
    fn snapshot_omits_symbols_that_never_went_ready() {
        // READY olmayan sembolün fiyatı bayat/sıfırdır; göndermek istemciyi
        // yanlış fiyatla işlem yapmaya yönlendirirdi.
        let r = Registry::new();
        r.set_symbols(
            "mt5-1",
            vec![entry(0, "EURUSD", sym_flag::READY), entry(1, "OLU", 0)],
        );
        r.update_last("mt5-1", 0, LastTick { bid: 1.1, ask: 1.2, last: 0.0, time_msc: 100 });
        r.update_last("mt5-1", 1, LastTick { bid: 9.9, ask: 9.9, last: 0.0, time_msc: 100 });

        let snap = r.snapshot(&[]);
        assert_eq!(snap.len(), 1, "yalnız READY sembol dönmeli");
        assert_eq!(snap[0].1, "EURUSD");
    }

    #[test]
    fn snapshot_filters_by_name() {
        let r = Registry::new();
        r.set_symbols(
            "mt5-1",
            vec![entry(0, "EURUSD", sym_flag::READY), entry(1, "GBPUSD", sym_flag::READY)],
        );
        r.update_last("mt5-1", 0, LastTick { bid: 1.1, ask: 1.2, last: 0.0, time_msc: 1 });
        r.update_last("mt5-1", 1, LastTick { bid: 1.3, ask: 1.4, last: 0.0, time_msc: 1 });

        let snap = r.snapshot(&["GBPUSD".to_string()]);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].1, "GBPUSD");
    }

    #[test]
    fn resetting_symbol_table_preserves_known_prices() {
        // EA sembol listesini değiştirdiğinde fiyatları sıfırlamak,
        // istemcilere gereksiz bir "veri yok" penceresi yaşatırdı.
        let r = Registry::new();
        r.set_symbols("mt5-1", vec![entry(0, "EURUSD", sym_flag::READY)]);
        r.update_last("mt5-1", 0, LastTick { bid: 1.1, ask: 1.2, last: 0.0, time_msc: 5 });

        r.set_symbols(
            "mt5-1",
            vec![entry(0, "EURUSD", sym_flag::READY), entry(1, "GBPUSD", sym_flag::READY)],
        );
        let snap = r.snapshot(&[]);
        assert_eq!(snap.len(), 1, "EURUSD fiyatı korunmalı, GBPUSD henüz fiyatsız");
        assert!((snap[0].2.bid - 1.1).abs() < 1e-12);
    }

    #[test]
    fn instances_are_listed_sorted() {
        let r = Registry::new();
        r.set_symbols("zeta", vec![entry(0, "A", sym_flag::READY)]);
        r.set_symbols("alpha", vec![entry(0, "B", sym_flag::READY)]);
        assert_eq!(r.instances(), vec!["alpha".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn unknown_instance_queries_do_not_panic() {
        let r = Registry::new();
        assert_eq!(r.resolve("yok", "EURUSD"), None);
        assert_eq!(r.name_of("yok", 0), None);
        assert!(r.symbol("yok", 0).is_none());
        assert!(r.snapshot(&[]).is_empty());
    }
}
