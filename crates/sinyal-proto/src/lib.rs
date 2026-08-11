//! # sinyal-proto
//!
//! MQL5 (EA, `SinyalBridge.dll` üzerinden) ile Rust (`sinyal-core`) arasında
//! paylaşılan bellek yerleşiminin **tek doğruluk kaynağı**.
//!
//! İki taraf ayrı derleyicilerle derlendiği için yapı yerleşimlerinin
//! eşleştiğini hiçbir bağlayıcı doğrulamaz — bir alan eklemek ve MQL5 tarafını
//! güncellemeyi unutmak, çalışma zamanında sessizce bozuk fiyat okumaya yol
//! açar. Buna karşı üç katmanlı savunma var:
//!
//! 1. Bu dosyadaki [`layout`] modülü her yapının boyutunu ve her alanın
//!    ofsetini **derleme zamanında** sabitler. Yanlışlıkla bir alan eklemek
//!    veya sırasını değiştirmek derlemeyi kırar.
//! 2. `gen-mqh` ikilisi MQL5 başlığını bu tanımlardan **üretir**; elle
//!    senkron tutma yoktur.
//! 3. Çalışma zamanında [`ring::Ring::attach`] slot boyutunu karşılaştırır;
//!    iki taraf farklı tanımla derlenmişse bağlanma hata verir
//!    ([`ring::RingError::SlotSizeMismatch`]) — bozuk veri okunmaz.
//!
//! ## Segmentler
//!
//! Her EA örneği (= bir MT5 terminali = bir broker hesabı) dört paylaşılan
//! bellek segmenti kullanır:
//!
//! | Segment | Yön | İçerik |
//! |---|---|---|
//! | `md`  | EA → çekirdek | tick akışı ([`msg::Tick`]) |
//! | `book`| EA → çekirdek | derinlik akışı ([`msg::Book`]) |
//! | `cmd` | çekirdek → EA | emir komutları ([`msg::Cmd`]) |
//! | `res` | EA → çekirdek | emir sonuçları / ticaret olayları ([`msg::Res`]) |
//! | `sym` | EA → çekirdek | sembol tablosu ([`msg::SymbolEntry`]) |
//!
//! Tick ve derinlik ayrı halkalarda tutulur: derinlik mesajı tick'ten ~13 kat
//! büyük, tek halkada birleştirmek tick slotlarını da o boyuta şişirirdi.
//!
//! ### Geçmiş kanalı — AYRI bir yazar
//!
//! Geçmiş barlar ([`bars`]) yukarıdaki segmentlerden bağımsız bir çiftte akar:
//!
//! | Segment | Yön | İçerik |
//! |---|---|---|
//! | `hist` | çekirdek → **service** | geçmiş istekleri ([`bars::HistReq`]) |
//! | `bars` | **service** → çekirdek | MT5'in kendi barları ([`bars::BarRec`]) |
//!
//! Yazan taraf EA değil, ayrı bir **MQL5 Service**'tir: `CopyRates` EA'da
//! çağıran thread'i bloklar ve zaman aşımı belgelenmemiştir (canlıda 30-60 sn
//! donma raporları var), bu yüzden tick toplama yoluna konamaz.
//!
//! Ayrı segment çifti aynı zamanda **tek-yazar kilidini korur**: EA `md/book/
//! res/sym/state` üzerinde tek yazar olarak kalır, service yalnızca `bars`a
//! yazar. İkisi birbirinin kilidine dokunmaz ve service olmadan da EA tam
//! işlevsel çalışır.

pub mod bars;
pub mod msg;
pub mod ring;
pub mod state;
pub mod symtab;
pub mod validate;

pub use bars::{bar_flag, timeframe, BarRec, HistReq, HIST_SYMBOL_LEN};
pub use msg::{
    action, book_type, exec_mode, filling, filling_mask, kind, order_type, read_fixed_str, res_kind,
    res_source, sym_flag, tick_flag, type_time, write_fixed_str, Book, BookLevel, Cmd, Res,
    SymbolEntry, Tick, COMMENT_LEN, MAX_BOOK_DEPTH, SYMBOL_NAME_LEN,
};
pub use ring::{Cell, Ring, RingError, RingHeader};
pub use state::{
    margin_mode, so_mode, trade_mode, AccountSnapshot, OrderRec, PositionRec, StateError,
    StateHeader, StateSnapshot, StateTable, MAX_ORDERS, MAX_POSITIONS,
};
pub use symtab::{SymTabError, SymbolTable, SymbolTableHeader};
pub use validate::{
    normalize_price, normalize_volume, resolve_filling, respects_stops_level, ValidationError,
};

/// Halka ve sembol tablosu segmentleri için önerilen kapasiteler.
///
/// Tick halkası bilinçli olarak cömert: 1M slot × 64 bayt ≈ 64 MB. 100k
/// tick/sn gibi uç bir yükte bile ~10 saniyelik tampon demek. Çekirdek
/// okuyucusu sıkı bir döngüde çalıştığından bu tamponun dolması yalnızca
/// okuyucu tamamen durduğunda mümkündür — ki o durumda `dropped` sayacı
/// alarm üretir ve zaten sistem bozuktur.
pub mod capacity {
    /// Tick halkası slot sayısı (≈64 MB).
    pub const TICKS: u64 = 1 << 20;
    /// Derinlik halkası slot sayısı (≈53 MB — mesaj 13 kat büyük, o yüzden
    /// slot sayısı düşük tutuldu; DOM güncellemeleri tick'ten çok daha seyrek).
    pub const BOOKS: u64 = 1 << 16;
    /// Komut halkası slot sayısı (≈1 MB). Emir hacmi tick hacminin yanında
    /// ihmal edilebilir.
    pub const CMDS: u64 = 1 << 13;
    /// Sonuç halkası slot sayısı (≈1 MB).
    pub const RESULTS: u64 = 1 << 13;
    /// Sembol tablosu kapasitesi.
    pub const SYMBOLS: u32 = 1024;
    /// Durum segmentindeki azami pozisyon sayısı.
    pub const POSITIONS: u32 = 512;
    /// Durum segmentindeki azami bekleyen emir sayısı.
    pub const ORDERS: u32 = 512;
    /// Geçmiş istek halkası slot sayısı. İstekler seyrek ve küçük.
    pub const HIST_REQS: u64 = 1 << 10;
    /// Geçmiş bar halkası slot sayısı (≈4 MB).
    ///
    /// Geçmiş akışı tanım gereği **patlamalıdır**: tek istek binlerce bar
    /// üretir. Halka dolarsa yazar yeniden DENEMEZ ve barlar kalıcı olarak
    /// kaybolur; bu yüzden tek istekte dönebilecek azami bar sayısının
    /// (5000) birkaç katı seçildi ki çekirdek kısa bir gecikmeyle bile
    /// yetişebilsin. Eksik teslimat ayrıca [`bars::BarRec::total`] ile
    /// yakalanır.
    pub const BARS: u64 = 1 << 15;
}

/// Bir EA örneğinin paylaşılan bellek segment adları.
///
/// `Local\` öneki segmenti oturuma hapseder — EA ve çekirdek aynı oturumda
/// çalıştığı için doğru olan bu; `Global\` yönetici hakkı ister ve gereksiz
/// yere makine geneline açar.
///
/// `instance`, broker/terminal başına benzersiz olmalıdır ("tüm pair'lerde
/// istediğimiz server" hedefi için VPS'te broker başına ayrı terminal +
/// ayrı instance çalışır).
pub struct SegmentNames {
    pub ticks: String,
    pub books: String,
    pub cmds: String,
    pub results: String,
    pub symbols: String,
    /// Hesap + açık pozisyonlar + bekleyen emirler (durum, akış değil).
    pub state: String,
    /// Çekirdek → service geçmiş istekleri.
    pub hist_reqs: String,
    /// Service → çekirdek geçmiş barları.
    pub bars: String,
}

impl SegmentNames {
    /// Verilen örnek kimliği için segment adlarını üret.
    pub fn new(instance: &str) -> Self {
        Self {
            ticks: format!("Local\\Sinyal.{instance}.md"),
            books: format!("Local\\Sinyal.{instance}.book"),
            cmds: format!("Local\\Sinyal.{instance}.cmd"),
            results: format!("Local\\Sinyal.{instance}.res"),
            symbols: format!("Local\\Sinyal.{instance}.sym"),
            state: format!("Local\\Sinyal.{instance}.state"),
            hist_reqs: format!("Local\\Sinyal.{instance}.hreq"),
            bars: format!("Local\\Sinyal.{instance}.bars"),
        }
    }
}

/// Derleme-zamanı yerleşim iddiaları.
///
/// Bu modül hiçbir kod üretmez; yalnızca `const` bağlamında değerlendirilen
/// iddialar içerir. Bir alanın boyutu, sırası veya dolgusu değişirse **derleme
/// kırılır** — hata mesajı hangi yapının bozulduğunu söyler.
///
/// Bir alanı bilinçli olarak değiştirdiysen: buradaki sabitleri güncelle,
/// [`ring::RING_VERSION`] ve [`symtab::SYMTAB_VERSION`]'ı artır, ardından
/// `cargo run -p sinyal-proto --bin gen-mqh` ile MQL5 başlığını yenile.
#[allow(clippy::assertions_on_constants)]
mod layout {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    // --- Tick: tam bir önbellek satırına oturmalı (Cell<Tick> = 64) ---
    const _: () = assert!(size_of::<Tick>() == 56, "Tick 56 bayt olmalı");
    const _: () = assert!(align_of::<Tick>() == 8);
    const _: () = assert!(size_of::<Cell<Tick>>() == 64, "Cell<Tick> tek önbellek satırı olmalı");
    const _: () = assert!(offset_of!(Tick, time_msc) == 0);
    const _: () = assert!(offset_of!(Tick, bid) == 8);
    const _: () = assert!(offset_of!(Tick, ask) == 16);
    const _: () = assert!(offset_of!(Tick, last) == 24);
    const _: () = assert!(offset_of!(Tick, volume_real) == 32);
    const _: () = assert!(offset_of!(Tick, recv_qpc) == 40);
    const _: () = assert!(offset_of!(Tick, symbol_id) == 48);
    const _: () = assert!(offset_of!(Tick, flags) == 52);
    const _: () = assert!(offset_of!(Tick, kind) == 54);

    // --- BookLevel / Book ---
    const _: () = assert!(size_of::<BookLevel>() == 24, "BookLevel 24 bayt olmalı");
    const _: () = assert!(offset_of!(BookLevel, price) == 0);
    const _: () = assert!(offset_of!(BookLevel, volume_real) == 8);
    const _: () = assert!(offset_of!(BookLevel, kind) == 16);

    const _: () = assert!(size_of::<Book>() == 824, "Book 824 bayt olmalı");
    const _: () = assert!(align_of::<Book>() == 8);
    const _: () = assert!(size_of::<Cell<Book>>() == 832, "Cell<Book> 13 önbellek satırı olmalı");
    const _: () = assert!(offset_of!(Book, time_msc) == 0);
    const _: () = assert!(offset_of!(Book, recv_qpc) == 8);
    const _: () = assert!(offset_of!(Book, symbol_id) == 16);
    const _: () = assert!(offset_of!(Book, depth) == 20);
    const _: () = assert!(offset_of!(Book, kind) == 22);
    const _: () = assert!(offset_of!(Book, truncated) == 23);
    const _: () = assert!(offset_of!(Book, levels) == 24);

    // --- Cmd ---
    const _: () = assert!(size_of::<Cmd>() == 184, "Cmd 184 bayt olmalı");
    const _: () = assert!(size_of::<Cell<Cmd>>() == 192, "Cell<Cmd> 3 önbellek satırı olmalı");
    const _: () = assert!(offset_of!(Cmd, client_id) == 0);
    const _: () = assert!(offset_of!(Cmd, volume) == 8);
    const _: () = assert!(offset_of!(Cmd, price) == 16);
    const _: () = assert!(offset_of!(Cmd, stoplimit) == 24);
    const _: () = assert!(offset_of!(Cmd, sl) == 32);
    const _: () = assert!(offset_of!(Cmd, tp) == 40);
    const _: () = assert!(offset_of!(Cmd, ticket) == 48);
    const _: () = assert!(offset_of!(Cmd, ticket_by) == 56);
    const _: () = assert!(offset_of!(Cmd, expiration) == 64);
    // magic 64 bit: client_id doğrudan buraya sığmalı ki idempotency
    // broker'ın ezebildiği `comment`'e bağlı kalmasın.
    const _: () = assert!(offset_of!(Cmd, magic) == 72);
    const _: () = assert!(size_of::<u64>() == 8);
    const _: () = assert!(offset_of!(Cmd, symbol_id) == 80);
    const _: () = assert!(offset_of!(Cmd, deviation) == 84);
    const _: () = assert!(offset_of!(Cmd, comment) == 88);
    const _: () = assert!(offset_of!(Cmd, action) == 120);
    const _: () = assert!(offset_of!(Cmd, order_type) == 121);
    const _: () = assert!(offset_of!(Cmd, filling) == 122);
    const _: () = assert!(offset_of!(Cmd, type_time) == 123);

    // --- Res ---
    const _: () = assert!(size_of::<Res>() == 184, "Res 184 bayt olmalı");
    const _: () = assert!(size_of::<Cell<Res>>() == 192, "Cell<Res> 3 önbellek satırı olmalı");
    const _: () = assert!(offset_of!(Res, client_id) == 0);
    const _: () = assert!(offset_of!(Res, order) == 8);
    const _: () = assert!(offset_of!(Res, deal) == 16);
    const _: () = assert!(offset_of!(Res, position) == 24);
    const _: () = assert!(offset_of!(Res, position_by) == 32);
    const _: () = assert!(offset_of!(Res, volume) == 40);
    const _: () = assert!(offset_of!(Res, price) == 48);
    const _: () = assert!(offset_of!(Res, bid) == 56);
    const _: () = assert!(offset_of!(Res, ask) == 64);
    const _: () = assert!(offset_of!(Res, recv_qpc) == 72);
    const _: () = assert!(offset_of!(Res, retcode) == 80);
    const _: () = assert!(offset_of!(Res, retcode_external) == 84);
    // SEND_ACK ile TRADE_TXN'i bağlayan tek resmî anahtar.
    const _: () = assert!(offset_of!(Res, request_id) == 88);
    const _: () = assert!(offset_of!(Res, comment) == 96);
    const _: () = assert!(offset_of!(Res, kind) == 128);
    const _: () = assert!(offset_of!(Res, txn_type) == 129);
    const _: () = assert!(offset_of!(Res, order_state) == 130);
    const _: () = assert!(offset_of!(Res, deal_type) == 131);
    const _: () = assert!(offset_of!(Res, order_type) == 132);
    const _: () = assert!(offset_of!(Res, source) == 133);

    // --- Durum segmenti ---
    const _: () = assert!(size_of::<AccountSnapshot>() == 512, "AccountSnapshot 512 bayt olmalı");
    const _: () = assert!(offset_of!(AccountSnapshot, time_msc) == 0);
    const _: () = assert!(offset_of!(AccountSnapshot, balance) == 8);
    const _: () = assert!(offset_of!(AccountSnapshot, equity) == 32);
    const _: () = assert!(offset_of!(AccountSnapshot, login) == 120);
    const _: () = assert!(offset_of!(AccountSnapshot, leverage) == 128);
    const _: () = assert!(offset_of!(AccountSnapshot, limit_orders) == 136);
    // Canlı para kilidinin dayanağı — yeri kaymamalı.
    const _: () = assert!(offset_of!(AccountSnapshot, trade_mode) == 144);
    const _: () = assert!(offset_of!(AccountSnapshot, margin_mode) == 145);
    const _: () = assert!(offset_of!(AccountSnapshot, currency) == 160);
    const _: () = assert!(offset_of!(AccountSnapshot, server) == 176);
    const _: () = assert!(offset_of!(AccountSnapshot, company) == 240);
    const _: () = assert!(offset_of!(AccountSnapshot, name) == 336);

    const _: () = assert!(size_of::<PositionRec>() == 192, "PositionRec 192 bayt olmalı");
    const _: () = assert!(offset_of!(PositionRec, ticket) == 0);
    const _: () = assert!(offset_of!(PositionRec, identifier) == 8);
    const _: () = assert!(offset_of!(PositionRec, magic) == 16);
    const _: () = assert!(offset_of!(PositionRec, volume) == 40);
    const _: () = assert!(offset_of!(PositionRec, symbol) == 96);
    const _: () = assert!(offset_of!(PositionRec, comment) == 128);
    const _: () = assert!(offset_of!(PositionRec, kind) == 160);

    const _: () = assert!(size_of::<OrderRec>() == 192, "OrderRec 192 bayt olmalı");
    const _: () = assert!(offset_of!(OrderRec, ticket) == 0);
    const _: () = assert!(offset_of!(OrderRec, magic) == 8);
    const _: () = assert!(offset_of!(OrderRec, position_id) == 16);
    const _: () = assert!(offset_of!(OrderRec, volume_initial) == 40);
    const _: () = assert!(offset_of!(OrderRec, volume_current) == 48);
    const _: () = assert!(offset_of!(OrderRec, symbol) == 96);
    const _: () = assert!(offset_of!(OrderRec, comment) == 128);
    const _: () = assert!(offset_of!(OrderRec, kind) == 160);

    const _: () = assert!(size_of::<StateHeader>() == 128);
    const _: () = assert!(align_of::<StateHeader>() == 64);

    // --- SymbolEntry ---
    const _: () = assert!(size_of::<SymbolEntry>() == 192, "SymbolEntry 192 bayt olmalı");
    const _: () = assert!(offset_of!(SymbolEntry, point) == 0);
    const _: () = assert!(offset_of!(SymbolEntry, tick_size) == 8);
    const _: () = assert!(offset_of!(SymbolEntry, tick_value) == 16);
    const _: () = assert!(offset_of!(SymbolEntry, contract_size) == 24);
    const _: () = assert!(offset_of!(SymbolEntry, volume_min) == 32);
    const _: () = assert!(offset_of!(SymbolEntry, volume_max) == 40);
    const _: () = assert!(offset_of!(SymbolEntry, volume_step) == 48);
    const _: () = assert!(offset_of!(SymbolEntry, volume_limit) == 56);
    const _: () = assert!(offset_of!(SymbolEntry, symbol_id) == 64);
    const _: () = assert!(offset_of!(SymbolEntry, digits) == 68);
    const _: () = assert!(offset_of!(SymbolEntry, filling_mode) == 72);
    const _: () = assert!(offset_of!(SymbolEntry, trade_mode) == 76);
    // Bu alan olmadan filling::AUTO doğru çözülemez.
    const _: () = assert!(offset_of!(SymbolEntry, trade_exemode) == 80);
    const _: () = assert!(offset_of!(SymbolEntry, expiration_mode) == 84);
    const _: () = assert!(offset_of!(SymbolEntry, order_mode) == 88);
    const _: () = assert!(offset_of!(SymbolEntry, stops_level) == 92);
    const _: () = assert!(offset_of!(SymbolEntry, freeze_level) == 96);
    const _: () = assert!(offset_of!(SymbolEntry, ticks_bookdepth) == 100);
    const _: () = assert!(offset_of!(SymbolEntry, flags) == 104);
    const _: () = assert!(offset_of!(SymbolEntry, name) == 112);

    // --- Başlıklar: üretici/tüketici sayaçları AYRI önbellek satırlarında ---
    const _: () = assert!(size_of::<RingHeader>() == 256);
    const _: () = assert!(align_of::<RingHeader>() == 64);
    const _: () = assert!(offset_of!(RingHeader, head) == 64, "head kendi satırında olmalı");
    const _: () = assert!(offset_of!(RingHeader, tail) == 128, "tail head'den ayrı satırda olmalı");
    // False sharing olmadığının asıl kanıtı: head ile tail arasında en az bir
    // tam önbellek satırı var.
    const _: () = assert!(offset_of!(RingHeader, tail) - offset_of!(RingHeader, head) >= 64);

    const _: () = assert!(size_of::<SymbolTableHeader>() == 64);
    const _: () = assert!(align_of::<SymbolTableHeader>() == 64);
    const _: () = assert!(offset_of!(SymbolTableHeader, generation) == 24);

    // --- Geçmiş kanalı ---
    const _: () = assert!(size_of::<HistReq>() == 64, "HistReq 64 bayt olmalı");
    const _: () = assert!(align_of::<HistReq>() == 8);
    const _: () = assert!(offset_of!(HistReq, to_msc) == 0);
    const _: () = assert!(offset_of!(HistReq, req_id) == 8);
    const _: () = assert!(offset_of!(HistReq, symbol_id) == 12);
    const _: () = assert!(offset_of!(HistReq, timeframe) == 16);
    const _: () = assert!(offset_of!(HistReq, count) == 20);
    // Sembol adı isteğin İÇİNDE taşınır: service grafiğe bağlı değildir
    // (`_Symbol` yok) ve `symbol_id`'yi ada çeviremez. Ofset MQL5 başlığına da
    // buradan gider — kayarsa service isteğin ortasından ad okumaya başlar.
    const _: () = assert!(offset_of!(HistReq, symbol) == 24);
    const _: () = assert!(HIST_SYMBOL_LEN == 32);
    const _: () = assert!(offset_of!(HistReq, _reserved) == 56);

    // `Cell<BarRec>` = 128 bayt. Bu, DİĞER HALKALARIN HİÇBİRİYLE AYNI DEĞİL
    // (Tick 64, Cmd/Res 192, Book 832) — kasıtlı. `RING_MAGIC` tüm halkalarda
    // aynı olduğundan, yanlış segment adına bağlanmayı yakalayan tek
    // çalışma-zamanı denetimi slot boyutu karşılaştırmasıdır.
    const _: () = assert!(size_of::<BarRec>() == 120, "BarRec 120 bayt olmalı");
    const _: () = assert!(align_of::<BarRec>() == 8);
    const _: () = assert!(size_of::<Cell<BarRec>>() == 128, "Cell<BarRec> 2 önbellek satırı olmalı");
    const _: () = assert!(size_of::<Cell<BarRec>>() != size_of::<Cell<Tick>>());
    const _: () = assert!(size_of::<Cell<BarRec>>() != size_of::<Cell<Cmd>>());
    const _: () = assert!(size_of::<Cell<BarRec>>() != size_of::<Cell<Book>>());
    const _: () = assert!(offset_of!(BarRec, time_msc) == 0);
    const _: () = assert!(offset_of!(BarRec, open) == 8);
    const _: () = assert!(offset_of!(BarRec, high) == 16);
    const _: () = assert!(offset_of!(BarRec, low) == 24);
    const _: () = assert!(offset_of!(BarRec, close) == 32);
    const _: () = assert!(offset_of!(BarRec, tick_volume) == 40);
    const _: () = assert!(offset_of!(BarRec, real_volume) == 48);
    const _: () = assert!(offset_of!(BarRec, req_id) == 56);
    const _: () = assert!(offset_of!(BarRec, symbol_id) == 60);
    const _: () = assert!(offset_of!(BarRec, timeframe) == 64);
    const _: () = assert!(offset_of!(BarRec, spread) == 68);
    const _: () = assert!(offset_of!(BarRec, flags) == 72);
    const _: () = assert!(offset_of!(BarRec, index) == 76);
    const _: () = assert!(offset_of!(BarRec, total) == 80);
    const _: () = assert!(offset_of!(BarRec, _reserved) == 88);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_names_are_session_local_and_instance_scoped() {
        let n = SegmentNames::new("icmarkets-1");
        assert_eq!(n.ticks, "Local\\Sinyal.icmarkets-1.md");
        assert_eq!(n.books, "Local\\Sinyal.icmarkets-1.book");
        assert_eq!(n.cmds, "Local\\Sinyal.icmarkets-1.cmd");
        assert_eq!(n.results, "Local\\Sinyal.icmarkets-1.res");
        assert_eq!(n.symbols, "Local\\Sinyal.icmarkets-1.sym");

        // Farklı broker'lar çakışmamalı — çoklu terminal hedefinin temeli.
        let other = SegmentNames::new("pepperstone-1");
        assert_ne!(n.ticks, other.ticks);
    }

    #[test]
    fn fixed_str_roundtrip() {
        let mut buf = [0u8; SYMBOL_NAME_LEN];
        write_fixed_str(&mut buf, "XAUUSD.m");
        assert_eq!(read_fixed_str(&buf), "XAUUSD.m");
        assert_eq!(buf[8], 0, "kalan NUL ile dolmalı");
    }

    #[test]
    fn fixed_str_overwrites_previous_content_fully() {
        let mut buf = [0u8; SYMBOL_NAME_LEN];
        write_fixed_str(&mut buf, "EURUSD.pro");
        write_fixed_str(&mut buf, "GOLD");
        assert_eq!(read_fixed_str(&buf), "GOLD", "eski içerik tamamen silinmeli");
    }

    #[test]
    fn fixed_str_truncates_on_char_boundary() {
        // Yarım UTF-8 dizisi bırakmak okuyan tarafta çözümlemeyi bozardı.
        let mut buf = [0u8; 8];
        write_fixed_str(&mut buf, "ÖÖÖÖÖ"); // her biri 2 bayt = 10 bayt
        let s = read_fixed_str(&buf);
        assert_eq!(s, "ÖÖÖÖ", "karakter sınırında kesilmeli");
        assert!(s.len() <= 8);
    }

    #[test]
    fn fixed_str_handles_exact_fit() {
        let mut buf = [0u8; 6];
        write_fixed_str(&mut buf, "EURUSD");
        // NUL sonlandırıcı için yer yok — okuma tampon sonunda durmalı.
        assert_eq!(read_fixed_str(&buf), "EURUSD");
    }

    #[test]
    fn defaults_are_sane() {
        // Doldurma modu varsayılanı AUTO olmalı: eski adaptörün IOC'yi sabit
        // kodlaması IOC desteklemeyen sembollerde emirleri reddettiriyordu.
        assert_eq!(Cmd::default().filling, filling::AUTO);
        assert_eq!(Cmd::default().type_time, type_time::GTC);
        assert_eq!(Book::default().kind, kind::BOOK);
        assert_eq!(Book::default().depth, 0);
    }

    #[test]
    fn book_levels_slice_respects_depth() {
        let mut b = Book { depth: 3, ..Default::default() };
        b.levels[0].price = 1.0;
        b.levels[1].price = 2.0;
        b.levels[2].price = 3.0;
        b.levels[3].price = 99.0; // depth dışında — görünmemeli
        let lv = b.levels();
        assert_eq!(lv.len(), 3);
        assert_eq!(lv.iter().map(|l| l.price).collect::<Vec<_>>(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn book_levels_slice_clamps_bogus_depth() {
        // Bozuk bir yazar MAX'tan büyük depth yazarsa dilim taşmamalı.
        let b = Book { depth: 9999, ..Default::default() };
        assert_eq!(b.levels().len(), MAX_BOOK_DEPTH);
    }
}
