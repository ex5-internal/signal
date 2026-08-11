//! Paylaşılan bellekte taşınan mesajlar.
//!
//! Bu dosyadaki her `#[repr(C)]` yapı MQL5 tarafında birebir aynı yerleşimle
//! bulunmak zorundadır. Elle senkron tutmak en büyük hata kaynağı olacağı için
//! MQL5 karşılıkları `gen-mqh` ikilisi tarafından ÜRETİLİR; `layout` modülündeki
//! derleme-zamanı iddiaları da boyut/hizalama kaymasını daha derleme aşamasında
//! yakalar. Bir alan eklerken: dolgu (`_pad*`) alanlarını buna göre düzelt,
//! `cargo test -p sinyal-proto` çalıştır, sonra `gen-mqh` ile başlığı yenile.
//!
//! Tasarım kuralları:
//! - Her hücre (`Cell<T>`) 2'nin kuvveti dostu bir boyuta yuvarlanır; `Tick`
//!   tam bir önbellek satırına (64 bayt) oturur.
//! - Tüm alanlar POD; `String`/`Vec`/işaretçi YOK (süreçler arası paylaşılıyor).
//! - Zaman alanları iki türlüdür: `time_msc` broker sunucu saati (epoch ms,
//!   sıralama/pencereleme için), `recv_qpc` ise yakalama anındaki yerel
//!   QueryPerformanceCounter değeri (gecikme ölçümü için — saatler arası
//!   kayma bunu etkilemez).

/// Sembol adları için üst sınır.
///
/// MQL5'te `SYMBOL_MAX_LENGTH` diye bir sabit **yoktur**; 31 karakterlik sınır
/// `CustomSymbolCreate` dokümanında düz metin olarak geçer ve resmen yalnızca
/// özel sembolleri bağlar. 31 bayt + NUL tam 32'ye oturduğu için bu değer
/// güvenli, ama EA tarafı 31 baytı aşan bir adı **sessizce kesmemelidir** —
/// kesilen ad, çekirdeğin yanlış sembole emir göndermesi demektir. Aşan sembol
/// tabloya hiç eklenmemeli ve loglanmalıdır.
pub const SYMBOL_NAME_LEN: usize = 32;

/// Emir yorumları için üst sınır. MT5 yorum alanını 31 karakterde keser.
pub const COMMENT_LEN: usize = 32;

/// Bir `Book` mesajında taşınan azami seviye sayısı (her iki taraf toplam).
///
/// MT5'in DOM'u broker'a göre tipik olarak 10-20 seviye verir; 32 rahat bir
/// üst sınır. Daha derini gerekirse burayı büyütmek hücre boyutunu da büyütür,
/// bu yüzden `Book` dolgusunun yeniden hesaplanması gerekir.
pub const MAX_BOOK_DEPTH: usize = 32;

// ---------------------------------------------------------------------------
// Mesaj türleri
// ---------------------------------------------------------------------------

/// `Tick.kind` / `Book.kind` değerleri.
pub mod kind {
    /// Normal piyasa tick'i (`SymbolInfoTick` veya `OnTick`).
    pub const TICK: u8 = 1;
    /// Derinlik anlık görüntüsü (`MarketBookGet`).
    pub const BOOK: u8 = 2;
    /// EA açılışta her sembol için bir kez "başlangıç" tick'i basar; tüketici
    /// bunu gerçek piyasa hareketi saymamalı (bayat olabilir).
    pub const SNAPSHOT: u8 = 3;
}

/// MT5 `ENUM_BOOK_TYPE` karşılıkları (`BookLevel.kind`).
pub mod book_type {
    pub const SELL: u8 = 0;
    pub const BUY: u8 = 1;
    pub const SELL_MARKET: u8 = 2;
    pub const BUY_MARKET: u8 = 3;
}

/// MT5 `TICK_FLAG_*` maskesi (`Tick.flags`).
///
/// CFD/forex'te `LAST` ve `VOLUME` çoğu zaman gelmez; yön çıkarımı için
/// `BUY`/`SELL` bayrakları yoksa tüketici mid/uptick kuralına düşmelidir.
pub mod tick_flag {
    pub const BID: u16 = 0x02;
    pub const ASK: u16 = 0x04;
    pub const LAST: u16 = 0x08;
    pub const VOLUME: u16 = 0x10;
    pub const BUY: u16 = 0x20;
    pub const SELL: u16 = 0x40;
}

// ---------------------------------------------------------------------------
// Tick — 56 bayt (Cell<Tick> = 64 bayt = tek önbellek satırı)
// ---------------------------------------------------------------------------

/// Tek bir piyasa tick'i.
///
/// MT5'in `MqlTick`'inden farklı olarak tamsayı `volume` alanı taşınmaz;
/// `volume_real` zaten daha hassastır ve ikisini birden taşımak hücreyi
/// önbellek satırından taşırırdı.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Tick {
    /// Broker sunucu saati, epoch milisaniye (`MqlTick.time_msc`).
    pub time_msc: i64,
    pub bid: f64,
    pub ask: f64,
    /// CFD/forex'te 0 olabilir.
    pub last: f64,
    /// CFD/forex'te 0 olabilir.
    pub volume_real: f64,
    /// Yakalama anındaki yerel `QueryPerformanceCounter`. Gecikme ölçümünün
    /// başlangıç damgası — broker saatiyle karıştırma.
    pub recv_qpc: u64,
    /// Sembol tablosundaki indeks (ad değil — 4 bayt vs 32 bayt).
    pub symbol_id: u32,
    /// `tick_flag::*` maskesi.
    pub flags: u16,
    /// `kind::*`.
    pub kind: u8,
    pub _pad: u8,
}

// ---------------------------------------------------------------------------
// Book — 824 bayt (Cell<Book> = 832 = 13 önbellek satırı)
// ---------------------------------------------------------------------------

/// Derinlik tablosunda tek bir seviye.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BookLevel {
    pub price: f64,
    pub volume_real: f64,
    /// `book_type::*`.
    pub kind: u8,
    pub _pad: [u8; 7],
}

/// Derinlik (DOM) anlık görüntüsü — artımlı değil, tam tablo.
///
/// Artımlı güncelleme taşımıyoruz: MT5 zaten `MarketBookGet` ile tam tabloyu
/// veriyor ve 32 seviyelik bir kopya, artımlı birleştirmenin karmaşıklığından
/// ve durum kayması riskinden ucuz. Tüketici her mesajı bağımsız işleyebilir.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Book {
    /// Broker sunucu saati, epoch milisaniye.
    pub time_msc: i64,
    /// Yakalama anındaki yerel `QueryPerformanceCounter`.
    pub recv_qpc: u64,
    pub symbol_id: u32,
    /// `levels` içinde geçerli olan seviye sayısı (≤ `MAX_BOOK_DEPTH`).
    ///
    /// Broker `MAX_BOOK_DEPTH`'ten fazla seviye verirse EA tarafı en iyi
    /// fiyatlardan başlayarak keser ve `truncated`'ı 1 yapar.
    pub depth: u16,
    /// `kind::*`.
    pub kind: u8,
    /// Broker daha derin bir tablo verdi ve kesildi.
    pub truncated: u8,
    pub levels: [BookLevel; MAX_BOOK_DEPTH],
    pub _pad: [u8; 32],
}

impl Default for Book {
    fn default() -> Self {
        Self {
            time_msc: 0,
            recv_qpc: 0,
            symbol_id: 0,
            depth: 0,
            kind: kind::BOOK,
            truncated: 0,
            levels: [BookLevel::default(); MAX_BOOK_DEPTH],
            _pad: [0; 32],
        }
    }
}

impl Book {
    /// Yalnızca geçerli seviyeler.
    pub fn levels(&self) -> &[BookLevel] {
        &self.levels[..(self.depth as usize).min(MAX_BOOK_DEPTH)]
    }
}

// ---------------------------------------------------------------------------
// Cmd — 120 bayt (Cell<Cmd> = 128)
// ---------------------------------------------------------------------------

/// `Cmd.action` — çekirdekten EA'ya gönderilen işlem türü.
///
/// Eski adaptörün yalnızca OPEN/CLOSE/MODIFY_SLTP desteklemesi en büyük
/// eksiğiydi; burada MT5'in tüm ticaret eylem yüzeyi karşılanır.
pub mod action {
    /// Piyasa emri (`TRADE_ACTION_DEAL`).
    pub const DEAL: u8 = 1;
    /// Bekleyen emir yerleştir (`TRADE_ACTION_PENDING`).
    pub const PENDING: u8 = 2;
    /// Pozisyonun SL/TP'sini değiştir (`TRADE_ACTION_SLTP`).
    pub const SLTP: u8 = 3;
    /// Bekleyen emri değiştir (`TRADE_ACTION_MODIFY`).
    pub const MODIFY: u8 = 4;
    /// Bekleyen emri sil (`TRADE_ACTION_REMOVE`).
    pub const REMOVE: u8 = 5;
    /// Ters pozisyonla kapat (`TRADE_ACTION_CLOSE_BY`).
    pub const CLOSE_BY: u8 = 6;
    /// Pozisyonu kapat — EA ters yönde `DEAL` üretir (tam veya kısmi).
    pub const CLOSE_POSITION: u8 = 7;
}

/// MT5 `ENUM_ORDER_TYPE` karşılıkları (`Cmd.order_type`).
pub mod order_type {
    pub const BUY: u8 = 0;
    pub const SELL: u8 = 1;
    pub const BUY_LIMIT: u8 = 2;
    pub const SELL_LIMIT: u8 = 3;
    pub const BUY_STOP: u8 = 4;
    pub const SELL_STOP: u8 = 5;
    pub const BUY_STOP_LIMIT: u8 = 6;
    pub const SELL_STOP_LIMIT: u8 = 7;
}

/// MT5 `ENUM_ORDER_TYPE_FILLING` (`Cmd.filling`).
///
/// Eski adaptör `IOC`'yi sabit kodladığı için IOC desteklemeyen sembollerde
/// emirler 10030 (INVALID_FILL) ile reddediliyordu. Varsayılan [`AUTO`]'dur.
///
/// # `AUTO` neden maskeye bakarak çözülemez
///
/// `SYMBOL_FILLING_MODE` maskesi **yalnızca** FOK/IOC/BOC bitlerini taşır;
/// `RETURN` maskede hiç görünmez. Doğru seçim `SYMBOL_TRADE_EXEMODE`'a
/// bağlıdır: INSTANT/REQUEST modunda FOK maskeden bağımsız izinlidir, MARKET
/// modunda ise RETURN kesinlikle yasaktır. Bu yüzden
/// [`SymbolEntry::trade_exemode`] olmadan `AUTO` çözülemez.
///
/// Çözümleme [`crate::resolve_filling`] içinde uygulanır.
pub mod filling {
    pub const FOK: u8 = 0;
    pub const IOC: u8 = 1;
    pub const RETURN: u8 = 2;
    /// Book Or Cancel — yalnızca limit/stop-limit emirlerde anlamlı.
    /// `AUTO` asla bunu seçmez; çağıran açıkça istemelidir.
    pub const BOC: u8 = 3;
    pub const AUTO: u8 = 255;
}

/// `SYMBOL_FILLING_MODE` maske bitleri.
///
/// DİKKAT: `RETURN` bu maskede **yoktur** — iznini [`exec_mode`] belirler.
pub mod filling_mask {
    pub const FOK: u32 = 1;
    pub const IOC: u32 = 2;
    pub const BOC: u32 = 4;
}

/// MT5 `SYMBOL_TRADE_EXEMODE` — sembolün emir yürütme modu.
///
/// Doğru doldurma modunu seçmenin belirleyici girdisi.
pub mod exec_mode {
    pub const REQUEST: u32 = 0;
    pub const INSTANT: u32 = 1;
    pub const MARKET: u32 = 2;
    pub const EXCHANGE: u32 = 3;
}

/// MT5 `ENUM_ORDER_TYPE_TIME` (`Cmd.type_time`).
pub mod type_time {
    pub const GTC: u8 = 0;
    pub const DAY: u8 = 1;
    pub const SPECIFIED: u8 = 2;
    pub const SPECIFIED_DAY: u8 = 3;
}

/// Çekirdekten EA'ya emir komutu.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Cmd {
    /// İstemcinin verdiği idempotency anahtarı. EA bunu `magic`+`comment`
    /// üzerinden MT5'e taşır ve sonuçta geri döndürür; aynı `client_id` ikinci
    /// kez gelirse TEKRAR YÜRÜTÜLMEZ.
    pub client_id: u64,
    pub volume: f64,
    /// Limit/stop fiyatı. `DEAL` için 0 → EA güncel bid/ask kullanır.
    pub price: f64,
    /// Yalnızca `*_STOP_LIMIT` emirlerinde kullanılır.
    pub stoplimit: f64,
    pub sl: f64,
    pub tp: f64,
    /// `SLTP`/`MODIFY`/`REMOVE`/`CLOSE_*` için hedef pozisyon veya emir bileti.
    pub ticket: u64,
    /// `CLOSE_BY` için karşı pozisyon bileti.
    pub ticket_by: u64,
    /// `type_time` SPECIFIED* ise son geçerlilik, epoch saniye.
    pub expiration: i64,
    /// MT5 `MqlTradeRequest.magic` — **64 bit** (`ulong`).
    ///
    /// `u32` olsaydı `client_id` buraya sığmazdı ve idempotency `comment`
    /// üzerinden taşınmak zorunda kalırdı; broker `comment`'i ezebildiği için
    /// bu güvenilmez. 64 bit sayesinde `client_id` doğrudan `magic`'e konur ve
    /// `OnTradeTransaction`'da bize ait olmayan olaylar güvenle ayıklanır.
    pub magic: u64,
    pub symbol_id: u32,
    /// İzin verilen azami kayma (point). 0 → çekirdek varsayılanı.
    pub deviation: u32,
    /// UTF-8, sonu boşluksa NUL ile dolgulanır.
    ///
    /// Idempotency için buna GÜVENME — broker yorumu değiştirebilir. Kimlik
    /// `magic` üzerinden taşınır.
    pub comment: [u8; COMMENT_LEN],
    /// `action::*`.
    pub action: u8,
    /// `order_type::*`.
    pub order_type: u8,
    /// `filling::*` — varsayılan `AUTO`.
    pub filling: u8,
    /// `type_time::*`.
    pub type_time: u8,
    /// Gelecekte alan eklemek için ayrılmış alan.
    ///
    /// Hücreyi 192 bayta (3 önbellek satırı) tamamlar. Komut halkası sıcak yol
    /// değil (saniyede birkaç mesaj), bu yüzden 60 baytlık israf önemsiz;
    /// karşılığında protokolü kırmadan alan eklenebiliyor.
    pub _reserved: [u8; 60],
}

impl Default for Cmd {
    fn default() -> Self {
        Self {
            client_id: 0,
            volume: 0.0,
            price: 0.0,
            stoplimit: 0.0,
            sl: 0.0,
            tp: 0.0,
            ticket: 0,
            ticket_by: 0,
            expiration: 0,
            magic: 0,
            symbol_id: 0,
            deviation: 0,
            comment: [0; COMMENT_LEN],
            action: 0,
            order_type: 0,
            filling: filling::AUTO,
            type_time: type_time::GTC,
            _reserved: [0; 60],
        }
    }
}

// ---------------------------------------------------------------------------
// Res — 120 bayt (Cell<Res> = 128)
// ---------------------------------------------------------------------------

/// `Res.kind` — sonucun hangi aşamadan geldiği.
///
/// `OrderSendAsync` iki aşamalı geri bildirim üretir: önce sunucunun isteği
/// kuyruğa aldığına dair yanıt (`SEND_ACK`), sonra gerçek yürütme
/// (`TRADE_TXN`). Tüketici emri `TRADE_TXN` gelmeden dolmuş saymamalıdır.
pub mod res_kind {
    /// `OrderSendAsync` çağrısının anlık dönüşü (kuyruğa alındı/reddedildi).
    pub const SEND_ACK: u8 = 1;
    /// `OnTradeTransaction` olayı — gerçek yürütme.
    pub const TRADE_TXN: u8 = 2;
    /// Komut EA tarafında doğrulamayı geçemedi (MT5'e hiç gitmedi).
    pub const REJECTED: u8 = 3;
    /// Aynı `client_id` daha önce yürütüldü — yok sayıldı.
    pub const DUPLICATE: u8 = 4;
}

/// EA'dan çekirdeğe emir sonucu / ticaret olayı.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Res {
    /// Komutun `client_id`'si. `TRADE_TXN` bizim göndermediğimiz bir işlemden
    /// (ör. elle kapatma) geliyorsa 0 olur.
    pub client_id: u64,
    pub order: u64,
    pub deal: u64,
    pub position: u64,
    pub volume: f64,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    /// Olayın yakalandığı andaki yerel `QueryPerformanceCounter`.
    pub recv_qpc: u64,
    /// MT5 `MqlTradeResult.retcode`.
    ///
    /// `SEND_ACK`'te 10008 (PLACED) "sunucuya gönderildi" demektir, **dolmuş
    /// değil**. Emri yalnızca `TRADE_TXN`'de 10009/10010 veya `DEAL_ADD`
    /// gördüğünde dolmuş say.
    pub retcode: u32,
    pub retcode_external: u32,
    /// Terminalin `OrderSendAsync`'e atadığı istek kimliği
    /// (`MqlTradeResult.request_id`).
    ///
    /// `SEND_ACK` ile `TRADE_TXN`'i bağlayan **tek resmî anahtar**.
    /// `MqlTradeTransaction` yapısında `magic`/`comment` bulunmadığı için
    /// eşleştirme buradan yapılır; EA ayrıca bilet→`client_id` yerel tablosu
    /// tutar.
    pub request_id: u32,
    /// `res_kind::*`.
    pub kind: u8,
    /// MT5 `ENUM_TRADE_TRANSACTION_TYPE`.
    pub txn_type: u8,
    pub _pad: [u8; 2],
    pub comment: [u8; COMMENT_LEN],
}

impl Default for Res {
    fn default() -> Self {
        Self {
            client_id: 0,
            order: 0,
            deal: 0,
            position: 0,
            volume: 0.0,
            price: 0.0,
            bid: 0.0,
            ask: 0.0,
            recv_qpc: 0,
            retcode: 0,
            retcode_external: 0,
            request_id: 0,
            kind: 0,
            txn_type: 0,
            _pad: [0; 2],
            comment: [0; COMMENT_LEN],
        }
    }
}

// ---------------------------------------------------------------------------
// SymbolEntry — 128 bayt
// ---------------------------------------------------------------------------

/// Sembol tablosundaki tek kayıt.
///
/// EA açılışta doldurur, çekirdek okur. Emir doğrulaması (hacim yuvarlama,
/// doldurma modu seçimi, fiyat normalizasyonu) BU veriye dayanır — çekirdek
/// broker özelliklerini tahmin etmez.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SymbolEntry {
    pub point: f64,
    pub tick_size: f64,
    pub tick_value: f64,
    pub contract_size: f64,
    pub volume_min: f64,
    pub volume_max: f64,
    pub volume_step: f64,
    /// `SYMBOL_VOLUME_LIMIT` — bir sembolde toplam açık hacim sınırı.
    /// 0 = sınırsız.
    pub volume_limit: f64,
    pub symbol_id: u32,
    pub digits: u32,
    /// `SYMBOL_FILLING_MODE` maskesi — [`filling_mask`] bitleri
    /// (FOK=1, IOC=2, **BOC=4**).
    ///
    /// `RETURN` bu maskede **yoktur**; iznini [`SymbolEntry::trade_exemode`]
    /// belirler. Yalnızca maskeye bakan bir `AUTO` çözümleyicisi BOC-only
    /// sembollerde yanlışlıkla FOK'a düşerdi.
    pub filling_mode: u32,
    /// `SYMBOL_TRADE_MODE` — DISABLED ise emir gönderilmemeli.
    pub trade_mode: u32,
    /// `SYMBOL_TRADE_EXEMODE` — [`exec_mode`] değerleri.
    ///
    /// Doğru doldurma modunu seçmenin belirleyici girdisi; bu alan olmadan
    /// `filling::AUTO` çözülemez.
    pub trade_exemode: u32,
    /// `SYMBOL_EXPIRATION_MODE` maskesi.
    pub expiration_mode: u32,
    /// `SYMBOL_ORDER_MODE` maskesi — hangi emir tipleri kabul ediliyor.
    pub order_mode: u32,
    /// `SYMBOL_TRADE_STOPS_LEVEL` — SL/TP ve bekleyen emir fiyatının güncel
    /// fiyata olabileceği asgari uzaklık (point). İhlali 10016 ile reddedilir.
    pub stops_level: u32,
    /// `SYMBOL_TRADE_FREEZE_LEVEL` — bu mesafe içindeki emirler
    /// değiştirilemez/iptal edilemez (point).
    pub freeze_level: u32,
    /// `SYMBOL_TICKS_BOOKDEPTH` — 0 ise broker bu sembolde DOM vermiyor.
    ///
    /// EA `MarketBookAdd`'i bu değer 0 ise **hiç çağırmamalıdır**; retail
    /// forex'te 0 beklenen durumdur ve gereksiz çağrı 4901 hatası üretir.
    pub ticks_bookdepth: u32,
    /// [`sym_flag`] bitleri.
    pub flags: u32,
    pub _pad0: u32,
    /// Broker'daki GERÇEK sembol adı (`GOLD#`, `XAUUSD.m` gibi).
    /// Kanonik ad eşlemesi çekirdek tarafında yapılandırmadan gelir.
    pub name: [u8; SYMBOL_NAME_LEN],
    /// Gelecekte alan eklemek için ayrılmış alan (kaydı 192 bayta tamamlar).
    pub _reserved: [u8; 48],
}

/// [`SymbolEntry::flags`] bitleri.
pub mod sym_flag {
    /// `MarketBookAdd` başarıyla çağrıldı — bu sembolde DOM akışı bekleniyor.
    pub const BOOK_SUBSCRIBED: u32 = 1 << 0;
    /// Sembol Market Watch'ta ve en az bir geçerli tick üretti (READY).
    ///
    /// Bu bit yoksa çekirdek sembolü canlı saymamalıdır — `SymbolSelect`
    /// başarısızlığı MQL5'te sessizdir ve bayat/sıfır fiyat döndürür.
    pub const READY: u32 = 1 << 1;
    /// Tick'ler yalnızca `OnTimer` taramasıyla toplanıyor (DOM/OnTick yok).
    ///
    /// Bu sembollerin gecikme sınıfı farklıdır: timer çözünürlüğü ~10-16 ms
    /// olduğu için ortalama ~8 ms ek gecikme taşırlar.
    pub const POLLED_ONLY: u32 = 1 << 2;
}

impl Default for SymbolEntry {
    fn default() -> Self {
        Self {
            point: 0.0,
            tick_size: 0.0,
            tick_value: 0.0,
            contract_size: 0.0,
            volume_min: 0.0,
            volume_max: 0.0,
            volume_step: 0.0,
            volume_limit: 0.0,
            symbol_id: 0,
            digits: 0,
            filling_mode: 0,
            trade_mode: 0,
            trade_exemode: 0,
            expiration_mode: 0,
            order_mode: 0,
            stops_level: 0,
            freeze_level: 0,
            ticks_bookdepth: 0,
            flags: 0,
            _pad0: 0,
            name: [0; SYMBOL_NAME_LEN],
            _reserved: [0; 48],
        }
    }
}

impl SymbolEntry {
    /// Broker sembol adını `&str` olarak döndür (NUL dolgusu atılır).
    pub fn name_str(&self) -> &str {
        let end = self.name.iter().position(|&b| b == 0).unwrap_or(SYMBOL_NAME_LEN);
        core::str::from_utf8(&self.name[..end]).unwrap_or("")
    }
}

/// Sabit uzunluklu bir bayt alanına UTF-8 yaz, kalanı NUL ile doldur.
///
/// Taşma halinde **karakter sınırında** keser — yarım UTF-8 dizisi bırakmaz,
/// aksi halde okuyan taraf `from_utf8` hatası alırdı.
pub fn write_fixed_str(dst: &mut [u8], src: &str) {
    dst.fill(0);
    let mut end = src.len().min(dst.len());
    while end > 0 && !src.is_char_boundary(end) {
        end -= 1;
    }
    dst[..end].copy_from_slice(&src.as_bytes()[..end]);
}

/// Sabit uzunluklu bayt alanını `&str` olarak oku (NUL dolgusu atılır).
pub fn read_fixed_str(src: &[u8]) -> &str {
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    core::str::from_utf8(&src[..end]).unwrap_or("")
}
