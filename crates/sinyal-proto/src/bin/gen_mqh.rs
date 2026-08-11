//! MQL5 başlığı üreteci.
//!
//! `sinyal-proto`'daki Rust tanımlarından `mql5/Include/Sinyal/Protocol.mqh`
//! dosyasını üretir. Bu dosya ELLE DÜZENLENMEZ.
//!
//! ```text
//! cargo run -p sinyal-proto --bin gen-mqh -- mql5/Include/Sinyal/Protocol.mqh
//! ```
//!
//! Argüman verilmezse çıktı stdout'a yazılır (fark almak için kullanışlı).
//!
//! # Neden üretiyoruz
//!
//! MQL5 ile Rust arasındaki yapı yerleşimini hiçbir bağlayıcı doğrulamaz. Elle
//! senkron tutulan bir başlık, bir alan eklendiğinde sessizce kayar ve
//! çalışma zamanında bozuk fiyat/emir verisine dönüşür. Üretilen başlık bu
//! sınıfı tamamen ortadan kaldırır; ayrıca üretilen `SINYAL_SIZEOF_*`
//! sabitleri EA'nın açılışta DLL'e `sinyal_sizeof()` sorup karşılaştırmasını
//! sağlar — iki taraf farklı sürümle derlenmişse EA hiç başlamaz.

use std::mem::size_of;

use sinyal_proto::msg::{Book, BookLevel, Cmd, Res, SymbolEntry, Tick};
use sinyal_proto::{
    action, book_type, exec_mode, filling, filling_mask, kind, order_type, res_kind, sym_flag,
    tick_flag, type_time, COMMENT_LEN, MAX_BOOK_DEPTH, SYMBOL_NAME_LEN,
};

fn main() -> std::io::Result<()> {
    let out = render();
    match std::env::args().nth(1) {
        Some(path) => {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, out)?;
            eprintln!("yazıldı: {path}");
        }
        None => print!("{out}"),
    }
    Ok(())
}

fn render() -> String {
    let mut s = String::with_capacity(16 * 1024);

    s.push_str(&format!(
        r#"//+------------------------------------------------------------------+
//|                                                     Protocol.mqh |
//|         ÜRETİLMİŞ DOSYA — ELLE DÜZENLEME. Değişiklikler kaybolur. |
//|                                                                  |
//| Kaynak: crates/sinyal-proto/src/msg.rs                           |
//| Yenile: cargo run -p sinyal-proto --bin gen-mqh -- \              |
//|             mql5/Include/Sinyal/Protocol.mqh                     |
//|                                                                  |
//| Bu dosyadaki yapılar Rust tarafıyla BAYT BAYT aynı olmalıdır.     |
//| EA açılışta SinyalVerifyLayout() çağırarak bunu doğrular; uyumsuz |
//| olursa başlamaz (sessiz bozuk veri okumaktansa gürültülü durmak). |
//+------------------------------------------------------------------+
#property strict

//--- yerleşim sürümü (Rust: ring::RING_VERSION)
#define SINYAL_PROTO_VERSION        {proto_version}

//--- boyut sınırları
#define SINYAL_MAX_BOOK_DEPTH       {max_book_depth}
#define SINYAL_COMMENT_LEN          {comment_len}
#define SINYAL_SYMBOL_NAME_LEN      {symbol_name_len}

//--- beklenen yapı boyutları (EA açılışta DLL ile karşılaştırır)
#define SINYAL_SIZEOF_TICK          {sz_tick}
#define SINYAL_SIZEOF_BOOKLEVEL     {sz_booklevel}
#define SINYAL_SIZEOF_BOOK          {sz_book}
#define SINYAL_SIZEOF_CMD           {sz_cmd}
#define SINYAL_SIZEOF_RES           {sz_res}
#define SINYAL_SIZEOF_SYMBOLENTRY   {sz_symentry}

"#,
        proto_version = sinyal_proto::ring::RING_VERSION,
        max_book_depth = MAX_BOOK_DEPTH,
        comment_len = COMMENT_LEN,
        symbol_name_len = SYMBOL_NAME_LEN,
        sz_tick = size_of::<Tick>(),
        sz_booklevel = size_of::<BookLevel>(),
        sz_book = size_of::<Book>(),
        sz_cmd = size_of::<Cmd>(),
        sz_res = size_of::<Res>(),
        sz_symentry = size_of::<SymbolEntry>(),
    ));

    // ---- sabitler ----
    s.push_str("//--- mesaj türü (Tick.kind / Book.kind)\n");
    s.push_str(&def("SINYAL_KIND_TICK", kind::TICK));
    s.push_str(&def("SINYAL_KIND_BOOK", kind::BOOK));
    s.push_str(&def("SINYAL_KIND_SNAPSHOT", kind::SNAPSHOT));

    s.push_str("\n//--- derinlik seviyesi türü (MT5 ENUM_BOOK_TYPE ile birebir)\n");
    s.push_str(&def("SINYAL_BOOK_SELL", book_type::SELL));
    s.push_str(&def("SINYAL_BOOK_BUY", book_type::BUY));
    s.push_str(&def("SINYAL_BOOK_SELL_MARKET", book_type::SELL_MARKET));
    s.push_str(&def("SINYAL_BOOK_BUY_MARKET", book_type::BUY_MARKET));

    s.push_str("\n//--- tick bayrakları (MT5 TICK_FLAG_* ile birebir)\n");
    s.push_str(&def16("SINYAL_TICK_FLAG_BID", tick_flag::BID));
    s.push_str(&def16("SINYAL_TICK_FLAG_ASK", tick_flag::ASK));
    s.push_str(&def16("SINYAL_TICK_FLAG_LAST", tick_flag::LAST));
    s.push_str(&def16("SINYAL_TICK_FLAG_VOLUME", tick_flag::VOLUME));
    s.push_str(&def16("SINYAL_TICK_FLAG_BUY", tick_flag::BUY));
    s.push_str(&def16("SINYAL_TICK_FLAG_SELL", tick_flag::SELL));

    s.push_str("\n//--- komut eylemi\n");
    s.push_str(&def("SINYAL_ACTION_DEAL", action::DEAL));
    s.push_str(&def("SINYAL_ACTION_PENDING", action::PENDING));
    s.push_str(&def("SINYAL_ACTION_SLTP", action::SLTP));
    s.push_str(&def("SINYAL_ACTION_MODIFY", action::MODIFY));
    s.push_str(&def("SINYAL_ACTION_REMOVE", action::REMOVE));
    s.push_str(&def("SINYAL_ACTION_CLOSE_BY", action::CLOSE_BY));
    s.push_str(&def("SINYAL_ACTION_CLOSE_POSITION", action::CLOSE_POSITION));

    s.push_str("\n//--- emir tipi (MT5 ENUM_ORDER_TYPE ile birebir)\n");
    s.push_str(&def("SINYAL_ORDER_BUY", order_type::BUY));
    s.push_str(&def("SINYAL_ORDER_SELL", order_type::SELL));
    s.push_str(&def("SINYAL_ORDER_BUY_LIMIT", order_type::BUY_LIMIT));
    s.push_str(&def("SINYAL_ORDER_SELL_LIMIT", order_type::SELL_LIMIT));
    s.push_str(&def("SINYAL_ORDER_BUY_STOP", order_type::BUY_STOP));
    s.push_str(&def("SINYAL_ORDER_SELL_STOP", order_type::SELL_STOP));
    s.push_str(&def("SINYAL_ORDER_BUY_STOP_LIMIT", order_type::BUY_STOP_LIMIT));
    s.push_str(&def("SINYAL_ORDER_SELL_STOP_LIMIT", order_type::SELL_STOP_LIMIT));

    s.push_str(
        "\n//--- doldurma modu.\n\
         //    AUTO cozumlemesi SYMBOL_FILLING_MODE maskesine BAKARAK YAPILAMAZ:\n\
         //    RETURN maskede yer almaz ve INSTANT/REQUEST modunda FOK maskeden\n\
         //    bagimsiz izinlidir. Belirleyici girdi SYMBOL_TRADE_EXEMODE'dur.\n",
    );
    s.push_str(&def("SINYAL_FILLING_FOK", filling::FOK));
    s.push_str(&def("SINYAL_FILLING_IOC", filling::IOC));
    s.push_str(&def("SINYAL_FILLING_RETURN", filling::RETURN));
    s.push_str(&def("SINYAL_FILLING_BOC", filling::BOC));
    s.push_str(&def("SINYAL_FILLING_AUTO", filling::AUTO));

    s.push_str("\n//--- SYMBOL_FILLING_MODE maske bitleri (RETURN bu maskede YOKTUR)\n");
    s.push_str(&def32("SINYAL_FILLMASK_FOK", filling_mask::FOK));
    s.push_str(&def32("SINYAL_FILLMASK_IOC", filling_mask::IOC));
    s.push_str(&def32("SINYAL_FILLMASK_BOC", filling_mask::BOC));

    s.push_str("\n//--- SYMBOL_TRADE_EXEMODE (dogru doldurma modunun belirleyicisi)\n");
    s.push_str(&def32("SINYAL_EXEMODE_REQUEST", exec_mode::REQUEST));
    s.push_str(&def32("SINYAL_EXEMODE_INSTANT", exec_mode::INSTANT));
    s.push_str(&def32("SINYAL_EXEMODE_MARKET", exec_mode::MARKET));
    s.push_str(&def32("SINYAL_EXEMODE_EXCHANGE", exec_mode::EXCHANGE));

    s.push_str("\n//--- SymbolEntry.flags bitleri\n");
    s.push_str(&def32("SINYAL_SYMFLAG_BOOK_SUBSCRIBED", sym_flag::BOOK_SUBSCRIBED));
    s.push_str(&def32("SINYAL_SYMFLAG_READY", sym_flag::READY));
    s.push_str(&def32("SINYAL_SYMFLAG_POLLED_ONLY", sym_flag::POLLED_ONLY));

    s.push_str("\n//--- geçerlilik süresi (MT5 ENUM_ORDER_TYPE_TIME ile birebir)\n");
    s.push_str(&def("SINYAL_TIME_GTC", type_time::GTC));
    s.push_str(&def("SINYAL_TIME_DAY", type_time::DAY));
    s.push_str(&def("SINYAL_TIME_SPECIFIED", type_time::SPECIFIED));
    s.push_str(&def("SINYAL_TIME_SPECIFIED_DAY", type_time::SPECIFIED_DAY));

    s.push_str("\n//--- sonuç türü\n");
    s.push_str(&def("SINYAL_RES_SEND_ACK", res_kind::SEND_ACK));
    s.push_str(&def("SINYAL_RES_TRADE_TXN", res_kind::TRADE_TXN));
    s.push_str(&def("SINYAL_RES_REJECTED", res_kind::REJECTED));
    s.push_str(&def("SINYAL_RES_DUPLICATE", res_kind::DUPLICATE));

    // ---- yapılar ----
    s.push_str(&format!(
        r#"
//+------------------------------------------------------------------+
//| Tek piyasa tick'i. {sz_tick} bayt.                                      |
//|                                                                  |
//| time_msc : broker sunucu saati (epoch ms) — sıralama/pencereleme  |
//| recv_qpc : yakalama anındaki YEREL QueryPerformanceCounter —      |
//|            gecikme ölçümü buradan yapılır, broker saatinden DEĞİL |
//| last/volume_real: CFD/forex'te 0 olabilir                         |
//+------------------------------------------------------------------+
struct SinyalTick
  {{
   long              time_msc;       // +0
   double            bid;            // +8
   double            ask;            // +16
   double            last;           // +24
   double            volume_real;    // +32
   ulong             recv_qpc;       // +40
   uint              symbol_id;      // +48
   ushort            flags;          // +52  SINYAL_TICK_FLAG_*
   uchar             kind;           // +54  SINYAL_KIND_*
   uchar             reserved0;      // +55  dolgu
  }};

//+------------------------------------------------------------------+
//| Derinlik tablosunda tek seviye. {sz_booklevel} bayt.                        |
//+------------------------------------------------------------------+
struct SinyalBookLevel
  {{
   double            price;          // +0
   double            volume_real;    // +8
   uchar             kind;           // +16  SINYAL_BOOK_*
   uchar             reserved0[7];   // +17  dolgu
  }};

//+------------------------------------------------------------------+
//| Derinlik anlık görüntüsü (artımlı DEĞİL, tam tablo). {sz_book} bayt.    |
//|                                                                  |
//| depth     : levels[] içinde geçerli seviye sayısı                 |
//| truncated : broker SINYAL_MAX_BOOK_DEPTH'ten fazla seviye verdi   |
//|             ve en iyi fiyatlardan başlayarak kesildi              |
//+------------------------------------------------------------------+
struct SinyalBook
  {{
   long              time_msc;       // +0
   ulong             recv_qpc;       // +8
   uint              symbol_id;      // +16
   ushort            depth;          // +20
   uchar             kind;           // +22  SINYAL_KIND_BOOK
   uchar             truncated;      // +23
   SinyalBookLevel   levels[SINYAL_MAX_BOOK_DEPTH]; // +24
   uchar             reserved0[32];  // dolgu
  }};

//+------------------------------------------------------------------+
//| Çekirdekten EA'ya emir komutu. {sz_cmd} bayt.                          |
//|                                                                  |
//| client_id : idempotency anahtarı. Aynı değer ikinci kez gelirse   |
//|             EA TEKRAR YÜRÜTMEZ — çift emir koruması.              |
//| filling   : SINYAL_FILLING_AUTO ise EA sembolün maskesinden seçer |
//| price     : DEAL için 0 → EA güncel bid/ask kullanır              |
//+------------------------------------------------------------------+
struct SinyalCmd
  {{
   ulong             client_id;      // +0
   double            volume;         // +8
   double            price;          // +16
   double            stoplimit;      // +24  yalnız *_STOP_LIMIT
   double            sl;             // +32
   double            tp;             // +40
   ulong             ticket;         // +48  hedef pozisyon/emir
   ulong             ticket_by;      // +56  CLOSE_BY karşı pozisyon
   long              expiration;     // +64  epoch saniye
   ulong             magic;          // +72  64 bit — client_id buraya sığar
   uint              symbol_id;      // +80
   uint              deviation;      // +84  point; 0 → varsayılan
   uchar             comment[SINYAL_COMMENT_LEN]; // +88
   uchar             action;         // +120 SINYAL_ACTION_*
   uchar             order_type;     // +121 SINYAL_ORDER_*
   uchar             filling;        // +122 SINYAL_FILLING_*
   uchar             type_time;      // +123 SINYAL_TIME_*
   uchar             reserved0[60];  // +124 gelecekte alan eklemek için
  }};

//+------------------------------------------------------------------+
//| EA'dan çekirdeğe emir sonucu / ticaret olayı. {sz_res} bayt.           |
//|                                                                  |
//| OrderSendAsync İKİ aşamalı geri bildirim üretir:                  |
//|   SEND_ACK  → sunucu isteği kuyruğa aldı (henüz DOLMADI)          |
//|   TRADE_TXN → gerçek yürütme (OnTradeTransaction)                 |
//| Emri TRADE_TXN gelmeden dolmuş sayma.                             |
//|                                                                  |
//| client_id : bizim göndermediğimiz bir işlemse (ör. elle kapatma) 0|
//+------------------------------------------------------------------+
struct SinyalRes
  {{
   ulong             client_id;      // +0
   ulong             order;          // +8
   ulong             deal;           // +16
   ulong             position;       // +24
   double            volume;         // +32
   double            price;          // +40
   double            bid;            // +48
   double            ask;            // +56
   ulong             recv_qpc;       // +64
   uint              retcode;        // +72  SEND_ACK'te 10008 = PLACED (DOLMADI!)
   uint              retcode_external; // +76
   uint              request_id;     // +80  SEND_ACK <-> TRADE_TXN bagi
   uchar             kind;           // +84  SINYAL_RES_*
   uchar             txn_type;       // +85  ENUM_TRADE_TRANSACTION_TYPE
   uchar             reserved0[2];   // +86  dolgu
   uchar             comment[SINYAL_COMMENT_LEN]; // +88
  }};

//+------------------------------------------------------------------+
//| Sembol tablosu kaydı. {sz_symentry} bayt.                                  |
//|                                                                  |
//| Çekirdeğin emir doğrulaması (hacim yuvarlama, doldurma modu       |
//| seçimi, fiyat normalizasyonu) TAMAMEN buna dayanır — broker       |
//| özellikleri asla tahmin edilmez.                                  |
//|                                                                  |
//| name : broker'daki GERÇEK ad (GOLD#, XAUUSD.m ...). Kanonik ad    |
//|        eşlemesi çekirdek yapılandırmasında yapılır.               |
//+------------------------------------------------------------------+
struct SinyalSymbolEntry
  {{
   double            point;          // +0
   double            tick_size;      // +8
   double            tick_value;     // +16
   double            contract_size;  // +24
   double            volume_min;     // +32
   double            volume_max;     // +40
   double            volume_step;    // +48
   double            volume_limit;   // +56  SYMBOL_VOLUME_LIMIT (0 = sınırsız)
   uint              symbol_id;      // +64
   uint              digits;         // +68
   uint              filling_mode;   // +72  SYMBOL_FILLING_MODE maskesi
   uint              trade_mode;     // +76  DISABLED ise emir gönderme
   uint              trade_exemode;  // +80  SYMBOL_TRADE_EXEMODE — filling'in belirleyicisi
   uint              expiration_mode;// +84
   uint              order_mode;     // +88  hangi emir tipleri kabul ediliyor
   uint              stops_level;    // +92  SL/TP asgari uzaklık (point)
   uint              freeze_level;   // +96  bu mesafede emir dondurulur
   uint              ticks_bookdepth;// +100 0 ise DOM YOK -> MarketBookAdd cagirma
   uint              flags;          // +104 SINYAL_SYMFLAG_*
   uint              reserved0;      // +108 dolgu
   uchar             name[SINYAL_SYMBOL_NAME_LEN]; // +112
   uchar             reserved1[48];  // +144 gelecekte alan eklemek için
  }};
"#,
        sz_tick = size_of::<Tick>(),
        sz_booklevel = size_of::<BookLevel>(),
        sz_book = size_of::<Book>(),
        sz_cmd = size_of::<Cmd>(),
        sz_res = size_of::<Res>(),
        sz_symentry = size_of::<SymbolEntry>(),
    ));

    s
}

fn def(name: &str, val: u8) -> String {
    format!("#define {name:<32}{val}\n")
}

fn def16(name: &str, val: u16) -> String {
    format!("#define {name:<32}0x{val:02X}\n")
}

fn def32(name: &str, val: u32) -> String {
    format!("#define {name:<32}{val}\n")
}

/// Üretilen MQL5 metnindeki bir yapının bayt boyutunu hesapla.
///
/// MQL5 varsayılanı `pack(1)`'dir — alanlar dolgusuz, bildirim sırasıyla
/// yerleşir. Bu yüzden boyut, alan boyutlarının basit toplamıdır.
///
/// Bu fonksiyon yalnızca test içindir ama üretecin en önemli güvenlik ağını
/// besler: üreteçteki düz metin yapı gövdeleri Rust tanımlarından sessizce
/// ayrışabilir (bir alan eklenip metin güncellenmeyebilir). Boyutu metinden
/// hesaplayıp `size_of` ile karşılaştırmak bu ayrışmayı derleme/test anında
/// yakalar — aksi halde ancak canlı terminalde EA başlamayı reddettiğinde
/// fark edilirdi.
#[cfg(test)]
fn mql5_struct_size(src: &str, name: &str) -> Option<usize> {
    let head = format!("struct {name}\n  {{\n");
    let start = src.find(&head)? + head.len();
    let body = &src[start..];
    let end = body.find("  };")?;

    let mut total = 0usize;
    for line in body[..end].lines() {
        let line = line.trim();
        // Yorumu at.
        let code = line.split("//").next().unwrap_or("").trim();
        let Some(decl) = code.strip_suffix(';') else { continue };
        let mut parts = decl.split_whitespace();
        let (ty, rest) = (parts.next()?, parts.next()?);

        // Dizi mi?
        let (_field, count) = match rest.find('[') {
            Some(i) => {
                let idx = rest[i + 1..].strip_suffix(']')?;
                let n = match idx.parse::<usize>() {
                    Ok(v) => v,
                    // Sabit adı → değerine çöz.
                    Err(_) => match idx {
                        "SINYAL_COMMENT_LEN" => COMMENT_LEN,
                        "SINYAL_SYMBOL_NAME_LEN" => SYMBOL_NAME_LEN,
                        "SINYAL_MAX_BOOK_DEPTH" => MAX_BOOK_DEPTH,
                        other => panic!("bilinmeyen dizi sabiti: {other}"),
                    },
                };
                (&rest[..i], n)
            }
            None => (rest, 1),
        };

        let unit = match ty {
            "ulong" | "long" | "double" => 8,
            "uint" | "int" | "float" => 4,
            "ushort" | "short" => 2,
            "uchar" | "char" | "bool" => 1,
            // İç içe yapı — özyinelemeli hesapla.
            nested if nested.starts_with("Sinyal") => {
                mql5_struct_size(src, nested).unwrap_or_else(|| panic!("iç yapı yok: {nested}"))
            }
            other => panic!("bilinmeyen MQL5 tipi: {other}"),
        };
        total += unit * count;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_header_declares_actual_rust_sizes() {
        // Üreteç boyutları ELDEN yazmaz, size_of'tan alır. Bu test, bir alan
        // eklendiğinde başlığın da kaydığını (yani üretecin gerçekten Rust'a
        // bağlı olduğunu) doğrular.
        let out = render();
        assert!(out.contains(&format!("#define SINYAL_SIZEOF_TICK          {}", size_of::<Tick>())));
        assert!(out.contains(&format!("#define SINYAL_SIZEOF_CMD           {}", size_of::<Cmd>())));
        assert!(out.contains(&format!("#define SINYAL_SIZEOF_RES           {}", size_of::<Res>())));
        assert!(out
            .contains(&format!("#define SINYAL_SIZEOF_BOOK          {}", size_of::<Book>())));
        assert!(out.contains(&format!(
            "#define SINYAL_SIZEOF_SYMBOLENTRY   {}",
            size_of::<SymbolEntry>()
        )));
    }

    #[test]
    fn generated_header_has_every_struct() {
        let out = render();
        for name in [
            "SinyalTick",
            "SinyalBookLevel",
            "SinyalBook",
            "SinyalCmd",
            "SinyalRes",
            "SinyalSymbolEntry",
        ] {
            assert!(out.contains(&format!("struct {name}")), "{name} eksik");
        }
    }

    #[test]
    fn filling_auto_is_documented_as_default() {
        // Eski adaptörün IOC'yi sabit kodlaması emirleri reddettiriyordu;
        // AUTO'nun başlıkta bulunması bu hatanın tekrarını engeller.
        let out = render();
        assert!(out.contains("SINYAL_FILLING_AUTO"));
        assert!(out.contains(&format!("#define SINYAL_FILLING_AUTO             {}", filling::AUTO)));
    }

    #[test]
    fn all_order_types_present_including_pending() {
        // Eski adaptörde limit/stop emir YOKTU — hedefin merkezinde bu var.
        let out = render();
        for t in [
            "SINYAL_ORDER_BUY_LIMIT",
            "SINYAL_ORDER_SELL_LIMIT",
            "SINYAL_ORDER_BUY_STOP",
            "SINYAL_ORDER_SELL_STOP",
            "SINYAL_ORDER_BUY_STOP_LIMIT",
            "SINYAL_ORDER_SELL_STOP_LIMIT",
        ] {
            assert!(out.contains(t), "{t} eksik");
        }
    }

    #[test]
    fn header_warns_against_manual_editing() {
        let out = render();
        assert!(out.contains("ÜRETİLMİŞ DOSYA"));
    }

    /// ÜRETECİN EN ÖNEMLİ TESTİ.
    ///
    /// Yapı gövdeleri üreteçte düz metin olarak duruyor; boyut sabitleri ise
    /// `size_of`'tan geliyor. Rust'a bir alan eklenip metin güncellenmezse
    /// ikisi sessizce ayrışır. Bu test metinden hesaplanan boyutu Rust'ınkiyle
    /// karşılaştırarak ayrışmayı burada yakalar — aksi halde hata ancak canlı
    /// terminalde EA başlamayı reddettiğinde görülürdü.
    #[test]
    fn generated_mql5_structs_have_the_same_size_as_rust() {
        let out = render();
        let cases: [(&str, usize); 6] = [
            ("SinyalTick", size_of::<Tick>()),
            ("SinyalBookLevel", size_of::<BookLevel>()),
            ("SinyalBook", size_of::<Book>()),
            ("SinyalCmd", size_of::<Cmd>()),
            ("SinyalRes", size_of::<Res>()),
            ("SinyalSymbolEntry", size_of::<SymbolEntry>()),
        ];
        for (name, rust_size) in cases {
            let mql = mql5_struct_size(&out, name)
                .unwrap_or_else(|| panic!("{name} üretilen başlıkta bulunamadı"));
            assert_eq!(
                mql, rust_size,
                "{name}: MQL5 metni {mql} bayt, Rust {rust_size} bayt — \
                 üreteçteki yapı gövdesi Rust tanımından ayrışmış"
            );
        }
    }

    #[test]
    fn exemode_and_fillmask_constants_are_emitted() {
        // Doğru doldurma modu seçimi bunlar olmadan yapılamaz; başlıkta
        // bulunmamaları EA'nın maskeye bakarak tahmin etmesine yol açardı.
        let out = render();
        for c in [
            "SINYAL_EXEMODE_INSTANT",
            "SINYAL_EXEMODE_MARKET",
            "SINYAL_EXEMODE_EXCHANGE",
            "SINYAL_FILLMASK_FOK",
            "SINYAL_FILLMASK_IOC",
            "SINYAL_FILLMASK_BOC",
            "SINYAL_SYMFLAG_READY",
            "SINYAL_SYMFLAG_POLLED_ONLY",
        ] {
            assert!(out.contains(c), "{c} eksik");
        }
        // RETURN'ün maskede OLMADIĞI başlıkta açıkça yazmalı.
        assert!(out.contains("RETURN bu maskede YOKTUR"));
    }
}
