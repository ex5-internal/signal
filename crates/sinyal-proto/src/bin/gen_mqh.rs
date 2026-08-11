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

use sinyal_proto::bars::{bar_flag, timeframe, BarRec, HistReq, HIST_SYMBOL_LEN};
use sinyal_proto::msg::{Book, BookLevel, Cmd, Res, SymbolEntry, Tick};
use sinyal_proto::state::{
    margin_mode, rec_flag, so_mode, trade_mode, AccountSnapshot, OrderRec, PositionRec,
};
use sinyal_proto::{
    action, book_type, day_of_week, exec_mode, filling, filling_mask, kind, order_type, res_kind,
    res_source, swap_mode, sym_flag, tick_flag, type_time, COMMENT_LEN, MAX_BOOK_DEPTH,
    SYMBOL_NAME_LEN,
};

/// `HistReq.symbol` alanının ofseti — MQL5 gövdesindeki yorumlara girer.
///
/// Elle yazılmazsa Rust tarafında bir alan eklendiğinde başlıktaki ofset
/// yorumu sessizce yalan söylerdi. `HIST_SYMBOL_LEN` de `sinyal_proto`'dan
/// geliyor: adın uzunluğu iki yerde tutulsaydı biri kayar ve service isteğin
/// ortasından okumaya başlardı.
const HIST_SYMBOL_OFF: usize = std::mem::offset_of!(HistReq, symbol);

/// `SymbolEntry`nin sözleşme/taşıma bloğunun ofsetleri.
///
/// Bu alanlar eski `_reserved`in İÇİNE yerleştiği için yapı boyutu DEĞİŞMEDİ;
/// yani üretecin en güçlü testi (`generated_mql5_structs_have_the_same_size_as_rust`)
/// bu bloğu hiç göremez — toplam 192 bayt kalır ve bir alan yanlış sıraya
/// yazılsa bile test GEÇER. Ofsetleri `offset_of!`tan üretmek, MQL5 gövdesinin
/// Rust'tan ayrışmasını yakalayan tek gerçek koruma.
const SYM_SWAP_LONG_OFF: usize = std::mem::offset_of!(SymbolEntry, swap_long);
const SYM_SWAP_SHORT_OFF: usize = std::mem::offset_of!(SymbolEntry, swap_short);
const SYM_MARGIN_INITIAL_OFF: usize = std::mem::offset_of!(SymbolEntry, margin_initial);
const SYM_MARGIN_HEDGED_OFF: usize = std::mem::offset_of!(SymbolEntry, margin_hedged);
const SYM_SWAP_MODE_OFF: usize = std::mem::offset_of!(SymbolEntry, swap_mode);
const SYM_SWAP_ROLLOVER_OFF: usize = std::mem::offset_of!(SymbolEntry, swap_rollover3d);
const SYM_RESERVED_OFF: usize = std::mem::offset_of!(SymbolEntry, _reserved);
/// Sözleşme bloğundan SONRA kalan dolgu — MQL5 `reserved1[]` uzunluğu.
const SYM_RESERVED_LEN: usize = size_of::<SymbolEntry>() - SYM_RESERVED_OFF;

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
#define SINYAL_REC_COMMENT_LEN      {rec_comment_len}
#define SINYAL_MAX_POSITIONS        {max_positions}
#define SINYAL_MAX_ORDERS           {max_orders}

//--- HistReq içindeki sembol adı alanının uzunluğu.
//    Service grafiğe BAĞLI DEĞİLDİR (_Symbol yok) ve symbol_id'yi ada
//    çevirebilmesi için adın istekle birlikte gelmesi gerekir.
#define SINYAL_HIST_SYMBOL_LEN      {hist_symbol_len}

//--- beklenen yapı boyutları (EA açılışta DLL ile karşılaştırır)
#define SINYAL_SIZEOF_TICK          {sz_tick}
#define SINYAL_SIZEOF_BOOKLEVEL     {sz_booklevel}
#define SINYAL_SIZEOF_BOOK          {sz_book}
#define SINYAL_SIZEOF_CMD           {sz_cmd}
#define SINYAL_SIZEOF_RES           {sz_res}
#define SINYAL_SIZEOF_SYMBOLENTRY   {sz_symentry}
#define SINYAL_SIZEOF_ACCOUNT       {sz_account}
#define SINYAL_SIZEOF_POSITION      {sz_position}
#define SINYAL_SIZEOF_ORDER         {sz_order}
#define SINYAL_SIZEOF_HISTREQ       {sz_histreq}
#define SINYAL_SIZEOF_BARREC        {sz_barrec}

"#,
        proto_version = sinyal_proto::ring::RING_VERSION,
        max_book_depth = MAX_BOOK_DEPTH,
        hist_symbol_len = HIST_SYMBOL_LEN,
        comment_len = COMMENT_LEN,
        symbol_name_len = SYMBOL_NAME_LEN,
        rec_comment_len = sinyal_proto::state::REC_COMMENT_LEN,
        max_positions = sinyal_proto::state::MAX_POSITIONS,
        max_orders = sinyal_proto::state::MAX_ORDERS,
        sz_tick = size_of::<Tick>(),
        sz_booklevel = size_of::<BookLevel>(),
        sz_book = size_of::<Book>(),
        sz_cmd = size_of::<Cmd>(),
        sz_res = size_of::<Res>(),
        sz_symentry = size_of::<SymbolEntry>(),
        sz_account = size_of::<AccountSnapshot>(),
        sz_position = size_of::<PositionRec>(),
        sz_order = size_of::<OrderRec>(),
        sz_histreq = size_of::<HistReq>(),
        sz_barrec = size_of::<BarRec>(),
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

    s.push_str(
        "\n//--- SYMBOL_SWAP_MODE ham degeri.\n\
         //    swap_long/swap_short'un BIRIMINI bu belirler; ham sayiyi para\n\
         //    sanmak swap'i buyukluk mertebesinde yanlis hesaplar:\n\
         //      POINTS            -> point cinsinden\n\
         //      CURRENCY_*        -> lot basina PARA (hangi para birimi moda bagli)\n\
         //      INTEREST_*        -> YILLIK FAIZ YUZDESI\n\
         //      REOPEN_*          -> para degil; pozisyon yeniden acilir\n\
         //    DISABLED (0) gercekten \"swap yok\" demektir, \"veri yok\" degil.\n",
    );
    s.push_str(&def32("SINYAL_SWAPMODE_DISABLED", swap_mode::DISABLED));
    s.push_str(&def32("SINYAL_SWAPMODE_POINTS", swap_mode::POINTS));
    s.push_str(&def32("SINYAL_SWAPMODE_CURRENCY_SYMBOL", swap_mode::CURRENCY_SYMBOL));
    s.push_str(&def32("SINYAL_SWAPMODE_CURRENCY_MARGIN", swap_mode::CURRENCY_MARGIN));
    s.push_str(&def32("SINYAL_SWAPMODE_CURRENCY_DEPOSIT", swap_mode::CURRENCY_DEPOSIT));
    s.push_str(&def32("SINYAL_SWAPMODE_INTEREST_CURRENT", swap_mode::INTEREST_CURRENT));
    s.push_str(&def32("SINYAL_SWAPMODE_INTEREST_OPEN", swap_mode::INTEREST_OPEN));
    s.push_str(&def32("SINYAL_SWAPMODE_REOPEN_CURRENT", swap_mode::REOPEN_CURRENT));
    s.push_str(&def32("SINYAL_SWAPMODE_REOPEN_BID", swap_mode::REOPEN_BID));

    s.push_str(
        "\n//--- swap_rollover3d = ENUM_DAY_OF_WEEK ham degeri.\n\
         //    DIKKAT: haftanin ilk gunu PAZAR (0), Pazartesi DEGIL. Bir gun\n\
         //    kaydirmak uc gunluk swap'i yanlis gune yazar ve hata sessizdir.\n\
         //    Forex'te tipik deger CARSAMBA (3).\n",
    );
    s.push_str(&def32("SINYAL_DOW_SUNDAY", day_of_week::SUNDAY));
    s.push_str(&def32("SINYAL_DOW_MONDAY", day_of_week::MONDAY));
    s.push_str(&def32("SINYAL_DOW_TUESDAY", day_of_week::TUESDAY));
    s.push_str(&def32("SINYAL_DOW_WEDNESDAY", day_of_week::WEDNESDAY));
    s.push_str(&def32("SINYAL_DOW_THURSDAY", day_of_week::THURSDAY));
    s.push_str(&def32("SINYAL_DOW_FRIDAY", day_of_week::FRIDAY));
    s.push_str(&def32("SINYAL_DOW_SATURDAY", day_of_week::SATURDAY));

    s.push_str("\n//--- SymbolEntry.flags bitleri\n");
    s.push_str(&def32("SINYAL_SYMFLAG_BOOK_SUBSCRIBED", sym_flag::BOOK_SUBSCRIBED));
    s.push_str(&def32("SINYAL_SYMFLAG_READY", sym_flag::READY));
    s.push_str(&def32("SINYAL_SYMFLAG_POLLED_ONLY", sym_flag::POLLED_ONLY));
    s.push_str(&def32("SINYAL_SYMFLAG_CHART", sym_flag::CHART));
    // Bu bit YOKSA swap/marjin alanlarindaki 0 "broker swap almiyor" DEGIL
    // "veri yok" demektir. Ayrimi kaybetmek simulatoru iyimser tarafa yanıltir.
    s.push_str(&def32("SINYAL_SYMFLAG_CONTRACT_DATA", sym_flag::CONTRACT_DATA));

    s.push_str("\n//--- Res.source: canli olay mi, mutabakat telafisi mi\n");
    s.push_str(&def("SINYAL_RESSRC_LIVE", res_source::LIVE));
    s.push_str(&def("SINYAL_RESSRC_RECONCILE", res_source::RECONCILE));

    s.push_str(
        "\n//--- ACCOUNT_TRADE_MODE karsiligi.\n\
         //    MT5'in ham int'i TELE YAZILMAZ: sayisal degerleri resmi dokumanda\n\
         //    YAYINLANMIYOR. EA switch/case ile bu degerlere cevirir; tanimadigi\n\
         //    degerde UNKNOWN yazar ve cekirdek UNKNOWN'i GERCEK HESAP gibi ele alir.\n",
    );
    s.push_str(&def("SINYAL_TRADEMODE_DEMO", trade_mode::DEMO));
    s.push_str(&def("SINYAL_TRADEMODE_CONTEST", trade_mode::CONTEST));
    s.push_str(&def("SINYAL_TRADEMODE_REAL", trade_mode::REAL));
    s.push_str(&def("SINYAL_TRADEMODE_UNKNOWN", trade_mode::UNKNOWN));

    s.push_str("\n//--- ACCOUNT_MARGIN_MODE karsiligi (hedging'de kapatma ticket ister)\n");
    s.push_str(&def("SINYAL_MARGINMODE_NETTING", margin_mode::NETTING));
    s.push_str(&def("SINYAL_MARGINMODE_EXCHANGE", margin_mode::EXCHANGE));
    s.push_str(&def("SINYAL_MARGINMODE_HEDGING", margin_mode::HEDGING));
    s.push_str(&def("SINYAL_MARGINMODE_UNKNOWN", margin_mode::UNKNOWN));

    s.push_str("\n//--- ACCOUNT_MARGIN_SO_MODE: so_call/so_so BIRIMI\n");
    s.push_str(&def("SINYAL_SOMODE_PERCENT", so_mode::PERCENT));
    s.push_str(&def("SINYAL_SOMODE_MONEY", so_mode::MONEY));
    s.push_str(&def("SINYAL_SOMODE_UNKNOWN", so_mode::UNKNOWN));

    s.push_str("\n//--- pozisyon/emir kaydi flags bitleri\n");
    s.push_str(&def("SINYAL_RECFLAG_SYMBOL_TRUNC", rec_flag::SYMBOL_TRUNCATED));
    s.push_str(&def("SINYAL_RECFLAG_COMMENT_TRUNC", rec_flag::COMMENT_TRUNCATED));

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

    s.push_str(
        "\n//--- SinyalBarRec.flags bitleri.\n\
         //    PARTIAL: bar OLUSMAKTA, close degismeye devam eder. CopyRates'in\n\
         //             son bari (start_pos=0) HER ZAMAN budur.\n\
         //    HAS_REAL_VOLUME: real_volume ANLAMLI. Bayrak yoksa 0 \"hacim sifir\"\n\
         //             DEGIL \"veri yok\" demektir — cogu forex brokeri yayinlamaz.\n\
         //    ERROR:   kayit veri degil HATA tasiyor; MQL5 hata kodu spread\n\
         //             alanindadir, total 0'dir.\n",
    );
    s.push_str(&def32("SINYAL_BARFLAG_PARTIAL", bar_flag::PARTIAL));
    s.push_str(&def32("SINYAL_BARFLAG_LAST", bar_flag::LAST));
    s.push_str(&def32("SINYAL_BARFLAG_HAS_REAL_VOLUME", bar_flag::HAS_REAL_VOLUME));
    s.push_str(&def32("SINYAL_BARFLAG_ERROR", bar_flag::ERROR));

    s.push_str(
        "\n//--- zaman dilimi = MT5 ENUM_TIMEFRAMES HAM degeri.\n\
         //    Kendi sirali enum'umuz DEGIL: service bu degeri dogrudan\n\
         //    CopyRates'e verecek ve aradaki her ceviri bir hata firsatidir.\n\
         //    DIKKAT: H1 = 16385, 60 DEGIL — dakika sanmak sessizce yanlis\n\
         //    dilim cekilmesine yol acar.\n",
    );
    s.push_str(&def32("SINYAL_TF_M1", timeframe::M1));
    s.push_str(&def32("SINYAL_TF_M5", timeframe::M5));
    s.push_str(&def32("SINYAL_TF_M15", timeframe::M15));
    s.push_str(&def32("SINYAL_TF_M30", timeframe::M30));
    s.push_str(&def32("SINYAL_TF_H1", timeframe::H1));
    s.push_str(&def32("SINYAL_TF_H4", timeframe::H4));
    s.push_str(&def32("SINYAL_TF_D1", timeframe::D1));

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
   ulong             client_id;      // +0   EA 0 birakir, cekirdek doldurur
   ulong             order;          // +8
   ulong             deal;           // +16
   ulong             position;       // +24
   ulong             position_by;    // +32  CLOSE_BY karsi pozisyon
   double            volume;         // +40
   double            price;          // +48
   double            bid;            // +56
   double            ask;            // +64
   ulong             recv_qpc;       // +72
   uint              retcode;        // +80  SEND_ACK'te 10008 = PLACED (DOLMADI!)
   uint              retcode_external; // +84
   uint              request_id;     // +88  SEND_ACK <-> TRADE_TXN bagi
   uint              reserved0;      // +92  dolgu
   uchar             comment[SINYAL_COMMENT_LEN]; // +96 (yalniz REQUEST'te dolu)
   uchar             kind;           // +128 SINYAL_RES_*
   uchar             txn_type;       // +129 ENUM_TRADE_TRANSACTION_TYPE
   uchar             order_state;    // +130 ENUM_ORDER_STATE
   uchar             deal_type;      // +131 ENUM_DEAL_TYPE
   uchar             order_type;     // +132 ENUM_ORDER_TYPE
   uchar             source;         // +133 SINYAL_RESSRC_*
   uchar             reserved1[2];   // +134
   uchar             reserved2[48];  // +136
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
//|                                                                  |
//| SÖZLEŞME/TAŞIMA BLOĞU (+{sym_swap_long_off}..) marjin ve gerçekçi PnL için        |
//| şarttır. Eski `reserved1`in içine yerleştiği için yapı boyutu     |
//| DEĞİŞMEDİ ve protokol sürümü artmadı; eski bir EA bu baytları     |
//| sıfır bırakır.                                                    |
//|                                                                  |
//| swap_long/swap_short'un BİRİMİ swap_mode'a bağlıdır — ham sayıyı  |
//| para sanma (SINYAL_SWAPMODE_* açıklamalarına bak).                |
//| margin_initial 0 ise broker sabit değer VERMEMİŞTİR; marjin       |
//| formülle bulunur: volume * contract_size * fiyat / leverage.      |
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
   double            swap_long;      // +{sym_swap_long_off} SYMBOL_SWAP_LONG (birim: swap_mode)
   double            swap_short;     // +{sym_swap_short_off} SYMBOL_SWAP_SHORT (birim: swap_mode)
   double            margin_initial; // +{sym_margin_initial_off} SYMBOL_MARGIN_INITIAL (0 -> formul)
   double            margin_hedged;  // +{sym_margin_hedged_off} SYMBOL_MARGIN_MAINTENANCE
   uint              swap_mode;      // +{sym_swap_mode_off} SINYAL_SWAPMODE_*
   uint              swap_rollover3d;// +{sym_swap_rollover_off} SINYAL_DOW_* (PAZAR=0)
   uchar             reserved1[{sym_reserved_len}];   // +{sym_reserved_off} gelecekte alan eklemek için
  }};

//+------------------------------------------------------------------+
//| Hesap durumu. {sz_account} bayt.                                        |
//|                                                                  |
//| String alanlar YALNIZCA OnInit'te doldurulur; sicak yolda string  |
//| donusumu yapilmaz. Sayisal alanlar saniyede bir tazelenir.        |
//|                                                                  |
//| trade_mode CANLI PARA KILIDININ dayanagidir. MT5'in ham int'i     |
//| TELE YAZILMAZ — EA switch/case ile SINYAL_TRADEMODE_* degerlerine |
//| cevirir, tanimadigi degerde UNKNOWN(255) yazar. Cekirdek UNKNOWN'i|
//| GERCEK HESAP gibi ele alir (emniyetli taraf).                     |
//|                                                                  |
//| margin_so_call/so_so BIRIMI so_mode'a baglidir (yuzde mi para mi).|
//+------------------------------------------------------------------+
struct SinyalAccount
  {{
   long              time_msc;              // +0
   double            balance;               // +8
   double            credit;                // +16
   double            profit;                // +24
   double            equity;                // +32
   double            margin;                // +40
   double            margin_free;           // +48
   double            margin_level;          // +56
   double            margin_so_call;        // +64
   double            margin_so_so;          // +72
   double            margin_initial;        // +80
   double            margin_maintenance;    // +88
   double            assets;                // +96
   double            liabilities;           // +104
   double            commission_blocked;    // +112
   long              login;                 // +120
   long              leverage;              // +128
   int               limit_orders;          // +136
   int               currency_digits;       // +140
   uchar             trade_mode;            // +144 SINYAL_TRADEMODE_*
   uchar             margin_mode;           // +145 SINYAL_MARGINMODE_*
   uchar             so_mode;               // +146 SINYAL_SOMODE_*
   uchar             trade_allowed;         // +147 ACCOUNT_TRADE_ALLOWED
   uchar             trade_expert;          // +148 ACCOUNT_TRADE_EXPERT
   uchar             terminal_trade_allowed;// +149 TERMINAL_TRADE_ALLOWED
   uchar             mql_trade_allowed;     // +150 MQL_TRADE_ALLOWED
   uchar             connected;             // +151 TERMINAL_CONNECTED
   uchar             fifo_close;            // +152
   uchar             hedge_allowed;         // +153
   uchar             str_truncated;         // +154
   uchar             reserved0[5];          // +155
   uchar             currency[16];          // +160
   uchar             server[64];            // +176
   uchar             company[96];           // +240
   uchar             name[96];              // +336
   uchar             reserved1[80];         // +432
  }};

//+------------------------------------------------------------------+
//| Acik pozisyon. {sz_position} bayt.                                      |
//|                                                                  |
//| ticket : emir gonderiminde kullanilacak anahtar                   |
//| identifier: gecmis/deal eslestirme anahtari — KAPATMA komutunda   |
//|             ticket kullanilir, identifier DEGIL                   |
//| POSITION_COMMISSION alani YOKTUR: MT5'te kullanimdan kalkti ve    |
//| daima 0 doner; komisyon yalnizca kapanista DEAL_COMMISSION'dan.   |
//+------------------------------------------------------------------+
struct SinyalPosition
  {{
   ulong             ticket;         // +0   POSITION_TICKET
   ulong             identifier;     // +8   POSITION_IDENTIFIER
   ulong             magic;          // +16  POSITION_MAGIC (= client_id)
   long              time_msc;       // +24  POSITION_TIME_MSC
   long              time_update_msc;// +32  POSITION_TIME_UPDATE_MSC
   double            volume;         // +40
   double            price_open;     // +48
   double            price_current;  // +56
   double            sl;             // +64
   double            tp;             // +72
   double            profit;         // +80
   double            swap;           // +88
   uchar             symbol[SINYAL_SYMBOL_NAME_LEN]; // +96
   uchar             comment[SINYAL_REC_COMMENT_LEN]; // +128
   uchar             kind;           // +160 0=BUY 1=SELL
   uchar             reason;         // +161 ENUM_POSITION_REASON
   uchar             digits;         // +162
   uchar             flags;          // +163 SINYAL_RECFLAG_*
   uchar             reserved0[28];  // +164
  }};

//+------------------------------------------------------------------+
//| Bekleyen emir. {sz_order} bayt.                                         |
//|                                                                  |
//| volume_current KALAN hacimdir; dolan = initial - current.         |
//| time_expiration SANIYE cinsindendir (ms varyanti yok).            |
//+------------------------------------------------------------------+
struct SinyalOrder
  {{
   ulong             ticket;         // +0   ORDER_TICKET
   ulong             magic;          // +8   ORDER_MAGIC (= client_id)
   ulong             position_id;    // +16  ORDER_POSITION_ID
   long              time_setup_msc; // +24
   long              time_expiration;// +32  SANIYE, 0 = suresiz
   double            volume_initial; // +40
   double            volume_current; // +48  KALAN
   double            price_open;     // +56
   double            price_stoplimit;// +64
   double            sl;             // +72
   double            tp;             // +80
   double            price_current;  // +88
   uchar             symbol[SINYAL_SYMBOL_NAME_LEN]; // +96
   uchar             comment[SINYAL_REC_COMMENT_LEN]; // +128
   uchar             kind;           // +160 ENUM_ORDER_TYPE
   uchar             state;          // +161 ENUM_ORDER_STATE
   uchar             type_time;      // +162
   uchar             type_filling;   // +163
   uchar             reason;         // +164 ENUM_ORDER_REASON
   uchar             digits;         // +165
   uchar             flags;          // +166
   uchar             reserved0[25];  // +167
  }};

//+------------------------------------------------------------------+
//| Geçmiş bar isteği (çekirdek -> SERVICE). {sz_histreq} bayt.                |
//|                                                                  |
//| Bu kanal EA'dan GEÇMEZ. CopyRates çağıran thread'i bloklar ve     |
//| zaman aşımı belgelenmemiştir (canlıda 30-60 sn donma raporları);  |
//| EA'nın tek thread'inde çağrılırsa tick toplama tamamen durur.     |
//| Bu yüzden isteği ayrı bir MQL5 Service karşılar.                  |
//|                                                                  |
//| to_msc  : bar açılış zamanı ÜST SINIRI (epoch ms). 0 = en son     |
//|           barlardan geriye doğru.                                 |
//| count   : service bunu TERMINAL_MAXBARS ile ayrıca KIRPAR.        |
//| symbol  : sembolün broker'daki GERÇEK adı, NUL dolgulu.           |
//|           Service grafiğe bağlı değildir; symbol_id'yi ada        |
//|           çevirebilmesinin ek FFI gerektirmeyen tek yolu budur.   |
//|           _reserved'in ilk {hist_symbol_len} baytını kaplar, yapı boyutu       |
//|           DEĞİŞMEZ.                                               |
//+------------------------------------------------------------------+
struct SinyalHistReq
  {{
   long              to_msc;         // +0   epoch MS, 0 = en yeniden geriye
   uint              req_id;         // +8   0 GEÇERSİZ
   uint              symbol_id;      // +12
   uint              timeframe;      // +16  SINYAL_TF_* (MT5 ham enum)
   uint              count;          // +20  istenen bar sayısı
   uchar             symbol[SINYAL_HIST_SYMBOL_LEN]; // +{hist_symbol_off} broker sembol adı
   uchar             reserved0[{hist_tail}];   // +{hist_tail_off}
  }};

//+------------------------------------------------------------------+
//| Tek geçmiş bar (SERVICE -> çekirdek). {sz_barrec} bayt.                   |
//|                                                                  |
//| time_msc MİLİSANİYEDİR; MqlRates.time ise SANİYE. Service 1000 ile|
//| ÇARPAR — çarpmazsa iki kaynağın bar sınırları 1000 kat kayar.     |
//|                                                                  |
//| spread MqlRates ile aynı şekilde int'tir (64 bit DEĞİL). ERROR    |
//| bayrağı kuruluysa bu alan bar verisi değil MQL5 HATA KODU taşır.  |
//|                                                                  |
//| index/total eksik teslimatı görünür kılar: halka dolarsa barlar   |
//| sessizce kaybolurdu, sayı tutmayınca fark edilir.                 |
//+------------------------------------------------------------------+
struct SinyalBarRec
  {{
   long              time_msc;       // +0   bar AÇILIŞI, epoch MS (saniye*1000)
   double            open;           // +8
   double            high;           // +16
   double            low;            // +24
   double            close;          // +32
   long              tick_volume;    // +40  tick sayısı — gerçek hacim DEĞİL
   long              real_volume;    // +48  yalnız HAS_REAL_VOLUME ile anlamlı
   uint              req_id;         // +56
   uint              symbol_id;      // +60
   uint              timeframe;      // +64  SINYAL_TF_*
   int               spread;         // +68  point; ERROR'da MQL5 hata kodu
   uint              flags;          // +72  SINYAL_BARFLAG_*
   uint              index;          // +76  0'dan başlar, ESKİDEN YENİYE
   uint              total;          // +80  bu isteğin toplam bar sayısı
   uint              reserved0;      // +84  dolgu
   uchar             reserved1[32];  // +88
  }};
"#,
        sym_swap_long_off = SYM_SWAP_LONG_OFF,
        sym_swap_short_off = SYM_SWAP_SHORT_OFF,
        sym_margin_initial_off = SYM_MARGIN_INITIAL_OFF,
        sym_margin_hedged_off = SYM_MARGIN_HEDGED_OFF,
        sym_swap_mode_off = SYM_SWAP_MODE_OFF,
        sym_swap_rollover_off = SYM_SWAP_ROLLOVER_OFF,
        sym_reserved_off = SYM_RESERVED_OFF,
        sym_reserved_len = SYM_RESERVED_LEN,
        hist_symbol_len = HIST_SYMBOL_LEN,
        hist_symbol_off = HIST_SYMBOL_OFF,
        hist_tail = size_of::<HistReq>() - HIST_SYMBOL_OFF - HIST_SYMBOL_LEN,
        hist_tail_off = HIST_SYMBOL_OFF + HIST_SYMBOL_LEN,
        sz_histreq = size_of::<HistReq>(),
        sz_barrec = size_of::<BarRec>(),
        sz_tick = size_of::<Tick>(),
        sz_booklevel = size_of::<BookLevel>(),
        sz_book = size_of::<Book>(),
        sz_cmd = size_of::<Cmd>(),
        sz_res = size_of::<Res>(),
        sz_symentry = size_of::<SymbolEntry>(),
        sz_account = size_of::<AccountSnapshot>(),
        sz_position = size_of::<PositionRec>(),
        sz_order = size_of::<OrderRec>(),
    ));

    s
}

// Ad alanı `<31` + AYRI bir boşluk; `<32` DEĞİL.
//
// Tam 32 karakterlik bir ad (`SINYAL_SWAPMODE_CURRENCY_DEPOSIT`) `<32` ile
// hiç boşluk almaz ve `#define SINYAL_SWAPMODE_CURRENCY_DEPOSIT4` üretilirdi:
// MQL5 bunu "gövdesi boş, adı ...DEPOSIT4 olan makro" diye kabul eder, hata
// VERMEZ, ve asıl sabit tanımsız kaldığı için EA derlenmez. 31+boşluk aynı
// sütun hizasını korur ama ayırıcıyı garanti eder.
fn def(name: &str, val: u8) -> String {
    format!("#define {name:<31} {val}\n")
}

fn def16(name: &str, val: u16) -> String {
    format!("#define {name:<31} 0x{val:02X}\n")
}

fn def32(name: &str, val: u32) -> String {
    format!("#define {name:<31} {val}\n")
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
                        "SINYAL_REC_COMMENT_LEN" => sinyal_proto::state::REC_COMMENT_LEN,
                        "SINYAL_HIST_SYMBOL_LEN" => HIST_SYMBOL_LEN,
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
        assert!(out
            .contains(&format!("#define SINYAL_SIZEOF_HISTREQ       {}", size_of::<HistReq>())));
        assert!(out
            .contains(&format!("#define SINYAL_SIZEOF_BARREC        {}", size_of::<BarRec>())));
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
            "SinyalAccount",
            "SinyalPosition",
            "SinyalOrder",
            "SinyalHistReq",
            "SinyalBarRec",
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
        let cases: [(&str, usize); 11] = [
            ("SinyalTick", size_of::<Tick>()),
            ("SinyalBookLevel", size_of::<BookLevel>()),
            ("SinyalBook", size_of::<Book>()),
            ("SinyalCmd", size_of::<Cmd>()),
            ("SinyalRes", size_of::<Res>()),
            ("SinyalSymbolEntry", size_of::<SymbolEntry>()),
            ("SinyalAccount", size_of::<AccountSnapshot>()),
            ("SinyalPosition", size_of::<PositionRec>()),
            ("SinyalOrder", size_of::<OrderRec>()),
            // Geçmiş kanalı: sembol adı `_reserved`'in ilk 32 baytına
            // BÖLÜNEREK taşındığı için MQL5 gövdesi Rust'takinden farklı
            // GÖRÜNÜR; boyut aynı kalmazsa service yanlış ofsetten okur.
            ("SinyalHistReq", size_of::<HistReq>()),
            ("SinyalBarRec", size_of::<BarRec>()),
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

    /// Sözleşme/taşıma bloğu boyut testiyle KORUNMUYOR — ofsetleri ayrıca doğrula.
    ///
    /// Bu alanlar eski `_reserved`in içine yerleşti, yani `SymbolEntry` hâlâ
    /// 192 bayt. `generated_mql5_structs_have_the_same_size_as_rust` yalnızca
    /// TOPLAMI karşılaştırdığı için, iki alanın yeri değişse veya biri MQL5
    /// gövdesinden hiç yazılmasa o test YEŞİL kalırdı — ve çekirdek `swap_mode`
    /// diye `margin_hedged`in üst yarısını okurdu.
    #[test]
    fn symbol_entry_contract_block_matches_rust_offsets() {
        let out = render();
        for (decl, off) in [
            ("double            swap_long;      ", SYM_SWAP_LONG_OFF),
            ("double            swap_short;     ", SYM_SWAP_SHORT_OFF),
            ("double            margin_initial; ", SYM_MARGIN_INITIAL_OFF),
            ("double            margin_hedged;  ", SYM_MARGIN_HEDGED_OFF),
            ("uint              swap_mode;      ", SYM_SWAP_MODE_OFF),
            ("uint              swap_rollover3d;", SYM_SWAP_ROLLOVER_OFF),
        ] {
            let want = format!("{decl}// +{off}");
            assert!(out.contains(&want), "başlıkta eksik veya ofseti kaymış: {want}");
        }

        // Ofsetlerin KENDİSİ de doğru olmalı; yoksa yukarıdaki döngü kaymış bir
        // yerleşimi kendi kendine onaylardı (iki taraf da offset_of!'tan geliyor).
        assert_eq!(SYM_SWAP_LONG_OFF, 144, "blok `name`den (112+32) hemen sonra başlamalı");
        assert_eq!(SYM_SWAP_SHORT_OFF, 152);
        assert_eq!(SYM_MARGIN_INITIAL_OFF, 160);
        assert_eq!(SYM_MARGIN_HEDGED_OFF, 168);
        assert_eq!(SYM_SWAP_MODE_OFF, 176);
        assert_eq!(SYM_SWAP_ROLLOVER_OFF, 180);

        // Kaydın toplam boyutu DEĞİŞMEMELİ: değişseydi RING_VERSION artmalıydı
        // ve bu test o kararı unutmayı yakalayan yer.
        assert_eq!(size_of::<SymbolEntry>(), 192, "boyut değiştiyse RING_VERSION artmalı");
        assert_eq!(SYM_RESERVED_OFF + SYM_RESERVED_LEN, size_of::<SymbolEntry>());
    }

    /// GOLD'un contract_size'ı 100'dür, forex'inki 100000 — marjin formülünün
    /// tek girdisi bu ve alan tabloda GERÇEKTEN taşınabilmeli.
    #[test]
    fn contract_and_margin_fields_survive_a_roundtrip() {
        let mut e = SymbolEntry { contract_size: 100.0, ..Default::default() };
        e.swap_long = -12.5;
        e.swap_short = 3.75;
        e.swap_mode = sinyal_proto::swap_mode::POINTS;
        e.swap_rollover3d = sinyal_proto::day_of_week::WEDNESDAY;
        e.margin_initial = 0.0; // 0 = formüle düş
        e.margin_hedged = 50.0;

        // MQL5 tarafı bu kaydı BAYT OFSETİYLE okur, alan adıyla değil. Bu
        // yüzden testi de baytlar üzerinden yapıyoruz: alanın Rust'ta doğru
        // değeri tutması yetmez, doğru BAYTTA durması gerekir.
        let raw = unsafe {
            core::slice::from_raw_parts(
                (&e as *const SymbolEntry).cast::<u8>(),
                size_of::<SymbolEntry>(),
            )
        };
        let f64_at = |off: usize| f64::from_le_bytes(raw[off..off + 8].try_into().unwrap());
        let u32_at = |off: usize| u32::from_le_bytes(raw[off..off + 4].try_into().unwrap());

        assert_eq!(f64_at(24), 100.0, "contract_size +24 — GOLD 100, forex 100000");
        assert_eq!(f64_at(SYM_SWAP_LONG_OFF), -12.5);
        assert_eq!(f64_at(SYM_SWAP_SHORT_OFF), 3.75);
        assert_eq!(f64_at(SYM_MARGIN_INITIAL_OFF), 0.0, "0 = sabit marjin yok, formüle düş");
        assert_eq!(f64_at(SYM_MARGIN_HEDGED_OFF), 50.0);
        assert_eq!(u32_at(SYM_SWAP_MODE_OFF), sinyal_proto::swap_mode::POINTS);
        assert_eq!(u32_at(SYM_SWAP_ROLLOVER_OFF), 3, "Çarşamba = 3 (PAZAR=0)");

        // Ad alanı bu bloğun HEMEN ÖNÜNDE duruyor — taşarsa swap_long'un üstüne
        // yazardı ve hata "swap saçma büyük" diye çok geç fark edilirdi.
        let mut named = SymbolEntry::default();
        sinyal_proto::write_fixed_str(&mut named.name, "XAUUSD.somewhatlongsuffix.pro");
        named.swap_long = -1.0;
        assert_eq!(named.swap_long, -1.0);
        assert_eq!(named.name_str(), "XAUUSD.somewhatlongsuffix.pro");
    }

    /// Her `#define` adı ile değeri arasında BOŞLUK olmalı.
    ///
    /// Bu gerçekten oldu: ad alanı `<32` genişlikteydi ve tam 32 karakterlik
    /// `SINYAL_SWAPMODE_CURRENCY_DEPOSIT` değerine yapışıp
    /// `#define SINYAL_SWAPMODE_CURRENCY_DEPOSIT4` üretti. MQL5 bunu geçerli
    /// (gövdesiz) bir makro sayar, HATA VERMEZ — asıl sabit tanımsız kalır ve
    /// EA "bilinmeyen tanımlayıcı" ile derlenmez. Uzun bir ad eklendiğinde
    /// tekrarlanmaması için ad uzunluğundan bağımsız test ediyoruz.
    #[test]
    fn every_define_separates_name_from_value() {
        let out = render();
        for line in out.lines() {
            let Some(rest) = line.strip_prefix("#define ") else { continue };
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            assert!(
                tokens.len() >= 2,
                "ad ile değer yapışmış (ad {} karakter?): {line}",
                rest.len()
            );
            assert!(
                tokens[0].chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'),
                "makro adında beklenmeyen karakter: {line}"
            );
        }
    }

    #[test]
    fn swap_and_dayofweek_constants_are_emitted() {
        // Bu sabitler olmadan `swap_long`ın birimi tahmin edilirdi: POINTS ile
        // CURRENCY_* arasındaki fark, swap'ta büyüklük mertebesi farkıdır.
        let out = render();
        for c in [
            "SINYAL_SWAPMODE_DISABLED",
            "SINYAL_SWAPMODE_POINTS",
            "SINYAL_SWAPMODE_CURRENCY_SYMBOL",
            "SINYAL_SWAPMODE_CURRENCY_MARGIN",
            "SINYAL_SWAPMODE_CURRENCY_DEPOSIT",
            "SINYAL_SWAPMODE_INTEREST_CURRENT",
            "SINYAL_SWAPMODE_INTEREST_OPEN",
            "SINYAL_SWAPMODE_REOPEN_CURRENT",
            "SINYAL_SWAPMODE_REOPEN_BID",
            "SINYAL_DOW_WEDNESDAY",
        ] {
            assert!(out.contains(c), "{c} eksik");
        }
        // Haftanın PAZAR ile başladığı başlıkta açıkça yazmalı — bir gün
        // kaydırmak üç günlük swap'ı yanlış güne yazar.
        assert!(out.contains("PAZAR (0)"));
        // Değerlerin kendisi de doğru olmalı, yalnızca varlığı değil.
        assert!(out.contains(&format!(
            "#define SINYAL_SWAPMODE_POINTS          {}",
            sinyal_proto::swap_mode::POINTS
        )));
        assert!(out.contains(&format!(
            "#define SINYAL_DOW_WEDNESDAY            {}",
            sinyal_proto::day_of_week::WEDNESDAY
        )));
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
            "SINYAL_SYMFLAG_CHART",
            "SINYAL_SYMFLAG_CONTRACT_DATA",
        ] {
            assert!(out.contains(c), "{c} eksik");
        }
        // RETURN'ün maskede OLMADIĞI başlıkta açıkça yazmalı.
        assert!(out.contains("RETURN bu maskede YOKTUR"));
    }

    #[test]
    fn hist_channel_constants_are_emitted() {
        // Service bu sabitler olmadan bar yazamaz: bayrak yoksa "oluşmakta
        // olan bar" ile "kapanmış bar" ayırt edilemez, dilim sabiti yoksa
        // CopyRates'e verilecek değer elle uydurulur.
        let out = render();
        for c in [
            "SINYAL_BARFLAG_PARTIAL",
            "SINYAL_BARFLAG_LAST",
            "SINYAL_BARFLAG_HAS_REAL_VOLUME",
            "SINYAL_BARFLAG_ERROR",
            "SINYAL_TF_M1",
            "SINYAL_TF_M5",
            "SINYAL_TF_M15",
            "SINYAL_TF_M30",
            "SINYAL_TF_H1",
            "SINYAL_TF_H4",
            "SINYAL_TF_D1",
            "SINYAL_HIST_SYMBOL_LEN",
        ] {
            assert!(out.contains(c), "{c} eksik");
        }
    }

    #[test]
    fn timeframe_defines_carry_raw_mt5_enum_values() {
        // H1'i 60 sanmak (dakika) sessizce YANLIŞ dilim çekilmesine yol açar
        // ve grafikte hiçbir hata görünmez — bu yüzden değerin kendisi test
        // edilir, yalnızca varlığı değil.
        let out = render();
        assert!(out.contains(&format!("#define SINYAL_TF_H1                    {}", timeframe::H1)));
        assert!(out.contains(&format!("#define SINYAL_TF_D1                    {}", timeframe::D1)));
        assert!(out.contains("H1 = 16385, 60 DEGIL"));
    }

    #[test]
    fn hist_req_symbol_field_matches_the_rust_offset() {
        // MQL5 gövdesindeki ofset yorumu ile Rust'taki GERÇEK ofset aynı
        // olmalı. Boyut testi yalnızca TOPLAMI karşılaştırdığı için, aynı
        // boyutta iki alanın yeri değişse o test GEÇER ve sembol adı isteğin
        // ortasından okunmaya başlardı.
        assert!(
            HIST_SYMBOL_OFF + HIST_SYMBOL_LEN <= size_of::<HistReq>(),
            "sembol alanı HistReq'in dışına taşıyor"
        );
        let out = render();
        assert!(out.contains(&format!(
            "uchar             symbol[SINYAL_HIST_SYMBOL_LEN]; // +{HIST_SYMBOL_OFF}"
        )));

        // Ofsetin kendisi de doğru olmalı: to_msc(8) + dört uint(16) = 24.
        assert_eq!(HIST_SYMBOL_OFF, 24);
        // Ve adı yazıp okumak gerçekten çalışmalı — alan var ama kullanılamaz
        // olsaydı çekirdek adı hiç doldurmazdı (bu daha önce oldu).
        let mut r = HistReq::default();
        assert!(r.set_symbol("EURUSD.pro"));
        assert_eq!(r.symbol_str(), "EURUSD.pro");
    }

    #[test]
    fn bar_time_unit_conversion_is_documented_in_header() {
        // MqlRates.time SANİYE, BarRec.time_msc MİLİSANİYE. Bu dönüşüm
        // unutulursa bar sınırları 1000 kat kayar ve hata sessizdir.
        let out = render();
        assert!(out.contains("MqlRates.time ise SANİYE"));
    }
}
