//+------------------------------------------------------------------+
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
#define SINYAL_PROTO_VERSION        3

//--- boyut sınırları
#define SINYAL_MAX_BOOK_DEPTH       32
#define SINYAL_COMMENT_LEN          32
#define SINYAL_SYMBOL_NAME_LEN      32
#define SINYAL_REC_COMMENT_LEN      32
#define SINYAL_MAX_POSITIONS        512
#define SINYAL_MAX_ORDERS           512

//--- beklenen yapı boyutları (EA açılışta DLL ile karşılaştırır)
#define SINYAL_SIZEOF_TICK          56
#define SINYAL_SIZEOF_BOOKLEVEL     24
#define SINYAL_SIZEOF_BOOK          824
#define SINYAL_SIZEOF_CMD           184
#define SINYAL_SIZEOF_RES           184
#define SINYAL_SIZEOF_SYMBOLENTRY   192
#define SINYAL_SIZEOF_ACCOUNT       512
#define SINYAL_SIZEOF_POSITION      192
#define SINYAL_SIZEOF_ORDER         192

//--- mesaj türü (Tick.kind / Book.kind)
#define SINYAL_KIND_TICK                1
#define SINYAL_KIND_BOOK                2
#define SINYAL_KIND_SNAPSHOT            3

//--- derinlik seviyesi türü (MT5 ENUM_BOOK_TYPE ile birebir)
#define SINYAL_BOOK_SELL                0
#define SINYAL_BOOK_BUY                 1
#define SINYAL_BOOK_SELL_MARKET         2
#define SINYAL_BOOK_BUY_MARKET          3

//--- tick bayrakları (MT5 TICK_FLAG_* ile birebir)
#define SINYAL_TICK_FLAG_BID            0x02
#define SINYAL_TICK_FLAG_ASK            0x04
#define SINYAL_TICK_FLAG_LAST           0x08
#define SINYAL_TICK_FLAG_VOLUME         0x10
#define SINYAL_TICK_FLAG_BUY            0x20
#define SINYAL_TICK_FLAG_SELL           0x40

//--- komut eylemi
#define SINYAL_ACTION_DEAL              1
#define SINYAL_ACTION_PENDING           2
#define SINYAL_ACTION_SLTP              3
#define SINYAL_ACTION_MODIFY            4
#define SINYAL_ACTION_REMOVE            5
#define SINYAL_ACTION_CLOSE_BY          6
#define SINYAL_ACTION_CLOSE_POSITION    7

//--- emir tipi (MT5 ENUM_ORDER_TYPE ile birebir)
#define SINYAL_ORDER_BUY                0
#define SINYAL_ORDER_SELL               1
#define SINYAL_ORDER_BUY_LIMIT          2
#define SINYAL_ORDER_SELL_LIMIT         3
#define SINYAL_ORDER_BUY_STOP           4
#define SINYAL_ORDER_SELL_STOP          5
#define SINYAL_ORDER_BUY_STOP_LIMIT     6
#define SINYAL_ORDER_SELL_STOP_LIMIT    7

//--- doldurma modu.
//    AUTO cozumlemesi SYMBOL_FILLING_MODE maskesine BAKARAK YAPILAMAZ:
//    RETURN maskede yer almaz ve INSTANT/REQUEST modunda FOK maskeden
//    bagimsiz izinlidir. Belirleyici girdi SYMBOL_TRADE_EXEMODE'dur.
#define SINYAL_FILLING_FOK              0
#define SINYAL_FILLING_IOC              1
#define SINYAL_FILLING_RETURN           2
#define SINYAL_FILLING_BOC              3
#define SINYAL_FILLING_AUTO             255

//--- SYMBOL_FILLING_MODE maske bitleri (RETURN bu maskede YOKTUR)
#define SINYAL_FILLMASK_FOK             1
#define SINYAL_FILLMASK_IOC             2
#define SINYAL_FILLMASK_BOC             4

//--- SYMBOL_TRADE_EXEMODE (dogru doldurma modunun belirleyicisi)
#define SINYAL_EXEMODE_REQUEST          0
#define SINYAL_EXEMODE_INSTANT          1
#define SINYAL_EXEMODE_MARKET           2
#define SINYAL_EXEMODE_EXCHANGE         3

//--- SymbolEntry.flags bitleri
#define SINYAL_SYMFLAG_BOOK_SUBSCRIBED  1
#define SINYAL_SYMFLAG_READY            2
#define SINYAL_SYMFLAG_POLLED_ONLY      4

//--- Res.source: canli olay mi, mutabakat telafisi mi
#define SINYAL_RESSRC_LIVE              0
#define SINYAL_RESSRC_RECONCILE         1

//--- ACCOUNT_TRADE_MODE karsiligi.
//    MT5'in ham int'i TELE YAZILMAZ: sayisal degerleri resmi dokumanda
//    YAYINLANMIYOR. EA switch/case ile bu degerlere cevirir; tanimadigi
//    degerde UNKNOWN yazar ve cekirdek UNKNOWN'i GERCEK HESAP gibi ele alir.
#define SINYAL_TRADEMODE_DEMO           0
#define SINYAL_TRADEMODE_CONTEST        1
#define SINYAL_TRADEMODE_REAL           2
#define SINYAL_TRADEMODE_UNKNOWN        255

//--- ACCOUNT_MARGIN_MODE karsiligi (hedging'de kapatma ticket ister)
#define SINYAL_MARGINMODE_NETTING       0
#define SINYAL_MARGINMODE_EXCHANGE      1
#define SINYAL_MARGINMODE_HEDGING       2
#define SINYAL_MARGINMODE_UNKNOWN       255

//--- ACCOUNT_MARGIN_SO_MODE: so_call/so_so BIRIMI
#define SINYAL_SOMODE_PERCENT           0
#define SINYAL_SOMODE_MONEY             1
#define SINYAL_SOMODE_UNKNOWN           255

//--- pozisyon/emir kaydi flags bitleri
#define SINYAL_RECFLAG_SYMBOL_TRUNC     1
#define SINYAL_RECFLAG_COMMENT_TRUNC    2

//--- geçerlilik süresi (MT5 ENUM_ORDER_TYPE_TIME ile birebir)
#define SINYAL_TIME_GTC                 0
#define SINYAL_TIME_DAY                 1
#define SINYAL_TIME_SPECIFIED           2
#define SINYAL_TIME_SPECIFIED_DAY       3

//--- sonuç türü
#define SINYAL_RES_SEND_ACK             1
#define SINYAL_RES_TRADE_TXN            2
#define SINYAL_RES_REJECTED             3
#define SINYAL_RES_DUPLICATE            4

//+------------------------------------------------------------------+
//| Tek piyasa tick'i. 56 bayt.                                      |
//|                                                                  |
//| time_msc : broker sunucu saati (epoch ms) — sıralama/pencereleme  |
//| recv_qpc : yakalama anındaki YEREL QueryPerformanceCounter —      |
//|            gecikme ölçümü buradan yapılır, broker saatinden DEĞİL |
//| last/volume_real: CFD/forex'te 0 olabilir                         |
//+------------------------------------------------------------------+
struct SinyalTick
  {
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
  };

//+------------------------------------------------------------------+
//| Derinlik tablosunda tek seviye. 24 bayt.                        |
//+------------------------------------------------------------------+
struct SinyalBookLevel
  {
   double            price;          // +0
   double            volume_real;    // +8
   uchar             kind;           // +16  SINYAL_BOOK_*
   uchar             reserved0[7];   // +17  dolgu
  };

//+------------------------------------------------------------------+
//| Derinlik anlık görüntüsü (artımlı DEĞİL, tam tablo). 824 bayt.    |
//|                                                                  |
//| depth     : levels[] içinde geçerli seviye sayısı                 |
//| truncated : broker SINYAL_MAX_BOOK_DEPTH'ten fazla seviye verdi   |
//|             ve en iyi fiyatlardan başlayarak kesildi              |
//+------------------------------------------------------------------+
struct SinyalBook
  {
   long              time_msc;       // +0
   ulong             recv_qpc;       // +8
   uint              symbol_id;      // +16
   ushort            depth;          // +20
   uchar             kind;           // +22  SINYAL_KIND_BOOK
   uchar             truncated;      // +23
   SinyalBookLevel   levels[SINYAL_MAX_BOOK_DEPTH]; // +24
   uchar             reserved0[32];  // dolgu
  };

//+------------------------------------------------------------------+
//| Çekirdekten EA'ya emir komutu. 184 bayt.                          |
//|                                                                  |
//| client_id : idempotency anahtarı. Aynı değer ikinci kez gelirse   |
//|             EA TEKRAR YÜRÜTMEZ — çift emir koruması.              |
//| filling   : SINYAL_FILLING_AUTO ise EA sembolün maskesinden seçer |
//| price     : DEAL için 0 → EA güncel bid/ask kullanır              |
//+------------------------------------------------------------------+
struct SinyalCmd
  {
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
  };

//+------------------------------------------------------------------+
//| EA'dan çekirdeğe emir sonucu / ticaret olayı. 184 bayt.           |
//|                                                                  |
//| OrderSendAsync İKİ aşamalı geri bildirim üretir:                  |
//|   SEND_ACK  → sunucu isteği kuyruğa aldı (henüz DOLMADI)          |
//|   TRADE_TXN → gerçek yürütme (OnTradeTransaction)                 |
//| Emri TRADE_TXN gelmeden dolmuş sayma.                             |
//|                                                                  |
//| client_id : bizim göndermediğimiz bir işlemse (ör. elle kapatma) 0|
//+------------------------------------------------------------------+
struct SinyalRes
  {
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
  };

//+------------------------------------------------------------------+
//| Sembol tablosu kaydı. 192 bayt.                                  |
//|                                                                  |
//| Çekirdeğin emir doğrulaması (hacim yuvarlama, doldurma modu       |
//| seçimi, fiyat normalizasyonu) TAMAMEN buna dayanır — broker       |
//| özellikleri asla tahmin edilmez.                                  |
//|                                                                  |
//| name : broker'daki GERÇEK ad (GOLD#, XAUUSD.m ...). Kanonik ad    |
//|        eşlemesi çekirdek yapılandırmasında yapılır.               |
//+------------------------------------------------------------------+
struct SinyalSymbolEntry
  {
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
  };

//+------------------------------------------------------------------+
//| Hesap durumu. 512 bayt.                                        |
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
  {
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
  };

//+------------------------------------------------------------------+
//| Acik pozisyon. 192 bayt.                                      |
//|                                                                  |
//| ticket : emir gonderiminde kullanilacak anahtar                   |
//| identifier: gecmis/deal eslestirme anahtari — KAPATMA komutunda   |
//|             ticket kullanilir, identifier DEGIL                   |
//| POSITION_COMMISSION alani YOKTUR: MT5'te kullanimdan kalkti ve    |
//| daima 0 doner; komisyon yalnizca kapanista DEAL_COMMISSION'dan.   |
//+------------------------------------------------------------------+
struct SinyalPosition
  {
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
  };

//+------------------------------------------------------------------+
//| Bekleyen emir. 192 bayt.                                         |
//|                                                                  |
//| volume_current KALAN hacimdir; dolan = initial - current.         |
//| time_expiration SANIYE cinsindendir (ms varyanti yok).            |
//+------------------------------------------------------------------+
struct SinyalOrder
  {
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
  };
