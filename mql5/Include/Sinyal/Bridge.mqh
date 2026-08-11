//+------------------------------------------------------------------+
//|                                                       Bridge.mqh |
//|      sinyal_bridge.dll C ABI bildirimleri + yerleşim doğrulaması  |
//|                                                                  |
//| Protocol.mqh ÜRETİLMİŞTİR, bu dosya elle yazılır.                 |
//+------------------------------------------------------------------+
#property strict

#include <Sinyal/Protocol.mqh>

//+------------------------------------------------------------------+
//| DLL bildirimleri                                                  |
//|                                                                  |
//| DLL 'MQL5\Libraries\sinyal_bridge.dll' altında olmalı ve x64      |
//| derlenmiş olmalıdır (MT5 build 2361'den beri 32-bit terminal yok).|
//| #import'ta sürücü harfli tam yol YAZILMAZ.                        |
//|                                                                  |
//| Tip eşlemesi: uint64->ulong, int64->long, uint32->uint,           |
//| uint16->ushort, uint8->uchar. 'bool' FFI sınırında KULLANILMAZ.   |
//+------------------------------------------------------------------+
#import "sinyal_bridge.dll"
// Yaşam döngüsü. Kapasite 0 verilirse varsayılan kullanılır.
long  SinyalOpen(string instance, uint tick_cap, uint book_cap,
                 uint cmd_cap, uint res_cap, uint sym_cap);
int   SinyalClose(long h);
int   SinyalSizeof(int what);
ulong SinyalQpc(void);
ulong SinyalQpcFrequency(void);
// Hata metni UTF-16 olarak yazılır; 'string&' DEĞİL 'ushort&[]' kullanılır.
int   SinyalLastError(ushort &buf[], int cap);

// Piyasa verisi (EA -> çekirdek)
int   SinyalPushTick(long h, uint symbol_id, long time_msc,
                     double bid, double ask, double last, double volume_real,
                     ushort flags, uchar kind);
int   SinyalPushBook(long h, uint symbol_id, long time_msc,
                     const double &prices[], const double &volumes[],
                     const uchar &kinds[], int depth);
int   SinyalSetSymbols(long h, const SinyalSymbolEntry &entries[], int count);

// Emirler
int   SinyalPopCmd(long h, SinyalCmd &cmd);
int   SinyalPushRes(long h, const SinyalRes &res);

// Teşhis
ulong SinyalTickPushFailures(long h);
ulong SinyalTickBacklog(long h);
#import

//--- dönüş kodları (Rust ffi.rs ile birebir)
#define SINYAL_OK               1
#define SINYAL_WOULD_BLOCK      0    // halka dolu/boş — HATA DEĞİL
#define SINYAL_ERR_HANDLE      -1
#define SINYAL_ERR_PANIC       -2
#define SINYAL_ERR_ARGS        -3
#define SINYAL_ERR_OTHER       -4

//--- SinyalSizeof() sorgu kodları
#define SINYAL_WHAT_TICK          1
#define SINYAL_WHAT_BOOKLEVEL     2
#define SINYAL_WHAT_BOOK          3
#define SINYAL_WHAT_CMD           4
#define SINYAL_WHAT_RES           5
#define SINYAL_WHAT_SYMBOLENTRY   6
#define SINYAL_WHAT_PROTOVERSION  100

//+------------------------------------------------------------------+
//| Köprünün son hata metnini oku.                                    |
//+------------------------------------------------------------------+
string SinyalErrorText()
  {
   ushort buf[];
   ArrayResize(buf, 512);
   int n = SinyalLastError(buf, 512);
   if(n <= 0)
      return "(hata metni yok)";
   return ShortArrayToString(buf, 0, n);
  }

//+------------------------------------------------------------------+
//| DLL içe aktarma izinlerini denetle.                               |
//|                                                                  |
//| İki ayrı ayar var ve ikisi de gerekli: terminal geneli ve EA      |
//| özelinde. Yalnız birini kontrol etmek, kullanıcıyı yanlış yere    |
//| bakmaya gönderir.                                                 |
//+------------------------------------------------------------------+
bool SinyalCheckDllPermission(string &problem)
  {
   if(!TerminalInfoInteger(TERMINAL_DLLS_ALLOWED))
     {
      problem = "Terminal DLL içe aktarmaya izin vermiyor. "
                "Araçlar > Seçenekler > Uzman Danışmanlar > "
                "'Allow DLL imports' işaretleyin.";
      return false;
     }
   if(!MQLInfoInteger(MQL_DLLS_ALLOWED))
     {
      problem = "Bu EA için DLL içe aktarma kapalı. EA'yı grafikten kaldırıp "
                "yeniden ekleyin ve açılan kutuda 'Allow DLL imports' "
                "işaretleyin.";
      return false;
     }
   return true;
  }

//+------------------------------------------------------------------+
//| Rust ile MQL5 yapı yerleşiminin aynı olduğunu doğrula.            |
//|                                                                  |
//| Protocol.mqh üretilmiş olsa bile iki taraf FARKLI SÜRÜMLE         |
//| derlenmiş olabilir (ör. DLL güncellendi, .ex5 eski kaldı). O      |
//| durumda sessizce bozuk fiyat/emir verisi okunur — bu yüzden EA    |
//| başlamayı reddetmelidir. Gürültülü durmak, sessiz bozulmaktan     |
//| çok daha iyidir.                                                  |
//+------------------------------------------------------------------+
bool SinyalVerifyLayout(string &problem)
  {
   int ver = SinyalSizeof(SINYAL_WHAT_PROTOVERSION);
   if(ver != SINYAL_PROTO_VERSION)
     {
      problem = StringFormat(
         "Protokol sürümü uyumsuz: DLL=%d, EA=%d. "
         "sinyal_bridge.dll ile SinyalCollector.ex5 farklı sürümlerden. "
         "deploy.ps1 ile ikisini birlikte yenileyin.",
         ver, SINYAL_PROTO_VERSION);
      return false;
     }

   int    want[6];
   int    what[6];
   string name[6];

   what[0] = SINYAL_WHAT_TICK;         want[0] = SINYAL_SIZEOF_TICK;         name[0] = "SinyalTick";
   what[1] = SINYAL_WHAT_BOOKLEVEL;    want[1] = SINYAL_SIZEOF_BOOKLEVEL;    name[1] = "SinyalBookLevel";
   what[2] = SINYAL_WHAT_BOOK;         want[2] = SINYAL_SIZEOF_BOOK;         name[2] = "SinyalBook";
   what[3] = SINYAL_WHAT_CMD;          want[3] = SINYAL_SIZEOF_CMD;          name[3] = "SinyalCmd";
   what[4] = SINYAL_WHAT_RES;          want[4] = SINYAL_SIZEOF_RES;          name[4] = "SinyalRes";
   what[5] = SINYAL_WHAT_SYMBOLENTRY;  want[5] = SINYAL_SIZEOF_SYMBOLENTRY;  name[5] = "SinyalSymbolEntry";

   for(int i = 0; i < 6; i++)
     {
      int dll_size = SinyalSizeof(what[i]);
      if(dll_size != want[i])
        {
         problem = StringFormat("%s boyutu uyumsuz: DLL=%d, başlık=%d",
                                name[i], dll_size, want[i]);
         return false;
        }
     }

   // Başlıktaki sabitler ile MQL5 derleyicisinin gerçekten ürettiği
   // yerleşim de örtüşmeli. pack(1) varsayımı bozulursa burada yakalanır.
   SinyalTick        t;
   SinyalBookLevel   bl;
   SinyalBook        bk;
   SinyalCmd         c;
   SinyalRes         r;
   SinyalSymbolEntry se;

   if(sizeof(t) != SINYAL_SIZEOF_TICK ||
      sizeof(bl) != SINYAL_SIZEOF_BOOKLEVEL ||
      sizeof(bk) != SINYAL_SIZEOF_BOOK ||
      sizeof(c) != SINYAL_SIZEOF_CMD ||
      sizeof(r) != SINYAL_SIZEOF_RES ||
      sizeof(se) != SINYAL_SIZEOF_SYMBOLENTRY)
     {
      problem = StringFormat(
         "MQL5 derleyicisinin ürettiği yerleşim başlıktan farklı "
         "(Tick=%d/%d Book=%d/%d Cmd=%d/%d Res=%d/%d Sym=%d/%d). "
         "Protocol.mqh elle düzenlenmiş olabilir — gen-mqh ile yenileyin.",
         sizeof(t), SINYAL_SIZEOF_TICK,
         sizeof(bk), SINYAL_SIZEOF_BOOK,
         sizeof(c), SINYAL_SIZEOF_CMD,
         sizeof(r), SINYAL_SIZEOF_RES,
         sizeof(se), SINYAL_SIZEOF_SYMBOLENTRY);
      return false;
     }

   return true;
  }

//+------------------------------------------------------------------+
//| Doğru MT5 doldurma modunu seç.                                    |
//|                                                                  |
//| SYMBOL_FILLING_MODE maskesine BAKARAK yapılamaz: RETURN maskede   |
//| yer almaz ve INSTANT/REQUEST modunda FOK maskeden bağımsız        |
//| izinlidir. Belirleyici girdi SYMBOL_TRADE_EXEMODE'dur.            |
//|                                                                  |
//| Eski adaptör ORDER_FILLING_IOC'yi sabit kodladığı için IOC        |
//| desteklemeyen sembollerde emirler 10030 ile reddediliyordu.       |
//|                                                                  |
//| Uygun mod yoksa TAHMİN ETMEZ, false döner — yanlış modla gönderip |
//| broker'dan red almak, sebebi loglardan anlaşılmayan bir hatadır.  |
//+------------------------------------------------------------------+
bool SinyalResolveFilling(uchar action, uchar requested,
                          uint exemode, uint fillmask,
                          ENUM_ORDER_TYPE_FILLING &out)
  {
   if(requested != SINYAL_FILLING_AUTO)
     {
      switch(requested)
        {
         case SINYAL_FILLING_FOK:    out = ORDER_FILLING_FOK;    return true;
         case SINYAL_FILLING_IOC:    out = ORDER_FILLING_IOC;    return true;
         case SINYAL_FILLING_RETURN: out = ORDER_FILLING_RETURN; return true;
         case SINYAL_FILLING_BOC:    out = ORDER_FILLING_BOC;    return true;
         default: return false;
        }
     }

   // Bekleyen emirlerde yürütme tipine bakılmaz.
   if(action == SINYAL_ACTION_PENDING ||
      action == SINYAL_ACTION_MODIFY  ||
      action == SINYAL_ACTION_REMOVE  ||
      action == SINYAL_ACTION_SLTP)
     {
      out = ORDER_FILLING_RETURN;
      return true;
     }

   bool has_fok = (fillmask & SINYAL_FILLMASK_FOK) != 0;
   bool has_ioc = (fillmask & SINYAL_FILLMASK_IOC) != 0;

   if(exemode == SINYAL_EXEMODE_INSTANT || exemode == SINYAL_EXEMODE_REQUEST)
     {
      // Bu modlarda FOK maskeden BAĞIMSIZ izinli.
      out = ORDER_FILLING_FOK;
      return true;
     }
   if(exemode == SINYAL_EXEMODE_MARKET)
     {
      if(has_fok) { out = ORDER_FILLING_FOK; return true; }
      if(has_ioc) { out = ORDER_FILLING_IOC; return true; }
      return false;   // RETURN bu modda YASAK — tahmin etme.
     }
   if(exemode == SINYAL_EXEMODE_EXCHANGE)
     {
      if(has_fok) { out = ORDER_FILLING_FOK; return true; }
      if(has_ioc) { out = ORDER_FILLING_IOC; return true; }
      out = ORDER_FILLING_RETURN;
      return true;
     }
   return false;
  }

//+------------------------------------------------------------------+
//| Wire emir tipini MT5 enum'una çevir.                              |
//|                                                                  |
//| CAST KULLANILMAZ. ENUM_ORDER_TYPE'ın sayısal değerleri MQL5       |
//| dokümanında verilmiyor; doğrudan cast, MetaQuotes ileride enum'a  |
//| değer eklerse sessizce yanlış emir tipi gönderirdi.               |
//+------------------------------------------------------------------+
bool SinyalMapOrderType(uchar wire, ENUM_ORDER_TYPE &out)
  {
   switch(wire)
     {
      case SINYAL_ORDER_BUY:             out = ORDER_TYPE_BUY;             return true;
      case SINYAL_ORDER_SELL:            out = ORDER_TYPE_SELL;            return true;
      case SINYAL_ORDER_BUY_LIMIT:       out = ORDER_TYPE_BUY_LIMIT;       return true;
      case SINYAL_ORDER_SELL_LIMIT:      out = ORDER_TYPE_SELL_LIMIT;      return true;
      case SINYAL_ORDER_BUY_STOP:        out = ORDER_TYPE_BUY_STOP;        return true;
      case SINYAL_ORDER_SELL_STOP:       out = ORDER_TYPE_SELL_STOP;       return true;
      case SINYAL_ORDER_BUY_STOP_LIMIT:  out = ORDER_TYPE_BUY_STOP_LIMIT;  return true;
      case SINYAL_ORDER_SELL_STOP_LIMIT: out = ORDER_TYPE_SELL_STOP_LIMIT; return true;
     }
   return false;
  }

//+------------------------------------------------------------------+
//| Wire geçerlilik süresini MT5 enum'una çevir (cast yok).           |
//+------------------------------------------------------------------+
bool SinyalMapTypeTime(uchar wire, ENUM_ORDER_TYPE_TIME &out)
  {
   switch(wire)
     {
      case SINYAL_TIME_GTC:           out = ORDER_TIME_GTC;           return true;
      case SINYAL_TIME_DAY:           out = ORDER_TIME_DAY;           return true;
      case SINYAL_TIME_SPECIFIED:     out = ORDER_TIME_SPECIFIED;     return true;
      case SINYAL_TIME_SPECIFIED_DAY: out = ORDER_TIME_SPECIFIED_DAY; return true;
     }
   return false;
  }

//+------------------------------------------------------------------+
//| Sabit uzunluklu uchar alanına metin yaz (NUL dolgulu).            |
//|                                                                  |
//| Sığmayan metin KESİLİR; sembol adları için bu kabul edilemez      |
//| olduğundan çağıran taşmayı ayrıca denetlemelidir.                 |
//+------------------------------------------------------------------+
void SinyalWriteFixed(uchar &dst[], int dst_len, string src)
  {
   for(int i = 0; i < dst_len; i++)
      dst[i] = 0;
   uchar tmp[];
   int n = StringToCharArray(src, tmp, 0, WHOLE_ARRAY, CP_UTF8);
   // StringToCharArray sondaki NUL'u da sayar.
   if(n > 0 && tmp[n - 1] == 0)
      n--;
   if(n > dst_len)
      n = dst_len;
   for(int i = 0; i < n; i++)
      dst[i] = tmp[i];
  }
//+------------------------------------------------------------------+
