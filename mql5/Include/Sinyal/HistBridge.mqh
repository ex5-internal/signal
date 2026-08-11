//+------------------------------------------------------------------+
//|                                                   HistBridge.mqh |
//|   sinyal_bridge.dll GEÇMİŞ kanalı bildirimleri + yerleşim kontrolü|
//|                                                                  |
//| Protocol.mqh ÜRETİLMİŞTİR, bu dosya elle yazılır.                 |
//|                                                                  |
//| DİKKAT — Bridge.mqh ile AYNI programda kullanılmaz.               |
//| İkisi de aynı DLL'den SinyalSizeof/SinyalLastError bildirir ve    |
//| MQL5 çift bildirimi hata sayar. Bölüşüm şöyle:                    |
//|   Bridge.mqh     -> EA (tick, DOM, emir, durum)                   |
//|   HistBridge.mqh -> Service (yalnız geçmiş barlar)                |
//| Zaten ayrı programlar: geçmişin service'e taşınma SEBEBİ, ikisini |
//| aynı thread'de çalıştırmanın imkânsız olmasıdır.                  |
//+------------------------------------------------------------------+
#property strict

#include <Sinyal/Protocol.mqh>

//+------------------------------------------------------------------+
//| DLL bildirimleri — GEÇMİŞ kanalı                                  |
//|                                                                  |
//| Bu, EA oturumundan (SinyalOpen) BAĞIMSIZ ve AYRI bir oturum       |
//| tipidir. Sebep: EA `md/book/res/sym/state` segmentlerinin TEK     |
//| yazarıdır; service'i o kilidin içine sokmak tek-yazar garantisini |
//| bozardı. Service yalnız `hreq`ten okur, yalnız `bars`a yazar.     |
//|                                                                  |
//| Dönüş sözleşmesi Bridge.mqh ile aynıdır ama tip 'long'dur:        |
//|   > 0  başarı (Open'da tanıtıcı, diğerlerinde 1)                  |
//|   = 0  "şu an yapılamadı" — halka boş (pop) / dolu (push)         |
//|   < 0  gerçek hata; ayrıntı SinyalLastError'da                    |
//|                                                                  |
//| Tanıtıcı bir indeks+kuşak çiftidir, ham işaretçi DEĞİLDİR: MQL5   |
//| tarafındaki bir yazım hatası geçersiz adrese erişime dönüşmez.    |
//+------------------------------------------------------------------+
#import "sinyal_bridge.dll"
// Yaşam döngüsü. Service YAZAR tarafıdır: segmentleri O oluşturur.
long  SinyalHistOpen(string instance);
long  SinyalHistClose(long h);
// Bekleyen istek al. 1 = istek var, 0 = yok (boş halka), < 0 hata.
long  SinyalHistPopReq(long h, SinyalHistReq &out);
// Bar yaz. 1 = yazıldı, 0 = halka DOLU (YENİDEN DENE), < 0 hata.
// 0'da vazgeçmek barı KALICI olarak kaybetmek demektir.
long  SinyalHistPushBar(long h, const SinyalBarRec &bar);

// Teşhis — Bridge.mqh'takiyle AYNI fonksiyonlar (çift bildirim yasak).
int   SinyalSizeof(int what);
int   SinyalLastError(ushort &buf[], int cap);
#import

//--- dönüş kodları (Rust ffi.rs ile birebir; Bridge.mqh ile aynı sayılar,
//    isimler geçmiş kanalına özgü tutuldu ki iki başlık çakışmasın)
#define SINYAL_HIST_OK           1
#define SINYAL_HIST_NONE         0    // halka boş — HATA DEĞİL
#define SINYAL_HIST_FULL         0    // halka dolu — HATA DEĞİL, yeniden dene
#define SINYAL_HIST_ERR_HANDLE  -1
#define SINYAL_HIST_ERR_PANIC   -2
#define SINYAL_HIST_ERR_ARGS    -3
#define SINYAL_HIST_ERR_OTHER   -4

//--- SinyalSizeof() sorgu kodları (Rust: ffi::sizeof_what)
#define SINYAL_WHAT_HISTREQ      10
#define SINYAL_WHAT_BARREC       11
#ifndef SINYAL_WHAT_PROTOVERSION
#define SINYAL_WHAT_PROTOVERSION 100
#endif

//+------------------------------------------------------------------+
//| CopyRates hata sınıflandırması.                                   |
//|                                                                  |
//| Bu ayrım İSTEĞE BAĞLI DEĞİLDİR: kalıcı bir hatada yeniden denemek |
//| service'i saniyelerce meşgul eder ve sıradaki istekleri geciktirir|
//| — geçmiş kuyruğu tek thread'dir. Geçici bir hatada denememek ise  |
//| terminal veriyi sunucudan indirirken gelen İLK isteği kaybetmek   |
//| demektir; sembolün geçmişi ilk kez istendiğinde bu NORMALDİR.     |
//+------------------------------------------------------------------+
//--- GEÇİCİ: terminal geçmişi henüz indirmedi / istek zaman aşımına uğradı
#define SINYAL_ERR_HISTORY_NOT_FOUND  4401
#define SINYAL_ERR_HISTORY_TIMEOUT    4403
//--- KALICI: yeniden denemek DÜZELTMEZ
#define SINYAL_ERR_MARKET_UNKNOWN_SYM 4301  // sembol yok
#define SINYAL_ERR_MARKET_NOT_SELECT  4302  // Market Watch'ta değil
#define SINYAL_ERR_HISTORY_BARS_LIMIT 4404  // terminal ayarı barları sınırlıyor
#define SINYAL_ERR_HISTORY_SMALL_BUF  4407  // hedef dizi yetersiz (statik dizi)

//--- İsteğin KENDİSİ bozuk (bilinmeyen dilim, count=0, ad boş).
//    MQL5'in "yanlış parametre" kodunu kullanıyoruz; uydurma kod
//    çekirdek loglarında MQL5 hatasıymış gibi görünürdü.
#define SINYAL_HIST_ERR_BAD_REQUEST   4003

//+------------------------------------------------------------------+
//| TÜKETİCİNİN zaman aşımı — çekirdeğin history.rs::DEFAULT_TIMEOUT'u |
//|                                                                  |
//| Bir isteğe ayırdığımız TOPLAM süre bunu AŞAMAZ. Aşarsa şu sessiz  |
//| döngü kurulur: çekirdek 8 sn'de vazgeçer, biz 15. saniyede barları|
//| yazarız, barlar artık hiçbir bekleyen isteğe ait olmadığı için    |
//| ÖKSÜZ sayılıp atılır, depo hiç dolmaz ve bir sonraki istek aynı   |
//| şeyi tekrarlar. Geçmişi istenenden KISA olan her sembolde (yeni   |
//| enstrüman, kısa geçmişli CFD) sonuç sonsuza kadar "timeout" ve    |
//| SIFIR bardır — üstelik her denemede terminal 15 sn meşgul edilir. |
//|                                                                  |
//| Bütçeyi uzatmak bu yüzden HİÇBİR ŞEY KAZANDIRMAZ: çekirdek zaten  |
//| 8 sn'den fazla beklemiyor.                                        |
//+------------------------------------------------------------------+
#define SINYAL_HIST_CONSUMER_TIMEOUT_MS 8000
//--- CopyRates yeniden denemelerine ayrılan azami süre.
#define SINYAL_HIST_BUDGET_MAX_MS       4000
//--- Halka dolu kaldığında beklenecek azami süre. İKİ KEZ harcanabilir
//    (önce veri barı, sonra ERROR barı), bu yüzden toplam hesabında
//    iki kat sayılır: 4000 + 1500 + 1500 = 7000 < 8000.
#define SINYAL_HIST_RING_WAIT_MAX_MS    1500

//--- Halka DOLU kaldı ve teslimat KESİLDİ.
//    MQL5'in hata uzayında bunun karşılığı YOK. 9000 bloğu BİZE aittir
//    ve MQL5 çalışma-zamanı hatalarıyla (4000-5xxx) çakışmaz — çekirdek
//    bu kodu gördüğünde sorunun terminalde değil KENDİSİNDE olduğunu
//    (barları yeterince hızlı çekmediğini) bilir.
#define SINYAL_HIST_ERR_RING_FULL     9001

//+------------------------------------------------------------------+
//| Köprünün son hata metnini oku.                                    |
//|                                                                  |
//| Bridge.mqh'daki SinyalErrorText ile aynı iş; ayrı ad kullanılıyor |
//| çünkü iki başlık aynı programa girerse fonksiyon çift tanımlanır. |
//+------------------------------------------------------------------+
string SinyalHistErrorText()
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
//| Service'te izin kutusu EA'dakinden BAŞKA bir yerdedir: Navigator  |
//| > Services > sağ tık > "Add service". Kullanıcıyı EA ayarlarına   |
//| göndermek onu yanlış yere baktırır.                               |
//+------------------------------------------------------------------+
bool SinyalHistCheckDllPermission(string &problem)
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
      problem = "Bu service için DLL içe aktarma kapalı. Navigator > "
                "Services altında service'i kaldırıp yeniden ekleyin ve "
                "açılan kutuda 'Allow DLL imports' işaretleyin.";
      return false;
     }
   return true;
  }

//+------------------------------------------------------------------+
//| Rust ile MQL5 yapı yerleşiminin aynı olduğunu doğrula.            |
//|                                                                  |
//| Geçmiş kanalında bu denetim EA'dakinden DAHA kritiktir: service   |
//| barları YAZAR. Yerleşim kaymışsa bozuk fiyatlar sessizce halkaya  |
//| girer ve oradan tüketicinin geçmiş serisine — yani sinyal üreten  |
//| modelin girdisine. Uyumsuzlukta service HİÇ BAŞLAMAMALIDIR.       |
//+------------------------------------------------------------------+
bool SinyalHistVerifyLayout(string &problem)
  {
   int ver = SinyalSizeof(SINYAL_WHAT_PROTOVERSION);
   if(ver != SINYAL_PROTO_VERSION)
     {
      problem = StringFormat(
         "Protokol sürümü uyumsuz: DLL=%d, service=%d. "
         "sinyal_bridge.dll ile SinyalHistory.ex5 farklı sürümlerden. "
         "deploy.ps1 ile ikisini birlikte yenileyin.",
         ver, SINYAL_PROTO_VERSION);
      return false;
     }

   int    want[2];
   int    what[2];
   string name[2];

   what[0] = SINYAL_WHAT_HISTREQ;  want[0] = SINYAL_SIZEOF_HISTREQ;  name[0] = "SinyalHistReq";
   what[1] = SINYAL_WHAT_BARREC;   want[1] = SINYAL_SIZEOF_BARREC;   name[1] = "SinyalBarRec";

   for(int i = 0; i < 2; i++)
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
   SinyalHistReq hr;
   SinyalBarRec  br;

   if(sizeof(hr) != SINYAL_SIZEOF_HISTREQ || sizeof(br) != SINYAL_SIZEOF_BARREC)
     {
      problem = StringFormat(
         "MQL5 derleyicisinin ürettiği yerleşim başlıktan farklı "
         "(HistReq=%d/%d BarRec=%d/%d). "
         "Protocol.mqh elle düzenlenmiş olabilir — gen-mqh ile yenileyin.",
         sizeof(hr), SINYAL_SIZEOF_HISTREQ,
         sizeof(br), SINYAL_SIZEOF_BARREC);
      return false;
     }

   return true;
  }

//+------------------------------------------------------------------+
//| CopyRates hatası GEÇİCİ mi?                                       |
//|                                                                  |
//| Yalnızca burada listelenenler yeniden denenir. "Bilinmiyorsa      |
//| dene" kuralı seçilmedi: tanınmayan bir kalıcı hata service'i her  |
//| istekte bütçe boyunca kilitler ve kuyruk büyür.                   |
//+------------------------------------------------------------------+
bool SinyalHistErrorIsTransient(const int err)
  {
   return (err == SINYAL_ERR_HISTORY_NOT_FOUND ||
           err == SINYAL_ERR_HISTORY_TIMEOUT);
  }

//+------------------------------------------------------------------+
//| Wire zaman dilimini MT5 enum'una çevir.                           |
//|                                                                  |
//| CAST KULLANILMAZ. Bilinmeyen bir sayıyı ENUM_TIMEFRAMES'e cast    |
//| etmek CopyRates'e anlamsız bir dilim verir; MT5 çoğu durumda      |
//| PERIOD_CURRENT (0) gibi davranır ve service SESSİZCE YANLIŞ       |
//| dilimin barlarını yayınlar. Grafik hatasız görünür, veri yanlış   |
//| olur — bulunması en zor hata sınıfı.                              |
//+------------------------------------------------------------------+
bool SinyalMapTimeframe(const uint wire, ENUM_TIMEFRAMES &out)
  {
   switch(wire)
     {
      case SINYAL_TF_M1:  out = PERIOD_M1;  return true;
      case SINYAL_TF_M5:  out = PERIOD_M5;  return true;
      case SINYAL_TF_M15: out = PERIOD_M15; return true;
      case SINYAL_TF_M30: out = PERIOD_M30; return true;
      case SINYAL_TF_H1:  out = PERIOD_H1;  return true;
      case SINYAL_TF_H4:  out = PERIOD_H4;  return true;
      case SINYAL_TF_D1:  out = PERIOD_D1;  return true;
     }
   return false;
  }

//+------------------------------------------------------------------+
//| Dilimin SANİYE uzunluğu. Bilinmiyorsa 0.                          |
//|                                                                  |
//| PARTIAL bayrağı buna dayanır: barın kapanıp kapanmadığı ancak     |
//| açılış zamanı + dilim uzunluğu ile bilinir.                       |
//+------------------------------------------------------------------+
int SinyalTimeframeSeconds(const uint wire)
  {
   switch(wire)
     {
      case SINYAL_TF_M1:  return 60;
      case SINYAL_TF_M5:  return 300;
      case SINYAL_TF_M15: return 900;
      case SINYAL_TF_M30: return 1800;
      case SINYAL_TF_H1:  return 3600;
      case SINYAL_TF_H4:  return 14400;
      case SINYAL_TF_D1:  return 86400;
     }
   return 0;
  }

//+------------------------------------------------------------------+
//| Bar HÂLÂ OLUŞMAKTA mı?                                            |
//|                                                                  |
//| `now` broker sunucu saati olmalı (TimeCurrent), yerel saat DEĞİL: |
//| bar zamanları sunucu saatindedir ve fark saatlerce olabilir.      |
//|                                                                  |
//| Dilim uzunluğu bilinmiyorsa OLUŞMAKTA kabul edilir. Asimetri      |
//| kasıtlı: kapanmış bir barı "oluşmakta" sanmak tüketiciye onu      |
//| yeniden istetir (ucuz); oluşmakta olanı "kapanmış" sanmak ise     |
//| değişmeye devam edecek bir close'u kalıcı seriye yazdırır (pahalı |
//| ve sessiz).                                                       |
//+------------------------------------------------------------------+
bool SinyalBarIsForming(const datetime open_time, const int tf_seconds,
                        const datetime now)
  {
   if(tf_seconds <= 0)
      return true;
   return (now < open_time + tf_seconds);
  }

//+------------------------------------------------------------------+
//| İstekteki sembol adını oku.                                       |
//|                                                                  |
//| Ad, HistReq._reserved'in ilk SINYAL_HIST_SYMBOL_LEN baytında      |
//| taşınır ve çekirdek tarafından yazılır. Service grafiğe bağlı     |
//| olmadığı için (_Symbol YOK) symbol_id'yi ada çevirmenin başka     |
//| yolu sembol tablosu segmentini okumaktır — o da ek FFI demektir.  |
//|                                                                  |
//| Alan NUL dolguludur ama dolu bir 32 baytta sonlandırıcı OLMAYABİLİR|
//| — bu yüzden okuma tampon sonunda da durur.                        |
//+------------------------------------------------------------------+
string SinyalHistReadSymbol(const SinyalHistReq &req)
  {
   uchar tmp[SINYAL_HIST_SYMBOL_LEN + 1];
   int n = 0;
   for(int i = 0; i < SINYAL_HIST_SYMBOL_LEN; i++)
     {
      if(req.symbol[i] == 0)
         break;
      tmp[n++] = req.symbol[i];
     }
   tmp[n] = 0;
   if(n == 0)
      return "";
   return CharArrayToString(tmp, 0, n, CP_UTF8);
  }
//+------------------------------------------------------------------+
