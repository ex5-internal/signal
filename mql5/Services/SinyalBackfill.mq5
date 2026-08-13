//+------------------------------------------------------------------+
//|                                               SinyalBackfill.mq5 |
//|     MT5 -> disk: broker'ın GEÇMİŞ tick'lerini 48 baytlık biçimde  |
//+------------------------------------------------------------------+
//| MİMARİ NOTLARI:
//|
//| 1. NEDEN ÜÇÜNCÜ BİR SERVICE. SinyalHistory tek seri döngüde bar
//|    isteklerini birer birer karşılıyor; geçmiş tick indirmesi SAATLER
//|    sürer ve o döngüye girseydi bar isteklerini o kadar süre AÇ
//|    BIRAKIRDI. EA'ya hiç konmaz: orada bloklama canlı tick KAYBETTİRİR
//|    ve kaybedilen tick geri gelmez. Ayrı service ayrı thread'dir;
//|    bloklandığında kimseyi etkilemez.
//|
//| 2. DOSYAYA YAZAR, PAYLAŞILAN BELLEĞE DEĞİL. Yeni bir shm halkası,
//|    yeni FFI ve RING_VERSION artışı GEREKMEZ — geçmiş verinin gecikme
//|    bütçesi yoktur, halka ise canlı akış için vardır. MQL5 dosya G/Ç'si
//|    MQL5\Files\ altına sınırlıdır; Rust ice-aktarıcı oradan okuyup
//|    veri/<instance>/ altına taşır.
//|
//| 3. ÖNCE ÖLÇ, SONRA İNDİR. XM'in kaç ay geriye tick verdiği HİÇBİR
//|    YERDE yazmıyor. Üstel geri sıçrama + ikili aramayla broker'ın
//|    verdiği en eski tarih ÖNCE bulunur ve LOGLANIR. Bu adım tek başına
//|    değerlidir: kullanıcı broker'ın ne verdiğini öğrenir.
//|
//| 4. GÜNLÜK PARÇALAR — BELLEK ZORUNLULUĞU. sizeof(MqlTick) = 60 bayt ve
//|    GOLD günde ~115 bin tick üretiyor. 3 ayı TEK diziye almak ~440 MB
//|    eder ve CopyTicksRange 4004 (ERR_NOT_ENOUGH_MEMORY) döndürür.
//|    Dizi her gün ArrayFree ile bırakılır; tepe bellek TEK GÜNDÜR.
//|
//| 5. TIMER YOK. Service'te OnTimer/EventSetTimer KULLANILMAZ; akış
//|    OnStart içindeki kendi döngümüzdür. IsStopped() her gün başında
//|    denetlenir, aksi halde saatler süren indirme durdurulamaz.
//|
//| 6. recv_ms BROKER ZAMANIDIR — CANLI KAYITTAN FARKLI. Canlı kayıtta
//|    (record.rs) recv_ms YEREL ALIM zamanıdır ve replay temposu YALNIZCA
//|    ondan üretilebilir. Geçmiş veride yerel alım zamanı YOKTUR ve
//|    uydurulamaz; bu yüzden recv_ms alanına time_msc'in KOPYASI yazılır.
//|    Sonuç: geri-doldurulmuş günlerde replay teması broker damgasından
//|    türer. Bu SESSİZ bir sapma olmasın diye üç yerde belgelenir —
//|    bu blokta, rapor.json içinde ve indirme başlangıç logunda.
//|
//| 7. GÜN SINIRI SUNUCU SAATİDİR — CANLI KAYITTAN FARKLI. MqlTick.time_msc
//|    ve CopyTicksRange'in from/to parametreleri broker sunucu saatinin
//|    epoch ms karşılığıdır. Canlı kaydedici ise günü UTC'ye göre kesiyor
//|    (record.rs: gün sınırı recv_ms/UTC). İki tarafın gün dosyaları bu
//|    yüzden sunucu-GMT farkı kadar KAYIK olabilir. Farkı uydurmuyoruz:
//|    ölçülen değer rapor.json içine "sunucu_gmt_farki_sn" olarak yazılır,
//|    ice-aktarıcı isterse ona göre yeniden kovalar.
//|
//| 8. YAZIM ATOMİKTİR. Gün önce "<ad>.tmp" dosyasına yazılır, boyu
//|    doğrulanır, sonra FileMove ile asıl ada taşınır. Doğrudan asıl ada
//|    yazsaydık, açılışta yapılan kırpma yüzünden çökme anında 0 baytlık
//|    bir dosya kalırdı; "zaten inmiş" denetimi onu TAMAMLANMIŞ sayıp o
//|    günü SONSUZA KADAR atlardı — sessiz ve kalıcı bir veri deliği.
//|
//| 9. BUGÜN İNMEZ — YALNIZCA TAMAMLANMIŞ GÜNLER. Sürmekte olan gün henüz
//|    bitmemiştir; onu indirmek dosyaya günün YARISINI yazardı. O dosyanın
//|    boyu 48'in katı olduğu için sonraki koşuların "zaten inmiş" denetimi
//|    onu TAMAMLANMIŞ sayar ve o gün SONSUZA KADAR yarım kalır — atomik
//|    yazımla kapattığımız deliğin aynısı, bu kez zamandan açılıyor.
//|    Bugünü kaybetmiyoruz: canlı kaydedici (`sinyald --record`) bugünü
//|    zaten kaydediyor ve ice-aktarıcı iki kaynağı birleştiriyor. Yarın bu
//|    gün TAM olarak inecek.
//+------------------------------------------------------------------+
#property service
#property copyright "Sinyal"
#property version   "1.00"
#property strict

//--- YALNIZCA Protocol.mqh. HistBridge.mqh/Bridge.mqh DLL #import taşır ve
//    MT5 "DLL içe aktarmaya izin ver" kapalıyken bu programı hiç
//    başlatmazdı. Geri-doldurma DLL'e MUHTAÇ DEĞİL: yalnızca dosya yazıyor.
//    Buradan tek ihtiyacımız SINYAL_KIND_TICK'in TEK KAYNAKTAN gelmesi.
#include <Sinyal/Protocol.mqh>

//--- girdiler -------------------------------------------------------
input string Sembol        = "GOLD";             // İndirilecek sembol
input int    GunSayisi     = 92;                 // Kaç TAMAMLANMIS gun geriye (~3 ay; bugun haric)
input string CiktiKlasor   = "Sinyal\\backfill"; // MQL5\Files altında
input int    GunlukButceMs = 60000;              // Bir gün için azami bekleme
input bool   AyrintiliLog  = true;               // Gün başına ayrıntılı log

//--- sabitler -------------------------------------------------------
#define SINYAL_BF_DAY_SEC      86400
#define SINYAL_BF_DAY_MS       86400000
#define SINYAL_BF_REC_SIZE     48          // record.rs: REC_SIZE — DEĞİŞMEZ
#define SINYAL_BF_MAX_BACK     128         // üstel merdivenin son basamağı
#define SINYAL_BF_RETRY_MS     250         // geçici hatada bekleme
#define SINYAL_BF_LOG_EVERY    10          // kaç günde bir özet satırı

//--- Tatil/hafta sonu koruması: bir örnekleme günü BOŞ çıkarsa DAHA
//    DERİNE doğru bu kadar gün daha denenir.
//
//    YÖN KRİTİK: derine (geçmişe) gidiyoruz, bugüne doğru DEĞİL.
//    Offset b+2'de tick bulmak "derinlik >= b+2" demektir ve b'nin de
//    ulaşılabilir olduğunu KANITLAR. Bugüne doğru yürüseydik (b-2) bulunan
//    veri b hakkında HİÇBİR ŞEY söylemezdi — daha yeni bir günde veri
//    olması daha eski bir günde de olmasını gerektirmez. 3 ek gün, art
//    arda 4 günlük pencere demektir; hafta sonu 2 gün olduğu için böyle bir
//    pencerede HER ZAMAN en az 2 işlem günü bulunur.
#define SINYAL_BF_HOLIDAY_WALK 3

//--- MQL5 hata kodları.
//
//    HistBridge.mqh'de de tanımlılar ama o başlık DLL #import taşıyor
//    (yukarıya bak). Sınıflandırma onunkiyle BİREBİR AYNI tutulmalı:
//    GEÇİCİ küme {4401, 4403}, gerisi kalıcı.
#define SINYAL_BF_ERR_NOT_ENOUGH_MEM   4004  // KALICI — dizi sığmadı
#define SINYAL_BF_ERR_MARKET_UNKNOWN   4301  // KALICI — sembol yok
#define SINYAL_BF_ERR_MARKET_NOT_SEL   4302  // KALICI — Market Watch'ta değil
#define SINYAL_BF_ERR_HISTORY_NOT_FND  4401  // GEÇİCİ — terminal henüz indirmedi
#define SINYAL_BF_ERR_HISTORY_TIMEOUT  4403  // GEÇİCİ — istek zaman aşımı

//--- durum ----------------------------------------------------------

// Tek GÜNLÜK tick tamponu. DİNAMİK olmak ZORUNDA: statik dizi
// CopyTicksRange'ten 4407 (hedef tampon yetersiz) döndürür. Ayrıca her
// günün sonunda ArrayFree ile BIRAKILIR — 3 ayı biriktirmek 440 MB ederdi.
MqlTick g_ticks[];

string g_out_dir   = "";     // sondaki '\' ayıklanmış çıktı klasörü
int    g_days_want = 92;     // denetlenmiş GunSayisi
int    g_budget_ms = 60000;  // denetlenmiş GunlukButceMs — ASIL indirme
int    g_probe_budget_ms = 15000; // derinlik örneklemesi — bkz. ProbeBack

// Bugünün SUNUCU saatine göre gece yarısı, epoch ms. Tüm gün penceresi
// aritmetiği bunun üzerinden — bkz. not 7.
long   g_today0_msc = 0;

// Teşhis sayaçları.
ulong  g_total_ticks   = 0;
int    g_days_written  = 0;   // dosyası yazılan gün (boş günler dahil)
int    g_days_empty    = 0;   // veri yok — hafta sonu/tatil, HATA DEĞİL
int    g_days_ticked   = 0;   // en az bir tick içeren gün (atlananlar DAHİL)
int    g_days_skipped  = 0;   // zaten inmiş, atlandı
int    g_days_failed   = 0;   // hata (indirme veya yazma)
int    g_retry_rounds  = 0;   // CopyTicksRange geçici hatayla yeniden denendi
int    g_probe_calls   = 0;   // derinlik ölçümünde yapılan gün isteği
ulong  g_started_ms    = 0;

// rapor.json'un "gunler" dizisi, satır satır biriktirilir.
string g_days_json = "";

//+------------------------------------------------------------------+
//| Girdileri kullanılabilir aralığa çek.                             |
//|                                                                  |
//| Service bir kez başlar ve saatlerce çalışır; burada denetlemezsek |
//| yanlış bir değer saatlerce sessizce yanlış davranış üretir        |
//| (ör. bütçe 0 -> her gün ilk denemede hata).                       |
//+------------------------------------------------------------------+
void ClampInputs()
  {
   g_days_want = GunSayisi;
   if(g_days_want < 1)    g_days_want = 1;
   if(g_days_want > 3650) g_days_want = 3650;   // 10 yıl: broker üst sınırının çok üstü
   if(g_days_want != GunSayisi)
      PrintFormat("Sinyal geri-doldurma: GunSayisi=%d aralık dışı, %d'e çekildi.",
                  GunSayisi, g_days_want);

   // ALT SINIR ŞART. CopyTicksRange bir sembolün geçmişi İLK kez
   // istendiğinde -1/4401 döndürür ve indirmeyi arka planda başlatır;
   // saniyenin altında bir bütçe o ilk isteği her gün için hataya çevirir
   // ve HİÇBİR gün inmez.
   g_budget_ms = GunlukButceMs;
   if(g_budget_ms < 1000)   g_budget_ms = 1000;
   if(g_budget_ms > 600000) g_budget_ms = 600000;
   if(g_budget_ms != GunlukButceMs)
      PrintFormat("Sinyal geri-doldurma: GunlukButceMs=%d aralık dışı, %d'e çekildi.",
                  GunlukButceMs, g_budget_ms);

   // Örnekleme bütçesi ASIL bütçenin çeyreği. Örnekleme veri İNDİRMİYOR,
   // yalnızca "var mı" diye soruyor; derinliğin ötesinde terminal 4401'i
   // bütçe dolana kadar tekrarlar ve tam bütçe ölçümü saatlere çıkarır.
   // 5 sn taban: terminalin sunucuya gidip dönmesi için gereken en az süre.
   g_probe_budget_ms = g_budget_ms / 4;
   if(g_probe_budget_ms < 5000)
      g_probe_budget_ms = 5000;
   if(g_probe_budget_ms > g_budget_ms)
      g_probe_budget_ms = g_budget_ms;

   // Sondaki ayraç ayıklanır: yoksa yol "Sinyal\backfill\\GOLD-...bin"
   // olurdu ve MQL5 sanal dosya sisteminde bu YOL BAŞKA bir addır.
   g_out_dir = CiktiKlasor;
   while(StringLen(g_out_dir) > 0)
     {
      string tail = StringSubstr(g_out_dir, StringLen(g_out_dir) - 1, 1);
      if(tail != "\\" && tail != "/")
         break;
      g_out_dir = StringSubstr(g_out_dir, 0, StringLen(g_out_dir) - 1);
     }
   if(StringLen(g_out_dir) == 0)
      g_out_dir = "Sinyal\\backfill";
  }

//+------------------------------------------------------------------+
//| Sembol adı DOSYA ADINDA kullanılabilir mi?                        |
//|                                                                  |
//| Ad, `<Sembol>-YYYYMMDD.bin` içine OLDUĞU GİBİ giriyor. "GOLD"     |
//| güvenli ama broker sembol adları serbesttir ve içinde eğik çizgi  |
//| bulunan bir ad (ör. "XAU/USD") yazımı çıktı klasörünün DIŞINA     |
//| taşırdı — ice-aktarıcı orayı hiç taramaz ve saatler süren indirme |
//| görünmez bir yere düşerdi. Nokta içeren ad da tehlikeli: ".." bir |
//| dizin yukarı demek.                                               |
//|                                                                  |
//| Bu denetim BAŞLANGIÇTA yapılır: gün gün başarısız olmak, kullanı- |
//| cıya aynı hatayı 92 kez göstermekten başka bir şey vermezdi.      |
//+------------------------------------------------------------------+
bool SymbolNameIsPathSafe(const string s)
  {
   int n = StringLen(s);
   if(n == 0)
      return false;
   string bad = "\\/:*?\"<>|.";
   for(int i = 0; i < n; i++)
     {
      if(StringFind(bad, StringSubstr(s, i, 1)) >= 0)
         return false;
     }
   return true;
  }

//+------------------------------------------------------------------+
//| 'back' gün geriye düşen günün BAŞLANGICI, epoch ms (sunucu saati).|
//+------------------------------------------------------------------+
long DayStartMsc(const int back)
  {
   return g_today0_msc - (long)back * (long)SINYAL_BF_DAY_MS;
  }

//+------------------------------------------------------------------+
//| Gün başlangıcından YYYYMMDD / YYYY.MM.DD metinleri.               |
//+------------------------------------------------------------------+
string DayStampCompact(const long day_start_msc)
  {
   MqlDateTime st;
   TimeToStruct((datetime)(day_start_msc / 1000), st);
   return StringFormat("%04d%02d%02d", st.year, st.mon, st.day);
  }

string DayStampDotted(const long day_start_msc)
  {
   MqlDateTime st;
   TimeToStruct((datetime)(day_start_msc / 1000), st);
   return StringFormat("%04d.%02d.%02d", st.year, st.mon, st.day);
  }

//+------------------------------------------------------------------+
//| Bir günün hedef dosya yolu.                                       |
//+------------------------------------------------------------------+
string DayPath(const long day_start_msc)
  {
   return StringFormat("%s\\%s-%s.bin", g_out_dir, Sembol,
                       DayStampCompact(day_start_msc));
  }

//+------------------------------------------------------------------+
//| CopyTicksRange hatası GEÇİCİ mi?                                  |
//|                                                                  |
//| HistBridge.mqh::SinyalHistErrorIsTransient ile AYNI küme. "Bilmi- |
//| yorsak deneriz" kuralı seçilmedi: tanınmayan bir kalıcı hata her  |
//| günü bütçe boyunca kilitlerdi ve 92 gün × 60 sn = 1,5 saat SAF    |
//| beklemeye giderdi.                                                |
//+------------------------------------------------------------------+
bool ErrorIsTransient(const int err)
  {
   return (err == SINYAL_BF_ERR_HISTORY_NOT_FND ||
           err == SINYAL_BF_ERR_HISTORY_TIMEOUT);
  }

//+------------------------------------------------------------------+
//| Dosyanın diskteki boyu; dosya yoksa/açılamıyorsa -1.              |
//+------------------------------------------------------------------+
long FileByteSize(const string path)
  {
   if(!FileIsExist(path))
      return -1;
   ResetLastError();
   int h = FileOpen(path, FILE_READ | FILE_BIN | FILE_SHARE_READ | FILE_SHARE_WRITE);
   if(h == INVALID_HANDLE)
      return -1;
   long sz = (long)FileSize(h);
   FileClose(h);
   return sz;
  }

//+------------------------------------------------------------------+
//| BİR GÜNÜ İNDİR — tüm indirme yolunun TEK kapısı.                  |
//|                                                                  |
//| Hem derinlik ölçümü hem asıl indirme buradan geçer; iki ayrı kopya|
//| olsaydı biri düzeltilip diğeri unutulurdu.                        |
//|                                                                  |
//| Dönüş: >= 0 kopyalanan tick sayısı, -1 KALICI hata (out_err dolu).|
//|                                                                  |
//| Üç durum ve hepsi FARKLI anlama gelir:                            |
//|   n < 0  -> GetLastError(); GEÇİCİ ise bütçe içinde yeniden dene, |
//|             KALICI ise hemen çık (denemek düzeltmez).             |
//|   n == 0 -> o gün veri YOK. Hafta sonu/tatil olabilir — HATA      |
//|             DEĞİL. Yine de BİR KEZ daha soruyoruz: terminalin     |
//|             sessizce "henüz hazır değil" dediği durumu boş günden |
//|             ayırmanın başka yolu yok ve boşu kabul etmek o günü   |
//|             KALICI olarak deler (dosya yazılır, bir daha bakılmaz)|
//|   n > 0  -> tamam.                                                |
//|                                                                  |
//| ResetLastError() her çağrıdan ÖNCE ŞART: GetLastError() önceki bir|
//| işlemin kodunu döndürürse hata sınıflandırması tamamen yanılır.   |
//+------------------------------------------------------------------+
int FetchDay(const long day_start_msc, const int budget_ms, int &out_err, uint &out_ms)
  {
   ulong t0       = GetTickCount64();
   ulong deadline = t0 + (ulong)budget_ms;
   bool  retried_empty = false;
   out_err = 0;

   // Pencere KAPALI aralıktır: CopyTicksRange from_msc ve to_msc'i DAHİL
   // eder. to_msc = gün başı + 24s olsaydı, ertesi günün 00:00:00.000
   // tick'i İKİ dosyaya birden yazılırdı ve ice-aktarıcı onu çift sayardı.
   ulong from_msc = (ulong)day_start_msc;
   ulong to_msc   = (ulong)(day_start_msc + (long)SINYAL_BF_DAY_MS - 1);

   while(!IsStopped())
     {
      // index 0 = EN ESKİ tick. Fiziksel düzen zaten eskiden yeniye;
      // bayrağı açıkça kurmak, araya giren bir çağrının indeksleme yönünü
      // çevirmesine karşı ucuz sigorta.
      ArraySetAsSeries(g_ticks, false);

      ResetLastError();
      int n = CopyTicksRange(Sembol, g_ticks, COPY_TICKS_ALL, from_msc, to_msc);

      if(n < 0)
        {
         int err = GetLastError();
         out_err = err;
         if(!ErrorIsTransient(err))
           {
            out_ms = (uint)(GetTickCount64() - t0);
            return -1;                    // KALICI — denemek düzeltmez
           }
         g_retry_rounds++;
        }
      else if(n == 0 && !retried_empty)
        {
         // Boşu kabul etmeden önce TEK bir doğrulama turu (yukarıya bak).
         // out_err SIFIRLANIR: terminal bu kez SORUYA CEVAP VERDİ. Daha önceki
         // geçici hatayı elde tutmak, bütçe dolduğunda gerçek bir boş günü
         // "hata" diye bildirirdi.
         out_err = 0;
         retried_empty = true;
        }
      else
        {
         out_err = 0;
         out_ms  = (uint)(GetTickCount64() - t0);
         return n;
        }

      if(GetTickCount64() >= deadline)
        {
         out_ms = (uint)(GetTickCount64() - t0);
         // BÜTÇE DOLDU AMA ELİMİZDE TEMİZ BİR "BOŞ" CEVABI VAR.
         //
         // Bunu hata saymak, gerçekten boş bir günü (hafta sonu/tatil) HER
         // KOŞUDA yeniden sordururdu: dosya yazılmaz, atlama denetimi devreye
         // girmez ve ~26 hafta sonu günü her seferinde bütçe kadar saf
         // beklemeye giderdi. Terminale zaten tam bütçesini verdik ve o
         // "veri yok" dedi; daha fazla beklemenin bir bilgisi yok.
         if(retried_empty && out_err == 0)
            return 0;
         // Bütçe doldu ve hata GEÇİCİydi: kalıcı gibi bildiriyoruz, çünkü
         // çağıran için sonuç aynı — bu günü şimdi alamadık. Kod korunur ki
         // logda "4401" görünsün ve kullanıcı bütçeyi artırabilsin.
         if(out_err == 0)
            out_err = SINYAL_BF_ERR_HISTORY_TIMEOUT;
         return -1;
        }
      Sleep(SINYAL_BF_RETRY_MS);
     }

   out_ms  = (uint)(GetTickCount64() - t0);
   out_err = 0;
   return -1;                              // durduruldu
  }

//+------------------------------------------------------------------+
//| DERİNLİK ÖRNEĞİ: 'back' gün geriye tick VAR MI?                   |
//|                                                                  |
//| Dönüş: tick GÖRÜLEN gün-geriye değeri (>= back), yoksa -1.        |
//|                                                                  |
//| Tek bir gün boş çıkabilir (hafta sonu/tatil) ve bunu "broker'ın   |
//| verisi burada bitiyor" sanmak 3 aylık hedefi SESSİZCE kısaltırdı. |
//| Bu yüzden art arda 4 güne bakılır — bkz. SINYAL_BF_HOLIDAY_WALK.  |
//+------------------------------------------------------------------+
int ProbeBack(const int back, int &out_err)
  {
   out_err = 0;
   for(int b = back; b <= back + SINYAL_BF_HOLIDAY_WALK && !IsStopped(); b++)
     {
      long ds  = DayStartMsc(b);
      int  err = 0;
      uint took = 0;
      g_probe_calls++;
      // Örnekleme KISA bütçeyle. Derinliğin ÖTESİNDE terminal 4401'i bütçe
      // boyunca tekrarlayabilir; tam bütçeyle 4 günlük pencere 4 dakika,
      // ~15 örnek ise BİR SAAT sürerdi — üstelik hiç veri indirmeden.
      int n = FetchDay(ds, g_probe_budget_ms, err, took);

      if(n < 0)
        {
         out_err = err;
         if(AyrintiliLog)
            PrintFormat("Sinyal geri-doldurma: örnek %s (%d gün geriye) — HATA %d (%u ms)",
                        DayStampDotted(ds), b, err, took);
         ArrayFree(g_ticks);
         // Kalıcı hata derinlik ölçümünü anlamsızlaştırır: 4301/4302 zaten
         // sembol sorunudur, 4004 bellek. Pencerenin kalanına bakmak
         // yalnızca aynı hatayı tekrarlardı.
         return -1;
        }
      if(n > 0)
        {
         if(AyrintiliLog)
            PrintFormat("Sinyal geri-doldurma: örnek %s (%d gün geriye) — %d tick (%u ms)",
                        DayStampDotted(ds), b, n, took);
         ArrayFree(g_ticks);
         return b;                         // derinlik EN AZ b gün
        }
      if(AyrintiliLog)
         PrintFormat("Sinyal geri-doldurma: örnek %s (%d gün geriye) — BOŞ (%u ms)",
                     DayStampDotted(ds), b, took);
      ArrayFree(g_ticks);
     }
   return -1;
  }

//+------------------------------------------------------------------+
//| BROKER DERİNLİĞİNİ ÖLÇ — üstel geri sıçrama + ikili arama.        |
//|                                                                  |
//| Dönüş: tick bulunabilen EN DERİN gün-geriye değeri.               |
//|                                                                  |
//| İki değişken tutulur:                                             |
//|   lo -> veri GÖRÜLDÜĞÜ kanıtlanmış en derin nokta                 |
//|   hi -> 4 günlük pencerede veri BULUNAMAYAN en sığ nokta          |
//| Arama lo'yu her turda KESİN olarak büyütür (p >= mid > lo), bu    |
//| yüzden sonludur.                                                   |
//+------------------------------------------------------------------+
int MeasureDepth(int &deepest_seen, int &fatal_err)
  {
   int lo = 0;        // 0 = bugün. "Kanıtlanmış" değil, aramanın tabanı.
   int hi = -1;       // henüz ulaşılamaz nokta bulunmadı
   deepest_seen = -1;
   fatal_err = 0;

   Print("Sinyal geri-doldurma: DERİNLİK ÖLÇÜMÜ başlıyor — broker'ın kaç gün "
         "geriye tick verdiği hiçbir yerde yazmıyor, ölçüyoruz.");

   //--- 1) üstel geri sıçrama: 1, 2, 4, ..., 128
   for(int back = 1; back <= SINYAL_BF_MAX_BACK && !IsStopped(); back *= 2)
     {
      int err = 0;
      int p = ProbeBack(back, err);
      if(p < 0)
        {
         // Sembolle ilgili KALICI hata ölçümü değil, işin tamamını bitirir.
         if(err == SINYAL_BF_ERR_MARKET_UNKNOWN ||
            err == SINYAL_BF_ERR_MARKET_NOT_SEL ||
            err == SINYAL_BF_ERR_NOT_ENOUGH_MEM)
           {
            fatal_err = err;
            return lo;
           }
         hi = back;
         break;
        }
      if(p > deepest_seen)
         deepest_seen = p;
      lo = back;
     }

   if(hi < 0)
     {
      // Merdivenin sonuna kadar veri var. DAHA DERİNİ ÖLÇÜLMEDİ: 128 gün
      // zaten istenen ~92 günü kapsıyor ve merdiveni uzatmak yalnızca
      // ihtiyaç dışı gün indirtirdi.
      if(deepest_seen > lo)
         lo = deepest_seen;
      return lo;
     }

   //--- 2) ikili arama: lo (veri var) ile hi (veri yok) arasında
   while(hi - lo > 1 && !IsStopped())
     {
      int mid = lo + (hi - lo) / 2;
      int err = 0;
      int p = ProbeBack(mid, err);
      if(p < 0)
        {
         if(err == SINYAL_BF_ERR_MARKET_UNKNOWN ||
            err == SINYAL_BF_ERR_MARKET_NOT_SEL ||
            err == SINYAL_BF_ERR_NOT_ENOUGH_MEM)
           {
            fatal_err = err;
            return lo;
           }
         hi = mid;
        }
      else
        {
         // p, mid..mid+3 aralığındadır ve hi'yi AŞABİLİR: hi'deki ilk
         // "boş" sonuç, terminalin o anda henüz indirmemiş olmasından da
         // gelebilir. Aşarsa döngü koşulu kendiliğinden biter; elde daha
         // derin bir KANIT olduğu için bu iyi bir sonuçtur.
         if(p > deepest_seen)
            deepest_seen = p;
         lo = p;
        }
     }

   return lo;
  }

//+------------------------------------------------------------------+
//| Bir günün tick'lerini 48 baytlık biçimde diske yaz.               |
//|                                                                  |
//| FileWriteStruct KULLANILMAZ. MQL5'in MqlTick yapı dolgusu bizim 48|
//| baytımızla aynı olmak ZORUNDA DEĞİLDİR ve uyuşmazlık DERLEME HATASI|
//| vermez — dosya sessizce kayar, üstelik 3 ay sonra "grafik saçmalı- |
//| yor" olarak fark edilir. Her alan AÇIKÇA ve sırayla yazılır.       |
//|                                                                  |
//| Ofsetler record.rs::TickRec ile birebir:                          |
//|   recv_ms i64@0  time_msc i64@8  bid f64@16  ask f64@24           |
//|   last f64@32  symbol_id u32@40  flags u16@44  kind u16@46        |
//+------------------------------------------------------------------+
bool WriteDayFile(const long day_start_msc, const int n)
  {
   string final_path = DayPath(day_start_msc);
   string tmp_path   = final_path + ".tmp";

   ResetLastError();
   int h = FileOpen(tmp_path, FILE_WRITE | FILE_BIN);
   if(h == INVALID_HANDLE)
     {
      PrintFormat("Sinyal geri-doldurma: HATA — dosya açılamadı (%s) hata=%d",
                  tmp_path, GetLastError());
      return false;
     }

   for(int i = 0; i < n; i++)
     {
      // recv_ms @0: GEÇMİŞ VERİDE YEREL ALIM ZAMANI YOK. time_msc'in
      // kopyası yazılır ve bu durum rapor.json'da belgelenir — bkz. not 6.
      FileWriteLong(h, g_ticks[i].time_msc);                 // recv_ms   @0
      FileWriteLong(h, g_ticks[i].time_msc);                 // time_msc  @8
      FileWriteDouble(h, g_ticks[i].bid);                    // bid       @16
      FileWriteDouble(h, g_ticks[i].ask);                    // ask       @24
      FileWriteDouble(h, g_ticks[i].last);                   // last      @32
      // symbol_id @40: 0. Gerçek kimliği ice-aktarıcı atar — bu dosyanın
      // sembol tablosu YOK ve buraya uydurma bir sayı yazmak, aktarıcının
      // eşlemesiyle çakışıp tick'leri BAŞKA sembole bağlardı.
      FileWriteInteger(h, 0, INT_VALUE);                     // symbol_id @40
      FileWriteInteger(h, (int)g_ticks[i].flags, SHORT_VALUE);   // flags @44
      FileWriteInteger(h, SINYAL_KIND_TICK, SHORT_VALUE);        // kind  @46
     }

   // Yazılan bayt sayısını KAYIT SAYISIYLA doğrula. Tick başına altı dönüş
   // değerini tek tek denetlemek yerine tek, uçtan uca denetim: disk dolsa
   // da bir alan atlansa da buraya düşer.
   long wrote = (long)FileTell(h);
   FileClose(h);

   long want = (long)n * SINYAL_BF_REC_SIZE;
   if(wrote != want)
     {
      PrintFormat("Sinyal geri-doldurma: HATA — %s için %I64d bayt beklenirken "
                  "%I64d yazıldı. Dosya ATILDI (yarım gün, hiç günden KÖTÜDÜR).",
                  final_path, want, wrote);
      FileDelete(tmp_path);
      return false;
     }

   // Kapanışta yapılan son boşaltma da başarısız olabilir; diskteki gerçek
   // boyu okuyoruz. Bu, "yazdım sandım" ile "yazıldı" arasındaki tek fark.
   long on_disk = FileByteSize(tmp_path);
   if(on_disk != want)
     {
      PrintFormat("Sinyal geri-doldurma: HATA — %s diskte %I64d bayt (beklenen "
                  "%I64d). Dosya ATILDI.", final_path, on_disk, want);
      FileDelete(tmp_path);
      return false;
     }

   // ATOMİK TAKAS. Ancak burada dosya "tamamlanmış" sayılır; yeniden
   // çalıştırıldığında atlama denetimi buna güvenir — bkz. not 8.
   ResetLastError();
   if(!FileMove(tmp_path, 0, final_path, FILE_REWRITE))
     {
      PrintFormat("Sinyal geri-doldurma: HATA — %s -> %s taşıması başarısız, hata=%d",
                  tmp_path, final_path, GetLastError());
      FileDelete(tmp_path);
      return false;
     }
   return true;
  }

//+------------------------------------------------------------------+
//| rapor.json'a bir gün satırı ekle.                                 |
//+------------------------------------------------------------------+
void AppendDayJson(const long day_start_msc, const int ticks, const uint ms,
                   const string durum)
  {
   if(StringLen(g_days_json) > 0)
      g_days_json += ",\n";
   g_days_json += StringFormat(
                     "    {\"tarih\":\"%s\",\"tick\":%d,\"ms\":%u,\"durum\":\"%s\"}",
                     DayStampDotted(day_start_msc), ticks, ms, durum);
  }

//+------------------------------------------------------------------+
//| rapor.json yaz.                                                   |
//|                                                                  |
//| Metin ASCII tutulur (Türkçe harf YOK): dosyayı Rust ice-aktarıcı  |
//| okuyacak ve kod sayfası tartışması çıkmasın.                      |
//+------------------------------------------------------------------+
//| `kapsanan_gun` DOĞRUDAN verilir, `start_back`ten TÜRETİLMEZ: bugün
//| indirilmediği için kapsanan gün sayısı start_back'in kendisidir ve bunu
//| burada bir kez daha hesaplamak, iki formülün zamanla ayrışması demekti.
void WriteReport(const int depth_back, const int deepest_seen,
                 const int covered_days, const int gmt_offset_sec,
                 const string bitis_durumu)
  {
   string path = g_out_dir + "\\rapor.json";
   ResetLastError();
   int h = FileOpen(path, FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(h == INVALID_HANDLE)
     {
      PrintFormat("Sinyal geri-doldurma: HATA — rapor yazilamadi (%s) hata=%d",
                  path, GetLastError());
      return;
     }

   ulong elapsed = GetTickCount64() - g_started_ms;
   int   day_span = covered_days;
   if(day_span < 0)
      day_span = 0;
   // Payda TICK İÇEREN gün sayısıdır. "yazılan - boş" YANLIŞ olurdu:
   // g_total_ticks atlanan (zaten inmiş) günlerin tick'lerini de sayıyor,
   // o günler ise g_days_written'a hiç girmiyor — oran şişerdi.
   double per_day = 0.0;
   if(g_days_ticked > 0)
      per_day = (double)g_total_ticks / (double)g_days_ticked;

   string s = "{\n";
   s += StringFormat("  \"sembol\": \"%s\",\n", Sembol);
   s += StringFormat("  \"bitis_durumu\": \"%s\",\n", bitis_durumu);
   s += StringFormat("  \"olculen_en_eski_tarih\": \"%s\",\n",
                     DayStampDotted(DayStartMsc(depth_back)));
   s += StringFormat("  \"olculen_en_eski_gun_geriye\": %d,\n", depth_back);
   s += StringFormat("  \"tick_gorulen_en_derin_gun\": \"%s\",\n",
                     (deepest_seen >= 0 ? DayStampDotted(DayStartMsc(deepest_seen)) : "yok"));
   s += StringFormat("  \"istenen_gun\": %d,\n", g_days_want);
   s += StringFormat("  \"kapsanan_gun\": %d,\n", day_span);
   s += StringFormat("  \"tickli_gun\": %d,\n", g_days_ticked);
   s += StringFormat("  \"yazilan_gun\": %d,\n", g_days_written);
   s += StringFormat("  \"bos_gun\": %d,\n", g_days_empty);
   s += StringFormat("  \"atlanan_gun\": %d,\n", g_days_skipped);
   s += StringFormat("  \"hatali_gun\": %d,\n", g_days_failed);
   s += StringFormat("  \"toplam_tick\": %I64u,\n", g_total_ticks);
   s += StringFormat("  \"islem_gunu_basina_tick\": %.1f,\n", per_day);
   s += StringFormat("  \"olcum_istegi\": %d,\n", g_probe_calls);
   s += StringFormat("  \"gecici_hata_tekrari\": %d,\n", g_retry_rounds);
   s += StringFormat("  \"gecen_ms\": %I64u,\n", elapsed);
   s += StringFormat("  \"kayit_bayt\": %d,\n", SINYAL_BF_REC_SIZE);
   s += StringFormat("  \"sunucu_gmt_farki_sn\": %d,\n", gmt_offset_sec);
   // Ice-aktarici bu uc satiri OKUMALI: canli kayitla ayni BICIMDE ama
   // ayni ANLAMDA degiller.
   //
   // Metin SAF ASCII: dosya FILE_ANSI ile yaziliyor ve Turkce harf ya da
   // uzun tire buraya kod sayfasina bagimli bayt olarak dusrdu.
   s += "  \"recv_ms_kaynagi\": \"broker time_msc kopyasi - YEREL ALIM ZAMANI DEGIL\",\n";
   s += "  \"gun_siniri\": \"BROKER SUNUCU SAATI - canli kayit UTC'ye gore kesiyor\",\n";
   s += "  \"symbol_id\": \"her kayitta 0 - gercek kimligi ice-aktarici atar\",\n";
   s += "  \"bugun\": \"INDIRILMEDI - surmekte olan gun yarim iner ve sonraki "
        "kosularda TAMAMLANMIS sanilirdi; bugunu canli kayit tutuyor\",\n";
   s += "  \"gunler\": [\n";
   s += g_days_json;
   s += "\n  ]\n}\n";

   FileWriteString(h, s);
   FileClose(h);
   PrintFormat("Sinyal geri-doldurma: rapor yazıldı — %s", path);
  }

//+------------------------------------------------------------------+
//| Teşhis özeti.                                                     |
//+------------------------------------------------------------------+
void ReportTelemetry()
  {
   ulong elapsed = GetTickCount64() - g_started_ms;
   PrintFormat("Sinyal geri-doldurma: yazilan=%d bos=%d atlanan=%d hatali=%d | "
               "toplam tick=%I64u | olcum-istegi=%d gecici-tekrar=%d | %I64u ms",
               g_days_written, g_days_empty, g_days_skipped, g_days_failed,
               g_total_ticks, g_probe_calls, g_retry_rounds, elapsed);
  }

//+------------------------------------------------------------------+
//| OnStart — service'in TAMAMI. Timer YOK, kendi döngümüz var.       |
//+------------------------------------------------------------------+
void OnStart()
  {
   g_started_ms = GetTickCount64();
   ClampInputs();

   //--- 0) sembol adı dosya adına giriyor: önce güvenli mi diye bak.
   if(!SymbolNameIsPathSafe(Sembol))
     {
      PrintFormat("Sinyal geri-doldurma: Sembol=\"%s\" dosya adında kullanılamaz "
                  "(\\ / : * ? \" < > | ve nokta yasak). İndirme YAPILMADI — "
                  "yazım çıktı klasörünün dışına taşabilirdi.", Sembol);
      return;
     }

   //--- 1) sembol. Service grafiğe BAĞLI DEĞİL (_Symbol yok); sembol
   //    Market Watch'ta değilse CopyTicksRange 4302 döndürür.
   ResetLastError();
   if(!SymbolSelect(Sembol, true))
     {
      PrintFormat("Sinyal geri-doldurma: SymbolSelect başarısız (%s) hata=%d — "
                  "sembol adı broker'daki adla BİREBİR aynı olmalı.",
                  Sembol, GetLastError());
      return;
     }

   //--- 2) gün ızgarası. Sunucu saati okunamazsa DURUYORUZ: yanlış bir
   //    ızgara üzerinde saatlerce indirmek, tamamen yanlış tarihli 92
   //    dosya üretir ve hiçbir yerde hata vermez.
   datetime now = TimeCurrent();
   if(now <= 0)
     {
      Print("Sinyal geri-doldurma: TimeCurrent() sunucu saati vermiyor "
            "(terminal bağlı değil?). Gün ızgarası kurulamadığı için DURULDU.");
      return;
     }
   datetime today0 = (datetime)((long)now - ((long)now % SINYAL_BF_DAY_SEC));
   g_today0_msc = (long)today0 * 1000;

   // Sunucu ile GMT arasındaki fark. Kullanmıyoruz — RAPORLUYORUZ; gün
   // sınırı sunucu saatidir ve ice-aktarıcı isterse bu farkla kaydırır.
   int gmt_offset_sec = (int)((long)now - (long)TimeGMT());

   //--- 3) çıktı klasörü
   if(!FolderCreate(g_out_dir))
     {
      int err = GetLastError();
      // Klasör zaten varsa FolderCreate false döner; gerçek bir engel
      // olup olmadığını yazma anında zaten anlayacağız. Yine de loglanır.
      if(AyrintiliLog)
         PrintFormat("Sinyal geri-doldurma: FolderCreate(%s) false (hata=%d) — "
                     "klasör zaten varsa bu normaldir.", g_out_dir, err);
     }

   PrintFormat("Sinyal geri-doldurma: başlatıldı — sembol=%s, istenen gün=%d, "
               "klasör=MQL5\\Files\\%s, günlük bütçe=%d ms",
               Sembol, g_days_want, g_out_dir, g_budget_ms);
   PrintFormat("Sinyal geri-doldurma: bugün (sunucu) %s, sunucu-GMT farkı %d sn. "
               "GÜN SINIRI SUNUCU SAATİDİR — canlı kayıt UTC'ye göre kesiyor.",
               DayStampDotted(g_today0_msc), gmt_offset_sec);
   Print("Sinyal geri-doldurma: DİKKAT — geçmiş veride YEREL ALIM ZAMANI yoktur; "
         "recv_ms alanına broker time_msc'i KOPYALANIR. Replay teması bu günlerde "
         "broker damgasından türer.");

   //--- 4) ISINMA TURU. Bir sembolün tick geçmişi İLK kez istendiğinde
   //    CopyTicksRange -1/4401 döndürüp indirmeyi arka planda başlatır.
   //    Bunu TAM bütçeyle ve BUGÜN üzerinde bir kez yapıyoruz; aksi halde
   //    ilk örnekleme kısa bütçesini bu tek seferlik kuruluma harcar ve
   //    "1 gün geriye veri yok" gibi TAMAMEN YANLIŞ bir ölçümle başlanır.
     {
      int  werr = 0;
      uint wms  = 0;
      int  wn   = FetchDay(DayStartMsc(0), g_budget_ms, werr, wms);
      PrintFormat("Sinyal geri-doldurma: ısınma (bugün) — %d tick, hata=%d, %u ms",
                  wn, werr, wms);
      ArrayFree(g_ticks);
      if(wn < 0 && (werr == SINYAL_BF_ERR_MARKET_UNKNOWN ||
                    werr == SINYAL_BF_ERR_MARKET_NOT_SEL))
        {
         PrintFormat("Sinyal geri-doldurma: %s için tick geçmişi alınamıyor (hata=%d). "
                     "Ölçüm ve indirme YAPILMADI.", Sembol, werr);
         WriteReport(0, -1, 0, gmt_offset_sec, StringFormat("kalici-hata-%d", werr));
         return;
        }
     }

   //--- 5) DERİNLİK ÖLÇÜMÜ
   int deepest_seen = -1;
   int fatal_err    = 0;
   int depth_back   = MeasureDepth(deepest_seen, fatal_err);

   if(IsStopped())
     {
      Print("Sinyal geri-doldurma: derinlik ölçümü sırasında DURDURULDU.");
      ReportTelemetry();
      // kapsanan_gun = 0: indirme HİÇ başlamadı ve "1 gün kapsandı" demek
      // sonraki koşuyu yanlış yönlendirirdi.
      WriteReport(depth_back, deepest_seen, 0, gmt_offset_sec, "durduruldu-olcumde");
      return;
     }
   if(fatal_err != 0)
     {
      PrintFormat("Sinyal geri-doldurma: KALICI hata %d — indirme yapılmadı. "
                  "(4301 sembol yok, 4302 Market Watch'ta değil, 4004 bellek)",
                  fatal_err);
      WriteReport(depth_back, deepest_seen, 0, gmt_offset_sec, "kalici-hata-olcumde");
      return;
     }

   PrintFormat("Sinyal geri-doldurma: broker en eski tick: %s (%d gün geriye)%s",
               DayStampDotted(DayStartMsc(depth_back)), depth_back,
               (depth_back >= SINYAL_BF_MAX_BACK
                ? " — merdiven sonuna kadar veri var, DAHA DERİNİ ÖLÇÜLMEDİ" : ""));

   //--- 6) indirme aralığı. Broker'ın verdiği ile kullanıcının istediğinin
   //    KÜÇÜĞÜ: fazlasını indirmek saatler, azını indirmek hedefi ıskalar.
   //    Üst sınır `g_days_want - 1` DEĞİL, `g_days_want`: bugün artık
   //    indirilmediği için (not 9) inecek günler back = 1..start_back ve
   //    bunların sayısı start_back'tir. Eski formülle 92 istenirken 91
   //    TAMAMLANMIŞ gün inerdi — bir günlük sessiz eksilme.
   int start_back = depth_back;
   if(start_back > g_days_want)
      start_back = g_days_want;
   if(start_back < 0)
      start_back = 0;

   if(depth_back < g_days_want)
      PrintFormat("Sinyal geri-doldurma: UYARI — %d TAMAMLANMIŞ gün istendi ama "
                  "broker yalnızca %d gün geriye veriyor. Hedef KISALDI.",
                  g_days_want, depth_back);

   // BUGÜN (back = 0) İNDİRİLMEZ — bkz. not 9. Sürmekte olan gün yarım iner ve
   // "zaten inmiş" denetimi onu KALICI olarak tamamlanmış sayardı.
   int total_days = start_back;      // back = start_back .. 1
   Print("Sinyal geri-doldurma: BUGÜN İNDİRİLMİYOR — gün henüz bitmedi ve yarım "
         "inen bir gün sonraki koşularda TAMAMLANMIŞ sanılıp bir daha hiç "
         "sorulmazdı. Bugünü canlı kaydedici tutuyor; yarın TAM inecek.");

   if(total_days <= 0)
     {
      Print("Sinyal geri-doldurma: inecek TAMAMLANMIŞ gün yok (broker yalnızca "
            "bugünü veriyor ya da GunSayisi=1). Hiçbir şey indirilmedi.");
      ReportTelemetry();
      WriteReport(depth_back, deepest_seen, 0, gmt_offset_sec, "tamamlanmis-gun-yok");
      return;
     }

   PrintFormat("Sinyal geri-doldurma: %d gün inecek — %s .. %s",
               total_days, DayStampDotted(DayStartMsc(start_back)),
               DayStampDotted(DayStartMsc(1)));

   //--- 7) GÜNLÜK indirme. Eskiden yeniye. BUGÜN (back = 0) HARİÇ.
   string bitis = "tamam";
   for(int back = start_back; back >= 1; back--)
     {
      // Kullanıcı saatler süren bir işi durdurabilmeli.
      if(IsStopped())
        {
         Print("Sinyal geri-doldurma: DURDURULDU — kalan günler inmedi. "
               "Yeniden başlatıldığında kaldığı yerden devam eder.");
         bitis = "durduruldu";
         break;
        }

      long   ds   = DayStartMsc(back);
      int    idx  = start_back - back + 1;
      string path = DayPath(ds);

      // ZATEN İNMİŞ Mİ? Boyu 48'in tam katı olan her dosya TAMAMLANMIŞTIR —
      // yazım atomik olduğu için yarım dosya bu ada hiç ulaşamaz (not 8).
      // 0 bayt da geçerlidir: "o gün veri yok" bilgisi de bir sonuçtur ve
      // yeniden sorup aynı boşluğu bulmak dakikaları çöpe atardı.
      long have = FileByteSize(path);
      if(have >= 0 && (have % SINYAL_BF_REC_SIZE) == 0)
        {
         g_days_skipped++;
         g_total_ticks += (ulong)(have / SINYAL_BF_REC_SIZE);
         if(have > 0)
            g_days_ticked++;
         if(AyrintiliLog)
            PrintFormat("Sinyal geri-doldurma: gün %d/%d %s — ATLANDI (zaten inmiş, "
                        "%I64d kayıt)", idx, total_days, DayStampDotted(ds),
                        have / SINYAL_BF_REC_SIZE);
         AppendDayJson(ds, (int)(have / SINYAL_BF_REC_SIZE), 0, "atlandi");
         continue;
        }
      if(have > 0)
         PrintFormat("Sinyal geri-doldurma: UYARI — %s boyu %I64d, 48'in katı DEĞİL. "
                     "Bozuk sayılıp YENİDEN indiriliyor.", path, have);

      int  err  = 0;
      uint took = 0;
      int  n    = FetchDay(ds, g_budget_ms, err, took);

      if(n < 0 && IsStopped())
        {
         // FetchDay durdurulunca da -1 döner (err=0). Bunu HATA saymak,
         // kullanıcının kendi durdurmasını raporda "hatalı gün" olarak
         // gösterirdi — sonraki koşuda gerçek bir sorun sanılırdı.
         Print("Sinyal geri-doldurma: DURDURULDU — sürmekte olan gün yarıda kesildi.");
         ArrayFree(g_ticks);
         bitis = "durduruldu";
         break;
        }

      if(n < 0)
        {
         g_days_failed++;
         PrintFormat("Sinyal geri-doldurma: gün %d/%d %s — HATA %d (%u ms)",
                     idx, total_days, DayStampDotted(ds), err, took);
         AppendDayJson(ds, 0, took, StringFormat("hata-%d", err));
         ArrayFree(g_ticks);

         // KALICI hatada devam etmek anlamsız: 4004 bellek bitti demektir ve
         // sonraki gün daha küçük olmayacak; 4301/4302 sembol sorunudur ve
         // 92 kez tekrarlanırdı.
         if(err == SINYAL_BF_ERR_NOT_ENOUGH_MEM ||
            err == SINYAL_BF_ERR_MARKET_UNKNOWN ||
            err == SINYAL_BF_ERR_MARKET_NOT_SEL)
           {
            PrintFormat("Sinyal geri-doldurma: KALICI hata %d — indirme DURDURULDU.", err);
            bitis = StringFormat("kalici-hata-%d", err);
            break;
           }
         continue;
        }

      if(n == 0)
         g_days_empty++;

      // Boş gün de YAZILIR (0 baytlık dosya). Yazmasaydık her yeniden
      // çalıştırmada aynı hafta sonlarını tekrar sorardık ve 3 aylık işte
      // bu ~26 gün × bütçe kadar saf bekleme demekti.
      if(!WriteDayFile(ds, n))
        {
         g_days_failed++;
         AppendDayJson(ds, n, took, "yazma-hatasi");
         ArrayFree(g_ticks);
         continue;
        }

      g_days_written++;
      g_total_ticks += (ulong)n;
      if(n > 0)
         g_days_ticked++;
      AppendDayJson(ds, n, took, (n == 0 ? "bos" : "tamam"));

      if(AyrintiliLog || (idx % SINYAL_BF_LOG_EVERY) == 0)
         PrintFormat("Sinyal geri-doldurma: gün %d/%d %s — %d tick, %u ms%s",
                     idx, total_days, DayStampDotted(ds), n, took,
                     (n == 0 ? " (BOŞ — hafta sonu/tatil)" : ""));

      // Tepe bellek TEK GÜN kalsın: dizi burada BIRAKILIR. Tutmak, en büyük
      // günün boyunu tüm koşu boyunca elde tutmak demekti.
      ArrayFree(g_ticks);
     }

   ReportTelemetry();
   WriteReport(depth_back, deepest_seen, start_back, gmt_offset_sec, bitis);
   Print("Sinyal geri-doldurma: bitti.");
  }
//+------------------------------------------------------------------+
