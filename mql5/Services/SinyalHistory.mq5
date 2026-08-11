//+------------------------------------------------------------------+
//|                                                SinyalHistory.mq5 |
//|        MT5 -> Sinyal köprüsü: MT5'in KENDİ geçmiş barlarını yayar |
//+------------------------------------------------------------------+
//| MİMARİ NOTLARI (hepsi MQL5 dokümanıyla doğrulanmıştır):
//|
//| 1. NEDEN SERVICE, NEDEN EA DEĞİL. CopyRates çağıran thread'i
//|    BLOKLAR ve zaman aşımı süresi DOKÜMANTE EDİLMEMİŞTİR; canlı
//|    hesaplarda 30-60 saniyelik donmalar raporlanmıştır. EA tek
//|    thread olduğu için (OnTick/OnTimer/OnBookEvent hepsi aynı
//|    thread) böyle bir çağrı tick toplamayı TAMAMEN durdururdu.
//|    Service kendi thread'inde çalışır; blokladığında kimseyi
//|    etkilemez ve Sleep() serbesttir.
//|
//| 2. TIMER YOK. Service'te OnTimer/EventSetTimer KULLANILMAZ; akış
//|    OnStart içindeki kendi döngümüzdür. IsStopped() her turda
//|    denetlenir, aksi halde terminal kapanışında service asılı kalır.
//|
//| 3. GRAFİĞE BAĞLI DEĞİLİZ. _Symbol ve _Period YOKTUR. Sembol adı
//|    isteğin içinde gelir (HistReq.symbol) ve SymbolSelect ile
//|    Market Watch'a alınır — seçili olmayan sembolde CopyRates
//|    4302 döndürür.
//|
//| 4. FİZİKSEL DİZİ DÜZENİ HER ZAMAN ESKİ -> YENİ. index 0 EN ESKİ
//|    bardır; ArraySetAsSeries bunu DEĞİŞTİRMEZ. Bayrağı yine de
//|    açıkça kuruyoruz: varsayıma güvenmek yerine ucuz sigorta.
//|
//| 5. DÖNÜŞ DEĞERİ KISMİ OLABİLİR. n > 0 "tamam" DEMEK DEĞİLDİR;
//|    n == count denetlenir. Terminal geçmişi sunucudan indirirken
//|    ilk çağrılar eksik döner — bu NORMALDİR, hata değil.
//|
//| 6. HALKA DOLARSA BAR KALICI KAYBOLUR. Yazar yeniden denemezse
//|    kayıp geri alınamaz; bu yüzden push 0 döndüğünde kısa Sleep ile
//|    YENİDEN DENENİR. Bütçe dolarsa kayıp SAYILIR ve çekirdeğe ERROR
//|    kaydıyla bildirilir — sessizce yarım seri göndermek, tüketicinin
//|    olmayan bir boşluğu gerçek sanmasına yol açardı.
//+------------------------------------------------------------------+
#property service
#property copyright "Sinyal"
#property version   "1.00"
#property strict

#include <Sinyal/HistBridge.mqh>

//--- girdiler -------------------------------------------------------
input string InstanceName      = "mt5-1";  // Köprü örnek adı (EA ile AYNI olmalı)
input int    MaxBarsPerRequest = 5000;     // İstek başına üst sınır (halka 32768 slot)
input int    HistoryBudgetMs   = 4000;     // CopyRates yeniden denemelerine ayrılan süre
input int    RetryMs           = 200;      // Geçici hatada bekleme
input int    RingWaitMs        = 1500;     // Halka dolu kaldığında azami bekleme
input int    IdleSleepMs       = 20;       // İstek yokken bekleme
input bool   VerboseLog        = true;     // İstek başına ayrıntılı log

//--- SinyalHistOpen başarısızsa kaç kez denenecek.
//    Terminal açılışında segmentler bir önceki oturumdan kalmış
//    olabilir; birkaç saniye sonra temizlenir. Sonsuz denemiyoruz:
//    kalıcı sebep genelde AYNI örnek adıyla ikinci bir service'tir
//    ve onu sessizce beklemek sorunu gizlerdi.
#define SINYAL_OPEN_ATTEMPTS 10
#define SINYAL_OPEN_WAIT_MS  1000

//--- durum ----------------------------------------------------------
long g_handle = 0;

// Bar tamponu. SICAK YOLDA ArrayResize YAPILMAZ, OnStart'ta bir kez
// ayrılır. DİNAMİK olmak ZORUNDA: statik dizi CopyRates'ten 4407
// (hedef tampon yetersiz) döndürür.
// Not: CopyRates dinamik diziyi kopyalanan eleman sayısına göre kendisi
// yeniden boyutlandırabilir; ön tahsis tekrar tekrar büyümeyi önler.
MqlRates g_rates[];

// Etkin bar tavanı: MaxBarsPerRequest ile TERMINAL_MAXBARS'ın küçüğü.
// TERMINAL_MAXBARS kullanıcı ayarıdır ve aşan istek CopyRates'ten
// 4404 döndürür — kırpmak, hata almaktan iyidir.
int g_bar_cap = 0;

// Süre girdilerinin DENETLENMİŞ kopyaları. Girdiler doğrudan
// kullanılmaz: negatif bir değer Sleep() sözleşmesinin dışındadır ve
// sıfır bütçe her isteği tek denemede hataya çevirirdi.
int g_idle_ms      = 20;
int g_retry_ms     = 200;
int g_budget_ms    = 15000;
int g_ring_wait_ms = 30000;

// Teşhis sayaçları.
ulong g_reqs_ok       = 0;
ulong g_reqs_error    = 0;   // ERROR kaydıyla yanıtlanan istek
ulong g_reqs_ignored  = 0;   // req_id = 0 — korele edilemez, yanıt bile yazılamaz
ulong g_bars_pushed   = 0;
ulong g_bars_lost     = 0;   // halka dolu kaldı, teslimat kesildi
ulong g_ring_waits    = 0;   // halka dolu bulundu (her deneme bir sayı)
ulong g_retry_rounds  = 0;   // CopyRates yeniden denendi
ulong g_partials      = 0;   // n < count döndü
ulong g_pop_errors    = 0;
ulong g_push_errors   = 0;
int   g_last_push_err = 0;
datetime g_last_report = 0;

//+------------------------------------------------------------------+
//| Süre girdilerini kullanılabilir aralığa çek.                      |
//|                                                                  |
//| Girdiler kullanıcıdan gelir ve service bir kez başlar; burada     |
//| denetlemezsek yanlış bir değer saatlerce sessizce yanlış davranış |
//| üretir (ör. bütçe 0 -> her istek ilk denemede hata).              |
//+------------------------------------------------------------------+
void ClampInputs()
  {
   g_idle_ms = IdleSleepMs;
   if(g_idle_ms < 1)    g_idle_ms = 1;
   if(g_idle_ms > 1000) g_idle_ms = 1000;

   g_retry_ms = RetryMs;
   if(g_retry_ms < 1)    g_retry_ms = 1;
   if(g_retry_ms > 5000) g_retry_ms = 5000;

   // ÜST SINIR ZORUNLU. Çekirdek bir isteği en fazla
   // SINYAL_HIST_CONSUMER_TIMEOUT_MS bekler; ondan uzun süren bir teslimat
   // ÖKSÜZ barlar üretir (bkz. HistBridge.mqh). Uzun bütçe hiçbir şey
   // kazandırmaz, yalnızca terminali meşgul eder ve kuyruğun başını tıkar.
   g_budget_ms = HistoryBudgetMs;
   if(g_budget_ms > SINYAL_HIST_BUDGET_MAX_MS)
     {
      PrintFormat("Sinyal geçmiş: HistoryBudgetMs=%d çok büyük, %d'e kırpıldı "
                  "(çekirdek zaten en fazla %d ms bekliyor).",
                  HistoryBudgetMs, SINYAL_HIST_BUDGET_MAX_MS,
                  SINYAL_HIST_CONSUMER_TIMEOUT_MS);
      g_budget_ms = SINYAL_HIST_BUDGET_MAX_MS;
     }
   if(g_budget_ms < g_retry_ms) g_budget_ms = g_retry_ms;

   g_ring_wait_ms = RingWaitMs;
   if(g_ring_wait_ms < 0) g_ring_wait_ms = 0;
   if(g_ring_wait_ms > SINYAL_HIST_RING_WAIT_MAX_MS)
     {
      PrintFormat("Sinyal geçmiş: RingWaitMs=%d çok büyük, %d'e kırpıldı "
                  "(veri barı ve ERROR barı için İKİ KEZ harcanabilir).",
                  RingWaitMs, SINYAL_HIST_RING_WAIT_MAX_MS);
      g_ring_wait_ms = SINYAL_HIST_RING_WAIT_MAX_MS;
     }
  }

//+------------------------------------------------------------------+
//| İstenen bar sayısını uygulanabilir tavana kırp.                   |
//+------------------------------------------------------------------+
int ClampCount(const uint want)
  {
   if(want == 0)
      return 0;
   if(want > (uint)g_bar_cap)
      return g_bar_cap;
   return (int)want;
  }

//+------------------------------------------------------------------+
//| Barı halkaya yaz; halka doluysa YENİDEN DENE.                     |
//|                                                                  |
//| Vazgeçmek barı KALICI olarak kaybetmektir: halka SPSC'dir ve      |
//| yazar için geri alma yoktur. Yine de sonsuz beklemiyoruz —        |
//| çekirdek ölmüşse service tüm kuyruğu bloklardı.                   |
//+------------------------------------------------------------------+
bool PushBarBlocking(const SinyalBarRec &bar)
  {
   ulong deadline = GetTickCount64() + (ulong)g_ring_wait_ms;
   while(!IsStopped())
     {
      long rc = SinyalHistPushBar(g_handle, bar);
      if(rc > 0)                          // sözleşme: >0 başarı
        {
         g_bars_pushed++;
         return true;
        }
      if(rc < 0)
        {
         g_push_errors++;
         if(g_last_push_err != (int)rc)
           {
            g_last_push_err = (int)rc;
            PrintFormat("Sinyal geçmiş: HATA — SinyalHistPushBar rc=%I64d (%s)",
                        rc, SinyalHistErrorText());
           }
         return false;
        }
      // rc == 0: halka DOLU. Çekirdek yetişemiyor.
      g_ring_waits++;
      if(GetTickCount64() >= deadline)
         return false;
      Sleep(1);
     }
   return false;
  }

//+------------------------------------------------------------------+
//| İsteği HATA kaydıyla yanıtla.                                     |
//|                                                                  |
//| Sessizce dönmek EN KÖTÜ davranıştır: çekirdek yanıtı süresiz      |
//| bekler ve "geçmiş yok" ile "geçmiş alınamadı" ayırt edilemez.     |
//| Hata kodu spread alanında taşınır, total 0'dır (bkz. bars.rs).    |
//+------------------------------------------------------------------+
void PushErrorBar(const SinyalHistReq &req, const int err_code)
  {
   SinyalBarRec bar;
   ZeroMemory(bar);
   bar.time_msc   = (long)TimeCurrent() * 1000;
   bar.req_id     = req.req_id;
   bar.symbol_id  = req.symbol_id;
   bar.timeframe  = req.timeframe;
   bar.spread     = err_code;          // ERROR'da bu alan HATA KODUDUR
   bar.flags      = SINYAL_BARFLAG_ERROR | SINYAL_BARFLAG_LAST;
   bar.index      = 0;
   bar.total      = 0;
   PushBarBlocking(bar);
   g_reqs_error++;
  }

//+------------------------------------------------------------------+
//| Bütçe içinde CopyRates dene; g_rates'e kopyalanan sayıyı döndür.  |
//|                                                                  |
//| Üç ayrı durum vardır ve karıştırmak pahalıdır:                    |
//|   n < 0  -> GetLastError(); GEÇİCİ ise yeniden dene, KALICI ise   |
//|             hemen çık (yeniden denemek yalnızca kuyruğu geciktirir)|
//|   n < want -> KISMİ. Terminal geçmişi indiriyor olabilir; bütçe    |
//|             dolana kadar dene, sonra ELDEKİNİ gönder.             |
//|   n == want -> tamam.                                             |
//|                                                                  |
//| ResetLastError() her çağrıdan ÖNCE ŞART: GetLastError() önceki bir |
//| işlemin kodunu döndürürse hata sınıflandırması tamamen yanılır.   |
//+------------------------------------------------------------------+
int CopyHistoryWithBudget(const string sym, const ENUM_TIMEFRAMES tf,
                          const long to_msc, const int want, int &out_err)
  {
   ulong deadline = GetTickCount64() + (ulong)g_budget_ms;
   int   last_ok  = -1;      // g_rates'in İÇİNDEKİ geçerli eleman sayısı
   out_err = 0;

   while(!IsStopped())
     {
      // index 0 = EN ESKİ bar. CopyRates fiziksel düzeni her zaman
      // eskiden yeniye kurar; bayrağı açıkça kurmak, araya giren bir
      // çağrının indeksleme yönünü çevirmesine karşı sigortadır.
      ArraySetAsSeries(g_rates, false);

      ResetLastError();
      int n;
      if(to_msc > 0)
        {
         // Tarihe göre istek: dokümana göre YALNIZCA verilen tarihten
         // ESKİ veya ona eşit barlar döner. to_msc MİLİSANİYEDİR,
         // datetime ise SANİYE.
         n = CopyRates(sym, tf, (datetime)(to_msc / 1000), want, g_rates);
        }
      else
        {
         // start_pos = 0 GÜNCEL (henüz kapanmamış) bardır; seri bu
         // yüzden en yeniden geriye doğru 'want' bar kapsar.
         n = CopyRates(sym, tf, 0, want, g_rates);
        }

      if(n < 0)
        {
         int err = GetLastError();
         out_err = err;
         if(!SinyalHistErrorIsTransient(err))
            return -1;                 // KALICI — denemek düzeltmez
         g_retry_rounds++;
        }
      else if(n >= want)
        {
         out_err = 0;
         return n;                     // tam teslimat
        }
      else
        {
         // KISMİ. Elde ne varsa onu koru; sonraki deneme diziyi EZER,
         // bu yüzden 'en iyi' değil 'SON' başarılı sayı saklanır.
         last_ok = n;
         g_partials++;
        }

      if(GetTickCount64() >= deadline)
         break;
      Sleep(g_retry_ms);
     }

   if(last_ok > 0)
     {
      out_err = 0;
      return last_ok;                  // bütçe doldu, eldekini gönder
     }
   if(out_err == 0)
      out_err = SINYAL_ERR_HISTORY_NOT_FOUND;
   return -1;
  }

//+------------------------------------------------------------------+
//| Kopyalanan barları BarRec'e çevirip halkaya yaz.                  |
//+------------------------------------------------------------------+
void PublishBars(const SinyalHistReq &req, const int n)
  {
   int      tf_sec = SinyalTimeframeSeconds(req.timeframe);
   datetime now    = TimeCurrent();    // SUNUCU saati — bar zamanları da öyle
   SinyalBarRec bar;

   for(int i = 0; i < n; i++)
     {
      ZeroMemory(bar);
      // MqlRates.time SANİYEDİR, BarRec.time_msc MİLİSANİYE. Çarpmayı
      // atlamak iki kaynağın bar sınırlarını 1000 kat kaydırır ve hata
      // hiçbir yerde patlamaz — yalnızca kesişimde çift/eksik bar olur.
      bar.time_msc    = (long)g_rates[i].time * 1000;
      bar.open        = g_rates[i].open;
      bar.high        = g_rates[i].high;
      bar.low         = g_rates[i].low;
      bar.close       = g_rates[i].close;
      bar.tick_volume = g_rates[i].tick_volume;
      bar.real_volume = g_rates[i].real_volume;
      bar.req_id      = req.req_id;
      bar.symbol_id   = req.symbol_id;
      bar.timeframe   = req.timeframe;
      bar.spread      = (int)g_rates[i].spread;   // MqlRates.spread INT'tir
      bar.index       = (uint)i;
      bar.total       = (uint)n;

      uint flags = 0;
      // real_volume == 0 "hacim sıfır" DEĞİL "veri yok" demektir; çoğu
      // forex broker'ı gerçek hacim yayınlamaz. Bayrağı yalnızca gerçek
      // bir ölçüm varken kuruyoruz ki tüketici 0'ı hacim sanmasın.
      if(g_rates[i].real_volume > 0)
         flags |= SINYAL_BARFLAG_HAS_REAL_VOLUME;
      if(i == n - 1)
        {
         flags |= SINYAL_BARFLAG_LAST;
         if(SinyalBarIsForming(g_rates[i].time, tf_sec, now))
            flags |= SINYAL_BARFLAG_PARTIAL;
        }
      bar.flags = flags;

      if(!PushBarBlocking(bar))
        {
         // Teslimat kesildi. Kalanları saymak, sessizce yarım seri
         // göndermekten iyidir; ayrıca çekirdeğe ERROR kaydı ile
         // bildiriyoruz, aksi halde LAST hiç gelmez ve istek asılı kalır.
         ulong lost = (ulong)(n - i);
         g_bars_lost += lost;
         PrintFormat("Sinyal geçmiş: UYARI — halka DOLU, req=%u için %I64u bar "
                     "KAYBEDİLDİ (%d/%d teslim edildi). Çekirdek barları "
                     "yeterince hızlı çekmiyor.",
                     req.req_id, lost, i, n);
         PushErrorBar(req, SINYAL_HIST_ERR_RING_FULL);
         return;
        }
     }

   g_reqs_ok++;
  }

//+------------------------------------------------------------------+
//| Tek bir isteği karşıla.                                           |
//+------------------------------------------------------------------+
void ServeRequest(const SinyalHistReq &req)
  {
   // req_id = 0 protokolde GEÇERSİZDİR. Hata kaydı bile yazamayız:
   // çekirdek yanıtı req_id ile eşleştirir, 0 hiçbir isteğe bağlanmaz.
   if(req.req_id == 0)
     {
      g_reqs_ignored++;
      Print("Sinyal geçmiş: req_id=0 olan istek ATLANDI (korele edilemez).");
      return;
     }

   string sym = SinyalHistReadSymbol(req);
   if(StringLen(sym) == 0)
     {
      // Sembol adı isteğin içinde gelir; boşsa çekirdek doldurmamıştır.
      PrintFormat("Sinyal geçmiş: req=%u sembol adı BOŞ (symbol_id=%u).",
                  req.req_id, req.symbol_id);
      PushErrorBar(req, SINYAL_ERR_MARKET_UNKNOWN_SYM);
      return;
     }

   ENUM_TIMEFRAMES tf;
   if(!SinyalMapTimeframe(req.timeframe, tf))
     {
      PrintFormat("Sinyal geçmiş: req=%u bilinmeyen zaman dilimi %u (%s).",
                  req.req_id, req.timeframe, sym);
      PushErrorBar(req, SINYAL_HIST_ERR_BAD_REQUEST);
      return;
     }

   int want = ClampCount(req.count);
   if(want <= 0)
     {
      PrintFormat("Sinyal geçmiş: req=%u geçersiz bar sayısı %u (%s).",
                  req.req_id, req.count, sym);
      PushErrorBar(req, SINYAL_HIST_ERR_BAD_REQUEST);
      return;
     }
   if((uint)want < req.count)
      PrintFormat("Sinyal geçmiş: req=%u istenen %u bar %d'e KIRPILDI "
                  "(tavan: MaxBarsPerRequest / TERMINAL_MAXBARS).",
                  req.req_id, req.count, want);

   // Service grafiğe bağlı olmadığı için sembolün Market Watch'ta olduğu
   // GARANTİ DEĞİLDİR; seçili değilse CopyRates 4302 döndürür.
   ResetLastError();
   if(!SymbolSelect(sym, true))
     {
      int err = GetLastError();
      PrintFormat("Sinyal geçmiş: SymbolSelect başarısız (%s) hata=%d", sym, err);
      PushErrorBar(req, (err != 0 ? err : SINYAL_ERR_MARKET_UNKNOWN_SYM));
      return;
     }

   int err_code = 0;
   int n = CopyHistoryWithBudget(sym, tf, req.to_msc, want, err_code);
   if(n <= 0)
     {
      PrintFormat("Sinyal geçmiş: req=%u %s dilim=%u — geçmiş alınamadı, hata=%d",
                  req.req_id, sym, req.timeframe, err_code);
      PushErrorBar(req, err_code);
      return;
     }

   if(VerboseLog)
      PrintFormat("Sinyal geçmiş: req=%u %s dilim=%u — %d/%d bar (%s)",
                  req.req_id, sym, req.timeframe, n, want,
                  (n == want ? "tam" : "KISMİ"));

   PublishBars(req, n);
  }

//+------------------------------------------------------------------+
//| Teşhis raporu.                                                    |
//+------------------------------------------------------------------+
void ReportTelemetry(const bool force)
  {
   datetime now = TimeLocal();
   if(!force && now - g_last_report < 60)
      return;
   g_last_report = now;
   if(!VerboseLog && !force)
      return;

   PrintFormat("Sinyal geçmiş: istek=%I64u/hata=%I64u/atlanan=%I64u | "
               "bar=%I64u kayip=%I64u | halka-bekleme=%I64u | "
               "yeniden-deneme=%I64u kismi=%I64u | pop-hata=%I64u push-hata=%I64u",
               g_reqs_ok, g_reqs_error, g_reqs_ignored,
               g_bars_pushed, g_bars_lost, g_ring_waits,
               g_retry_rounds, g_partials, g_pop_errors, g_push_errors);

   if(g_bars_lost > 0)
      Print("Sinyal geçmiş: UYARI — halka doldu ve bar KAYBEDİLDİ. "
            "Çekirdek yetişemiyor veya duruyor.");
  }

//+------------------------------------------------------------------+
//| Köprüyü aç. Geçici başarısızlığa karşı sınırlı yeniden deneme.    |
//+------------------------------------------------------------------+
bool OpenBridge()
  {
   for(int attempt = 1; attempt <= SINYAL_OPEN_ATTEMPTS && !IsStopped(); attempt++)
     {
      g_handle = SinyalHistOpen(InstanceName);
      if(g_handle > 0)
         return true;
      PrintFormat("Sinyal geçmiş: köprü açılamadı (%d/%d) — %s",
                  attempt, SINYAL_OPEN_ATTEMPTS, SinyalHistErrorText());
      Sleep(SINYAL_OPEN_WAIT_MS);
     }
   g_handle = 0;
   // En olası kalıcı sebep: aynı InstanceName ile ikinci bir service
   // çalışıyor. Köprü bunu bilerek reddeder — iki yazar halkayı bozardı.
   Print("Sinyal geçmiş: köprü AÇILAMADI. Aynı örnek adıyla ikinci bir "
         "service çalışıyor olabilir.");
   return false;
  }

//+------------------------------------------------------------------+
//| OnStart — service'in TAMAMI. Timer YOK, kendi döngümüz var.       |
//+------------------------------------------------------------------+
void OnStart()
  {
   string problem = "";

   if(!SinyalHistCheckDllPermission(problem))
     {
      Print("Sinyal geçmiş: ", problem);
      return;
     }
   if(!SinyalHistVerifyLayout(problem))
     {
      // Sessizce bozuk bar yazmaktansa hiç başlamamak doğru: bu barlar
      // sinyal üreten modelin girdisi olacak.
      Print("Sinyal geçmiş: YERLEŞİM UYUMSUZ — ", problem);
      return;
     }

   ClampInputs();

   if(!OpenBridge())
      return;

   // TERMINAL_MAXBARS kullanıcı ayarıdır ("Max bars in chart") ve aşan
   // istek CopyRates'ten hata döndürür. Çalışma zamanında okunur çünkü
   // kullanıcı terminali yeniden başlatmadan değiştirebilir.
   long term_max = TerminalInfoInteger(TERMINAL_MAXBARS);
   g_bar_cap = MaxBarsPerRequest;
   if(g_bar_cap < 1)
      g_bar_cap = 1;
   if(term_max > 0 && term_max < (long)g_bar_cap)
      g_bar_cap = (int)term_max;

   // Tamponu BİR KEZ ayır — döngüde ArrayResize YAPILMAZ.
   if(ArrayResize(g_rates, g_bar_cap) != g_bar_cap)
     {
      PrintFormat("Sinyal geçmiş: %d barlık tampon ayrılamadı.", g_bar_cap);
      SinyalHistClose(g_handle);
      g_handle = 0;
      return;
     }
   ArraySetAsSeries(g_rates, false);   // index 0 = EN ESKİ

   PrintFormat("Sinyal geçmiş: başlatıldı — örnek=%s, bar tavanı=%d "
               "(TERMINAL_MAXBARS=%I64d), bütçe=%dms, halka beklemesi=%dms",
               InstanceName, g_bar_cap, term_max, g_budget_ms, g_ring_wait_ms);

   SinyalHistReq req;
   while(!IsStopped())
     {
      // Her turda sıfırla: kısmi bir yazımda önceki isteğin sembol adı
      // yeni req_id ile eşleşip YANLIŞ sembolün barlarını yayınlatırdı.
      ZeroMemory(req);
      long rc = SinyalHistPopReq(g_handle, req);
      if(rc > 0)                          // sözleşme: >0 = istek var
        {
         ServeRequest(req);
        }
      else if(rc == SINYAL_HIST_NONE)
        {
         // İstek yok. Meşgul bekleme YAPMIYORUZ: geçmiş isteği seyrektir
         // ve 20 ms gecikme, CopyRates'in kendi gecikmesinin yanında
         // ölçülemez.
         Sleep(g_idle_ms);
        }
      else
        {
         // Gerçek hata. Sıkı döngüde log basmamak için bekliyoruz.
         g_pop_errors++;
         PrintFormat("Sinyal geçmiş: HATA — SinyalHistPopReq rc=%I64d (%s)",
                     rc, SinyalHistErrorText());
         Sleep(200);
        }

      ReportTelemetry(false);
     }

   ReportTelemetry(true);
   SinyalHistClose(g_handle);
   g_handle = 0;
   Print("Sinyal geçmiş: durduruldu.");
  }
//+------------------------------------------------------------------+
