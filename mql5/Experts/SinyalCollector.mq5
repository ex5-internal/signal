//+------------------------------------------------------------------+
//|                                             SinyalCollector.mq5 |
//|          MT5 -> Sinyal köprüsü: tick + DOM toplama, emir yürütme |
//+------------------------------------------------------------------+
//| MİMARİ NOTLARI (hepsi MQL5 dokümanıyla doğrulanmıştır):
//|
//| 1. TEK THREAD. Bir EA'nın OnTick/OnTimer/OnBookEvent/
//|    OnTradeTransaction işleyicileri AYNI thread'de sırayla çalışır
//|    ("Expert Advisors, like all other MQL programs, are
//|    single-threaded"). Paylaşımlı bellekteki halkalar SPSC olduğu
//|    için bu şarttır. FARKLI grafiklerdeki EA'lar ayrı thread'lerde
//|    olduğundan, aynı InstanceName ile ikinci bir kopya açılırsa
//|    köprü bunu reddeder (isimli mutex).
//|
//| 2. TICK KAYBI KAÇINILMAZDIR. Doküman: "In case when OnTick function
//|    for the previous quote is being processed when a new quote is
//|    received, the new quote will be ignored." Aynı kural OnTimer ve
//|    OnChartEvent için de geçerli. Bu yüzden işleyiciler MİKROSANİYE
//|    mertebesinde kalmalı: hesaplama, string işlemi, Print YOK.
//|    Kaybı gizlemiyoruz — g_terminal_gaps ile ölçüp raporluyoruz.
//|
//| 3. OnBookEvent İSTİSNA. DOM olayları asla atlanmaz, kuyruğa alınır.
//|    Bu yüzden en kritik yol burasıdır ve en kısa tutulmalıdır.
//|
//| 4. TIMER ÇÖZÜNÜRLÜĞÜ 10-16 ms. Doküman: "timer events are generated
//|    no more than 1 time in 10-16 milliseconds due to hardware
//|    limitations." EventSetMillisecondTimer(1) İŞE YARAMAZ. Kendi
//|    grafiği ve DOM'u olmayan semboller bu yüzden farklı bir gecikme
//|    sınıfındadır ve SINYAL_SYMFLAG_POLLED_ONLY ile işaretlenir.
//|
//| 5. OnTradeTransaction kuyruğu 1024 ve yavaş işlemede eskiler EZİLİR.
//|    İşleyici yalnızca halkaya yazar; her türlü ağır iş OnTimer'a
//|    taşınmıştır.
//+------------------------------------------------------------------+
#property copyright "Sinyal"
#property version   "1.00"
#property strict

#include <Sinyal/Bridge.mqh>

//--- girdiler -------------------------------------------------------
input string InstanceName    = "mt5-1";   // Köprü örnek adı (broker başına BENZERSİZ)
input string SymbolList      = "";        // Virgüllü sembol listesi; boş = Market Watch
input int    TimerMs         = 16;        // Timer periyodu (donanım tabanı 10-16 ms)
input bool   EnableTrading   = false;     // Emir yürütmeyi aç (varsayılan KAPALI)
input bool   EnableBook      = true;      // DOM aboneliğini dene
input int    MaxSymbols      = 512;       // Üst sınır (Market Watch limiti 1000)
input bool   VerboseLog      = true;      // Açılışta ayrıntılı teşhis
input int    ReconcileSec    = 5;         // Gerçekleşmiş sonuç mutabakatı (0 = kapalı)

//--- durum ----------------------------------------------------------
long   g_handle = 0;

// Sembol tablosu. g_name[] ALFABETİK SIRALIDIR; OnBookEvent sıcak yolunda
// ikili arama yapabilmek için. g_id[i], g_name[i]'nin wire kimliğidir.
string g_name[];
uint   g_id[];
int    g_count = 0;

// Sembol başına son görülen tick durumu. Yeni tick tespiti YALNIZCA
// time_msc ile yapılamaz: aynı ms içinde birden fazla tick olabilir ve
// time_msc geri gidebilir (sunucu düzeltmesi/yeniden bağlanma).
long   g_last_msc[];
double g_last_bid[];
double g_last_ask[];
double g_last_last[];
double g_last_vol[];

// Sembol özellikleri (emir doğrulaması için; SinyalSetSymbols'a da gider)
uint   g_exemode[];
uint   g_fillmask[];
uint   g_digits[];
double g_tick_size[];
double g_vol_min[];
double g_vol_max[];
double g_vol_step[];
uchar  g_book_sub[];     // MarketBookAdd başarılı mı
uchar  g_ready[];        // en az bir geçerli tick geldi mi

// --- durum toplama (hesap / pozisyon / emir) ---
// Sıcak yolda tahsis olmasın diye ÖNCEDEN ayrılır.
SinyalAccount  g_acc;
SinyalPosition g_pos[];
SinyalOrder    g_ord[];
datetime       g_last_state_pub = 0;
// OnTrade tetiklendiğinde işaretlenir; bir sonraki timer'da hemen yayımla.
bool           g_state_dirty = true;
ulong          g_state_pubs  = 0;
ulong          g_state_errs  = 0;
// Sonuç halkasına yazılamayan olay sayısı (emir olayı KAYBI).
ulong          g_res_dropped = 0;

// Teşhis sayaçları. OnTick/OnBookEvent içinde yalnızca ARTIRILIR;
// raporlama OnTimer'da yapılır (sıcak yolda Print yasak).
ulong  g_ticks_pushed   = 0;
ulong  g_ticks_dropped  = 0;   // halka dolu
ulong  g_books_pushed   = 0;
ulong  g_books_dropped  = 0;
ulong  g_terminal_gaps  = 0;   // terminalin ATLADIĞI tahmini tick sayısı
ulong  g_msc_regress    = 0;   // time_msc geriye gitti (anomali)
ulong  g_cmds_done      = 0;
ulong  g_cmds_rejected  = 0;
ulong  g_timer_skips    = 0;   // OnTimer olayı düşürüldü
// Mutabakat turu (gerçekleşmiş kâr/komisyon/swap).
//
// `g_recon_last_deal` bir SU HATTI: MT5'te deal biletleri artan sırada
// verilir, o yüzden "bundan büyük olanlar yeni" demek yeterli ve tur başına
// tablo tutmaya gerek yok. 0 = henüz hiç tur koşmadı (ilk tur yayımlamaz).
ulong  g_recon_last_deal = 0;
datetime g_recon_last_run = 0;
ulong  g_recon_sent      = 0;  // mutabakattan yayımlanan olay
ulong  g_recon_failed    = 0;  // HistorySelect başarısız
ulong  g_last_timer_qpc = 0;
ulong  g_qpc_freq       = 1;
datetime g_last_report  = 0;

// --- sessiz başarısızlığa karşı ---
// Köprü GERÇEK hata döndürdü (halka dolu DEĞİL). Bu sayaç yokken hatalar
// sessizce yutuluyor ve "neden veri akmıyor?" sorusu cevaplanamıyordu.
ulong  g_push_errors    = 0;
int    g_last_push_err  = 0;
// SymbolInfoTick başarısız — sembol Market Watch'tan düşmüş olabilir.
ulong  g_symtick_fails  = 0;
// Bir önceki rapordaki tick sayısı; durgunluk tespiti için.
ulong  g_ticks_prev_report = 0;

// DOM tamponu — SICAK YOLDA TAHSİS YAPILMAZ, önceden ayrılır.
MqlBookInfo g_book[];
double      g_bk_price[];
double      g_bk_volume[];
uchar       g_bk_kind[];

//+------------------------------------------------------------------+
//| Sembolü ikili aramayla bul. Bulunamazsa -1.                       |
//|                                                                  |
//| OnBookEvent sıcak yolunda kullanılır: aynı grafikteki BAŞKA bir   |
//| program başka bir sembole abone olursa bizim işleyicimiz de       |
//| tetiklenir (broadcast). Doğrusal tarama 100+ sembolde çok pahalı. |
//+------------------------------------------------------------------+
// REFERANSLA alınır, değerle DEĞİL. MQL5'te string'i değerle geçirmek
// içeriğini kopyalar; bu fonksiyon OnBookEvent ve OnTradeTransaction gibi
// olay başına çağrılan sıcak yollarda kullanılıyor ve oradaki her tahsis
// kuyruk baskısına dönüşür (işlem kuyruğu 1024, taşarsa ESKİ olaylar
// sessizce ezilir). Karşılaştırma zaten tahsis yapmıyor; imza da yapmasın.
int SymbolIndexOf(const string &sym)
  {
   int lo = 0, hi = g_count - 1;
   while(lo <= hi)
     {
      int mid = (lo + hi) >> 1;
      int cmp = StringCompare(g_name[mid], sym);
      if(cmp == 0) return mid;
      if(cmp < 0)  lo = mid + 1;
      else         hi = mid - 1;
     }
   return -1;
  }

//+------------------------------------------------------------------+
//| Sembol listesini hazırla (girdiden veya Market Watch'tan).        |
//+------------------------------------------------------------------+
bool BuildSymbolList()
  {
   string raw[];
   int n = 0;

   if(StringLen(SymbolList) > 0)
     {
      string parts[];
      n = StringSplit(SymbolList, ',', parts);
      ArrayResize(raw, n);
      for(int i = 0; i < n; i++)
        {
         string s = parts[i];
         StringTrimLeft(s);
         StringTrimRight(s);
         raw[i] = s;
        }
     }
   else
     {
      // Market Watch'taki semboller. DİKKAT: SymbolName indeksleri
      // KARARSIZDIR; yalnızca başlangıç listesini kurmak için kullanılır,
      // wire kimliği olarak ASLA kullanılmaz.
      n = SymbolsTotal(true);
      ArrayResize(raw, n);
      for(int i = 0; i < n; i++)
         raw[i] = SymbolName(i, true);
     }

   // Boşları at, MaxSymbols'a kırp, alfabetik sırala (ikili arama için).
   int keep = 0;
   for(int i = 0; i < n && keep < MaxSymbols; i++)
     {
      if(StringLen(raw[i]) == 0)
         continue;
      // 31 baytı aşan adı SESSİZCE KESMİYORUZ — kesilmiş ad, çekirdeğin
      // yanlış sembole emir göndermesi demek olurdu.
      if(StringLen(raw[i]) >= SINYAL_SYMBOL_NAME_LEN)
        {
         Print("Sinyal: sembol adı çok uzun, ATLANDI: ", raw[i]);
         continue;
        }
      raw[keep++] = raw[i];
     }
   if(keep == 0)
     {
      Print("Sinyal: hiç sembol yok.");
      return false;
     }
   ArrayResize(raw, keep);
   // Basit ekleme sıralaması — açılışta bir kez, sıcak yol değil.
   for(int i = 1; i < keep; i++)
     {
      string key = raw[i];
      int j = i - 1;
      while(j >= 0 && StringCompare(raw[j], key) > 0)
        {
         raw[j + 1] = raw[j];
         j--;
        }
      raw[j + 1] = key;
     }

   g_count = keep;
   ArrayResize(g_name, keep);   ArrayResize(g_id, keep);
   ArrayResize(g_last_msc, keep);  ArrayResize(g_last_bid, keep);
   ArrayResize(g_last_ask, keep);  ArrayResize(g_last_last, keep);
   ArrayResize(g_last_vol, keep);
   ArrayResize(g_exemode, keep);   ArrayResize(g_fillmask, keep);
   ArrayResize(g_digits, keep);    ArrayResize(g_tick_size, keep);
   ArrayResize(g_vol_min, keep);   ArrayResize(g_vol_max, keep);
   ArrayResize(g_vol_step, keep);
   ArrayResize(g_book_sub, keep);  ArrayResize(g_ready, keep);

   for(int i = 0; i < keep; i++)
     {
      g_name[i] = raw[i];
      g_id[i]   = (uint)i;          // sıralı dizideki konum = kalıcı wire kimliği
      g_last_msc[i] = 0;  g_last_bid[i] = 0; g_last_ask[i] = 0;
      g_last_last[i] = 0; g_last_vol[i] = 0;
      g_book_sub[i] = 0;  g_ready[i] = 0;
     }
   return true;
  }

//+------------------------------------------------------------------+
//| Sembolleri Market Watch'a al, özellikleri oku, DOM'a abone ol.    |
//+------------------------------------------------------------------+
void PrepareSymbols(SinyalSymbolEntry &out[], int &out_count)
  {
   ArrayResize(out, g_count);
   out_count = 0;

   int n_selected = 0, n_book = 0, n_nobook = 0, n_failed = 0;

   for(int i = 0; i < g_count; i++)
     {
      string sym = g_name[i];

      if(!SymbolSelect(sym, true))
        {
         // SymbolSelect başarısızlığı SESSİZDİR ve sonrasında
         // SymbolInfoTick bayat/sıfır döner. Tabloya eklemiyoruz.
         Print("Sinyal: SymbolSelect başarısız (", sym, ") hata=", GetLastError());
         n_failed++;
         continue;
        }
      n_selected++;

      // Özellikleri bool+referans aşırı yüklemesiyle oku: doğrudan biçim
      // başarısızlıkta sessizce 0 döner ve volume_step=0 tabloya sızarsa
      // çekirdekte her emir reddedilir.
      double point = 0, tick_size = 0, tick_value = 0, contract = 0;
      double vmin = 0, vmax = 0, vstep = 0, vlimit = 0;
      long digits = 0, fillmode = 0, trademode = 0, exemode = 0;
      long expmode = 0, ordmode = 0, stops = 0, freeze = 0, bookdepth = 0;
      // Sözleşme/taşıma verisi — marjin ve gerçekçi PnL bunlar olmadan
      // hesaplanamaz. Simülatör kayma/swap modelliyorsa girdisi buradan gelir.
      double swap_long = 0, swap_short = 0, margin_init = 0, margin_maint = 0;
      long swapmode = 0, rollover3d = 0;

      bool ok = true;
      ok = ok && SymbolInfoDouble(sym, SYMBOL_POINT, point);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE, tick_size);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE, tick_value);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE, contract);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN, vmin);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX, vmax);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP, vstep);
      ok = ok && SymbolInfoDouble(sym, SYMBOL_VOLUME_LIMIT, vlimit);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_DIGITS, digits);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_FILLING_MODE, fillmode);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_TRADE_MODE, trademode);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_TRADE_EXEMODE, exemode);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_EXPIRATION_MODE, expmode);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_ORDER_MODE, ordmode);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_TRADE_STOPS_LEVEL, stops);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_TRADE_FREEZE_LEVEL, freeze);
      ok = ok && SymbolInfoInteger(sym, SYMBOL_TICKS_BOOKDEPTH, bookdepth);

      // Sözleşme/taşıma bloğu AYRI bir bayrakla okunur, `ok`a bağlanmaz:
      // burada bir okuma başarısız olursa sembolü tabloya HİÇ ALMAMAK
      // (emir gönderilemez hale getirmek) orantısız olurdu. Ama sessizce 0
      // yazmak da olmaz — 0 swap "bedava taşıma" demektir ve simülatörü tam da
      // kapatmaya çalıştığımız yönde iyimser yapar. Bu yüzden başarı
      // SINYAL_SYMFLAG_CONTRACT_DATA ile bildirilir; bayrak yoksa tüketici
      // swap/marjin alanlarını "veri yok" saymalıdır.
      bool ok_contract = true;
      ok_contract = ok_contract && SymbolInfoDouble(sym, SYMBOL_SWAP_LONG, swap_long);
      ok_contract = ok_contract && SymbolInfoDouble(sym, SYMBOL_SWAP_SHORT, swap_short);
      ok_contract = ok_contract && SymbolInfoDouble(sym, SYMBOL_MARGIN_INITIAL, margin_init);
      ok_contract = ok_contract && SymbolInfoDouble(sym, SYMBOL_MARGIN_MAINTENANCE, margin_maint);
      ok_contract = ok_contract && SymbolInfoInteger(sym, SYMBOL_SWAP_MODE, swapmode);
      ok_contract = ok_contract && SymbolInfoInteger(sym, SYMBOL_SWAP_ROLLOVER3DAYS, rollover3d);

      if(!ok || vstep <= 0.0)
        {
         Print("Sinyal: sembol özellikleri okunamadı, ATLANDI: ", sym,
               " (vstep=", vstep, " hata=", GetLastError(), ")");
         n_failed++;
         continue;
        }

      g_exemode[i]   = (uint)exemode;
      g_fillmask[i]  = (uint)fillmode;
      g_digits[i]    = (uint)digits;
      g_tick_size[i] = tick_size;
      g_vol_min[i]   = vmin;
      g_vol_max[i]   = vmax;
      g_vol_step[i]  = vstep;

      uint flags = 0;

      // contract_size marjin formülünün ÇARPANIDIR
      // (volume * contract_size * fiyat / leverage). 0 kalırsa marjin de 0
      // çıkar, yani kaldıraç kontrolü tamamen devre dışı kalır — bu yüzden
      // sıfır sözleşme büyüklüğü "veri var" sayılmaz.
      if(ok_contract && contract > 0.0)
         flags |= SINYAL_SYMFLAG_CONTRACT_DATA;
      else
         PrintFormat("Sinyal: %s icin sozlesme/swap verisi OKUNAMADI "
                     "(contract=%.2f hata=%d) — swap ve marjin MODELLENEMEZ",
                     sym, contract, GetLastError());

      // GRAFİK SEMBOLÜ ayrı bir gecikme sınıfıdır ve bunu tüketiciye DOĞRU
      // bildirmek zorundayız: `OnTick` YALNIZCA grafik sembolü için tetiklenir,
      // yani o sembol terminalin verdiği her tick olayını olay güdümlü alır.
      // Taramaya bağlı değildir.
      //
      // Önceki sürüm bayrağı yalnızca DOM'a bakarak kuruyordu, bu yüzden grafik
      // sembolü de POLLED_ONLY görünüyordu. Bu YANLIŞ bilgiydi: sinyal üreten
      // bir tüketici o sembolü "16 ms örneklemeli, aradaki tickler kayıp"
      // sanırdı — oysa tam tersine en yüksek sadakat orada.
      bool is_chart = (sym == _Symbol);
      if(is_chart)
         flags |= SINYAL_SYMFLAG_CHART;

      // DOM: bookdepth 0 ise MarketBookAdd'i HİÇ çağırma. Retail forex'te
      // 0 beklenen durumdur ve gereksiz çağrı 4901 üretir.
      if(EnableBook && bookdepth > 0)
        {
         if(MarketBookAdd(sym))
           {
            g_book_sub[i] = 1;
            flags |= SINYAL_SYMFLAG_BOOK_SUBSCRIBED;
            n_book++;
           }
         else
           {
            Print("Sinyal: MarketBookAdd başarısız (", sym, ") hata=", GetLastError());
            if(!is_chart)
               flags |= SINYAL_SYMFLAG_POLLED_ONLY;
            n_nobook++;
           }
        }
      else
        {
         if(!is_chart)
            flags |= SINYAL_SYMFLAG_POLLED_ONLY;
         n_nobook++;
        }

      // Isınma: ilk geçerli tick gelmeden READY işaretlemiyoruz.
      MqlTick tk;
      if(SymbolInfoTick(sym, tk) && tk.time_msc > 0 &&
         (tk.bid > 0 || tk.ask > 0 || tk.last > 0))
        {
         g_ready[i] = 1;
         flags |= SINYAL_SYMFLAG_READY;
        }

      SinyalSymbolEntry e;
      e.point           = point;
      e.tick_size       = tick_size;
      e.tick_value      = tick_value;
      e.contract_size   = contract;
      e.volume_min      = vmin;
      e.volume_max      = vmax;
      e.volume_step     = vstep;
      e.volume_limit    = vlimit;
      e.symbol_id       = g_id[i];
      e.digits          = (uint)digits;
      e.filling_mode    = (uint)fillmode;
      e.trade_mode      = (uint)trademode;
      e.trade_exemode   = (uint)exemode;
      e.expiration_mode = (uint)expmode;
      e.order_mode      = (uint)ordmode;
      e.stops_level     = (uint)stops;
      e.freeze_level    = (uint)freeze;
      e.ticks_bookdepth = (uint)bookdepth;
      e.flags           = flags;
      e.reserved0       = 0;
      SinyalWriteFixed(e.name, SINYAL_SYMBOL_NAME_LEN, sym);
      // Sözleşme/taşıma bloğu. Okuma başarısızsa 0 yazılır ve
      // SINYAL_SYMFLAG_CONTRACT_DATA KURULMAZ — tüketici ayırt edebilsin.
      e.swap_long       = swap_long;
      e.swap_short      = swap_short;
      e.margin_initial  = margin_init;
      e.margin_hedged   = margin_maint;
      e.swap_mode       = (uint)swapmode;
      e.swap_rollover3d = (uint)rollover3d;
      ArrayInitialize(e.reserved1, 0);

      out[out_count++] = e;
     }

   if(VerboseLog)
     {
      // Grafik sembolünü AYRICA yaz: sinyal üretiminin hangi sembolde en
      // yüksek sadakatle çalıştığını görmenin tek yolu bu satır.
      PrintFormat("Sinyal: sembol hazırlığı — seçilen=%d, DOM=%d, yalnız-polling=%d, başarısız=%d",
                  n_selected, n_book, n_nobook, n_failed);
      PrintFormat("Sinyal: grafik sembolü=%s (OnTick ile olay güdümlü, en yüksek sadakat) — "
                  "diğer %d sembol %d ms taramayla örnekleniyor",
                  _Symbol, n_selected - 1, TimerMs);
     }
  }

//+------------------------------------------------------------------+
//| OnInit                                                            |
//+------------------------------------------------------------------+
int OnInit()
  {
   string problem = "";

   if(!SinyalCheckDllPermission(problem))
     {
      Print("Sinyal: ", problem);
      return INIT_FAILED;
     }
   if(!SinyalVerifyLayout(problem))
     {
      // Sessizce bozuk veri okumaktansa hiç başlamamak doğru.
      Print("Sinyal: YERLEŞİM UYUMSUZ — ", problem);
      return INIT_FAILED;
     }

   if(!BuildSymbolList())
      return INIT_FAILED;

   g_handle = SinyalOpen(InstanceName, 0, 0, 0, 0, 0);
   if(g_handle == 0)
     {
      // En olası sebep: aynı InstanceName ile ikinci bir EA çalışıyor.
      // Köprü bunu bilerek reddeder — iki yazar halkayı bozardı.
      Print("Sinyal: köprü açılamadı — ", SinyalErrorText());
      return INIT_FAILED;
     }

   SinyalSymbolEntry entries[];
   int entry_count = 0;
   PrepareSymbols(entries, entry_count);
   if(entry_count == 0)
     {
      Print("Sinyal: kullanılabilir sembol yok.");
      SinyalClose(g_handle);
      g_handle = 0;
      return INIT_FAILED;
     }

   // Sembol tablosu, tick akışından ÖNCE yayımlanmalı: aksi halde çekirdek
   // tanımadığı bir symbol_id ile tick alır.
   if(SinyalSetSymbols(g_handle, entries, entry_count) != SINYAL_OK)
     {
      Print("Sinyal: sembol tablosu yazılamadı — ", SinyalErrorText());
      SinyalClose(g_handle);
      g_handle = 0;
      return INIT_FAILED;
     }

   // DOM tamponlarını ÖNCEDEN ayır — sıcak yolda ArrayResize yapılmaz.
   ArrayResize(g_book, SINYAL_MAX_BOOK_DEPTH * 2);
   ArrayResize(g_bk_price, SINYAL_MAX_BOOK_DEPTH);
   ArrayResize(g_bk_volume, SINYAL_MAX_BOOK_DEPTH);
   ArrayResize(g_bk_kind, SINYAL_MAX_BOOK_DEPTH);
   // Durum tamponlari — sicak yolda tahsis YAPILMAZ.
   // MQL5'te sifir uzunluklu dizinin tampon adresi tanimsiz oldugu icin
   // en az 1 eleman ayrilir; gercek sayi ayri gecer.
   ArrayResize(g_pos, SINYAL_MAX_POSITIONS);
   ArrayResize(g_ord, SINYAL_MAX_ORDERS);
   FillAccountStatics();

   g_qpc_freq = SinyalQpcFrequency();
   if(g_qpc_freq == 0) g_qpc_freq = 1;
   g_last_timer_qpc = SinyalQpc();

   // 1 ms İSTEMEK ANLAMSIZ: donanım tabanı 10-16 ms.
   int period = TimerMs;
   if(period < 10) period = 10;
   EventSetMillisecondTimer(period);

   PrintFormat("Sinyal: başlatıldı — örnek=%s, sembol=%d, timer=%dms, ticaret=%s",
               InstanceName, entry_count, period,
               (EnableTrading ? "AÇIK" : "kapalı"));
   if(!EnableTrading)
      Print("Sinyal: emir yürütme KAPALI (EnableTrading=false). "
            "Canlı hesapta açmadan önce demo hesapta doğrulayın.");
   return INIT_SUCCEEDED;
  }

//+------------------------------------------------------------------+
//| OnDeinit                                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   EventKillTimer();

   // YALNIZCA başarıyla abone olduklarımız için Release — fazladan Release
   // aynı grafikteki üçüncü taraf programların aboneliğini düşürür.
   for(int i = 0; i < g_count; i++)
      if(g_book_sub[i] == 1)
         MarketBookRelease(g_name[i]);

   if(g_handle != 0)
     {
      ReportTelemetry(true);
      SinyalClose(g_handle);
      g_handle = 0;
     }
   Print("Sinyal: durduruldu (sebep=", reason, ")");
  }

//+------------------------------------------------------------------+
//| Bir sembolün güncel tick'ini oku ve DEĞİŞTİYSE yayınla.           |
//|                                                                  |
//| Yeni tick tespiti yalnızca time_msc ile YAPILAMAZ: aynı ms içinde |
//| birden çok tick olabilir. Bileşik anahtar kullanılır.             |
//+------------------------------------------------------------------+
void PushIfChanged(const int idx, const uchar kind)
  {
   MqlTick tk;
   if(!SymbolInfoTick(g_name[idx], tk))
     {
      // Sessizce dönmek, sembolün Market Watch'tan düşmesini görünmez
      // kılıyordu. Sayaç OnTimer raporunda görünür.
      g_symtick_fails++;
      return;
     }

   if(tk.time_msc == g_last_msc[idx] &&
      tk.bid == g_last_bid[idx] && tk.ask == g_last_ask[idx] &&
      tk.last == g_last_last[idx] && tk.volume_real == g_last_vol[idx])
      return;   // değişmemiş

   if(tk.time_msc < g_last_msc[idx])
      g_msc_regress++;   // sunucu düzeltmesi/yeniden bağlanma — kabul et, işaretle

   g_last_msc[idx]  = tk.time_msc;
   g_last_bid[idx]  = tk.bid;
   g_last_ask[idx]  = tk.ask;
   g_last_last[idx] = tk.last;
   g_last_vol[idx]  = tk.volume_real;

   if(g_ready[idx] == 0 && tk.time_msc > 0)
      g_ready[idx] = 1;

   int rc = SinyalPushTick(g_handle, g_id[idx], tk.time_msc,
                           tk.bid, tk.ask, tk.last, tk.volume_real,
                           (ushort)tk.flags, kind);
   if(rc == SINYAL_OK)
      g_ticks_pushed++;
   else if(rc == SINYAL_WOULD_BLOCK)
      g_ticks_dropped++;
   else
     {
      // ESKİDEN BURASI SESSİZDİ: negatif dönüş ne sayaca ne loga giriyordu,
      // yani köprü hata verse bile "veri yok" ile ayırt edilemiyordu.
      g_push_errors++;
      if(g_last_push_err != rc)
        {
         g_last_push_err = rc;
         PrintFormat("Sinyal: HATA — SinyalPushTick rc=%d (%s) sembol=%s",
                     rc, SinyalErrorText(), g_name[idx]);
        }
     }
  }

//+------------------------------------------------------------------+
//| OnTick — yalnızca grafik sembolü için tetiklenir.                 |
//|                                                                  |
//| Çok sembollü toplama için OnTick'e GÜVENİLMEZ; burada yalnızca    |
//| grafik sembolünün düşük gecikmeli yolu olarak kullanılır.         |
//+------------------------------------------------------------------+
void OnTick()
  {
   if(g_handle == 0) return;
   int idx = SymbolIndexOf(_Symbol);
   if(idx >= 0)
      PushIfChanged(idx, SINYAL_KIND_TICK);
  }

//+------------------------------------------------------------------+
//| OnBookEvent — DOM değişimi. EN KRİTİK YOL, EN KISA OLMALI.        |
//|                                                                  |
//| Olaylar asla atlanmaz ve birikir; buradaki her mikrosaniye kuyruk |
//| baskısına dönüşür. Hesaplama/string/Print YOK.                    |
//|                                                                  |
//| Aynı grafikteki başka bir program başka sembole abone olursa bu   |
//| işleyici de tetiklenir — bu yüzden ilk iş filtreleme.             |
//+------------------------------------------------------------------+
void OnBookEvent(const string &symbol)
  {
   if(g_handle == 0) return;
   int idx = SymbolIndexOf(symbol);
   if(idx < 0 || g_book_sub[idx] == 0)
      return;   // bize ait değil

   int n = MarketBookGet(symbol, g_book);
   if(n > 0)
     {
      int depth = n;
      if(depth > SINYAL_MAX_BOOK_DEPTH)
         depth = SINYAL_MAX_BOOK_DEPTH;
      for(int i = 0; i < depth; i++)
        {
         g_bk_price[i]  = g_book[i].price;
         // volume DEĞİL volume_real: kesirli lotlu enstrümanlarda
         // tamsayı alan yuvarlanır.
         g_bk_volume[i] = g_book[i].volume_real;
         g_bk_kind[i]   = (uchar)g_book[i].type;
        }
      // depth<=0 ile DLL'i çağırma: boş dizinin tampon adresi tanımsız.
      int rc = SinyalPushBook(g_handle, g_id[idx], (long)TimeCurrent() * 1000,
                              g_bk_price, g_bk_volume, g_bk_kind, depth);
      if(rc == SINYAL_OK)      g_books_pushed++;
      else if(rc == SINYAL_WOULD_BLOCK) g_books_dropped++;
     }

   // DOM değişimi genellikle fiyat değişimidir; tick yolunu da besle.
   PushIfChanged(idx, SINYAL_KIND_TICK);
  }

//+------------------------------------------------------------------+
//| OnTradeTransaction — YALNIZCA halkaya yaz.                        |
//|                                                                  |
//| Kuyruk 1024 ve yavaş işlemede eskiler EZİLİR. HistorySelect,      |
//| OrderSelect, PositionSelect, SymbolInfo*, Print, dosya I/O ve     |
//| tablo TARAMASI burada YAPILMAZ.                                   |
//|                                                                  |
//| TEK istisna: sıralı sembol tablosunda İKİLİ ARAMA (SymbolIndexOf).|
//| Dolum anındaki bid/ask'i önbellekten okumanın başka yolu yok ve   |
//| ölçümün değeri log(n) karşılaştırmadan kat kat büyük. Bunu        |
//| doğrusal taramaya ya da SymbolInfoTick'e çevirmek YASAK.          |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest    &request,
                        const MqlTradeResult     &result)
  {
   if(g_handle == 0) return;

   // BURADA KİMLİK ARAMASI YAPILMAZ.
   //
   // Önceki sürüm `request_id`'yi 256 elemanlı bir tabloda doğrusal olarak
   // arıyordu. Bu, terminalin 1024'lük işlem kuyruğunu geciktirir ve kuyruk
   // taştığında ESKİ işlemler sessizce ezilir — yani emir olayı kaybedilir.
   //
   // İzin verilen işlem sınıfı: alan kopyalama, QPC okuma, halkaya tek push.
   // YASAK: History*Select, OrderSelect, PositionSelect, SymbolInfo*, Print,
   // string işlemleri, tablo taraması.
   //
   // `client_id` eşleştirmesi ÇEKİRDEKTE geç bağlamayla yapılır:
   // client_id ↔ request_id ↔ order ↔ position tablosu üzerinden.
   SinyalRes r;
   r.client_id        = 0;               // çekirdek dolduracak

   // BİLET SEÇİMİ — korelasyonun kilit noktası.
   //
   // `OrderSendAsync` anında `MqlTradeResult.order` SIFIRDIR, bu yüzden
   // SEND_ACK bilet taşımaz. Bileti öğrenebileceğimiz ilk yer
   // TRADE_TRANSACTION_REQUEST olayıdır: orada `result` TAM DOLU gelir ve
   // `result.order` gerçek bileti taşır — oysa aynı olayda `trans.order`
   // çoğu zaman 0'dır.
   //
   // Önceki sürüm bileti yalnızca `trans`'tan okuyordu; bu yüzden
   // "bilet → client_id" bağı hiç kurulamıyor ve dolum olayları kimliksiz
   // kalıyordu. Artık REQUEST olayında `result` tercih ediliyor.
   r.order            = (trans.order != 0) ? trans.order : result.order;
   r.deal             = (trans.deal  != 0) ? trans.deal  : result.deal;
   r.position         = trans.position;
   r.position_by      = trans.position_by;
   r.volume           = (trans.volume != 0.0) ? trans.volume : result.volume;
   r.price            = (trans.price  != 0.0) ? trans.price  : result.price;

   // DOLUM ANINDAKİ PİYASA — giriş maliyetini AYIRMANIN tek yolu.
   //
   // `price` tek başına "ne ödedim"i söyler, "neden"i söylemez: spread mi
   // genişti, motorun istediği fiyata mı ulaşılamadı? `ask - bid` ile
   // `price - ask` bunu ayırır. Sonradan tick akışından eşleştirmek AYNI
   // ŞEY DEĞİL — aradaki milisaniyelerde piyasa değişmiş olur ve ölçülen
   // kayma gerçekte olmayan bir şeyi gösterir.
   //
   // SICAK YOL KURALI: burada `SymbolInfoTick` ÇAĞRILMAZ. Terminalin işlem
   // kuyruğu 1024 elemanlı ve taşarsa ESKİ olaylar sessizce ezilir — yani
   // ölçüm uğruna olayın kendisini kaybederdik. Tick yolunun zaten
   // güncellediği önbellekten okuyoruz: maliyeti bir ikili arama.
   int bidx = SymbolIndexOf(trans.symbol);
   if(bidx >= 0)
     {
      r.bid = g_last_bid[bidx];
      r.ask = g_last_ask[bidx];
     }
   else
     {
      // Sembol tablomuzda yok (biz toplamıyoruz). Sıfır bırakmak
      // "ölçemedim" demek ve çekirdek alanı tele hiç koymaz.
      r.bid = 0.0;
      r.ask = 0.0;
     }

   r.recv_qpc         = SinyalQpc();
   r.retcode          = (uint)result.retcode;
   r.retcode_external = (uint)result.retcode_external;
   r.request_id       = (uint)result.request_id;
   r.reserved0        = 0;
   r.kind             = SINYAL_RES_TRADE_TXN;
   r.txn_type         = (uchar)trans.type;
   r.order_state      = (uchar)trans.order_state;
   r.deal_type        = (uchar)trans.deal_type;
   r.order_type       = (uchar)trans.order_type;
   r.source           = SINYAL_RESSRC_LIVE;
   // MQL5 yerel struct'i SIFIRLAMAZ; gerceklesmis alanlar canli yolda
   // okunamiyor ve cop deger halkaya girmemeli.
   r.profit           = 0.0;
   r.commission = 0.0;
   r.swap           = 0.0;
   ArrayInitialize(r.reserved1, 0);
   ArrayInitialize(r.reserved2, 0);

   // MqlTradeTransaction'da 'comment' alanı YOKTUR; yorum yalnızca
   // MqlTradeResult'ta ve pratikte yalnızca REQUEST olayında doludur.
   // Diğer olaylarda yazmak boşuna iş — sıcak yolda bundan kaçınıyoruz.
   if(trans.type == TRADE_TRANSACTION_REQUEST)
      SinyalWriteFixed(r.comment, SINYAL_COMMENT_LEN, result.comment);
   else
      ArrayInitialize(r.comment, 0);

   if(SinyalPushRes(g_handle, r) != SINYAL_OK)
      g_res_dropped++;
  }

//+------------------------------------------------------------------+
//| Hesabın DEĞİŞMEYEN alanlarını doldur — YALNIZCA OnInit'te.        |
//|                                                                  |
//| String dönüşümleri pahalıdır ve sıcak yolda yeri yoktur. Hesap    |
//| değişirse MT5 zaten OnDeinit/OnInit'i yeniden çalıştırır (REASON_ |
//| ACCOUNT), yani önbellek geçerliliğini korur.                      |
//+------------------------------------------------------------------+
void FillAccountStatics()
  {
   ZeroMemory(g_acc);
   g_acc.login           = AccountInfoInteger(ACCOUNT_LOGIN);
   g_acc.leverage        = AccountInfoInteger(ACCOUNT_LEVERAGE);
   g_acc.limit_orders    = (int)AccountInfoInteger(ACCOUNT_LIMIT_ORDERS);
   g_acc.currency_digits = (int)AccountInfoInteger(ACCOUNT_CURRENCY_DIGITS);
   g_acc.trade_mode      = SinyalMapTradeMode(AccountInfoInteger(ACCOUNT_TRADE_MODE));
   g_acc.margin_mode     = SinyalMapMarginMode(AccountInfoInteger(ACCOUNT_MARGIN_MODE));
   g_acc.so_mode         = SinyalMapSoMode(AccountInfoInteger(ACCOUNT_MARGIN_SO_MODE));
   g_acc.fifo_close      = (uchar)(AccountInfoInteger(ACCOUNT_FIFO_CLOSE) ? 1 : 0);
   g_acc.hedge_allowed   = (uchar)(AccountInfoInteger(ACCOUNT_HEDGE_ALLOWED) ? 1 : 0);
   g_acc.margin_so_call  = AccountInfoDouble(ACCOUNT_MARGIN_SO_CALL);
   g_acc.margin_so_so    = AccountInfoDouble(ACCOUNT_MARGIN_SO_SO);

   SinyalWriteFixed(g_acc.currency, 16, AccountInfoString(ACCOUNT_CURRENCY));
   SinyalWriteFixed(g_acc.server,   64, AccountInfoString(ACCOUNT_SERVER));
   SinyalWriteFixed(g_acc.company,  96, AccountInfoString(ACCOUNT_COMPANY));
   SinyalWriteFixed(g_acc.name,     96, AccountInfoString(ACCOUNT_NAME));
   ArrayInitialize(g_acc.reserved0, 0);
   ArrayInitialize(g_acc.reserved1, 0);

   PrintFormat("Sinyal: hesap #%I64d @ %s (%s) — mod=%s, marjin=%s, kaldirac=1:%I64d",
               g_acc.login,
               AccountInfoString(ACCOUNT_SERVER),
               AccountInfoString(ACCOUNT_CURRENCY),
               (g_acc.trade_mode == SINYAL_TRADEMODE_DEMO ? "DEMO"
                : g_acc.trade_mode == SINYAL_TRADEMODE_CONTEST ? "YARISMA"
                : g_acc.trade_mode == SINYAL_TRADEMODE_REAL ? "GERCEK" : "BILINMIYOR"),
               (g_acc.margin_mode == SINYAL_MARGINMODE_HEDGING ? "hedging"
                : g_acc.margin_mode == SINYAL_MARGINMODE_NETTING ? "netting"
                : g_acc.margin_mode == SINYAL_MARGINMODE_EXCHANGE ? "exchange" : "bilinmiyor"),
               g_acc.leverage);
   if(g_acc.trade_mode != SINYAL_TRADEMODE_DEMO)
      Print("Sinyal: DIKKAT — bu hesap DEMO DEGIL. Cekirdek canli-para kilidi devrede.");
  }

//+------------------------------------------------------------------+
//| Hesabın DEĞİŞEN alanlarını tazele (saniyede bir).                 |
//+------------------------------------------------------------------+
void RefreshAccountDynamics()
  {
   g_acc.time_msc           = (long)TimeCurrent() * 1000;
   g_acc.balance            = AccountInfoDouble(ACCOUNT_BALANCE);
   g_acc.credit             = AccountInfoDouble(ACCOUNT_CREDIT);
   g_acc.profit             = AccountInfoDouble(ACCOUNT_PROFIT);
   g_acc.equity             = AccountInfoDouble(ACCOUNT_EQUITY);
   g_acc.margin             = AccountInfoDouble(ACCOUNT_MARGIN);
   g_acc.margin_free        = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   g_acc.margin_level       = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   g_acc.margin_initial     = AccountInfoDouble(ACCOUNT_MARGIN_INITIAL);
   g_acc.margin_maintenance = AccountInfoDouble(ACCOUNT_MARGIN_MAINTENANCE);
   g_acc.assets             = AccountInfoDouble(ACCOUNT_ASSETS);
   g_acc.liabilities        = AccountInfoDouble(ACCOUNT_LIABILITIES);
   g_acc.commission_blocked = AccountInfoDouble(ACCOUNT_COMMISSION_BLOCKED);

   // Emir kapısının dayanağı olan dört izin + bağlantı.
   g_acc.trade_allowed          = (uchar)(AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ? 1 : 0);
   g_acc.trade_expert           = (uchar)(AccountInfoInteger(ACCOUNT_TRADE_EXPERT) ? 1 : 0);
   g_acc.terminal_trade_allowed = (uchar)(TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) ? 1 : 0);
   g_acc.mql_trade_allowed      = (uchar)(MQLInfoInteger(MQL_TRADE_ALLOWED) ? 1 : 0);
   g_acc.connected              = (uchar)(TerminalInfoInteger(TERMINAL_CONNECTED) ? 1 : 0);
  }

//+------------------------------------------------------------------+
//| Açık pozisyonları ve bekleyen emirleri topla, durumu yayımla.     |
//|                                                                  |
//| Numaralandırma GERİYE doğru yapılır: liste tam o sırada değişirse |
//| ileri yönde indeks kayar ve kayıt atlanır. Bilet alınamayan slot  |
//| 'unstable' işaretlenir ama tarama DURDURULMAZ (break değil        |
//| continue) — tek bir kararsız kayıt yüzünden tüm listeyi kaybetmek |
//| daha kötü olurdu.                                                 |
//|                                                                  |
//| Bu fonksiyon terminal-YEREL okuma yapar (sunucu gidiş-dönüşü yok),|
//| bu yüzden 1 Hz güvenlidir. HistorySelect BURADA ÇAĞRILMAZ.        |
//+------------------------------------------------------------------+
void PublishState()
  {
   RefreshAccountDynamics();

   bool unstable = false;

   // --- pozisyonlar ---
   int pos_total = PositionsTotal();
   int pw = 0;
   for(int i = pos_total - 1; i >= 0 && pw < SINYAL_MAX_POSITIONS; i--)
     {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) { unstable = true; continue; }
      if(!PositionSelectByTicket(tk)) { unstable = true; continue; }

      string sym = PositionGetString(POSITION_SYMBOL);
      SinyalPosition p;
      ZeroMemory(p);
      p.ticket          = tk;
      p.identifier      = (ulong)PositionGetInteger(POSITION_IDENTIFIER);
      p.magic           = (ulong)PositionGetInteger(POSITION_MAGIC);
      p.time_msc        = (long)PositionGetInteger(POSITION_TIME_MSC);
      p.time_update_msc = (long)PositionGetInteger(POSITION_TIME_UPDATE_MSC);
      p.volume          = PositionGetDouble(POSITION_VOLUME);
      p.price_open      = PositionGetDouble(POSITION_PRICE_OPEN);
      p.price_current   = PositionGetDouble(POSITION_PRICE_CURRENT);
      p.sl              = PositionGetDouble(POSITION_SL);
      p.tp              = PositionGetDouble(POSITION_TP);
      p.profit          = PositionGetDouble(POSITION_PROFIT);
      p.swap            = PositionGetDouble(POSITION_SWAP);
      p.kind            = (uchar)PositionGetInteger(POSITION_TYPE);
      p.reason          = (uchar)PositionGetInteger(POSITION_REASON);
      p.digits          = (uchar)SymbolInfoInteger(sym, SYMBOL_DIGITS);
      p.flags           = 0;
      if(StringLen(sym) >= SINYAL_SYMBOL_NAME_LEN) p.flags |= SINYAL_RECFLAG_SYMBOL_TRUNC;
      SinyalWriteFixed(p.symbol,  SINYAL_SYMBOL_NAME_LEN,  sym);
      SinyalWriteFixed(p.comment, SINYAL_REC_COMMENT_LEN, PositionGetString(POSITION_COMMENT));
      ArrayInitialize(p.reserved0, 0);
      g_pos[pw++] = p;
     }

   // --- bekleyen emirler ---
   int ord_total = OrdersTotal();
   int ow = 0;
   for(int i = ord_total - 1; i >= 0 && ow < SINYAL_MAX_ORDERS; i--)
     {
      ulong tk = OrderGetTicket(i);
      if(tk == 0) { unstable = true; continue; }

      string sym = OrderGetString(ORDER_SYMBOL);
      SinyalOrder o;
      ZeroMemory(o);
      o.ticket          = tk;
      o.magic           = (ulong)OrderGetInteger(ORDER_MAGIC);
      o.position_id     = (ulong)OrderGetInteger(ORDER_POSITION_ID);
      o.time_setup_msc  = (long)OrderGetInteger(ORDER_TIME_SETUP_MSC);
      o.time_expiration = (long)OrderGetInteger(ORDER_TIME_EXPIRATION);
      o.volume_initial  = OrderGetDouble(ORDER_VOLUME_INITIAL);
      o.volume_current  = OrderGetDouble(ORDER_VOLUME_CURRENT);
      o.price_open      = OrderGetDouble(ORDER_PRICE_OPEN);
      o.price_stoplimit = OrderGetDouble(ORDER_PRICE_STOPLIMIT);
      o.sl              = OrderGetDouble(ORDER_SL);
      o.tp              = OrderGetDouble(ORDER_TP);
      o.price_current   = OrderGetDouble(ORDER_PRICE_CURRENT);
      o.kind            = (uchar)OrderGetInteger(ORDER_TYPE);
      o.state           = (uchar)OrderGetInteger(ORDER_STATE);
      o.type_time       = (uchar)OrderGetInteger(ORDER_TYPE_TIME);
      o.type_filling    = (uchar)OrderGetInteger(ORDER_TYPE_FILLING);
      o.reason          = (uchar)OrderGetInteger(ORDER_REASON);
      o.digits          = (uchar)SymbolInfoInteger(sym, SYMBOL_DIGITS);
      o.flags           = 0;
      if(StringLen(sym) >= SINYAL_SYMBOL_NAME_LEN) o.flags |= SINYAL_RECFLAG_SYMBOL_TRUNC;
      SinyalWriteFixed(o.symbol,  SINYAL_SYMBOL_NAME_LEN,  sym);
      SinyalWriteFixed(o.comment, SINYAL_REC_COMMENT_LEN, OrderGetString(ORDER_COMMENT));
      ArrayInitialize(o.reserved0, 0);
      g_ord[ow++] = o;
     }

   int rc = SinyalSetState(g_handle, g_acc,
                           g_pos, pw, pos_total,
                           g_ord, ow, ord_total,
                           (long)TimeCurrent() * 1000,
                           (uchar)(unstable ? 1 : 0));
   if(rc == SINYAL_OK) g_state_pubs++;
   else                g_state_errs++;

   if(pos_total > SINYAL_MAX_POSITIONS || ord_total > SINYAL_MAX_ORDERS)
      PrintFormat("Sinyal: UYARI — durum listesi KESILDI (pozisyon %d/%d, emir %d/%d)",
                  pw, pos_total, ow, ord_total);
  }

//+------------------------------------------------------------------+
//| OnTrade — durum değişti, bir sonraki timer'da hemen yayımla.      |
//|                                                                  |
//| Olay-güdümlü + heartbeat hibrit: emir/pozisyon değişimi anında    |
//| yakalanır, ayrıca saniyede bir düzenli yayın yapılır.             |
//+------------------------------------------------------------------+
void OnTrade()
  {
   g_state_dirty = true;
  }

//+------------------------------------------------------------------+
//| MUTABAKAT — gerçekleşmiş kâr/komisyon/swap.                       |
//|                                                                  |
//| NEDEN AYRI BİR TUR                                                 |
//|                                                                  |
//| `OnTradeTransaction` sıcak yoldur: terminalin işlem kuyruğu 1024   |
//| elemanlı ve işleyici yavaşlarsa ESKİ olaylar sessizce ezilir.      |
//| `HistoryDealGetDouble` orada çağrılamaz. Ama gerçekleşmiş sonuç    |
//| YALNIZCA geçmişte vardır — canlı olay onu taşımaz.                 |
//|                                                                  |
//| Çözüm: `OnTimer` içinde, sıcak yol DIŞINDA, seyrek bir tur.        |
//|                                                                  |
//| `client_id` BURADA DA 0 bırakılır. Eşleştirme çekirdekte           |
//| `order`/`position` üzerinden geç bağlamayla yapılıyor; EA'da       |
//| ikinci bir tablo tutmak aynı bilgiyi iki yerde tutmak olurdu ve    |
//| ikisi kaydığında hangisinin doğru olduğu bilinemezdi.              |
//|                                                                  |
//| İLK TURDA HİÇBİR ŞEY YAYIMLANMAZ: yalnızca su hattı işaretlenir.   |
//| Aksi halde EA her açılışta son bir saatin tüm deal'lerini "yeni"   |
//| diye gönderir ve istemci kapanmış işlemleri tekrar sayardı.        |
//+------------------------------------------------------------------+
void ReconcileDeals()
  {
   if(ReconcileSec <= 0) return;

   datetime now_s = TimeCurrent();
   if(g_recon_last_run != 0 && (now_s - g_recon_last_run) < ReconcileSec)
      return;
   g_recon_last_run = now_s;

   // Pencere sabit: bir saat, 5 saniyelik turda fazlasıyla yeterli ve
   // taramayı sınırlı tutar. Geçmişin tamamını seçmek her turda büyüyen
   // bir maliyet olurdu.
   if(!HistorySelect(now_s - 3600, now_s + 60))
     {
      g_recon_failed++;
      return;
     }

   int n = HistoryDealsTotal();
   ulong high = g_recon_last_deal;
   bool  first = (g_recon_last_deal == 0);

   for(int i = 0; i < n; i++)
     {
      ulong t = HistoryDealGetTicket(i);
      if(t == 0) continue;
      if(t > high) high = t;
      if(first) continue;                 // ilk tur: yalnız su hattı
      if(t <= g_recon_last_deal) continue; // zaten bildirildi

      // BAKİYE/KREDİ hareketleri işlem değildir; emir akışına karıştırmak
      // istemcinin "kimliksiz dolum" saymasına yol açardı.
      long dtype = HistoryDealGetInteger(t, DEAL_TYPE);
      if(dtype != DEAL_TYPE_BUY && dtype != DEAL_TYPE_SELL) continue;

      SinyalRes r;
      r.client_id        = 0;             // çekirdek bağlayacak
      r.order            = (ulong)HistoryDealGetInteger(t, DEAL_ORDER);
      r.deal             = t;
      r.position         = (ulong)HistoryDealGetInteger(t, DEAL_POSITION_ID);
      r.position_by      = 0;
      r.volume           = HistoryDealGetDouble(t, DEAL_VOLUME);
      r.price            = HistoryDealGetDouble(t, DEAL_PRICE);
      r.bid              = 0.0;           // geçmişte yok; UYDURULMAZ
      r.ask              = 0.0;
      r.recv_qpc         = SinyalQpc();
      r.retcode          = 0;             // broker cevabı değil, geçmiş kaydı
      r.retcode_external = 0;
      r.request_id       = 0;
      r.reserved0        = 0;
      r.kind             = SINYAL_RES_TRADE_TXN;
      r.txn_type         = 0;
      r.order_state      = 0;
      r.deal_type        = (uchar)dtype;
      r.order_type       = 0;
      r.source           = SINYAL_RESSRC_RECONCILE;
      // ASIL YÜK — bunlar için var bu tur.
      r.profit           = HistoryDealGetDouble(t, DEAL_PROFIT);
      r.commission       = HistoryDealGetDouble(t, DEAL_COMMISSION);
      r.swap             = HistoryDealGetDouble(t, DEAL_SWAP);
      ArrayInitialize(r.reserved1, 0);
      ArrayInitialize(r.reserved2, 0);
      ArrayInitialize(r.comment, 0);

      if(SinyalPushRes(g_handle, r) != SINYAL_OK)
         g_res_dropped++;
      else
         g_recon_sent++;
     }

   g_recon_last_deal = high;
  }

//+------------------------------------------------------------------+
//| Reddedilen komut için sonuç bildir.                               |
//+------------------------------------------------------------------+
void PushRejection(const ulong client_id, const string why)
  {
   SinyalRes r;
   r.client_id = client_id;
   r.order = 0; r.deal = 0; r.position = 0;
   r.volume = 0.0; r.price = 0.0; r.bid = 0.0; r.ask = 0.0;
   r.recv_qpc = SinyalQpc();
   r.retcode = 0; r.retcode_external = 0; r.request_id = 0; r.reserved0 = 0;
   r.position_by = 0;
   r.kind = SINYAL_RES_REJECTED;
   r.txn_type = 0; r.order_state = 0; r.deal_type = 0; r.order_type = 0;
   r.source = SINYAL_RESSRC_LIVE;
   // MQL5 yerel struct'i SIFIRLAMAZ; gerceklesmis alanlar canli yolda
   // okunamiyor ve cop deger halkaya girmemeli.
   r.profit = 0.0;
   r.commission = 0.0;
   r.swap = 0.0;
   ArrayInitialize(r.reserved1, 0);
   ArrayInitialize(r.reserved2, 0);
   SinyalWriteFixed(r.comment, SINYAL_COMMENT_LEN, why);
   SinyalPushRes(g_handle, r);
   g_cmds_rejected++;
  }

//+------------------------------------------------------------------+
//| Tek bir komutu yürüt.                                             |
//+------------------------------------------------------------------+
void ExecuteCmd(const SinyalCmd &cmd)
  {
   if(!EnableTrading)
     {
      PushRejection(cmd.client_id, "ticaret kapali");
      return;
     }
   if(cmd.symbol_id >= (uint)g_count)
     {
      PushRejection(cmd.client_id, "bilinmeyen symbol_id");
      return;
     }
   int idx = (int)cmd.symbol_id;
   string sym = g_name[idx];

   MqlTradeRequest req;
   MqlTradeResult  res;
   ZeroMemory(req);
   ZeroMemory(res);

   req.symbol    = sym;
   req.magic     = cmd.magic;      // client_id burada taşınır (64 bit)
   req.deviation = (cmd.deviation > 0 ? cmd.deviation : 20);
   req.volume    = cmd.volume;
   req.sl        = cmd.sl;
   req.tp        = cmd.tp;

   ENUM_ORDER_TYPE otype;
   ENUM_ORDER_TYPE_FILLING fill;
   ENUM_ORDER_TYPE_TIME    ttime;

   if(!SinyalMapTypeTime(cmd.type_time, ttime))
     {
      PushRejection(cmd.client_id, "gecersiz type_time");
      return;
     }
   if(!SinyalResolveFilling(cmd.action, cmd.filling,
                            g_exemode[idx], g_fillmask[idx], fill))
     {
      // Tahmin etmiyoruz: yanlış modla göndermek 10030 ile reddedilir ve
      // sebebi loglardan anlaşılmaz.
      PushRejection(cmd.client_id, "uygun doldurma modu yok");
      return;
     }
   req.type_filling = fill;
   req.type_time    = ttime;

   switch(cmd.action)
     {
      case SINYAL_ACTION_DEAL:
        {
         if(!SinyalMapOrderType(cmd.order_type, otype))
           { PushRejection(cmd.client_id, "gecersiz order_type"); return; }
         MqlTick tk;
         if(!SymbolInfoTick(sym, tk))
           { PushRejection(cmd.client_id, "tick yok"); return; }
         req.action = TRADE_ACTION_DEAL;
         req.type   = otype;
         req.price  = (cmd.price > 0.0)
                      ? cmd.price
                      : ((otype == ORDER_TYPE_BUY) ? tk.ask : tk.bid);
         break;
        }
      case SINYAL_ACTION_PENDING:
        {
         if(!SinyalMapOrderType(cmd.order_type, otype))
           { PushRejection(cmd.client_id, "gecersiz order_type"); return; }
         req.action    = TRADE_ACTION_PENDING;
         req.type      = otype;
         req.price     = cmd.price;
         req.stoplimit = cmd.stoplimit;
         if(cmd.type_time == SINYAL_TIME_SPECIFIED ||
            cmd.type_time == SINYAL_TIME_SPECIFIED_DAY)
            req.expiration = (datetime)cmd.expiration;
         break;
        }
      case SINYAL_ACTION_SLTP:
        {
         req.action   = TRADE_ACTION_SLTP;
         req.position = cmd.ticket;
         break;
        }
      case SINYAL_ACTION_MODIFY:
        {
         req.action    = TRADE_ACTION_MODIFY;
         req.order     = cmd.ticket;
         req.price     = cmd.price;
         req.stoplimit = cmd.stoplimit;
         break;
        }
      case SINYAL_ACTION_REMOVE:
        {
         req.action = TRADE_ACTION_REMOVE;
         req.order  = cmd.ticket;
         break;
        }
      case SINYAL_ACTION_CLOSE_BY:
        {
         req.action      = TRADE_ACTION_CLOSE_BY;
         req.position    = cmd.ticket;
         req.position_by = cmd.ticket_by;
         break;
        }
      case SINYAL_ACTION_CLOSE_POSITION:
        {
         if(!PositionSelectByTicket(cmd.ticket))
           { PushRejection(cmd.client_id, "pozisyon yok"); return; }
         long ptype = PositionGetInteger(POSITION_TYPE);
         double pvol = PositionGetDouble(POSITION_VOLUME);
         MqlTick tk;
         if(!SymbolInfoTick(sym, tk))
           { PushRejection(cmd.client_id, "tick yok"); return; }
         req.action   = TRADE_ACTION_DEAL;
         req.position = cmd.ticket;
         req.volume   = (cmd.volume > 0.0 && cmd.volume < pvol) ? cmd.volume : pvol;
         if(ptype == POSITION_TYPE_BUY)
           { req.type = ORDER_TYPE_SELL; req.price = tk.bid; }
         else
           { req.type = ORDER_TYPE_BUY;  req.price = tk.ask; }
         break;
        }
      default:
         PushRejection(cmd.client_id, "bilinmeyen action");
         return;
     }

   // OrderSendAsync: çağıranı bloklamaz. Gerçek sonuç OnTradeTransaction'dan
   // gelir; buradaki retcode yalnızca "kuyruğa alındı" demektir.
   bool sent = OrderSendAsync(req, res);

   SinyalRes ack;
   ack.client_id        = cmd.client_id;
   ack.order            = res.order;
   ack.deal             = res.deal;
   ack.position         = 0;
   ack.volume           = res.volume;
   ack.price            = res.price;
   ack.bid              = res.bid;
   ack.ask              = res.ask;
   ack.recv_qpc         = SinyalQpc();
   ack.retcode          = (uint)res.retcode;
   ack.retcode_external = (uint)res.retcode_external;
   ack.request_id       = (uint)res.request_id;
   ack.reserved0        = 0;
   ack.position_by      = 0;
   ack.kind             = SINYAL_RES_SEND_ACK;
   ack.txn_type         = 0;
   ack.order_state      = 0;
   ack.deal_type        = 0;
   ack.order_type       = cmd.order_type;
   ack.source           = SINYAL_RESSRC_LIVE;
   // MQL5 yerel struct'i SIFIRLAMAZ; gerceklesmis alanlar canli yolda
   // okunamiyor ve cop deger halkaya girmemeli.
   ack.profit           = 0.0;
   ack.commission = 0.0;
   ack.swap           = 0.0;
   ArrayInitialize(ack.reserved1, 0);
   ArrayInitialize(ack.reserved2, 0);
   SinyalWriteFixed(ack.comment, SINYAL_COMMENT_LEN, res.comment);
   SinyalPushRes(g_handle, ack);

   if(sent) g_cmds_done++;
   else     g_cmds_rejected++;
  }

//+------------------------------------------------------------------+
//| Teşhis raporu — YALNIZCA OnTimer'dan çağrılır.                    |
//+------------------------------------------------------------------+
void ReportTelemetry(const bool force)
  {
   datetime now = TimeLocal();
   if(!force && now - g_last_report < 60)
      return;
   g_last_report = now;
   if(!VerboseLog && !force)
      return;

   ulong ring_lost = SinyalTickPushFailures(g_handle);
   ulong backlog   = SinyalTickBacklog(g_handle);
   ulong delta     = g_ticks_pushed - g_ticks_prev_report;

   PrintFormat("Sinyal: tick=%I64u (+%I64u) halka-kayip=%I64u birikmis=%I64u | "
               "dom=%I64u (kayip=%I64u) | kopru-hata=%I64u symtick-hata=%I64u | "
               "timer-atlama=%I64u msc-geri=%I64u | emir=%I64u/red=%I64u | "
               "durum=%I64u/hata=%I64u res-kayip=%I64u | mutabakat=%I64u/hata=%I64u",
               g_ticks_pushed, delta, ring_lost, backlog,
               g_books_pushed, g_books_dropped,
               g_push_errors, g_symtick_fails,
               g_timer_skips, g_msc_regress,
               g_cmds_done, g_cmds_rejected,
               g_state_pubs, g_state_errs, g_res_dropped,
               g_recon_sent, g_recon_failed);

   // Mutabakat kapalıysa bunu SÖYLE: gerçekleşmiş kâr/komisyon/swap telde
   // hiç görünmez ve istemci "alan yok" ile "ölçüm kapalı"yı ayıramaz.
   if(ReconcileSec <= 0)
      Print("Sinyal: mutabakat KAPALI (ReconcileSec=0) — gerceklesmis "
            "kar/komisyon/swap telde GORUNMEZ.");

   // Durum hiç yayımlanmadıysa çekirdek hesap/pozisyon göremez. Bu sessiz
   // kalırsa "account boş dönüyor" sorununun sebebi anlaşılamaz.
   if(g_state_pubs == 0)
      Print("Sinyal: UYARI — durum HİÇ yayımlanmadı. Çekirdek hesap ve "
            "pozisyon bilgisi göremez.");
   if(g_state_errs > 0)
      PrintFormat("Sinyal: UYARI — durum yayını %I64u kez BAŞARISIZ oldu.", g_state_errs);

   if(ring_lost > 0)
      Print("Sinyal: UYARI — halka doldu ve tick KAYBEDİLDİ. "
            "Çekirdek yetişemiyor veya duruyor.");

   if(g_push_errors > 0)
      PrintFormat("Sinyal: UYARI — köprü %I64u kez HATA döndürdü (son kod=%d). "
                  "Bu 'halka dolu' DEĞİL, gerçek bir hata.",
                  g_push_errors, g_last_push_err);

   // DURGUNLUK DEDEKTÖRÜ.
   //
   // Bu olmadan sistem sessizce donuyordu: sayaçların hepsi sıfır ve temiz
   // görünüyor, ama saatlerdir tek tick akmıyor. Hazır sembol varken hiç
   // tick gelmemesi canlı piyasada normal DEĞİLDİR — terminalin veri
   // bağlantısı kopmuş olabilir.
   if(!force && delta == 0)
     {
      int ready = 0;
      for(int i = 0; i < g_count; i++)
         if(g_ready[i] == 1) ready++;
      if(ready > 0)
        {
         PrintFormat("Sinyal: DİKKAT — son raporda HİÇ tick gelmedi "
                     "(%d hazır sembol var). Terminal veri bağlantısını ve "
                     "Market Watch'ta fiyatların hareket edip etmediğini kontrol edin. "
                     "Bağlantı: %s",
                     ready,
                     (TerminalInfoInteger(TERMINAL_CONNECTED) ? "VAR" : "YOK"));
        }
     }

   g_ticks_prev_report = g_ticks_pushed;
  }

//+------------------------------------------------------------------+
//| OnTimer — komut drenajı + DOM'suz sembollerin taranması.          |
//|                                                                  |
//| Çözünürlük donanım tabanlı 10-16 ms'dir; bu semboller bu yüzden   |
//| POLLED_ONLY olarak işaretlenir ve farklı bir gecikme sınıfındadır.|
//+------------------------------------------------------------------+
void OnTimer()
  {
   if(g_handle == 0) return;

   // Timer olayı düşürülmüş mü? (işleyici periyottan uzun sürdüyse)
   ulong now_qpc = SinyalQpc();
   if(g_last_timer_qpc > 0 && g_qpc_freq > 0)
     {
      double elapsed_ms = (double)(now_qpc - g_last_timer_qpc) * 1000.0 / (double)g_qpc_freq;
      if(elapsed_ms > (double)TimerMs * 2.5)
         g_timer_skips++;
     }
   g_last_timer_qpc = now_qpc;

   // 1) Komutları boşalt. Düşük gecikme için sınır koymuyoruz ama
   //    işleyicinin uzaması timer olayı düşürür — makul bir tavan var.
   SinyalCmd cmd;
   int drained = 0;
   while(drained < 64 && SinyalPopCmd(g_handle, cmd) == SINYAL_OK)
     {
      ExecuteCmd(cmd);
      drained++;
     }

   // 2) Sembolleri tara. DOM'lu semboller zaten OnBookEvent ile push
   //    ediliyor; onları da taramak zararsız (değişmemişse yayın yok).
   for(int i = 0; i < g_count; i++)
      PushIfChanged(i, SINYAL_KIND_TICK);

   // 3) Durumu yayımla: saniyede bir, veya OnTrade tetiklediyse hemen.
   //    Olay-güdümlü + heartbeat hibrit. HistorySelect BURADA ÇAĞRILMAZ.
   datetime now_s = TimeLocal();
   if(g_state_dirty || now_s != g_last_state_pub)
     {
      PublishState();
      g_last_state_pub = now_s;
      g_state_dirty = false;
     }

   // 4) Gerçekleşmiş sonuç mutabakatı. Kendi periyodunu kendi yönetir;
   //    burada her turda çağrılması zararsız (içeride erken döner).
   ReconcileDeals();

   ReportTelemetry(false);
  }
//+------------------------------------------------------------------+
