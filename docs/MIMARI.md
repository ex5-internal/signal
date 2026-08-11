# Mimari — ve neden böyle

Bu belge **kararların gerekçesini** anlatır. Ne yapıldığını koddan okuyabilirsin;
buradaki asıl değer, denenip **çürütülmüş** alternatiflerdir. Bir şeyi
"basitleştirmeden" önce buraya bak.

---

## Amaç

Sinyal bir piyasa verisi feed'i **değildir**. Amacı, ayrı geliştirilen bir
**sinyal üreten sistemi**, işlemin **gerçekten yapılacağı** MT5 terminalinin
gördüğü veriyle beslemektir.

Bu tek cümle mimarinin tamamını belirler. Sinyal sistemi başka bir kaynaktan
(public feed, başka broker, başka grafik) beslenirse, stratejinin gördüğü fiyat
ile emrin doldurulduğu fiyat ayrışır — ve bu ayrışma tam olarak kaçınılmak
istenen şeydir.

Pratik sonucu: **veri, yürütme mekânından çıkmalı ve test ile canlı arasında
hiçbir yerde ayrışmamalı.**

---

## Üç süreç

```
MT5 Terminal (tek süreç)                     ayrı süreç
┌───────────────────────────────────┐       ┌──────────────┐      istemci
│                                   │       │              │
│  SinyalCollector.mq5  (EA)        │  shm  │              │      ┌────────┐
│    OnTick / OnBookEvent  ─────────┼──md──▶│   sinyald    │──ws──▶ sinyal │
│    OnTradeTransaction    ─────────┼──res─▶│              │      │ sistemi│
│    OnTimer               ◀────────┼──cmd──┤              │◀─────┤        │
│                                   │       │              │      └────────┘
│  SinyalHistory.mq5  (Service)     │       │              │
│    OnStart + CopyRates   ─────────┼─bars─▶│              │
│                          ◀────────┼─hreq──┤              │
└───────────────────────────────────┘       └──────────────┘
        sinyal_bridge.dll (Rust cdylib, terminalin İÇİNDE)
```

| Süreç | Görevi | Bloklanabilir mi |
|---|---|---|
| **EA** | tick, derinlik, emir olayları, hesap durumu | **ASLA** |
| **Service** | MT5'in kendi mum geçmişi (`CopyRates`) | evet, zararsız |
| **sinyald** | WebSocket dağıtımı, emir yönlendirme, mum deposu | — |

### Neden EA ve Service ayrı programlar

Bu en önemli karar ve **kanıta dayanıyor**, tercihe değil.

`CopyRates`, MQL5 dokümanına göre EA/script içinde **çağıran thread'i
bloklar** ve zaman aşımı süresi **hiç belgelenmemiştir**:

> "The function will return the amount of data that will be ready by the moment
> of timeout expiration" — [CopyRates](https://www.mql5.com/en/docs/series/copyrates)

Forumda canlı hesaplarda 30-60 saniyelik donmalar raporlanmış (MT5'e özgü,
MT4'te görülmüyor). MQL5 içinden çağrı **iptal edilemez**.

Bir EA'nın tüm işleyicileri **tek thread'de sırayla** çalışır:

> "Each script, each service and each Expert Advisor runs in its own separate
> thread." — [Program Running](https://www.mql5.com/en/docs/runtime/running)

Yani `CopyRates` EA'nın içinde olsaydı, tek bir çağrı tick toplamayı 30 saniye
durdururdu. İlk plan "OnTimer'da ölçerek yap, gerekirse taşı" idi; **kanıt bunu
çürüttü.** Bilinmeyen bir üst sınırı gecikme-kritik yola koymak yanlıştır.

Service'in kendi thread'i var, `Sleep()` serbest (yalnızca custom indikatörlerde
yasak), yani yeniden deneme döngüsü **ancak orada** kurulabilir. Ayrıca ayrı bir
segment çifti kullandığı için EA'nın tek-yazar kilidine dokunmaz — **Service
kapalıyken sistem tam işlevsel kalır**, yalnızca MT5 geçmişi gelmez.

---

## Veri sadakati — en kritik bölüm

### İki mum kaynağı vardır ve ASLA karıştırılmazlar

| `src_kind` | Kaynak | Nasıl üretilir |
|---|---|---|
| `"mt5"` | `CopyRates` | Broker'ın kendi serisi — MT5 grafiğinde gördüğün |
| `"tick"` | tick akışı | `mid = (bid + ask) / 2` ile bizim ürettiğimiz |

**Bunlar aynı fiyat serisi değildir.** MT5'in FX/CFD barları bid tabanlıdır;
bizimkiler mid'dir. Fark yarım spread kadardır:

| Sembol | Yaklaşık fark |
|---|---|
| EURUSD | 0,5–1 pip |
| **GOLD** | **20–30 point** |

Bunları aynı seride birleştirmek, geçmişin bittiği yerde **görünmez bir
basamak** üretir. Sinyal sistemi o basamağın üzerinde bir sinyal üretirse,
canlıda karşılığı olmayan bir harekete tepki vermiş olur.

Bu yüzden iki kaynak **ayrı depolarda** tutulur ve her cevap hangisinden
geldiğini söyler. Birleştirme YAPILMAZ.

> **Oluşmakta olan bar**: `candle` mesajı yalnızca bar KAPANDIĞINDA gelir ve
> daima tick kaynaklıdır. `src_kind: "mt5"` serisiyle çalışan bir istemci son
> barı tick akışından kendisi kurmalı — ve **bid**'den, mid'den değil.

### Gecikme sınıfları — hangi sembolde her tick geliyor

MQL5'te `OnTick` **yalnızca EA'nın bağlı olduğu grafiğin sembolü** için
tetiklenir. Diğer semboller `OnTimer` taramasıyla okunur ve timer çözünürlüğü
donanım nedeniyle 10-16 ms'dir:

> "timer events are generated no more than 1 time in 10-16 milliseconds due to
> hardware limitations."

| Sınıf | Bayrak | Sonuç |
|---|---|---|
| Grafik sembolü | `chart: true` | Olay güdümlü, **her tick** |
| DOM'lu sembol | `book_depth > 0` | `OnBookEvent` asla atlanmaz |
| Diğerleri | `polled_only: true` | 16 ms örnekleme; **ara tickler görülmez** |

Sinyal hangi sembolde üretilecekse **EA o sembolün grafiğinde durmalıdır**.
Şu anki kurulumda bu **GOLD**'dur.

### Kayıpsız tick MT5 EA API'sinden elde EDİLEMEZ

> "In case when OnTick function for the previous quote is being processed when a
> new quote is received, the new quote will be ignored."

Aynı kural `OnTimer` ve `OnChartEvent` için de geçerli. `OnBookEvent` istisnadır.
Bu yüzden iki ayrı kayıp metriği vardır ve **karıştırılmamalıdır**:

- `halka-kayip` — bizim halkamız doldu (çekirdek yetişemiyor). **0 olmalı.**
- terminal kaynaklı atlama — MT5'in kendi sınırı. Ölçülür, garanti edilemez.

---

## Paylaşımlı bellek

Windows dosya eşlemesi (`CreateFileMappingW`), `Local\` önekli — oturuma
hapsedilmiş, yönetici hakkı istemez.

| Segment | Yön | İçerik | `Cell` boyutu |
|---|---|---|---|
| `md` | EA → çekirdek | tick | 64 |
| `book` | EA → çekirdek | derinlik | 832 |
| `cmd` | çekirdek → EA | emir komutları | 192 |
| `res` | EA → çekirdek | emir sonuçları | 192 |
| `sym` | EA → çekirdek | sembol tablosu | — |
| `state` | EA → çekirdek | hesap/pozisyon/emir | — |
| `hreq` | çekirdek → **Service** | geçmiş isteği | — |
| `bars` | **Service** → çekirdek | MT5 barları | **128** |

Halkalar **SPSC**'dir (tek yazar, tek okur). Bu güvenli çünkü bir EA'nın tüm
işleyicileri tek thread'de çalışır — ama **farklı grafiklerdeki EA'lar ayrı
thread'lerdedir.** Aynı `InstanceName` ile ikinci kopya açılırsa iki thread aynı
halkaya yazardı. Bu yüzden isimli bir mutex ikinci yazarı **reddeder**:
opsiyonel sertleştirme değil, **doğruluk şartı**.

> `Cell<BarRec>` = 128 bayt ve bu **kasıtlı olarak** diğer halkaların hiçbiriyle
> aynı değil. `RING_MAGIC` tüm halkalarda aynı olduğu için, yanlış segment adına
> bağlanmayı yakalayan tek çalışma-zamanı denetimi **slot boyutu**dur.

### Yazar asla bloklanmaz

Halka dolduğunda `push` mesajı yazmaz, `false` döner ve `push_failures`
sayacını artırır. Ticaret terminalinin thread'ini yavaş bir okuyucu yüzünden
bekletmek kabul edilemez.

Bu sayacın **anlamı üreticiye bağlıdır** ve karıştırmak metriği işe yaramaz
hale getirir:
- **Yeniden denemeyen üretici** (üretimdeki EA): sayı = kalıcı **kayıp mesaj**.
- **Yeniden deneyen üretici** (Service, testler): mesaj kaybolmaz; yalnızca
  geri basınç ölçüsü.

---

## Emir olaylarının kimliğe bağlanması

`MqlTradeTransaction` yapısında **`magic` ve `comment` YOKTUR**, ve
`MqlTradeResult.request_id` yalnızca `TRADE_TRANSACTION_REQUEST` olayında
doludur. Yani dolumu bildiren `DEAL_ADD` olayı, tek başına bakıldığında hangi
emre ait olduğunu **söylemez**.

EA'da tablo taraması seçenek değil: terminalin işlem kuyruğu 1024 elemanlı ve
işleyici yavaşlarsa **eski olaylar sessizce ezilir**. Bu yüzden EA ham alanları
iletir, eşleştirme çekirdekte yapılır.

Bağ dört yoldan kurulur:

1. `SEND_ACK` — çekirdeğin ürettiği, `request_id` taşıyan olay
2. `TRADE_TRANSACTION_REQUEST` — `request_id` **ve** `result.order` (asıl bilet)
3. Durum yayınındaki `POSITION_MAGIC` — çekirdek yeniden başlasa bile bağları
   geri getirir; **mutabakatın temeli**
4. Hedging'de açılış emrinin bileti = pozisyon bileti

Olay sırası **garantili değildir**: `DEAL_ADD`, onu açıklayan `REQUEST`'ten önce
gelebilir. Çözülemeyen olaylar 5 saniyelik bir tamponda tutulur ve bağ kurulunca
**geriye dönük** atfedilir. Bağ hiç kurulamazsa olay `id: ""` ile yayımlanır —
"bize ait değil / terminalden elle yapılmış" demektir. **Olay asla atılmaz.**

> **Ters yönde çıkarım YAPILMAZ.** "Pozisyonu bilen emri de bilir" yanlıştır:
> K1'in açtığı pozisyonu K2 kapatabilir. Bu çıkarım bir kez yazıldı ve canlı
> testte kapanış olaylarını yanlış komuta atfetti.

---

## Doldurma modu maskeye bakarak seçilemez

`SYMBOL_FILLING_MODE` maskesi yalnızca FOK/IOC/BOC taşır; `RETURN` maskede
**yoktur**. Doğru seçim `SYMBOL_TRADE_EXEMODE`'a bağlıdır: INSTANT/REQUEST'te
FOK maskeden bağımsız izinli, MARKET'te RETURN yasak.

Bu teorik değil — aynı broker'ın iki hesabında `exec_mode` 1 ve 2,
`filling_mask` 1 ve 2 olarak farklı okundu. Sabit kodlanmış bir doldurma modu
bu durumlardan birinde kesin `10030` alırdı.

---

## Yerleşim kayması nasıl engelleniyor

Rust ile MQL5 arasındaki yapı yerleşimini **hiçbir bağlayıcı doğrulamaz**. Dört
katmanlı savunma:

1. **Derleme zamanı (Rust)** — `sinyal-proto/src/lib.rs` içindeki `layout`
   modülü her alanın ofsetini `offset_of!` ile sabitler. Alan eklemek veya
   sırasını değiştirmek **derlemeyi kırar**.
2. **Üretim** — `Protocol.mqh` elle yazılmaz, `gen-mqh` Rust tanımlarından
   üretir.
3. **Üreteç testi** — üretilen MQL5 metni ayrıştırılıp boyutu `size_of` ile
   karşılaştırılır.
4. **Çalışma zamanı** — EA açılışta `SinyalSizeof` ile DLL'e sorar ve MQL5'in
   kendi `sizeof`'uyla karşılaştırır; uyuşmazsa **başlamaz**.

Detay ve tuzaklar: [GELISTIRME.md](GELISTIRME.md)
