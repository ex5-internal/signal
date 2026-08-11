# Sinyal — Düşük Gecikmeli MetaTrader 5 Adaptörü

MT5 terminalinden tick ve derinlik akışını **mikrosaniye mertebesinde** dışarı
taşıyan, karşılığında market/limit/stop emirlerini yürüten köprü. Dışarıya
WebSocket ile açılır.

```
MT5 Terminal              ayrı süreç                 istemciniz
┌──────────────┐         ┌──────────────┐          ┌──────────┐
│ SinyalCollector.mq5 │  │  sinyald     │          │          │
│   OnTick        ────┼──┼─► paylaşımlı │──ws://───┼─►  feed  │
│   OnBookEvent       │  │   bellek     │          │  + emir  │
│   OnTradeTransaction│◄─┼─── (SPSC)    │◄─────────┼──        │
└──────────────┘         └──────────────┘          └──────────┘
        sinyal_bridge.dll (Rust cdylib, süreç içi)
```

---

## Durum

Canlı demo hesapta (XM Global, hedging) uçtan uca doğrulandı.

| Bileşen | Durum | Test |
|---|---|---|
| `crates/sinyal-proto` — paylaşılan bellek yerleşimi + emir doğrulama | ✅ | 55 |
| `crates/sinyal-shm` — Windows shm, QPC, tek-örnek kilidi | ✅ | 14 |
| `crates/sinyal-bridge` — MT5'e yüklenen DLL (C ABI) | ✅ | 29 |
| `crates/sinyal-core` — `sinyald`: shm okuma + WebSocket feed + emir | ✅ | 23 |
| `tools/latency-bench` — gecikme/kayıp ölçüm aracı | ✅ | 7 |
| `mql5/` — toplayıcı EA + ABI başlıkları | ✅ derlendi, canlı çalışıyor | — |

**128 test, hepsi geçiyor.**

### Ölçülen gecikme

Paylaşımlı bellek turu (üretici `push` → tüketici `pop`, **ayrı süreçler**):

| | 20k tick/sn | tavan |
|---|---|---|
| p50 | 100 ns | 200 ns |
| p99 | 8,3 µs | 12,7 µs |
| kayıp | 0 / 400.000 | 0 / 2.000.000 |
| hız | — | **21,6M tick/sn** |

Canlı MT5 akışında uçtan uca (EA yakalama → WebSocket mesajı):
**p50 36 µs · p90 523 µs · p99 823 µs · max 1,15 ms**

```bash
cargo run --release -p latency-bench -- run --count 2000000 --rate 0 --symbols 200
```

### Canlı doğrulanan emir yolu

Demo hesapta, 0.01 lot ile:

| Test | Sonuç |
|---|---|
| Bekleyen `BUY_LIMIT` | `queued → ack(10008) → txn(10009)` |
| İptal | `txn(10009)` |
| Market `BUY` EURUSD | 1.15402'den doldu |
| Pozisyon kapatma | 1.15385'ten kapandı |
| Aynı `id` ile tekrar | `duplicate` — çift pozisyon yok |
| Geçersiz sembol / side / tip | Hepsi açık mesajla reddedildi |

---

## Kullanım

### Kurulum

```powershell
.\tools\deploy.ps1 -Compile
```

Rust'ı derler, `Protocol.mqh`'yi yeniden üretir, DLL'in x64 olduğunu doğrular,
dosyaları terminalin veri klasörüne kopyalar, EA'yı derler.

Terminalde (İngilizce menüler):

1. **Tools → Options → Expert Advisors → `Allow DLL imports`** işaretli olmalı
2. `SinyalCollector`'ı bir grafiğe sürükleyin
3. **Toolbox → Experts** sekmesindeki logu okuyun — EA açılışta yerleşim
   doğrulaması yapar, uyumsuzsa **başlamaz** ve sebebini yazar
4. Emir yürütmek için: grafikte **F7** → `Inputs` → `EnableTrading` = `true`,
   ve **Algo Trading** düğmesi yeşil olmalı

### Çekirdeği çalıştırma

```bash
sinyald --instance mt5-1 --bind 127.0.0.1:8787
sinyald --instance mt5-1 --enable-trading --token GIZLI
```

Varsayılan olarak yalnızca `127.0.0.1` dinler ve **emir yürütme kapalıdır**.

### WebSocket protokolü

**Abone ol:**
```json
{"op":"subscribe","channels":["tick.*","book.GOLD","order"]}
```
```json
{"t":"tick","s":"EURUSD","b":1.15412,"a":1.15431,"ms":1786467441043,"lat_us":36,"src":"mt5-1"}
```

`lat_us` — EA'nın tick'i yakalamasından bu mesajın üretilmesine kadar geçen
gerçek süre. Akışın sağlığını doğrudan gösterir.

**Sembolleri listele:** `{"op":"symbols"}` → her sembol için `digits`,
`tick_size`, `volume_step`, `exec_mode`, `filling_mask`, `stops_level`,
`book_depth`, `polled_only`, `ready`.

**Anlık fiyat:** `{"op":"snapshot","symbols":["EURUSD"]}`

**Emir:**
```json
{"op":"order","id":"benim-1","symbol":"EURUSD","side":"buy","volume":0.1}
{"op":"order","id":"benim-2","action":"pending","symbol":"GOLD","type":"buy_limit","volume":0.01,"price":3300.50}
{"op":"close","id":"benim-3","ticket":942420795,"volume":0.05}
{"op":"cancel","id":"benim-4","ticket":942418505}
{"op":"modify_sltp","id":"benim-5","ticket":942420795,"sl":1.14,"tp":1.17}
```

`id` **idempotency anahtarıdır** — aynı `id` ikinci kez gelirse reddedilir.

Emir sonucu **iki aşamalıdır ve karıştırılmamalıdır**:

- `kind:"ack"` + `retcode:10008` → istek sunucuya iletildi. **DOLMADI.**
- `kind:"txn"` + `retcode:10009` → gerçekten yürütüldü.

---

## Tasarımı belirleyen bulgular

Aşağıdakiler MQL5 dokümanından doğrulandı ve mimariyi doğrudan şekillendirdi.

### SPSC güvenli — ama tek koşulla

*"Each script, each service and each Expert Advisor runs in its own separate
thread"* ve *"All events are processed one after another in the order they are
received."* Tek bir EA'nın tüm işleyicileri aynı thread'de sırayla çalışır →
halkaların tek-yazar varsayımı geçerli.

**Ama** farklı grafiklerdeki EA'lar ayrı thread'lerdedir. Aynı `InstanceName`
ile ikinci kopya açılırsa iki thread aynı halkaya yazar. Bu yüzden `SinyalOpen`
isimli bir mutex ile ikinci yazarı **reddeder** — opsiyonel sertleştirme değil,
doğruluk şartı.

### Kayıpsız tick MT5 EA API'sinden elde edilemez

*"In case when OnTick function for the previous quote is being processed when a
new quote is received, the new quote will be ignored."* Aynı kural `OnTimer` ve
`OnChartEvent` için de geçerli. `OnBookEvent` **istisnadır** — asla atlanmaz.

Bu yüzden iki ayrı kayıp metriği var ve karıştırılmamalı:

- `halka-kayip` — halka doldu (çekirdek yetişemiyor). 0 olmalı.
- terminal kaynaklı atlama — MT5'in kendi sınırı, ölçülür, garanti edilemez.

Ölçüm: canlı XM akışında sembol başına en küçük ardışık tick aralığı **70 ms**,
bizim tarama periyodumuz 16 ms. Yani pratikte tüm tickler yakalanıyor.

### `OnTimer` çözünürlüğü 10-16 ms, 1 ms değil

*"timer events are generated no more than 1 time in 10-16 milliseconds due to
hardware limitations."* Sonuç: sembollerin **iki gecikme sınıfı** var ve bu
`SymbolEntry.flags` ile çekirdeğe bildirilir.

| Sınıf | Yol | Gecikme |
|---|---|---|
| Grafik sembolü | `OnTick` | olay güdümlü |
| DOM'lu sembol | `OnBookEvent` | olay güdümlü |
| Diğerleri (`POLLED_ONLY`) | `OnTimer` taraması | ort. ~8 ms, en kötü ~16 ms+ |

### Doldurma modu maskeye bakarak seçilemez

`SYMBOL_FILLING_MODE` maskesi yalnızca FOK/IOC/BOC taşır; `RETURN` maskede
**yoktur**. Doğru seçim `SYMBOL_TRADE_EXEMODE`'a bağlıdır: INSTANT/REQUEST'te
FOK maskeden bağımsız izinli, MARKET'te RETURN yasak.

Bu teorik değil — aynı broker'ın iki hesabında `exec_mode` 1 (INSTANT) ve
2 (MARKET), `filling_mask` 1 (FOK) ve 2 (IOC) olarak farklı okundu. Sabit
kodlanmış bir doldurma modu bu durumlardan birinde kesin 10030 alırdı.

---

## Yerleşim kayması nasıl engelleniyor

Rust ile MQL5 arasındaki yapı yerleşimini hiçbir bağlayıcı doğrulamaz. Dört
katmanlı savunma:

1. **Derleme zamanı (Rust)** — `sinyal-proto/src/lib.rs` içinde ~60
   `const assert!`, her alanın ofsetini sabitler.
2. **Üretim** — `Protocol.mqh` elle yazılmaz, `gen-mqh` Rust tanımlarından üretir.
3. **Üreteç testi** — üretilen MQL5 metni ayrıştırılıp boyutu `size_of` ile
   karşılaştırılır; üreteçteki metnin Rust'tan ayrışmasını yakalar.
4. **Çalışma zamanı** — EA açılışta `SinyalSizeof` ile DLL'e sorar ve MQL5'in
   kendi `sizeof`'uyla karşılaştırır; uyuşmazsa **başlamaz**.

Protokol değişirse `RING_VERSION` artırılır, uyumsuz sürümler bağlanamaz.

---

## Bilinen eksikler

Canlı paraya geçmeden önce kapatılmalı:

1. **Ara `txn` olayları müşteri kimliğine bağlanamıyor.**
   `MqlTradeTransaction`'da `magic`/`comment` yok ve `request_id` yalnızca
   `TRADE_TRANSACTION_REQUEST`'te dolu. Dolum bilgisini taşıyan `DEAL_ADD`
   olayı `id:""` ile geliyor. Bilet→`client_id` tablosu gerekiyor.

2. **Hesap bilgisi protokolde yok.** Bakiye/equity/marjin görünmüyor; emir
   marjini önden doğrulanamıyor.

3. **Pozisyon/emir sorgulama yok.** İstemci yeniden bağlanınca mevcut durumu
   bilemiyor. `OnTradeTransaction` kuyruğu 1024'te taşıp eskileri ezdiği için
   periyodik mutabakat şart.

4. **Sembol listesi yalnızca EA açılışında okunuyor.** Market Watch'a sonradan
   eklenen sembol, EA yeniden başlamadan görünmez. Dinamik ekleme için
   sembol kimliklerinin kararlı (append-only) olması gerekiyor.

---

## Lisans

UNLICENSED — özel proje.
