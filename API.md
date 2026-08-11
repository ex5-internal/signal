# Sinyal API — sinyal üreten sisteme verilecek uç

Tek WebSocket ucu. Piyasa verisi token istemez; hesap ve emir ister.

```
ws://144.76.111.177:8787
```

| | |
|---|---|
| Token (yalnız işlem için) | `pQZwo7U7wyAppJCHzjtxfGxUnLdZddd4JRKhW6idHoA` |
| Protokol | JSON, satır başına bir mesaj (bir çerçevede birden fazla satır gelebilir) |
| TLS | **YOK.** `ws://` düz metin — token yol üzerinde açık gider. |
| Ana sembol | `GOLD` (XAUUSD değil), digits 2, point 0.01 |

Bağlantı kurulur kurulmaz sunucu `hello` gönderir:

```json
{"t":"hello","proto":3,"mode":"live","instances":["mt5-1"],"trading":true,
 "public_feed":true,"auth_required_for_trading":true,"level":"public"}
```

---

## 1. Canlı tick — sinyal üretiminin asıl girdisi

```json
{"op":"subscribe","channels":["tick.GOLD"]}
```

```json
{"t":"tick","s":"GOLD","b":4367.95,"a":4368.21,"ms":1786488480123,"lat_us":249,"src":"mt5-1"}
```

- `b` / `a` — bid / ask. `l` (last) yalnızca sıfırdan farklıysa gönderilir.
- `ms` — **broker sunucu saati**, epoch milisaniye. Yerel saat veya UTC DEĞİL.
- `lat_us` — EA'nın tick'i yakalamasından bu mesajın üretilmesine kadar geçen
  gerçek süre. Ölçülen değer: **p50 ~250 µs**. Akışın sağlığını doğrudan gösterir.

Kanal adında **sembol büyük/küçük harf duyarlıdır**, broker'daki ad ne ise o
yazılmalı. Joker: `tick.*`.

### Sadakat: hangi sembolde her tick geliyor

`{"op":"symbols"}` cevabındaki iki alan gecikme sınıfını söyler:

- `chart: true` — EA'nın bağlı olduğu grafiğin sembolü. `OnTick` ile **olay
  güdümlü**; terminalin verdiği her tick olayı alınır. **En yüksek sadakat
  burasıdır.**
- `polled_only: true` — yalnızca 16 ms'lik taramayla örnekleniyor. İki tarama
  arasında birden fazla tick oluşursa **aradakiler görülmez**.

Şu an `chart: true` olan sembol **GOLD**. Sinyal başka bir sembolde
üretilecekse EA o sembolün grafiğine taşınmalıdır.

---

## 2. Mumlar

```json
{"op":"candles","symbol":"GOLD","tf":"M1","count":500}
```

```json
{"t":"candles","s":"GOLD","tf":"M1","src_kind":"mt5","hist":"ok",
 "items":[{"t":1786488420000,"o":4367.52,"h":4367.855,"l":4367.29,"c":4367.82,"ticks":53}]}
```

Dilimler: `M1 M5 M15 M30 H1 H4`. Bar alanları kısaltmalı: `t` (açılış zamanı,
broker saati, epoch **ms**), `o h l c`, `ticks` (bar içi tick sayısı —
**gerçek hacim değil**), `partial` (yalnız `true` ise gönderilir).

### `src_kind` — bu alanı OKUYUN

İki ayrı kaynak var ve **asla karıştırılmazlar**:

| `src_kind` | Kaynak | Anlamı |
|---|---|---|
| `"mt5"` | `CopyRates` | MT5'in grafikte gösterdiği **gerçek broker serisi** |
| `"tick"` | Tick akışı | Bizim `mid=(bid+ask)/2` ile ürettiğimiz seri |

Bunlar **aynı fiyat serisi değildir**. MT5'in FX barları bid tabanlıdır; bizim
barlarımız mid'dir. GOLD'da fark **20-30 point** mertebesindedir. Strateji MT5
ile aynı seriyi görmek zorundaysa `src_kind: "mt5"` beklenmelidir.

### `hist` — "veri yok" ile "sistem bozuk" ayrımı

| `hist` | Anlamı |
|---|---|
| `"ok"` | MT5 geçmişi alındı |
| `"off"` | MQL5 Service çalışmıyor → yalnızca tick'ten üretilen mumlar var |
| `"failed"` | Service hata bildirdi; kod `hist_note` içinde |

`hist: "off"` gelirse grafiğin kısa olması **veri yokluğu değil, kurulum
eksikliğidir**. Sessizce kabul edilmemeli.

### Canlı bar

```json
{"op":"subscribe","channels":["candle.GOLD.M1"]}
```

`candle` mesajı **yalnızca bar KAPANDIĞINDA** gelir ve daima **tick**
kaynaklıdır. Oluşmakta olan barın hareketini görmek için `tick` akışına abone
olup son barı istemcinin kendisi güncellemesi gerekir.

> `src_kind: "mt5"` serisiyle çalışıyorsanız oluşan barı **bid**'den kurun,
> mid'den değil — aksi halde geçmişin bittiği yerde yarım spread kadar
> görünmez bir basamak oluşur.

Joker yalnızca sembol yerinde geçerlidir: `candle.*.M5` çalışır,
`candle.GOLD.*` **çalışmaz** (sessizce yok sayılır).

---

## 3. Anlık fiyat ve sembol bilgisi

```json
{"op":"snapshot","symbols":["GOLD"]}
{"op":"symbols"}
```

`symbols` her sembol için `digits`, `point`, `tick_size`, `volume_min/max/step`,
`exec_mode`, `filling_mask`, `stops_level`, `book_depth`, `polled_only`,
`chart`, `ready` verir. Emir göndermeden önce hacim ve fiyat bunlara göre
yuvarlanmalıdır.

`ready: false` olan sembolün fiyatına güvenilmez.

**Derinlik (DOM) yok**: bu broker GOLD dahil hiçbir sembolde `book_depth`
yayınlamıyor (`book_depth: 0`). Orderbook'a bel bağlanamaz.

---

## 4. İşlem — token gerekir

```json
{"op":"auth","token":"pQZwo7U7wyAppJCHzjtxfGxUnLdZddd4JRKhW6idHoA"}
```
```json
{"t":"authed","level":"trader"}
```

Yükseldikten sonra: `account`, `positions`, `orders`, `order`, `close`,
`cancel`, `modify_sltp` ve `subscribe → "order"` kanalı açılır.

```json
{"op":"subscribe","channels":["order"]}
{"op":"order","id":"benim-1","symbol":"GOLD","side":"buy","type":"market","volume":0.01}
{"op":"order","id":"benim-2","action":"pending","symbol":"GOLD","type":"buy_limit","volume":0.01,"price":4350.00}
{"op":"close","id":"benim-3","ticket":942649399}
{"op":"cancel","id":"benim-4","ticket":942418505}
{"op":"modify_sltp","id":"benim-5","ticket":942649399,"sl":4300,"tp":4400}
```

`id` **idempotency anahtarıdır** — aynı `id` ikinci kez gelirse `duplicate`
döner ve emir GÖNDERİLMEZ. Çift pozisyon açılmaz.

### Emir sonucu iki aşamalıdır — karıştırmayın

```json
{"t":"order","id":"benim-1","kind":"queued"}
{"t":"order","id":"benim-1","kind":"ack","retcode":10008}
{"t":"order","id":"benim-1","kind":"txn","retcode":10009}
{"t":"order","id":"benim-1","kind":"txn","order":942649399,"deal":931503131,"position":942649399,"volume":0.01,"price":4367.95}
```

- `kind: "ack"` + `retcode: 10008` → istek sunucuya iletildi. **DOLMADI.**
- `kind: "txn"` + `retcode: 10009` → gerçekten yürütüldü.

Emri yalnızca ikincisiyle "gerçekleşti" sayın.

`id: ""` gelen bir olay "bize ait değil" demektir — terminalden elle yapılmış
bir işlem. Olay asla atılmaz, kimliksiz yayımlanır.

---

## 5. Bilinmesi gereken sınırlar

- **Mumlar bellekte.** `sinyald` yeniden başlarsa tick'ten üretilen seri
  sıfırlanır. MT5 serisi `CopyRates` ile yeniden çekilir.
- **Yuvarlanan pencere: 5.000 bar** / sembol / dilim. M1'de ~3,5 günlük
  piyasa açık süresi.
- **Hız sınırı yok.** Geçmiş isteklerinde eş zamanlılık tavanı var (4) ama
  istek/saniye sınırı yok.
- `lagged` mesajı gelirse akışta **boşluk** oluşmuştur; mum geçmişi yeniden
  çekilmelidir.
- Zaman alanlarının hepsi **broker sunucu saatidir**. Broker DST uygulayabilir.

---

## Kurulum ön koşulu

Geçmiş barların gelmesi için MT5'te **iki program** çalışıyor olmalı:

1. `SinyalCollector` EA — GOLD grafiğinde (tick toplar)
2. `SinyalHistory` Service — Navigator → Services (geçmiş çeker)

Service ayrı bir programdır çünkü `CopyRates` çağıran thread'i bloklar ve
canlıda 30-60 saniyelik donmalar raporlanmıştır; EA'nın içinde olsaydı tick
akışını durdururdu.

Service çalışmıyorsa `hist: "off"` gelir ve **yalnızca tick'ten üretilen
mumlar** servis edilir.
