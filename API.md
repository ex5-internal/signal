# Sinyal API — sinyal üreten sisteme verilecek uç

Tek WebSocket ucu. Piyasa verisi token istemez; hesap ve emir ister.

> **Bu belgeyi okuyan otomatik bir sisteme:** buradaki her alan adı ve
> davranış koddan doğrulanmıştır. Tahmin etme; bir şey belirsizse
> [docs/MIMARI.md](docs/MIMARI.md) içinde gerekçesi vardır.

---

## Önce bunu oku: bu sistem NE DEĞİLDİR

| | |
|---|---|
| ❌ Genel amaçlı piyasa verisi feed'i | Tek bir MT5 terminaline bağlıdır; o terminal kapanınca veri durur |
| ❌ Birden çok broker toplayıcısı | Şu an tek örnek (`mt5-1`) çalışıyor |
| ❌ Backtest motoru | Mumlar bellekte; `sinyald` yeniden başlarsa tick serisi sıfırlanır |
| ❌ Garantili teslimat | Halka dolarsa EA tick düşürür (sayaçla bildirir); `lagged` mesajı boşluk demektir |
| ❌ Kayıpsız tick kaynağı | MT5 EA API'si bunu vermez — bkz. aşağıdaki "sadakat" bölümü |
| ❌ TLS'li | `ws://` düz metin; token açık gider |

Ne **olduğu**: işlemin gerçekten yapıldığı terminalin gördüğü veriyi, aynı
terminale emir gönderebilen bir uçla birlikte sunan köprü. Değeri, veri ile
yürütmenin **aynı yerden** gelmesidir.

---

## En kısa çalışan istemci

```javascript
const ws = new WebSocket("ws://144.76.111.177:8787");

ws.onopen = () => {
  ws.send(JSON.stringify({op: "symbols"}));                       // digits/point
  ws.send(JSON.stringify({op: "candles", symbol: "GOLD", tf: "M1", count: 500}));
  ws.send(JSON.stringify({op: "subscribe", channels: ["tick.GOLD"]}));
};

ws.onmessage = (ev) => {
  // Bir çerçevede BIRDEN FAZLA satır gelebilir — mutlaka böl.
  for (const line of String(ev.data).split("\n")) {
    if (!line.trim()) continue;
    const m = JSON.parse(line);

    switch (m.t) {
      case "hello":
        if (m.mode !== "live") console.warn("CANLI DEGIL:", m.mode);
        break;

      case "candles":
        if (m.hist === "off") {
          // "Veri yok" DEĞİL — kurulum eksik. Sessizce kabul etme.
          throw new Error("MT5 gecmisi yok: " + m.hist_note);
        }
        if (m.src_kind !== "mt5") {
          console.warn("mid-fiyat serisi geldi, MT5 serisi degil");
        }
        gecmisiYukle(m.items);   // {t,o,h,l,c,ticks,partial?}
        break;

      case "tick":
        // Oluşan barı BID'den güncelle (MT5 serisiyle tutması için).
        sonBariGuncelle(m.s, m.b, m.ms);
        break;

      case "lagged":
        // Akışta boşluk oluştu — geçmişi yeniden çek.
        ws.send(JSON.stringify({op: "candles", symbol: "GOLD", tf: "M1", count: 500}));
        break;

      case "error":
        console.error("sunucu:", m.msg);
        break;
    }
  }
};
```

İşlem için ek olarak:

```javascript
ws.send(JSON.stringify({op: "auth", token: TOKEN}));       // -> {"t":"authed","level":"trader"}
ws.send(JSON.stringify({op: "subscribe", channels: ["order"]}));
ws.send(JSON.stringify({op: "order", id: "s-001", symbol: "GOLD",
                        side: "buy", type: "market", volume: 0.01}));

// Emri YALNIZCA bununla gerçekleşmiş say:
//   m.t === "order" && m.kind === "txn" && m.retcode === 10009
```


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

> ⚠️ **Çıplak `"tick"` SESSİZCE yok sayılır.** Kanal adı `tick.<SEMBOL>` veya
> `tick.*` biçiminde olmak zorundadır. Tanınmayan kanal adı hata döndürmez,
> sadece hiç veri gelmez (`server.rs:496-519`). Bu ölçüm yapılırken bize de
> oldu: `{"channels":["tick"]}` gönderildi, 20 saniye sessizlik, "piyasa
> kapalı" sanıldı — piyasa açıktı.
>
> `ms` alanı ölçüldü: **UTC + 10800 sn, tam UTC+3** (2026-08-13). `Bar.t` ile
> aynı tabandadır. Doğrudan "epoch UTC" sanıp çevirirseniz 3 saat kayarsınız;
> broker DST uygularsa fark mevsimlik değişir, sabit varsaymayın.

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

**Ölçüldü (2026-08-13, GOLD):** `hist: "ok"` ile taze çekilen barda `close`,
o andaki `bid` ile **birebir eşit** (fark 0.00). `src_kind: "mt5"` barları
GOLD'da bid tabanlıdır. Ayrıntı ve yöntem: [docs/OLCUMLER.md](docs/OLCUMLER.md).

> Karşılaştırma yaparken `hist: "cached"` cevabını kullanmayın — bar depodan
> gelir ve o arada fiyat kaymış olur; ölçüm kirlenir, seri değil.

### 🔴 Backtest ile canlı AYNI fiyat tabanında DEĞİL

Bu, mumları kullanan her strateji için en önemli maddedir:

| kip | `src_kind` | fiyat tabanı |
|---|---|---|
| canlı | `mt5` | **BID** |
| replay / backtest | `tick` | **MID** |

Replay'de MT5 geçmişi **yoktur**, bu yüzden `src_kind` daima `"tick"` ve `hist`
daima `"off"`tur. Yani `candles`'a dayanan bir backtest MID barlarla, aynı
stratejinin canlı hâli BID barlarla çalışır. Her işlemde **yarım spread**
kadar sistematik fark demektir — GOLD'da ~28 point.

**Çözüm — istemci tarafında, bugün:** replay'de mumu `candles`'tan almayın;
tick akışındaki **`b` (bid)** alanından kendiniz kurun. Replay tick'leri bid ve
ask'i ayrı ayrı taşır, yani bid barı üretmek için gereken her şey kayıtta
mevcuttur. Böylece backtest de canlı da BID olur ve taban belirsizliği kalkar.

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
{"op":"order","id":"benim-6","symbol":"GOLD","side":"buy","type":"market","volume":0.01,"sl":4366.85,"tp":4381.85}
```

`id` **idempotency anahtarıdır** — aynı `id` ikinci kez gelirse `duplicate`
döner ve emir GÖNDERİLMEZ. Çift pozisyon açılmaz.

### 🔴 `cancel` DOLMUŞ emri geri almaz — "0 bekleyen emir" temizlik kanıtı DEĞİL

Bekleyen emirleri toplu iptal edip sonra `{"op":"orders"}` ile "kalan: 0"
görmek, **hiçbir şeyin açık kalmadığı anlamına gelmez.** Dolan emir artık
bekleyen emir değildir — **pozisyondur** ve `cancel` ona dokunmaz.

```
kalan bekleyen emir: 0
```

Bu çıktı iki farklı dünyayla uyumludur:
- hepsi iptal edildi ✔
- **hepsi doldu ve pozisyona döndü** ✖

Yani başarıyı başarısızlıktan ayıramaz.

**Bu gerçek bir olaydır, uydurma bir uyarı değil.** `stops_level` ölçümü
sırasında piyasaya 4–60 point mesafede 57 `buy_limit` kuruldu; GOLD'da o
mesafedeki limitler saniyeler içinde doldu. Temizlik yalnızca `cancel`
gönderdi, "0 bekleyen emir" görüp tamam sayıldı — geriye **42 korumasız
pozisyon** kaldı ve saatlerce fark edilmedi. Üstelik başka bir sisteme ait
sanılıp yanlış teşhis yazıldı.

**Doğru temizlik iki adımlıdır:**

```json
{"op":"orders"}      → kalan bekleyen emirler   (cancel ile temizlenir)
{"op":"positions"}   → DOLMUŞ olanlar           (close ile temizlenir)
```

İkisini de sorgulamadan "temizlendi" demeyin. Piyasaya yakın `pending` emirle
deney yapan her istemci için geçerlidir.

### 🛡️ Stop'u EMİRLE BİRLİKTE gönderin — korumasız pencere hiç doğmasın

`order` işlemi **`sl` ve `tp` alanlarını kabul eder** (`0` = konmadı). MT5 emri
ve stop'u **tek istekte** işler; pozisyon **SL ile doğar**.

**Canlıda ölçüldü (2026-08-13, GOLD):**

```json
{"op":"order",...,"volume":0.01,"sl":4366.85,"tp":4381.85}
→ positions: ticket=945733772 sl=4366.85 tp=4381.85
```

İki adımlı yolda (`order` → dolum bekle → bileti öğren → `modify_sltp`)
pozisyon, dolum olayı gelene kadar **korumasızdır**. Köprü tam o aralıkta
zombileşirse pozisyon saatlerce SL'siz kalır — tüketici sistemin bildirdiği
"11 pozisyon 10+ saat korumasız" olayı bu penceredir. Emirle birlikte
gönderim bu pencereyi **tamamen kapatır**.

Bilinmesi gerekenler:

- Stop geçersizse (`stops_level` ihlali) MT5 **tüm isteği** `10016` ile
  reddeder: korumasız pozisyon **oluşmaz**. İki adımlı yolda aynı hata
  pozisyon **açıkken** olur.
- ⚠️ **Bu yol için `sltp_unverified` doğrulaması KURULMAZ.** Doğrulama defteri
  yalnızca `modify_sltp` komutunda devreye girer. Emirle birlikte SL
  gönderdiyseniz dolumdan sonra `positions[].sl` değerini **kendiniz okuyun**.
- Stop'u sonradan **değiştirmek** için (trailing vb.) yine `modify_sltp`
  kullanılır; o yolda doğrulama devrededir.

### ⏱️ Süreli emir: `expire_sn` kullanın, `expiration` bir tuzaktır

```json
{"op":"order","id":"x-1","action":"pending","type":"buy_limit",
 "symbol":"GOLD","volume":0.01,"price":4331.49,"expire_sn":120}
```

`expire_sn` = **kaç saniye sonra düşsün**. `time` vermenize gerek yok,
`specified` varsayılır. Saat dilimi hesabını köprü yapar.

#### Neden ham `expiration` tehlikeli — ÖLÇÜLDÜ

MT5 `expiration` alanını **broker sunucu saati** sayar. Gerçek UTC epoch
göndermek (sunucu UTC+3):

| gönderilen | sonuç |
|---|---|
| `UTC + 120 sn` | **`retcode 10022`** — emir hiç kurulmaz |
| `UTC + 1 gün` | **kabul edilir ama 3 saat ERKEN dolar**, sessizce |

İkincisi gürültü çıkarmadığı için tehlikelidir. `expire_sn` bu sınıfı tamamen
ortadan kaldırır.

#### Artık sessizce yok sayılmıyor

Aşağıdakiler **açık hatayla reddedilir** (eskiden `expiration` sessizce düşer,
emir sonsuza kadar beklerdi):

- `expiration` verilip `time` verilmemesi
- `expire_sn` ile `expiration`ın birlikte gönderilmesi
- `expire_sn` ile `time: "gtc"`/`"day"` çelişkisi

#### Süre DAKİKAYA yuvarlanır — istenenden fazla olur, asla az olmaz

MT5 son kullanmayı dakika sınırına kırpar. Aşağı yuvarlamak istenenden **az**
süre vermek olurdu; köprü **yukarı** yuvarlar.

`expire_sn: 120` için ölçülen gerçek ömür: **151 sn** (120 ≤ ömür < 180).
Kesin saniye gerekiyorsa emri kendiniz iptal edin; `expire_sn` bir **taban**
garantisidir, kesin süre değil.

> Ham `expiration` hâlâ kabul ediliyor ama **broker saatinde** verilmeli ve
> `time: "specified"` şart. Yeni kodda kullanmayın.

Süre dolduğunda **`{"t":"order","kind":"expired"}` gelir** ve biletin
düştüğünü anlarsınız. Ayrıca `{"op":"orders"}` ile de doğrulayabilirsiniz.

### ⚠️ `expired` olayında `retcode` 10009 OLABİLİR — bu dolum DEĞİLDİR

MT5 süre dolumunu ayrı bir olay türü olarak bildirmez; dolan bir emrin
listeden düşmesiyle **aynı** işlemi (`txn_type: 2`, `ORDER_DELETE`) üretir
ve `retcode` hâlâ 10009 (`DONE`) gelebilir — çünkü emrin *yerleştirilmesi*
başarılıydı, yürütülmesi değil. İkisini ayıran tek alan `order_state`
(`6` = `EXPIRED`) ve köprü bu ayrımı sizin için `kind` alanına yansıtır.

**Yalnızca `retcode`a bakan bir istemci, süresi dolmuş emri dolmuş sanar**
ve var olmayan bir pozisyonu yönetmeye çalışır.

### Emir sonucu iki aşamalıdır — karıştırmayın

```json
{"t":"order","id":"benim-1","kind":"queued"}
{"t":"order","id":"benim-1","kind":"ack","retcode":10008}
{"t":"order","id":"benim-1","kind":"txn","retcode":10009}
{"t":"order","id":"benim-1","kind":"txn","order":942649399,"deal":931503131,"position":942649399,"volume":0.01,"price":4367.95,"bid":4367.60,"ask":4367.90,"order_state":4,"txn_type":6}
```

- `kind: "ack"` + `retcode: 10008` → istek sunucuya iletildi. **DOLMADI.**
- `kind: "txn"` + `retcode: 10009` → gerçekten yürütüldü.

Emri yalnızca ikincisiyle "gerçekleşti" sayın.

`kind` değerleri: `queued` · `ack` · `txn` · `expired` · `rejected` ·
`duplicate` · `sltp_unverified`.

### 🛡️ Stop broker tarafında, köprü stop'u YEDEK — ikisi de dursun

Bu bir tercih değil, **kural**:

| Katman | Rol |
|---|---|
| `modify_sltp` ile broker'a yazılan SL | **ASIL koruma.** MT5 sunucusu tutar; `sinyald` ölse, VPS kapansa, ağ kopsa bile çalışır. |
| İstemcinin kendi stop mantığı | **YEDEK.** Kaldırmayın. |

**Bu iddia canlıda ÖLÇÜLDÜ (2026-08-13, GOLD).** Pozisyon açıldı, SL kondu,
`sinyald` süreci `Stop-Process -Force` ile **öldürüldü**, 15 sn beklendi,
yeniden başlatıldı:

```
SL kur              -> sl=4369.28
sinyald ÖLDÜR       -> süreç yok
yeniden başlat      -> sl=4369.28   (değişmedi)
```

Stop gerçekten broker tarafındadır. Yöntem ve ham çıktı:
[docs/OLCUMLER.md](docs/OLCUMLER.md#6-stop-gerçekten-broker-tarafında-mı-tüketicinin-asıl-şikâyeti)

Köprüdeki stop **tek başına yeterli değildir**: köprü zombi kalırsa
(bağlantı ayakta görünür ama hiçbir şey işlemez) pozisyonlar saatlerce
kapanmadan durur. Bu gerçekten yaşandı: köprü 8 saat zombi kaldı, 11
pozisyon 6 saatlik sınırı aştığı hâlde 10+ saat açık kaldı.

Broker stop'u da **tek başına yeterli değildir**: aşağıya bakın.

**Çift tetiklenme zararsızdır.** İkisi aynı anda tetiklenirse ikinci
kapatma "pozisyon yok" hatası alır. Bu beklenen davranıştır — ama
loglayın, sessiz geçmesin.

### ⚠️ `modify_sltp`in KABUL edilmesi, stop'un KURULDUĞU anlamına gelmez

Broker SL'i sessizce düşürebilir: `stops_level` / `freeze_level` ihlali,
requote, ya da "invalid stops" (`10016`). Pozisyon o an **korumasız**
kalır ve emir cevabına bakan bir istemci bunu göremez.

Bu yüzden köprü komuttan sonra **3 saniye** boyunca durum yayınındaki
`positions[].sl` değerini izler. İstenen değere ulaşmazsa:

```json
{"t":"order","id":"benim-5","kind":"sltp_unverified","ticket":942649399,
 "istenen_sl":4300.0,"gercek_sl":0.0,"state_age_ms":840,
 "comment":"durum yayinindaki sl istenen degere ulasmadi — broker stop'u uygulamamis olabilir"}
```

> ⚠️ **`kind` alanına bakın, `t` alanına değil.** Bu uyarı `t: "order"`
> mesajının içinde `kind: "sltp_unverified"` olarak gelir — `t` alanı
> `"sltp_unverified"` **olmaz**. Ölçüm yapılırken bu hataya biz düştük:
> uyarı gelmişti, filtre `t`ye baktığı için "gelmedi" sanıldı.

Canlıda doğrulandı: LONG pozisyona fiyatın **üstünde** SL gönderildiğinde
broker `retcode 10016 "Invalid stops"` döndü ve uyarı **geldi**
(`istenen_sl:4428.62, gercek_sl:0.0, state_age_ms:223`). Geçersiz deneme,
önceden kurulmuş geçerli SL'i **bozmadı**.

- **Bu bir HATA DEĞİL, bir UYARIDIR.** Emir kabul edilmiş olabilir;
  söylenen tek şey, kurulduğunun **doğrulanamadığı**dır.
- Gördüğünüzde: kendi yedek stop'unuzu **devrede tutun** ve komutu farklı
  bir seviyeyle (`stops_level` dışında) tekrarlamayı düşünün.
- `retcode` **yoktur** — bu bizim gözlemimiz, broker cevabı değil. Uydurma
  bir kod koymak, olayı emir sonucu sanmanıza yol açardı.
- **Pozisyon KAPANDIYSA uyarı GELMEZ.** Kapanmış pozisyonun stop'u da
  yoktur; bu tamamen normaldir ve stop'a değerek kapanan her pozisyonda
  tekrarlanırdı. Buna alarm vermek `sltp_unverified`in tamamını gürültüye
  çevirirdi. Karar, doğrulama penceresinin dolduğu **an** verilir:
  pozisyon o an hâlâ açık ve stop'suzsa uyarı gelir; artık yoksa gelmez.
- `gercek_sl` **yoksa** pozisyonu hiç göremedik demektir — durum görüntüsü
  **bayat** (>5 sn), kırpılmış, ya da hiç gelmemiş. `0` gönderilmez: "stop
  yok" ile "bakamadık" aynı şey değildir. Bu uyarının işaret ettiği yer
  broker değil, **köprüdür**.
- `state_age_ms` **büyükse suç broker'da olmayabilir** — zombi köprü durum
  görüntüsünü dondurur ve stop gerçekte kurulmuş olsa bile burada eski
  değer görünür. "Broker reddetti" ile "köprü ölmüş" arasındaki farkı
  ayırmanın tek yolu bu sayıdır.
- Doğrulama penceresi **dolmadan uyarı üretilmez**. Durum yayını saniyede
  bir tazeleniyor; erken bakıp alarm çalmak, doğru kurulmuş stop'ları da
  yanlış damgalardı.
- Broker fiyatı kendi `tick_size` ızgarasına yuvarlarsa bu **başarısızlık
  sayılmaz**; tolerans bir ızgara adımıdır.
- Yalnızca **SL** doğrulanır, TP doğrulanmaz. Bilinçli kapsam sınırı:
  doğrulanmayan bir TP kâr kaçırır, doğrulanmayan bir SL hesabı boşaltır.
- Uyarı `order` kanalından gelir — ayrı bir aboneliğe gerek yok.
- Sayaçlar daemon telemetrisine de düşer: `[live] stop: modify_sltp
  gonderilen=N dogrulanan=N dogrulanamayan=N kapanmis=N bekleyen=N`.
  `kapanmis` beklenmedik biçimde büyükse, stop komutlarınız pozisyonlar
  kapandıktan **sonra** gidiyor demektir.

### Giriş maliyetini ayırmak: `bid` / `ask`

Dolum olayı, dolumun VURDUĞU piyasayı taşır. `price` tek başına "ne
ödedim"i söyler ama "neden"i söylemez:

| Ölçmek istediğiniz | Hesap (alışta) |
|---|---|
| Spread maliyeti | `ask - bid` |
| Kayma (istenen fiyata ulaşılamadı) | `price - ask` |
| Toplam giriş maliyeti | `price - bid` |

Satışta yönler terstir: kayma `bid - price`.

- **Türetilmiş bir `spread` alanı BİLEREK yok.** `ask - bid`'i kendiniz
  hesaplayın; iki kaynak, biri güncellenmeyince sessizce tutarsızlaşırdı.
- Ölçüm yoksa alanlar **hiç gönderilmez** — `"bid":0` gelmez.
- Asıl kaynak `txn` olayıdır. `ack`te normalde bulunmazlar; istisna
  **requote** (`retcode` 10004) — orada MT5 kendi piyasa fiyatını döner ve
  o da gerçek bir ölçümdür. `queued` / `rejected` / `duplicate` köprünün
  kendi ürettiği olaylardır, alan asla bulunmaz.
- **Paper/replay kipinde bu alanlar HİÇ gelmez.** Simülatör dolumu tek
  fiyattan modelliyor; uydurma bir spread üretmek, kayma analizini
  gerçek ölçümle karıştırırdı.
- Bu değerler EA'nın **dolum anındaki** tick önbelleğinden okunur. Sonradan
  `tick` akışından eşleştirmek aynı şey değildir: aradaki milisaniyelerde
  piyasa değişir ve ölçtüğünüz kayma gerçekte olmayan bir şey olur.

#### Ölçülen ayrışma (GOLD, 0.01 lot, 2026-08-13)

GOLD'da `contract_size=100`, `point=0.01` → 0.01 lot için **1$ = 100 point**.

| bileşen | ölçülen | point |
|---|---|---|
| spread | 0.57$ | 57 |
| **dolum kayması** | **0.03$** | **3** |
| kalan — *ulaşılamaz fiyat* | ~0.25$ | ~25 |

Maliyetin büyük kısmı **kayma değil**: motorun verdiği fiyat ile piyasanın
farkı. İkisi ayrı şeydir çünkü müdahale aracı ayrıdır:

- **kayma** → `deviation` alanıyla sınırlanabilir
- **ulaşılamaz fiyat** → `deviation` işe yaramaz; yalnızca **emir tipiyle**
  (LIMIT) müdahale edilebilir

> Daha önce bu belge üzerinden "kayma 27 point" bilgisi verildiyse **yanlıştı**;
> ölçüm 3 point gösterdi. Düzeltmenin gerekçesi:
> [docs/OLCUMLER.md](docs/OLCUMLER.md#2-giriş-maliyetinin-ayrışması).

**Simülatör bu bileşeni hiç modellemiyor** — paper/replay defteri bu yüzden
sistematik olarak iyimserdir.

### Ham MT5 durumu: `order_state` / `txn_type`

`kind` özet bir etikettir; ham MT5 enum değerleri de geçirilir.

- `order_state` — `ENUM_ORDER_STATE`. **`3` = `PARTIAL`**: emir KISMEN
  doldu, kalan hacim hâlâ piyasada. `kind` bunu ayırt etmez (ikisi de
  `txn`). Diğerleri: `1` `PLACED`, `2` `CANCELED`, `4` `FILLED`,
  `5` `REJECTED`, `6` `EXPIRED`.
- `txn_type` — `ENUM_TRADE_TRANSACTION_TYPE`. `2` = `ORDER_DELETE`,
  `6` = `DEAL_ADD`, `10` = `REQUEST`. **Sıra sezgisel değildir**:
  `HISTORY_*` 3-5, `DEAL_*` 6-8 — yani `DEAL_*` `HISTORY_*`'tan SONRA
  gelir.

Her ikisinde de `0` "ölçüm yok" ile aynı sayıya denk düştüğü için
gönderilmez.

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

## 5b. Muhtemelen aradığınız ve ZATEN VAR olan şeyler

Entegre olan sistemler bunları eksik sanıp kendi tarafında yeniden yazmaya
kalkıyor. Hepsi mevcut:

| İhtiyaç | İşlem / alan |
|---|---|
| Stop'u **broker tarafına** taşımak | `{"op":"modify_sltp","id":"..","ticket":N,"sl":..,"tp":..}` |
| Açık pozisyonun SL/TP'si | `positions[].sl`, `.tp` |
| Açık pozisyonun **yüzen** kârı ve swap'ı | `positions[].profit`, `.swap` |
| LIMIT / STOP emir | `action:"pending"` + `type:"buy_limit"` … |
| Bekleyen emri iptal | `{"op":"cancel","id":"..","ticket":N}` |
| Bekleyen emrin **kalan** hacmi | `orders[].volume_initial` − `.volume_current` |
| Dolum anındaki **spread ve kayma** | `order` olayındaki `bid` / `ask` |
| **Kısmi** dolumu tam dolumdan ayırmak | `order` olayındaki `order_state` (`3` = `PARTIAL`) |
| Bekleyen emrin süresinin dolduğu | `{"t":"order","kind":"expired"}` |
| Broker SL'i gerçekten kurdu mu | `{"t":"order","kind":"sltp_unverified"}` uyarısı |
| Stop'u **emirle birlikte** koymak (korumasız pencere yok) | `order` içindeki `sl` / `tp` |
| Açık pozisyonun stop'unu **sonradan** değiştirmek | `{"op":"modify_sltp","id":"..","ticket":N,"sl":..,"tp":..}` |

> **Stop'u köprüde tutmayın — ama köprü stop'unu da KALDIRMAYIN.**
> SL'i tercihen **emirle birlikte** (`order` içindeki `sl`), değiştirirken
> `modify_sltp` ile broker'a yazın: MT5 sunucusu tutar ve `sinyald` ölse bile
> korur. Köprüdeki stop **yedek** olarak kalsın. Broker SL'i reddedebilir;
> `modify_sltp` yolunda o durumda `sltp_unverified` gelir ve yedeğiniz tek
> koruma olur. Ayrıntı: §4, "Stop broker tarafında, köprü stop'u YEDEK".

### Henüz OLMAYAN ve bilmeniz gerekenler

| Eksik | Ne yapmalı |
|---|---|
| Kapanış `txn`'inde **gerçekleşmiş** kâr/komisyon/swap | Şimdilik dolum fiyatlarından hesaplayın |
| Simülatörde komisyon/swap | Paper/replay PnL'i **iyimserdir** |
| Paper/replay'de dolum anı `bid`/`ask` | Simülatör ölçmez ve **uydurmaz**; alan hiç gelmez |

---

## 6. Sadakat — sinyal sisteminin bilmesi ZORUNLU olanlar

Bunlar hata değil, **sistemin doğası**. Bilmeden yazılan bir strateji canlıda
beklediğinden farklı davranır.

### Kayıpsız tick YOKTUR

MQL5 dokümanı birebir: *"In case when OnTick function for the previous quote is
being processed when a new quote is received, the new quote will be ignored."*
Bu MT5'in kendi sınırıdır, bizim değil. İki ayrı kayıp vardır ve
**karıştırılmamalıdır**:

- **Halka kaybı** — bizim tarafımız yetişemedi. Ölçülüyor ve **0**.
- **Terminal kaynaklı atlama** — MT5 atladı. Ölçülemiyor, garanti edilemiyor.

Pratikte: ölçülen en sık ardışık tick aralığı 72 ms, tarama periyodumuz 16 ms.
Sakin piyasada kayıp yok. **Volatil anlarda garanti yok.**

### Her sembolde her tick gelmez

`symbols` cevabındaki iki alan bunu söyler:

- `chart: true` → EA'nın bağlı olduğu grafik. **Olay güdümlü, her tick.**
- `polled_only: true` → 16 ms örnekleme. **Ara tickler görülmez.**

Şu an `chart: true` olan tek sembol **GOLD**. Strateji başka bir sembolde
çalışacaksa EA o sembole taşınmalıdır — aksi halde girdi eksiktir.

### Mum kaynağı ile canlı fiyat aynı tabanda değil

`src_kind: "mt5"` serisi broker'ın kendi barlarıdır (forex/CFD'de **bid**).
Tick akışı ise **bid ve ask'i ayrı ayrı** verir. Oluşan barı `mid` ile
kurarsan geçmişin bittiği yerde yarım spread kadar sahte bir basamak oluşur —
**GOLD'da 20-30 point**. Oluşan barı `b` (bid) ile kur.

### Zaman broker saatidir

`tick.ms` ve `Bar.t` alanları **broker sunucu saatidir** — yerel saat veya UTC
değil. Broker DST uygulayabilir. Bar sınırları bu saatten türetilir; kendi
saatinle yeniden hesaplama.

### Emir dolumu anlık değildir

`ack` (10008) "sunucuya iletildi" demek. `txn` (10009) "yürütüldü" demek.
Arada gerçek kayma olur. Ölçülen uçtan uca gecikme p50 ~250 µs **bizim
tarafımızda**; broker tarafındaki süre buna dahil değildir.

### GOLD'un GÜNLÜK İŞLEM MOLASI VAR — "sistem çöktü" sanma

Forex çiftleri hafta içi 7/24 akar, **metaller akmaz.** XM'de GOLD günde bir
kez ~1 saat kapanır (gözlenen: **00:00–01:00 civarı**, sunucu saati). O
aralıkta:

- `tick.GOLD` aboneliğine **hiç mesaj gelmez** — bağlantı sağlıklıdır
- `snapshot` son bilinen fiyatı verir ama **bayattır**
- Emir gönderirsen `kind:"txn"` + **`retcode: 10018`** (`MARKET_CLOSED`) döner

Aynı anda `tick.EURUSD` akmaya devam eder. Yani "hiç tick gelmiyor" ile
"bu sembolde tick gelmiyor" farklı şeylerdir.

Sinyal sistemi bunu şöyle ayırt etmeli:

```javascript
// Akış sağlıklı mı? BAŞKA bir sembole bak, GOLD'a değil.
// GOLD sessizse ve EURUSD akıyorsa → mola, arıza değil.
// İkisi de sessizse → gerçekten sorun var (EA durmuş, terminal kapanmış).
```

Emir tarafında `10018` **geçici** bir durumdur; yeniden denemek yerine
piyasa açılana kadar beklenmelidir.

### `id: ""` gelen olaylar

Bize ait olmayan işlemler — terminalden elle yapılmış. Olay **asla atılmaz**,
kimliksiz yayımlanır. Sinyal sistemi bunları kendi emri sanmamalı ama yok da
saymamalı: hesabın durumunu değiştirmişlerdir.

---

## 7. Backtest — 3 aylık gerçek tick verisi HAZIR

Beklemeye gerek yok. Broker'ın tick geçmişi geri çekildi:

```
26.378.012 tick     13 Mayıs → 13 Ağustos 2026     67 gün     1,18 GB
```

Sayı **diskteki dosyalardan** doğrulandı (2026-08-13) ve canlı kayıt sürdüğü
için her gün artar. Tüm dosyalar 48'e tam bölünüyor, eksik sembol tablosu yok.

Bunlar **gerçek broker tick'leri** — bardan türetilmiş sentetik veri değil.
Gerçek spread ve gerçek tick zamanlaması korunuyor.

```bash
sinyald --replay ./veri --replay-date 20260513-20260812 \
        --replay-speed 1000 --bind 127.0.0.1:8788 --capacity 262144
```

Bağlanın: `ws://127.0.0.1:8788` — token istemez.

### 🔴 `--replay-speed 0` KULLANMAYIN — verinin %99'unu düşürür

Ölçüldü (tek gün, 541.771 tick, `--capacity 262144`):

| `--replay-speed` | ulaşan | düşen | süre |
|---|---|---|---|
| `0` | 4.968 | **649.416** | 1,1 sn |
| `5000` | 346.045 | 205.792 | 17,4 sn |
| **`1000`** | **541.771** | **0** | 86,4 sn |

Darboğaz **istemci değil, üretici**: hiç işlem yapmayan, geleni doğrudan diske
yazan bir istemciyle de aynı kayıp ölçüldü. `speed 0`'da üretici hiç
frenlenmiyor.

Üç şey **zorunlu**:

1. **`--replay-speed 1000`** kullanın (gün başına ~86 sn).
2. **`lagged` mesajlarını sayın.** Bir tane bile gelirse koşumu iptal edin —
   `{"t":"lagged","dropped":N}`.
3. **`replay_done.ticks`e güvenmeyin.** O alan *oynatılan* tick'i söyler,
   *ulaşan*ı değil. `speed 0` turunda istemci 4.968 tick aldı ve `replay_done`
   yine `"ticks":541771` dedi.

> **Replay süreç başına BİR KEZ oynatılır.** İlk istemciyi bekler, oynatır,
> biter. Sonradan bağlanan istemci **sıfır tick** alır ama yine dolu bir
> `replay_done` görür. Yeni koşum için süreci yeniden başlatın.

**Aynı protokol.** Tek fark `hello.mode = "replay"`. Kod yolunuz değişmez;
yalnızca bağlandığınız port değişir.

| | |
|---|---|
| `--replay-speed 0` | Beklemeden en hızlı. 66 gün dakikalar sürer. |
| `--replay-speed 1` | Gerçek zamanlı |
| `--replay-from/to` | `09:00`/`17:00` — **her güne ayrı ayrı** uygulanır |
| `replay_done` | Yalnızca **aralığın sonunda**. Gün geçişinde bağlantı kopmaz. |

Tek bağlantıda 66 gün akar; gün sınırında durumunuz sıfırlanmaz, yani
**gün-aşırı pozisyon taşıma da test edilir**.

### Replay'de neyin farklı olduğunu bilin

**MT5 mum geçmişi YOK.** `candles` cevabı `src_kind: "tick"` ve
`hist: "off"` döner; mumlar yalnızca kayıttaki tick'lerden üretilir.

**Simülatör iyimser.** Komisyon, swap, requote ve kısmi dolum modellenmiyor;
kayma varsayılanı gerçekte ölçülenden düşük. Yani replay PnL'i canlıdan
**daha iyi** çıkar. Bar tabanlı bir testin gerçek yürütmede tersine dönmesi
bu yüzden olur — replay tick tabanlı olduğu için çok daha yakın, ama
**birebir değil**.

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
