# Ölçümler — tahmin değil, sayılar

Bu dosya **canlı demo hesapta ölçülmüş** değerleri tutar. Amacı tek: sinyal
sistemine varsayım göndermemek. Her madde nasıl ölçüldüğünü yazar ki
tekrarlanabilsin ve şartlar değişince yeniden ölçülebilsin.

**Kural:** buraya yalnızca ölçülmüş sayı girer. Tahmin, hatırlanan değer veya
"muhtemelen" yazılmaz. Bir madde yanlış çıkarsa **silinmez, düzeltilir ve
düzeltildiği söylenir** — yanlış sayı sessizce kaybolursa ona dayanan kararlar
öksüz kalır.

Ortam: XM demo, `GOLD`, sunucu saati UTC+3, 0.01 lot.
Ölçüm tarihi: **2026-08-13**.

---

## 1. Mumlar BID mi, MID mi? (sinyal sisteminin sorduğu soru)

**Cevap: kaynağına göre değişir ve cevabın içindeki `src_kind` alanı hangisi
olduğunu söyler.** Tek bir cevap yok; bu yüzden alan var.

| `src_kind` | Fiyat tabanı | Nereden |
|---|---|---|
| `"mt5"` | **BID** | broker'ın `CopyRates` serisi |
| `"tick"` | **MID** `(bid+ask)/2` | bizim tick'ten ürettiğimiz seri (`candles.rs:242-248`) |

### GOLD ölçümü — `src_kind: "mt5"` gerçekten BID

Yöntem: oluşmakta olan M1 barının `close`'u ile aynı andaki tick'in
`bid`/`mid`/`ask` değerleri karşılaştırıldı, 6 tur.

Kritik ayrım **`hist` alanı**: `hist: "ok"` = bar MT5'ten **o an** çekildi,
`hist: "cached"` = depodaki görüntü kullanıldı ve bu arada fiyat kaymış
olabilir. Yalnızca `ok` turları temiz ölçümdür.

```
tur 2: hist=ok      bid=4371.48  close=4371.48   -> fark 0.00
tur 5: hist=ok      bid=4371.19  close=4371.19   -> fark 0.00
tur 1,3,4,6: hist=cached — bar bayat, fiyat kaymış, ölçüm kirli
```

**Taze çekilen barda `close` ile `bid` BİREBİR eşit.** GOLD'da `src_kind:"mt5"`
barları bid tabanlıdır. EURUSD'de de aynı sonuç (|c−bid| = 1 point, spread 19
point).

### ⚠️ Bunun backtest'e etkisi — en önemli madde

**Replay kipinde `src_kind` DAİMA `"tick"`tir, yani MID** (`replay.rs:119-124`;
MT5 geçmişi replay'de yoktur, `hist` daima `"off"`).

Yani bugünkü haliyle:

| kip | mum kaynağı | fiyat tabanı |
|---|---|---|
| canlı | `mt5` | **BID** |
| replay / backtest | `tick` | **MID** |

**Backtest ile canlı farklı fiyat tabanında çalışıyor.** Sinyal sisteminin
senaryo tablosundaki 1 ile 2 arasındaki fark varsayımsal değil; şu an ikisi
aynı anda gerçek — biri canlıda, biri backtest'te.

**Çözüm (istemci tarafında, bugün yapılabilir):** replay'de mumu `candles`
cevabından alma; tick akışındaki **`b` (bid)** alanından kendin kur. Replay
tick'leri bid ve ask'i ayrı ayrı taşır, yani bid barı üretmek için gereken her
şey kayıtta var. O zaman backtest de canlı da BID olur ve senaryo belirsizliği
ortadan kalkar.

---

## 2. Giriş maliyetinin ayrışması

Yöntem: `order` olayına dolum anındaki `bid`/`ask` eklendi (EA'da önbelleklenmiş
`g_last_bid[]`/`g_last_ask[]`, sıcak yolda `SymbolInfoTick` çağrılmadan).
Piyasa alışı, 0.01 GOLD.

```
snapshot GOLD  bid=4378.02  ask=4378.58  spread=0.56
BUY 0.01 GOLD
   txn ret=10009  px=4378.28   (bid/ask henüz yok — ilk aşama)
   txn ret=0      px=4378.28   bid=4377.68  ask=4378.25
      -> spread = 0.57   kayma = 0.03
```

GOLD'da `contract_size=100`, `point=0.01` → 0.01 lot için **1$ = 100 point**.

| bileşen | ölçülen | point |
|---|---|---|
| spread | 0.57$ | 57 |
| **dolum kayması** | **0.03$** | **3** |
| kalan — *ulaşılamaz fiyat* | ~0.25$ | ~25 |

### ⚠️ DÜZELTME — daha önce yanlış söylendi

Bu ölçümden **önce** "kayma 27 point, simülatör 27 kat iyimser" denmişti.
**Yanlıştı.** Gerçek dolum kayması **3 point**. Kalan ~25 point kayma değil,
motorun verdiği fiyat ile piyasanın farkı — **ayrı bir bileşen**.

Ayrım pratikte önemli çünkü iki bileşene farklı araçla müdahale edilir:

- **kayma** → `deviation` alanıyla sınırlandırılabilir
- **ulaşılamaz fiyat** → `deviation` işe yaramaz; yalnızca **emir tipiyle**
  (LIMIT) müdahale edilebilir

Bu yüzden LIMIT emir tezi artık tahmin değil, ölçüme dayanıyor.

### Simülatörde durum

`sim.rs::DEFAULT_SLIPPAGE_POINTS = 1.0` → ölçülen 3. Küçük mesele.

**Asıl boşluk:** simülatör *ulaşılamaz fiyat* bileşenini **hiç modellemiyor**.
Paper/replay defterindeki rakamlar bu yüzden sistematik olarak iyimser. Sinyal
sisteminin bildirdiği `fvg_scan` sapması (bar-backtest +7.10$ / gerçek −4.87$)
tam bu bileşendir.

---

## 3. Zaman tabanı — tam UTC+3

Yöntem: 8 tick'in `ms` alanı yerel UTC saatiyle karşılaştırıldı.

```
TICK zamani - UTC farki: 10800 sn  (tam 3 saat)
```

`tick.ms` ve `Bar.t` **aynı tabanda**: broker sunucu saati, epoch ms olarak
kodlanmış. İkisi kendi aralarında tutarlıdır.

**Tuzak:** bu değerleri doğrudan "epoch UTC" sanıp çevirirsen **3 saat**
kayarsın. Broker DST uygularsa fark mevsimlik değişir — sabit 3 varsayma,
gerekiyorsa yeniden ölç.

---

## 4. `subscribe` sessizce yok sayıyor

Kanal adı `tick.GOLD` veya `tick.*` biçimindedir. **Çıplak `"tick"` sessizce
yok sayılır** — hata dönmez, sadece hiç veri gelmez (`server.rs:496-519`,
test `server.rs:2418`).

Bu ölçüm sırasında bana da oldu: `{"channels":["tick"]}` gönderdim, 20 saniye
hiçbir şey gelmedi ve "piyasa kapalı" sandım. Piyasa açıktı.

Aynı sessiz-yok-sayma ailesinden:
- `candle.GOLD.*` çalışmaz (joker yalnız sembol yerinde geçerli)
- `expiration` tek başına yok sayılır (`"time":"specified"` şart)

---

## 5. Geçmiş veri stoğu

| | |
|---|---|
| tick sayısı | 25.426.116 |
| gün | 92/92 |
| boyut | 1.14 GB |
| indirme süresi | 15.8 sn |
| broker tick derinliği (ölçülen) | 128 gün → 2026-04-07 |

Ölçüm yöntemi: `SinyalBackfill` servisi üstel + ikili aramayla broker'ın gerçek
tick derinliğini ölçer, sonra günü gün indirir.

İçe aktarma çakışan günleri **birleştirir**, üzerine yazmaz. Broker saati ile
UTC gün sınırı farkından doğan 16.904 yinelenen kayıt atıldı.

---

---

## 6. Stop gerçekten broker tarafında mı? (tüketicinin asıl şikâyeti)

Tüketici sistem şunu bildirmişti: *"köprü 8 saat zombi kaldı, 11 pozisyon 10+
saat korumasız kaldı."* Bunun tek doğru testi köprüyü **gerçekten öldürmektir**.

Yöntem: 0.01 GOLD alındı → `modify_sltp` ile SL kondu → `positions`'tan
doğrulandı → **`sinyald` süreci `Stop-Process -Force` ile öldürüldü** → 15 sn
beklendi → köprü yeniden başlatıldı → pozisyon tekrar soruldu.

```
ticket = 945682242
2) SL kur          -> positions: sl=4369.28 tp=4384.28      [GEÇTİ]
4) sinyald ÖLDÜR   -> süreç yok
5) yeniden başlat  -> ticket=945682242 sl=4369.28 tp=4384.28 [GEÇTİ]
6) temizlik        -> pozisyon kapatıldı
```

**SL, köprü ölüp yeniden doğduktan sonra değişmeden duruyor.** Stop broker
tarafındadır; köprünün ölmesi pozisyonu korumasız bırakmaz.

### Broker stop'u REDDEDERSE sessiz kalmıyor

LONG pozisyona bilerek geçersiz SL (fiyatın **üstünde**) gönderildi:

```json
{"t":"order","kind":"ack","retcode":10016,"comment":"Invalid stops"}
{"t":"order","kind":"sltp_unverified","ticket":945688906,
 "istenen_sl":4428.62,"gercek_sl":0.0,"state_age_ms":223,
 "comment":"durum yayinindaki sl istenen degere ulasmadi - broker stop'u uygulamamis olabilir"}
```

Uyarı **geldi**. Ayrıca geçersiz deneme, önceden kurulmuş geçerli SL'i
**bozmadı** (4369.28 olduğu gibi kaldı).

> **Kendi ölçüm betiğimdeki hata:** ilk turda "uyarı gelmedi" raporlandı. Uyarı
> gelmişti; betik `t` alanında `sltp_unverified` arıyordu, oysa uyarı
> `t:"order"` + `kind:"sltp_unverified"` biçiminde geliyor. Sistem değil,
> kontrol yanlıştı. Buraya yazılıyor çünkü aynı hatayı tüketici sistem de
> yapabilir: **`kind` alanına bakın, `t` alanına değil.**

---

## 7. Kayma kalibrasyonu — maliyet üçe ayrılıyor

Tüketici sistem toplam giriş maliyetini **85 point** ölçmüştü. Bizim tek
ölçümümüz spread 57 + kayma 3 = 60 veriyordu. 60 ≠ 85; fark nereden geliyor?

Doğru ayrıştırma (ALIŞ için):

```
toplam = fiyat − KARAR_ANI_bid
       = (fiyat − dolum_ask)       → KAYMA       (deviation ile sınırlanır)
       + (dolum_ask − dolum_bid)   → SPREAD      (broker'ın fiyatı)
       + (dolum_bid − karar_bid)   → SÜRÜKLENME  (karar ile dolum arasında
                                                  piyasanın hareketi)
```

### 1. tur — 10 örnek, GEÇERSİZ (yöntem hatası)

```
KAYMA       ort=−2.5   medyan=0
SPREAD      ort=56.2   medyan=57
SÜRÜKLENME  ort=28.5   medyan=18    min=−10  max=106
TOPLAM      ort=82.2   medyan=78
```

Toplam 82.2, tüketicinin 85'iyle örtüştü. **Ama bu tur geçersizdir:** "karar
anı" fiyatı `snapshot` ile alınıyordu ve toplama döngüsü yüzünden 4 saniyeye
kadar bayattı. Sürüklenme bu yüzden şişti. `gecikme_ms` değerleri de (~12000)
gerçek gecikme değil, betiğin kendi döngü süresiydi.

### 2. tur — sıkı karar anı, GEÇERLİ

Tick akışına abone olunup **en son gelen tick** karar fiyatı sayıldı ve emir
aynı anda gönderildi. Gerçek bir sinyal sisteminin davranışı budur.

```
KAYMA       ort=0      medyan=0     min=0    max=0
SPREAD      ort=54.6   medyan=55    min=54   max=55
SÜRÜKLENME  ort=−9     medyan=−2    min=−65  max=14
TOPLAM      ort=45.6   medyan=52    min=−11  max=69
GECİKME     ort=48 ms  medyan=48    min=39   max=58
```

**Sonuçlar:**

1. **Kayma 10/10 örnekte tam SIFIR.** `deviation` ayarıyla uğraşmanın bu
   broker'da GOLD için hiçbir getirisi yok.
2. **Spread çok kararlı**: 54–55 point, sapma yok. Modellenmesi kolay.
3. **Sürüklenme karar gecikmesine doğrudan bağlı.** 4 sn bayat kararla
   ort. +28.5 point, 48 ms'lik kararla ort. **−9 point** (yani hafif
   LEHTE). Maliyetin "ulaşılamaz fiyat" diye görünen kısmı büyük ölçüde
   **kendi karar gecikmenizdir**, brokerin kötülüğü değil.
4. Toplam giriş maliyeti 85 → **46 point**e düşüyor; neredeyse tamamı
   spread.

> **Bu, tüketici sistem için doğrudan eyleme dönüşür:** sinyal ile emir
> arasındaki gecikmeyi kısaltmak, `deviation` ayarlamaktan **kat kat**
> değerlidir. 4 saniyelik gecikme GOLD'da işlem başına ~28 point (0.28$)
> ödetiyor; 0.01 lotta brüt kârın üçte biri.

Simülatör için: `DEFAULT_SLIPPAGE_POINTS = 1.0` ölçülen **0**'a karşı zaten
muhafazakâr; asıl eksik **sürüklenme** bileşeni ve o, karar gecikmesinin
fonksiyonu — sabit bir sayı olarak modellenemez.

---

## 8. Koruma yolunun adversaryal denetimi

37 ajanlı salt-okunur kod denetimi: 5 arıza merceği (dolum–SL penceresi,
reddedilme yolları, bileşen ölümleri, kısmi/çoklu pozisyon, çifte tetik),
ardından her iddiayı **çürütmeye** çalışan ikinci tur. 32 iddiadan **9'u
çürütüldü**, 23'ü kaldı.

İkisi canlıda **bizzat doğrulandı**:

### 8a. `order` işlemi `sl`/`tp` alıyor — ve çalışıyor ✅

```json
{"op":"order",...,"volume":0.01,"sl":4366.85,"tp":4381.85}
→ positions: ticket=945733772 sl=4366.85 tp=4381.85
```

Yol kodda uçtan uca bağlıydı (`wire.rs:145-148` → `server.rs:1517-1518` →
`msg.rs:325-326` → `SinyalCollector.mq5:981-982`) ama **API.md'de hiç
yazmıyordu**. Tüketici bu yüzden zorunlu olarak iki adımlı, korumasız
pencereli yolu kullanıyordu. **Düzeltme kod değil, belge idi** — API.md'ye
eklendi.

Kalan uyarı: emirle gelen SL, `sltp_unverified` defterine **kaydolmuyor**
(`arm_sltp_verify` yalnızca `ClientMsg::ModifySltp` dalından çağrılıyor,
`server.rs:1175`). Bu yolu kullanan istemci `positions[].sl`i kendisi
okumalı.

### 8b. `symbol_id = 0` hatası — gerçek ama LATENT ⚠️

`submit_simple` (`server.rs:1432`) `..Default::default()` kullanıyor, yani
`modify_sltp` / `close` / `cancel` komutları **`symbol_id = 0`** ile gidiyor.
EA de `req.symbol = g_name[0]` yapıyor (`SinyalCollector.mq5:969-977`).

Ölçüm: tablodaki 0. sembol **AUDUSD**. EURUSD pozisyonuna `modify_sltp`
gönderildi → **çalıştı** (`sl=1.1519` kuruldu). Kapatma da çalıştı, üstelik
`close` yolunda `req.price` AUDUSD'nin fiyatından okunuyor
(`SinyalCollector.mq5:1067-1075`) — EURUSD pozisyonu için ~0.65 fiyat
gönderiliyor.

**Yani MT5, `position` bileti verildiğinde `symbol` ve `price` alanlarını
yok sayıyor.** Hata gerçek, bugün zarar vermiyor, ama **broker/MT5 davranışına
bağımlı** — başka bir yürütme kipinde sessizce kırılabilir. Düzeltilmeli.

### 8c. Denetimin çürüttükleri

9 iddia çürütüldü. Bunlar **sorun değil**, buraya not düşülüyor ki tekrar
gündeme gelince yeniden araştırılmasın: komut halkası dolduğunda sessiz düşme,
broker retcode'unun hiç yorumlanmaması, kısmi dolumda doğan ek pozisyonlar,
EA'daki "pozisyon yok" kapısının TOCTOU olması ve `id: ""` eşleştirme iddiası
bunlar arasında.

### 8d. Doğrulanmayı bekleyen kalan bulgular

23 bulgunun geri kalanı **kodda okundu, canlıda ölçülmedi**. Ana temalar:

- `state_age_ms` EA'nın yayın yaşını değil, köprünün **okuma** anını damgalıyor
  (`state.rs:141`) → zombi köprüde bayatlık ölçüsü yalan söyleyebilir
- Doğrulama defteri **tek atışlık**: 3 sn penceresinden sonra stop'un yerinde
  kaldığı bir daha kontrol edilmiyor (`server.rs:2158`)
- İstemci köprünün zombi olduğunu **anlayamıyor**: `ping` canlılık kanıtı
  değil, `positions` cevabında yaş alanı yok (`server.rs:1027`)
- Köprüde **yedek stop mantığı yok** — `SltpGuard` belgesi olduğunu ima ediyor,
  kodun geri kalanı "yedek istemcide" diyor (`server.rs:388`)
- Çok örnekli kurulumda komut **rastgele** bir örneğe gidiyor
  (`HashMap::keys().next()`, `server.rs:1429`)

Tam liste ve her bulgunun çürütme gerekçesi denetim çıktısında.

---

## Henüz ÖLÇÜLMEMİŞ — bunlara güvenme

Aşağıdakiler kodda yazılı ama **canlıda doğrulanmadı**. Sinyal sistemine
"çalışıyor" diye bildirilmemeli:

- `{"kind":"expired"}` olayının gerçekten üretilmesi
- `RECONCILE` yoluyla gerçekleşmiş `profit`/`commission`/`swap` — henüz yok
- Köprü **öldüğü sırada** gönderilen bir `modify_sltp`in akıbeti (yukarıdaki
  test köprüyü SL kurulduktan *sonra* öldürüyor; kurulmadan önce öldürseydi
  ne olurdu ölçülmedi)

Bu oturumda birim testleri geçen **iki** entegrasyon hatası ancak gerçek turda
yakalandı (kayıt→replay sembol şeması; `deploy.ps1`'in başarısız derlemeyi
başarı sayması). Birim testi burada yeterli kanıt değildir.
