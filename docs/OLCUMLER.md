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

## 9. Replay'i veri taşıma aracı olarak kullanmak

Tüketici sistem, 3 aylık tick verisini dosya kopyalamadan almak için replay
akışını "işleme" değil **"indirme"** aracı olarak kullanmayı önerdi: soketten
geleni hiç işlemeden diske yaz, sonra çevrimdışı koştur. Fikir doğru, ama
önerilen komut **verinin %99'unu kaybettiriyor**.

### Envanter (2026-08-13 ölçümü)

| | |
|---|---|
| dosya | **67** gün, `20260513` … `20260813` |
| tick | **26.378.012** |
| boyut | 1.266.144.576 bayt (1,18 GB) |
| 48'e bölünmeyen dosya | **YOK** — hepsi tam |
| sembol tablosu | her tick gününe karşılık `symbols-YYYYMMDD.jsonl`, **eksik yok** |

> **Belgelerdeki iki sayı da yanlıştı.** API.md 25.828.292 tick / 66 gün,
> OLCUMLER.md 25.426.116 tick / 92 gün diyordu. 92 sayısı geri-doldurmanın
> *denediği* takvim günü, 25,4 M ise *tekilleştirme öncesi* indirilen tick
> sayısıydı. Diskteki gerçek: **67 gün / 26.378.012 tick** — ve canlı kayıt
> sürdüğü için her gün artıyor.

### `symbol_id` günler arasında DEĞİŞMEMİŞ

`replay.rs:84-93` tabloyu her gün yeniden yüklüyor, yani değişebilir. 67 günün
tamamı tarandı: **tek bir eşleme var.**

```
0=AUDUSD 1=EURUSD 2=GBPUSD 3=GOLD 4=NZDUSD
5=USDCAD 6=USDCHF 7=USDCNH 8=USDJPY 9=USDSEK
```

Bu veri kümesi için `GOLD = 3` sabittir. Yine de gün başına doğrulamak
doğrudur; garanti protokolde değil, veride.

### ⚠️ `--replay-speed 0` verinin %99'unu düşürüyor

Hız taraması, tek gün (`20260812`, 541.771 tick), `--capacity 262144`:

| `--replay-speed` | ulaşan tick | `lagged` olayı | düşen | süre |
|---|---|---|---|---|
| **0** | 4.968 | 13.446 | 649.416 | 1,1 sn |
| 5000 | 346.045 | 307 | 205.792 | 17,4 sn |
| **1000** | **541.771** | **0** | **0** | **86,4 sn** |

**Darboğaz istemci değil, üretici.** Hiçbir işlem yapmayan, geleni doğrudan
diske yazan bir istemciyle de ölçüldü: `speed 0`'da yine 649.840 tick düştü.
"İşleme yapmazsam kanal dolmaz" varsayımı **yanlış** — `speed 0`'da üretici
hiç frenlenmiyor ve soket taşıyamıyor.

**Kayıpsız hız: `--replay-speed 1000`.** Gün başına ~86 sn → 67 gün ≈ 96 dk.

### ⚠️ `replay_done.ticks` ULAŞAN tick'i DEĞİL, OYNATILANI söyler

`speed 0` turunda istemci 4.968 tick aldı; `replay_done` yine şunu dedi:

```json
{"t":"replay_done","ticks":541771,"last_ms":1786589997937,"days":1,"days_played":1}
```

Yalnızca `replay_done`'a bakan bir istemci, 541.771 tick aldığını sanır.
**`lagged` sayılmadan bu akıştan çıkan hiçbir sonuç geçerli değildir.**
Tüketicinin "tek `lagged` gelirse koşumu iptal et" kararı doğrudur ve
zorunludur.

### ⚠️ Replay süreç başına BİR KEZ oynatılır

İlk istemciyi bekler, oynatır, biter. Oynatma bittikten **sonra** bağlanan
istemci **sıfır tick** alır ama yine `ticks: 541771` içeren bir `replay_done`
görür — ölçüldü.

Yani: **bir süreç = bir oynatma = bir istemci.** Yeni koşum için süreç
yeniden başlatılmalı.

### Şu an ayakta olan

```
PID 27816  CANLI   0.0.0.0:8787   (üretim — dokunulmadı)
PID 44396  REPLAY  0.0.0.0:8788   --replay-date 20260812 --replay-speed 1000
```

---

## 10. Kayıt dosyası `time_msc`'e göre SIRALI DEĞİL

Tüketici sistem bildirdi, doğrulandı ve **onların ölçtüğünden büyük çıktı**.
`ticks-20260812.bin`, 541.771 kayıt:

```
recv_ms  gore geri sicrama:        0     -> dosya recv_ms'e gore SIRALI
time_msc gore geri sicrama:   80.428     -> en buyuk sicrama TAM 3.00 saat
```

Onlar 31.543 demişti (yalnız GOLD); tüm sembollerde **80.428**.

Dosyada **iki kayıt grubu** var:

| grup | kayıt | oran | kaynak |
|---|---|---|---|
| `recv−time_msc = 0` | 189.300 | %34,9 (hepsi GOLD) | geri doldurma |
| `recv−time_msc ≈ −3sa` | 352.471 | %65,1 | canlı kayıt |

**Kök sebep bizde:** geri doldurma `recv_ms` alanına **broker saatini** yazıyor
(`backfill.rs:16-22`, bilinen ve belgeli bir karar), canlı kayıt ise gerçek UTC
alım zamanını. Birleştirme `recv_ms`'e göre sıralıyor. Aynı piyasa anı için
canlı kaydın `recv_ms`'i 3 saat küçük olduğundan, `recv_ms` sıralaması iki
grubu **`time_msc` ekseninde iç içe geçiriyor**.

Belgede bunun **tempo** etkisi yazılıydı; **`time_msc`'in monoton olmaktan
çıktığı** yazılı değildi. Asıl zarar veren bu: `time_msc`'e göre sıralamadan
bar kuran biri, kovalara doğru düşen ama `close`'u yanlış tick'ten alan barlar
üretir. Sessiz ve fark edilmesi zor.

**Tüketici için kural:** ham dosyadan ya da replay'den bar kurarken
**`time_msc`'e göre sıralayın**. Dosya sırası piyasa sırası değildir.

Kalıcı çözüm (yapılmadı): geri doldurma `recv_ms`'e gerçek UTC karşılığını
yazsın, ya da kaydın kaynağı `flags` ile bildirilsin.

---

## 11. `stops_level` GERÇEKTEN `0` — önceki "20–25 point" iddiası YANLIŞTI

### ❌ Önce yanlış ölçüldü, tüketici ona göre kod değiştirdi

İlk turda "eşik 20–25 point" bildirildi. **Yanlıştı.** Tüketici sistem buna
dayanarak "limit fiyatı piyasaya 30 point'ten yakınsa doğrudan market" kuralı
koydu — yani tam kazanmaya çalıştığı LIMIT avantajının bir kısmından oldu.

**Yöntem hatası:** açık pozisyonun SL'i fiyata kademeli yaklaştırılıyordu. O
yöntem bozuk, çünkü (a) stop tetiklenip pozisyonu kapatıyor, test kendini
bitiriyor, (b) fiyat oynayınca SL yanlış tarafa geçip `10013` üretiyor.
Elde kalan tek `10016` örneği bu gürültüdendi.

### ✅ Doğru ölçüm: bekleyen emirle, pozisyon hiç açılmadan

```
buy_limit, piyasadan 4 … 60 point asagida, 3 tur, 57 emir
  -> 57/57 KABUL. Tek bir 10016 YOK.

buy_limit sabit 100 pt asagida, SL limit fiyatina 1 … 100 point
  -> 13/13 KABUL. SL mesafesi 1 POINT bile kabul ediliyor.
```

**Sembol tablosundaki `stops_level: 0` gerçekten doğru.** Bu brokerda GOLD
için yerleştirme mesafesi kısıtı yoktur.

### Gerçek sebep: mesafe değil, TARAF

Tüketicinin 19 limit emrinden 2'sinin `10016` almasının kalan tek makul
açıklaması: limit fiyatı, karar ile sunucunun emri işlemesi arasında piyasanın
**yanlış tarafına** geçti. `buy_limit` mevcut ask'in üstüne düşerse
geçersizdir.

Tüketicinin ölçtüğü gecikme zinciri **1,238 sn**; GOLD bu sürede 10–20 point
oynayabiliyor. Yani kural şu: **mesafe, gecikme boyunca fiyatın oynayabileceği
miktardan büyük olmalı.** Broker dayatması değil, gecikmenin sonucu — ve
gecikmeyi kısaltmak mesafeyi daraltmayı mümkün kılar.

Bu, §7'deki "karar gecikmesini kısaltmak `deviation` ayarlamaktan kat kat
değerli" bulgusunun ikinci kez, farklı bir yoldan doğrulanmasıdır.

---

## 12. Süreli emir: `expiration` broker saatinde, `expire_sn` eklendi

| gönderilen `expiration` | sonuç |
|---|---|
| `UTC + 120 sn` | **`retcode 10022`** (INVALID_EXPIRATION) |
| `UTC + 10800 + 120` | kabul |
| `UTC + 86400` | kabul — **3 saat ERKEN dolar, sessizce** |

MT5 alanı **sunucu saati** sayıyor. Kabul edilen emrin telden dönen hâli
mekanizmayı doğruluyor: `time_setup_msc` ile o andaki gerçek UTC arasındaki
fark **10.809 sn = 3 saat**.

Çözüm olarak `expire_sn` (göreli saniye) eklendi. Köprü broker saatini **son
tick'ten** okur ve mutlak damgayı kendisi hesaplar.

**İki kusur ölçümle bulundu ve düzeltildi:**

1. **Bayat tick.** İlk sürüm tick damgasını doğrudan "şu an" saydı. 21 sn
   bayat bir tick, 120 sn istenen emri 99 sn'ye düşürdü. Artık tick'in
   `recv_ms`'i ile yerel saat farkı ekleniyor.
2. **Dakikaya kırpma.** MT5 son kullanmayı dakika sınırına **aşağı** yuvarlıyor
   — 120 sn istenen emir 72 sn yaşadı. Aşağı yuvarlamak sessizce eksik teslim
   etmektir; köprü artık **yukarı** yuvarlıyor. Ölçülen sonuç: `expire_sn: 120`
   → gerçek ömür **151 sn** (120 ≤ ömür < 180).

`expire_sn` bir **taban** garantisidir, kesin süre değil.

### `{"kind":"expired"}` ÜRETİLİYOR — doğrulandı

```
kind=expired  ret=0  order=946117963  state=6  txn=2
```

`state=6` (`ORDER_STATE_EXPIRED`), `txn=2` (`ORDER_DELETE`). Süre dolduğu anda
geldi.

### Sessiz yok saymanın sonu

Üç hatalı kullanım artık açık hatayla reddediliyor (canlıda doğrulandı):
`expiration` + `time` yok · `expire_sn` + `expiration` birlikte ·
`expire_sn` + `time:"gtc"`.

---

## 13. GOLD'da KOMİSYON YOK — maliyetin tamamı spread

Tüketici sistemin 66 günlük backtest sonucu tek bir bilinmeyene takılmıştı:
komisyon 0$ ise 1.000$ → 10.127$; 1$ ise **margin call**. Ölçüldü.

**Yöntem — MQL5 betiği gerekmedi:** pozisyon aç, kapat, `account.balance`ı
önce/sonra oku, fiyattan hesaplanan K/Z ile karşılaştır. Fark = komisyon + swap.

| hacim | dolan | kayma | spread | **komisyon** |
|---|---|---|---|---|
| 0,01 lot | 0,01 | 0 point | 57 point | **0,0000$** |
| 0,10 lot | 0,10 | 0 point | 55 point | **0,0000$** |
| 0,50 lot | 0,50 | 0 point | 53 point | **0,0000$** |

Ayrıca 0,01 lotta 5 ardışık tur: her birinde `bakiye_farkı == fiyat_K/Z`,
sapma 0,0000. Toplam **8 gidiş-dönüş, 3 hacim, istisnasız sıfır.**

### Kayma her hacimde sıfır, kısmi dolum yok

0,50 lotta bile `dolan == istenen`. Ölçek büyütmek bu hacimlerde ek maliyet
getirmiyor (0,50 lot ≈ 217.000$ nominal, 1:1000 kaldıraçla marj sorun olmadı).

### Spread dağılımı (408 tick, 90 sn)

```
min=40  p50=52  p90=53  p99=54  max=59   (point)
ortalama 49 point = 0,49$ / 0.01 lot
```

### Hesap kimliği

```
login 318262494 · XMGlobal-MT5 7 · XM Global Limited
USD · 1:1000 · demo · hedging
```

`hedging`: pozisyon kapatma **ticket zorunlu** ister, sembolle kapatılmaz.

### ⚠️ Çekince: bu bir DEMO hesap

Demo sunucular bazen komisyonu modellemez. Ölçüm bu hesapta kesin; **gerçek
hesapta aynı hesap tipiyle yeniden ölçülmelidir.** Spread'in ~52 point olması
komisyonsuz bir Standard/Micro profiliyle tutarlı (Zero hesabında spread çok
daha dar olur ve komisyon çıkar), yani sonuç makul — ama demo'dan.

### Swap: ölçüm sürüyor

Gece devri beklendiği için henüz yok. Korumalı çift açık bırakıldı
(`946181832` + `946181926`, ters yönler, piyasa riski ~0, yalnız swap
birikir). Broker 00:00 = 21:00 UTC devrinden sonra `positions[].swap` okunacak.

> **Kendi ölçüm hatam:** bu turda "gecikme 14.000 ms" değerleri üretildi;
> bunlar **gerçek gecikme değil**, betiğin olay toplama penceresi. Gerçek
> uçtan uca gecikme §7'de ölçüldü: **48 ms** medyan. 14 sn hiçbir yerde
> kullanılmamalı.

---

## 14. Tick `flags`: BUY/SELL bitleri İŞE YARAMAZ, BID/ASK bitleri YARAR

Tüketici sistem `tick_delta` motorunda taker tarafını `flags`in BUY (32) /
SELL (64) bitinden türetmeyi denedi ve bitlerin birlikte set olduğunu bildirdi
(~%90). Doğrulandı ve **daha kesin çıktı.**

`ticks-20260805.bin`, GOLD, 398.482 tick — **tamamı**:

| flags | ikili | BUY | SELL | adet | % |
|---|---|---|---|---|---|
| 1254 | `0b0000010011100110` | X | X | 364.449 | 91,5 |
| 1124 | `0b0000010001100100` | X | X | 18.329 | 4,6 |
| 1250 | `0b0000010011100010` | X | X | 15.703 | 3,9 |

```
BUY+SELL BIRLIKTE : 398.482  (%100,0)
yalniz BUY        :       0
yalniz SELL       :       0
```

**Tek bir tick bile tek taraflı değil.** Bayrağı olduğu gibi "taker tarafı"
diye kullanan bir istemci, öncelik hangisindeyse **her tick'i** o tarafa
yazar. Sessiz ve tamamen yanlış bir sinyal kaynağı olur.

### Ama BID(2) / ASK(4) bitleri gerçek bilgi taşıyor

Aynı tabloda fiyat yönüyle çapraz bakınca:

| flags | BID biti | ASK biti | bid değişti mi |
|---|---|---|---|
| 1254 | ✔ | ✔ | **%100 değişti** (52,2 yukarı / 47,8 aşağı) |
| 1250 | ✔ | ✗ | **%100 değişti** (60 yukarı / 40 aşağı) |
| 1124 | ✗ | ✔ | **%100 DEĞİŞMEDİ** |

Yani `ASK` biti set ve `BID` biti yokken bid **hiç** oynamıyor — 18.329
örnekte istisnasız. Kotasyonun hangi tarafının hareket ettiği bilgisi
bayrakta **gerçekten var**; yanlış olan yalnızca BUY/SELL yorumu.

**Tüketiciye öneri:** taker tarafı için `32`/`64`e bakmayın (bilgi yok);
kotasyon tarafı için `2`/`4`e bakın (bilgi var, %100 tutarlı).

> `128` (0x80) ve `1024` (0x400) bitleri de her kayıtta görünüyor. Bunlar
> MT5'in belgeli `TICK_FLAG_*` listesinde **yok**; broker/terminale özgü.
> Anlamları bilinmiyor, bir şey çıkarmayın.

Bizim `sinyal_proto::tick_flag` sabitleri MT5'inkiyle birebir aynıdır
(`BID=2 ASK=4 LAST=8 VOLUME=16 BUY=32 SELL=64`), yani yorumlama farkı yok —
alan MT5'ten olduğu gibi taşınıyor.

---

## 15. SWAP ÖLÇÜLDÜ — ve GOLD'da LONG pahalı

Korumalı çift (0,01 LONG + 0,01 SHORT) gece devrine bırakıldı. Devir broker
00:00 = **21:00 UTC**. Devirden hemen sonra okunan değerler:

| yön | 0,01 lot / gece |
|---|---|
| **LONG** | **−0,89$** |
| **SHORT** | **+0,11$** |

Asimetri büyük ve yön belirleyici: long taşımak short taşımanın 8 katı
maliyetli, üstelik short **kazandırıyor**.

### Bağlam: bu, bir işlemin kârı kadar

Tüketici sistemin ölçtüğü ortalama işlem kârı **~0,96$**. Yani bir long
pozisyonu bir gece taşımak, **bir işlemin tüm kârını** siler.

Ölçüm anında hesapta 44 pozisyon açıktı (43 long + 1 short, 42'si tüketici
sistemin):

```
yuzen K/Z          : +73,83$
swap yuku          : −38,16$   (43 × −0,89  +  1 × +0,11)
swap dahil net     : +35,67$
```

**Tek gecede yüzen kârın %52'si swap'e gitti.** Sistemin ortalama tutma
süresi 22–30 dakika olduğu için normalde swap hiç devreye girmez — ama o gece
42 pozisyon devri geçti.

### ⚠️ Devir anında GOLD KAPALI — çıkamazsınız

Bunlar birleşince gerçek bir tuzak oluyor: GOLD'un günlük molası **broker
00:00–01:00**, yani **swap'in kesildiği anla aynı saat**.

Ölçüldü — devirden 3 dakika sonra pozisyon kapatma denemesi:

```json
{"kind":"txn","retcode":10018}   // MARKET_CLOSED
```

Yani devir yaklaşırken pozisyonunuz açıksa: swap kesilir **ve** bir saat
boyunca çıkamazsınız. "Devirden hemen önce kapat" stratejisi, bir saniye geç
kalırsa hiç çalışmaz.

**Pratik kural:** GOLD pozisyonlarını broker 23:5x'ten **önce** kapatın;
o pencereyi kaçırırsanız 01:00'e kadar mahsursunuz.

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
