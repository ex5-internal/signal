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

## Henüz ÖLÇÜLMEMİŞ — bunlara güvenme

Aşağıdakiler kodda yazılı ama **canlıda doğrulanmadı**. Sinyal sistemine
"çalışıyor" diye bildirilmemeli:

- **Broker stop'unun kurulduğunun doğrulanması** (`sltp_unverified`) — kod
  yazıldı, 443 birim testi geçiyor, **canlı testi yapılmadı**
- `{"kind":"expired"}` olayının gerçekten üretilmesi
- `RECONCILE` yoluyla gerçekleşmiş `profit`/`commission`/`swap` — henüz yok

Bu oturumda birim testleri geçen **iki** entegrasyon hatası ancak gerçek turda
yakalandı (kayıt→replay sembol şeması; `deploy.ps1`'in başarısız derlemeyi
başarı sayması). Birim testi burada yeterli kanıt değildir.
