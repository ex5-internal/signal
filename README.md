# Sinyal — MT5 adaptörü

MetaTrader 5 terminalinden tick, derinlik, mum ve hesap durumunu **mikrosaniye
mertebesinde** dışarı taşıyan; karşılığında market/limit/stop emirlerini yürüten
köprü. Dışarıya tek bir WebSocket ucundan açılır.

**Bu bir piyasa verisi feed'i değildir.** Amacı, ayrı geliştirilen bir sinyal
üreten sistemi, işlemin **gerçekten yapılacağı** terminalin gördüğü veriyle
beslemektir — böylece test ile canlı arasında veri farkı oluşmaz.

---

## Belgeler

| | |
|---|---|
| [**API.md**](API.md) | WebSocket protokolü. Sinyal sistemine verilecek uç. |
| [**docs/MIMARI.md**](docs/MIMARI.md) | Kararların **gerekçesi**. Bir şeyi basitleştirmeden önce oku. |
| [**docs/GELISTIRME.md**](docs/GELISTIRME.md) | Dağıtım, protokol değiştirme, **sessizce bozan tuzaklar**. |

Yeni başlıyorsan sırayla oku. `MIMARI.md` "neden böyle", `GELISTIRME.md` "neye
dikkat" sorusunu cevaplar; ikisi de koddan çıkarılamayacak bilgi içerir.

---

## Neye benziyor

```
MT5 Terminal                                 ayrı süreç        istemci
┌───────────────────────────────┐           ┌──────────┐      ┌────────┐
│ SinyalCollector.mq5  (EA)     │──md/res──▶│          │──ws──▶ sinyal │
│   tick, derinlik, emir olayı  │◀──cmd─────│ sinyald  │      │ sistemi│
│                               │           │          │◀─────┤        │
│ SinyalHistory.mq5  (Service)  │──bars────▶│          │      └────────┘
│   CopyRates ile MT5 geçmişi   │◀──hreq────│          │
└───────────────────────────────┘           └──────────┘
      sinyal_bridge.dll (Rust, terminalin İÇİNDE)
```

**EA asla bloklanmaz** — tick toplama yolu budur. `CopyRates` çağıran thread'i
30-60 saniye durdurabildiği için geçmiş okuma ayrı bir **Service**'e taşındı.
Service kapalıyken sistem tam işlevsel kalır, yalnızca MT5 geçmişi gelmez.

Gerekçelerin tamamı: [docs/MIMARI.md](docs/MIMARI.md)

---

## Durum

Canlı demo hesapta (XM Global, hedging) uçtan uca doğrulandı.

| Bileşen | Test |
|---|---|
| `crates/sinyal-proto` — bellek yerleşimi, durum tabloları, emir doğrulama | 66 |
| `crates/sinyal-shm` — Windows shm, QPC, tek-örnek kilidi, token üretimi | 17 |
| `crates/sinyal-bridge` — MT5'e yüklenen DLL (C ABI) + geçmiş oturumu | 48 |
| `crates/sinyal-core` — `sinyald`: feed, mum, emir, korelasyon, yetki, geçmiş, kayıt, replay, simülatör | 232 |
| `crates/sinyal-proto` üreteç testleri | 15 |
| `tools/latency-bench` | 7 |
| `mql5/` — EA + Service + ABI başlıkları | derlendi, canlı çalışıyor |

**385 test, hepsi geçiyor.**

Doğrulamayı kendin çalıştır:

```bash
pwsh -File tools/kabul-testi.ps1 ws://144.76.111.177:8787
```

20 maddelik uçtan uca kontrol: bağlantı, sembol bilgisi, MT5 geçmişi (altı
zaman dilimi), hata yolları, canlı akış, yetki kapısı, emir aç-kapat. Test
"piyasa kapalı" ile "sistem bozuk" ayrımını yapar.

### Ölçülen gecikme

Canlı MT5 akışında uçtan uca (EA yakalama → WebSocket mesajı):
**p50 247 µs**, p99 ~800 µs.

Paylaşımlı bellek turu (ayrı süreçler): p50 100 ns, p99 8,3 µs, tavan
21,6M tick/sn, 2M mesajda kayıp 0.

### Canlı kabul testi (genel IP üzerinden, 20/20)

```
GOLD M1   300 bar   299 dk     src=mt5   OHLC tutarlı   son bar partial ✓
GOLD M5   300 bar   26 saat    src=mt5
GOLD M15  300 bar   5 gün      src=mt5
GOLD H1   300 bar   19 gün     src=mt5
GOLD H4   300 bar   70 gün     src=mt5
canlı tick        12 sn'de 94 tick, p50 247 µs
emir              0.01 GOLD @ 4370.29 açıldı ve kapandı
korelasyon        8 olayın 8'i kimlikli, kimliksiz 0
```

---

## Kurulum

```powershell
.\tools\deploy.ps1 -Compile
cargo build --release -p sinyal-core     # deploy bunu YAPMAZ
```

Terminalde:

1. **Tools → Options → Expert Advisors → "Allow DLL imports"** işaretli olmalı
2. `SinyalCollector`'ı **sinyal üreteceğin sembolün grafiğine** sürükle
   (şu an GOLD — `OnTick` yalnızca grafik sembolü için tetiklenir, diğerleri
   16 ms taramayla örneklenir)
3. **Navigator → Services → `SinyalHistory` → Add → Start**
4. **Toolbox → Experts** log'unu oku; EA açılışta yerleşim doğrulaması yapar ve
   uyumsuzsa **başlamaz**

Tuzaklar ve sıralama: [docs/GELISTIRME.md](docs/GELISTIRME.md)

## Üç kip

Aynı WebSocket protokolü, üç farklı yürütme kipi. Sinyal sisteminin **kodu
değişmez** — yalnızca bağlandığı port değişir. Fark `hello.mode` alanında
açıkça ilan edilir.

| Kip | Veri | Yürütme | `hello.mode` |
|---|---|---|---|
| **canlı** | MT5'ten gerçek zamanlı | **gerçek emir** | `live` |
| **paper** | MT5'ten gerçek zamanlı | simüle | `paper` |
| **replay** | diskteki kayıttan | simüle | `replay` |

`replay` + gerçek yürütme kombinasyonu **yasaktır** — kayda emir göndermek
anlamsızdır ve kaza riski taşır.

### Canlı

```bash
# yalnız yerel, emir kapalı (varsayılan)
sinyald --instance mt5-1

# ağa açık, grafik token'sız, işlem token'lı
sinyald --instance mt5-1 --bind 0.0.0.0:8787 --enable-trading --token GIZLI

sinyald --generate-token
```

Varsayılan olarak yalnızca `127.0.0.1` dinler ve **emir yürütme kapalıdır**.
Piyasa verisi (tick, derinlik, mum, sembol) **token istemez**; hesap ve emir
ister. `--token` verilmezse bağlantı doğrudan `trader` seviyesinde başlar.

### Paper — canlı veri, risksiz yürütme

```bash
sinyald --instance mt5-1 --bind 0.0.0.0:8787 --enable-trading --token GIZLI \
        --paper-bind 0.0.0.0:8789 --paper-balance 10000 --sim-slippage 1
```

**Aynı süreçte ikinci dinleyici.** Ayrı daemon olamaz: halkalar SPSC'dir, iki
okuyucu sözleşmeyi kırar. Ayrı **port** ise bilinçli bir güvenlik sınırıdır —
paper stratejisi yanlışlıkla gerçek emir gönderemez, çünkü o soketin komut
kanalı hiç kurulmamıştır.

İkisi **aynı anda** çalışır: strateji 8789'da paper koşarken sen 8787'den
gerçek hesabı izlersin. Veri, mum deposu ve geçmiş ortaktır.

### Kayıt ve replay — "canlıda gibi" test

```bash
# canlı koşarken tick akışını diske yaz
sinyald --instance mt5-1 --record ./veri

# kaydı aynı protokolden yeniden oynat
sinyald --replay ./veri --replay-date 20260811 --bind 127.0.0.1:8788
sinyald --replay ./veri --replay-date 20260811 \
        --replay-from 09:00 --replay-to 17:00 --replay-speed 3
```

`--replay-speed 0` beklemeden en hızlı oynatır; sıra korunur. Aynı kayıt + aynı
bayraklar → **aynı çıktı** (belirlenimci).

Kayıt biçimi başlıksız, sabit 48 baytlık kayıtlardır; dosya uzunluğu tek
doğruluk kaynağıdır, yani çökmede yarım kayıt sessizce kırpılır. Kayıt **ayrı
bir thread'de** yapılır ve sıcak yolda hiç I/O yoktur — kanal dolarsa kayıt
düşürülür ve **sayılır** (canlı akış kayıttan önceliklidir).

Disk maliyeti: ~56 MB/gün (10 sembol).

### Simülatör neyi modeller, neyi modellemez

Paper ve replay açılışta bunu kendisi ekrana yazar. Kısaca:

**Modellenir** — spread; **aleyhte** kayma (varsayılan 1 point, sıfır DEĞİL);
bekleyen emir ve SL/TP tetiklenmesi (aynı tick ikisini de vurursa **SL
kazanır**); `stops_level` (10016); marjin (10019); hacim ızgarası (10014).

**Modellenmez** — komisyon, swap, requote/deviation penceresi, kur çevrimi,
kısmi dolum, emir son kullanma, stop-out, `freeze_level`.

> Simüle dolum **gerçek dolum değildir.** Sıfır kayma varsayılanı bilinçli
> olarak reddedildi: simülatörü gerçekte olduğundan kârlı gösteren sessiz bir
> yalan olurdu.

---

## Bilinen eksikler

1. **Komisyon ve swap simülasyonda modellenmiyor.** Veriler artık sembol
   tablosunda taşınıyor (`swap_long/short`, `swap_mode`) ama simülatör henüz
   kullanmıyor. Uzun süre taşınan pozisyonlarda PnL canlıdan sapar.
2. **Oluşmakta olan bar bayat.** MT5 serisi ~20 sn'de bir tazeleniyor; son barı
   istemci tick'ten kurmalı — **bid**'den, mid'den değil (GOLD'da fark 20-30
   point).
3. **Hız sınırı yok.** Geçmiş isteğinde eş zamanlılık tavanı var (4) ama
   istek/saniye sınırı yok. Uç internete açıksa bu istek MT5'in içinde iş
   tetikler.
4. **`TickRec` iki yerde tanımlı** (`record.rs` ve `replay.rs`). İkisi bugün
   aynı, ama tek bir disk biçiminin iki bağımsız tanımı sessiz kaymanın klasik
   kaynağıdır.
5. **`CandleStore` yalnızca sembol adıyla anahtarlı**, `instance` ile değil —
   ikinci bir broker eklenirse iki serinin GOLD'u karışır.
6. **TLS yok.** `ws://` düz metin; token ağ üzerinde açık gider.
7. **Terminal kaynaklı tick kaybı ölçülmüyor.** Halka kaybı ayrıca ölçülüyor
   ve 0.
8. **Kurulum yarı otomatik.** `deploy.ps1` dosyaları kopyalayıp derliyor, ama
   DLL terminal açıkken değiştirilemiyor, EA'yı grafiğe sen sürüklüyorsun ve
   Service'i Navigator'dan sen başlatıyorsun. Araştırıldı: üçü de
   otomatikleştirilebilir ama **terminalin kapalı olmasını** gerektiriyor ve
   terminali zarifçe kapatmanın belgelenmiş bir CLI yolu yok.

---

## Lisans

UNLICENSED — özel proje.

