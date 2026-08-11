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
| `crates/sinyal-core` — `sinyald`: feed, mum, emir, korelasyon, yetki, geçmiş | 92 |
| `crates/sinyal-proto` üreteç testleri | 11 |
| `tools/latency-bench` | 7 |
| `mql5/` — EA + Service + ABI başlıkları | derlendi, canlı çalışıyor |

**241 test, hepsi geçiyor.**

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

## Çalıştırma

```bash
# yalnız yerel, emir kapalı
sinyald --instance mt5-1

# ağa açık, grafik token'sız, işlem token'lı
sinyald --instance mt5-1 --bind 0.0.0.0:8787 --enable-trading --token GIZLI

sinyald --generate-token
```

Varsayılan olarak yalnızca `127.0.0.1` dinler ve **emir yürütme kapalıdır**.

Piyasa verisi (tick, derinlik, mum, sembol) **token istemez**; hesap ve emir
ister. `--token` verilmezse bağlantı doğrudan `trader` seviyesinde başlar.

---

## Bilinen eksikler

1. **Tick kaydı ve replay yok.** "Canlıda gibi test" için kayıt-ve-yeniden-oynat
   katmanı gerekiyor; şu an test ancak canlı akışla yapılabilir.
2. **Oluşmakta olan bar bayat.** MT5 serisi ~20 sn'de bir tazeleniyor; son barı
   istemci tick'ten kurmalı — **bid**'den, mid'den değil (GOLD'da fark 20-30
   point).
3. **Hız sınırı yok.** Geçmiş isteğinde eş zamanlılık tavanı var (4) ama
   istek/saniye sınırı yok. Uç internete açıksa bu istek MT5'in içinde iş
   tetikler.
4. **Swap, komisyon, seans bilgisi taşınmıyor.** Gerçekçi PnL hesabı için
   gerekli.
5. **`CandleStore` yalnızca sembol adıyla anahtarlı**, `instance` ile değil —
   ikinci bir broker eklenirse iki serinin GOLD'u karışır.
6. **TLS yok.** `ws://` düz metin; token ağ üzerinde açık gider.
7. **Terminal kaynaklı tick kaybı ölçülmüyor.** Halka kaybı ayrıca ölçülüyor
   ve 0.

---

## Lisans

UNLICENSED — özel proje.
