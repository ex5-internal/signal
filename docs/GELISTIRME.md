# Geliştirme ve dağıtım — tuzaklar dahil

Bu belge **sessizce bozan** şeyleri toplar. Buradaki maddelerin çoğu bu projede
gerçekten yaşandı; kalanlar araştırma sırasında dokümante edilmiş tuzaklar.

---

## Derleme ve test

```bash
cargo test --workspace
cargo build --release -p sinyal-core
```

Test sayısı 241 (2026-08-11). Hepsi geçmeli — bu depoda testler yalnızca
"çalışıyor mu" değil, **daha önce yaşanmış hataları sabitlemek** için var.
Bir test kırılırsa önce onun neden yazıldığını oku.

`cargo` PATH'te olmayabilir:
```powershell
$env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
```

---

## Dağıtım

```powershell
.\tools\deploy.ps1 -Compile
```

Yaptıkları: Rust köprüsünü derler → `Protocol.mqh`'yi yeniden üretir → DLL'in
x64 olduğunu doğrular → dosyaları terminalin veri klasörüne kopyalar → **EA ve
Service'i** derler.

### ⚠ `deploy.ps1` daemon'ı DERLEMEZ

Yalnızca `sinyal-bridge` (DLL) derlenir. `sinyald` ayrıca derlenmelidir:

```bash
cargo build --release -p sinyal-core
```

**Bu unutuldu ve bir süre eski ikili çalıştı** — yeni protokol alanları
cevaplarda hiç görünmedi, sebebi de hemen anlaşılmadı. Bir alan ekledikten sonra
cevapta görmüyorsan **ilk bakacağın yer budur.**

### ⚠ DLL terminal açıkken güncellenemez

Windows dosyayı kilitler. `deploy.ps1` bunu **fatal hata saymaz**, yalnızca
uyarır ve devam eder:

```
!!  sinyal_bridge.dll guncellenemedi (terminal yuklemis durumda).
!!  EA ESKI DLL ile calismaya devam edecek.
```

Yani "dağıtım başarılı göründü" ama DLL eski kaldı. Çözüm sırası:

1. EA'yı grafikten kaldır (veya terminali tamamen kapat)
2. `deploy.ps1`'i tekrar çalıştır
3. EA'yı geri sürükle

Pratik yöntem: arka planda kopyalamayı yeniden deneyen bir döngü kurup
kullanıcıdan terminali kapatmasını istemek — böylece kilit açıldığı anda
yakalanır ve zamanlama koordinasyonu gerekmez.

### ⚠ Yeni MQL5 dosyası eklersen `$targets`'a ekle

`deploy.ps1` içindeki `$targets` sözlüğünde olmayan dosya **hiç kopyalanmaz**.
Bu yaşandı: `HistBridge.mqh` ve `Services/SinyalHistory.mq5` eklendi ama listeye
yazılmadı; terminalde hiç görünmediler ve Service "yok" sanıldı.

---

## MT5 tarafı — çalıştırma

Terminalde **iki ayrı program** olmalı:

| Program | Nerede | Nasıl başlatılır |
|---|---|---|
| `SinyalCollector` (EA) | GOLD grafiğinde | Navigator'dan grafiğe sürükle |
| `SinyalHistory` (Service) | grafiğe bağlı değil | Navigator → Services → Add → Start |

Ön koşul: **Tools → Options → Expert Advisors → "Allow DLL imports"** işaretli.

### ⚠ Service'i komut satırından başlatmak MÜMKÜN DEĞİL

MT5 Service'lerinin CLI'si yok; yalnızca Navigator'dan başlatılır. Otomatikleştirilemez.

Service çalışmıyorsa sistem **sessiz kalmaz**:
```json
{"src_kind":"tick","hist":"off","hist_note":"MQL5 Service calismiyor"}
```

### ⚠ MT5, CLI ile derlenen EA'yı otomatik YENİDEN YÜKLEMEYEBİLİR

`metaeditor64.exe /compile:...` ile derlemek `.ex5`'i günceller ama terminal
grafikteki EA'yı her zaman yeniden yüklemez. Log'da `durduruldu (sebep=5)`
(REASON_RECOMPILE) satırı yoksa **eski derleme çalışmaya devam ediyordur.**

Kesin çözüm: EA'yı grafikten kaldır ve geri sürükle.

### ⚠ Aynı `InstanceName` ile ikinci EA reddedilir

Bu **doğru davranıştır**. İki EA aynı halkaya yazsaydı veriyi sessizce bozardı.
Log'da şöyle görünür:

```
köprü açılamadı — 'mt5-1' örneğini başka bir yazar tutuyor
```

Yani EA'yı başka bir grafiğe taşıyorsan **önce eskisini kaldır**, sonra yenisine
sürükle. Sıra ters olursa yeni EA açılmaz.

---

## Protokol değiştirme — sırayla

Yeni bir alan veya yapı eklerken **her adım gereklidir**; birini atlamak
derlemeyi kırmaz, sessizce bozar.

1. **`crates/sinyal-proto/src/*.rs`** — yapıyı ekle/değiştir.
2. **`lib.rs` içindeki `layout` modülü** — `size_of`, `align_of`,
   `size_of::<Cell<T>>()` ve **her alanın** `offset_of!` iddiası.
   > Bu adımı atlamak derlemeyi KIRMAZ. Üreteç testi yalnızca **toplam boyutu**
   > karşılaştırır; aynı boyutta iki alanın yeri değişse test **geçer** ve veri
   > sessizce bozulur.
3. **`bin/gen_mqh.rs`** — üç ayrı yere dokunulur:
   - `format!` bloğundaki `#define SINYAL_SIZEOF_*` + karşılık gelen argüman
   - sabitler bölümü (`def`/`def32` çağrıları)
   - MQL5 yapı gövdesi (**düz metin** — Rust'tan ayrışabilir)
4. **`gen_mqh.rs` testleri** — `cases` dizisi ve
   `generated_header_has_every_struct` listesi.
   > Eklemezsen testler yeşil kalır ama yeni yapı **hiç doğrulanmaz.**
5. **`Bridge.mqh` / `HistBridge.mqh`** — `#import` bildirimleri ve
   `SinyalVerifyLayout` içindeki dizi uzunlukları.
   > MQL5 DLL imza doğrulaması **YAPMAZ**. Yanlış parametre sayısı/tipi =
   > sessiz yığın bozulması. `SinyalSizeof` yalnızca **yapı** boyutlarını
   > doğrular, **fonksiyon** imzalarını değil — bu, eklenen yolun en kırılgan
   > noktasıdır.
6. **`session.rs`** — `create` ve `open` **her zaman birlikte** güncellenmeli.
   > Birine ekleyip diğerine eklememek derleme hatası vermez: EA segmenti
   > kurar, çekirdek hiç okumaz. Tamamen sessiz.
7. **`ffi.rs`** — her export `catch_unwind` ile sarılmalı.
   > Rust 1.81'den beri `extern "C"` sınırından unwind **süreci ABORT eder**,
   > yani açık pozisyonlu MT5 terminalini öldürür.

### `Session` alan sırası

```rust
pub struct Session {
    ticks: Ring<Tick>,      // halkalar ÖNCE
    ...
    _mem_ticks: SharedMem,  // eşlemeler SONRA
}
```

Rust alanları **bildirim sırasıyla** düşürür. Ters yazarsan eşleme kaldırılır
ve halka kaldırılmış belleğe işaret eder (use-after-unmap).

### `RING_VERSION` ne zaman artırılır

**Mevcut** bir yapının yerleşimi değiştiğinde. Yeni segment eklemek sürüm
artırmaz — eski EA/çekirdek ikilileri etkilenmez.

Artırırsan: DLL + `.ex5` + çekirdek **birlikte** yenilenmelidir. Ayrıca
dağıtımdan önce **`sinyald` durdurulmalıdır** — segment, son tanıtıcı kapanana
kadar yaşar; çekirdek eskisini açık tutarken EA yeniden başlarsa var olan eski
segmente bağlanır ve teşhis edilemeyen bir `OnInit` hatası alır.

---

## MQL5 semantiği — dokümante tuzaklar

Bunlar MQL5 dokümanından ve forumdan doğrulandı. Kaynaklar
[MIMARI.md](MIMARI.md) içinde.

| Tuzak | Doğrusu |
|---|---|
| `MqlRates.time` milisaniye sanmak | **SANİYEDİR.** Tick'teki `time_msc` ms'dir; 1000 ile çarpılmalı |
| `CopyRates`'in son barını kapanmış sanmak | `start_pos = 0` **oluşmakta olan** bardır |
| Dönen değeri `> 0` diye kontrol etmek | **Kısmi** dönebilir; `n == count` karşılaştırılmalı |
| `ArraySetAsSeries` dizi düzenini değiştirir sanmak | Fiziksel düzen **her zaman** eski→yeni |
| `real_volume == 0`'ı "hacim sıfır" sanmak | "**veri yok**" demek; çoğu forex broker'ı yayınlamaz |
| Tüm hataları geçici sayıp sonsuz retry | 4401/4403 geçici; **4404/4301/4302/4407 KALICI** |
| `GetLastError()`'u `ResetLastError()` olmadan okumak | Bayat hata kodu okunur, yanlış teşhis |
| `Bars()` kadar bar istemek | `CopyRates` yalnızca `TERMINAL_MAXBARS` görür |
| Service'te `OnTimer`/`EventSetTimer` | **YASAK.** Yalnız `OnStart` + kendi döngün |
| Service'te `Sleep()` yasak sanmak | **Serbest.** Yalnızca custom indikatörlerde yasak |
| Service'te `_Symbol`/`_Period` kullanmak | Service grafiğe **bağlı değil**; `SymbolSelect` şart |
| `MqlTradeTransaction`'da `comment` aramak | **Yok.** `result.comment` kullanılmalı |

---

## Ortam

- PowerShell 5.1'de `&&` **çalışmaz** (parse hatası). `pwsh` (7+) kullan veya
  komutları ayır.
- `tools/deploy.ps1` **BOM'lu UTF-8** olmak zorunda — PowerShell 5.1 aksi halde
  dosyayı ayrıştıramaz.
- MQL5 log'ları **UTF-16LE**'dir: `iconv -f UTF-16LE -t UTF-8` veya
  `Get-Content -Encoding Unicode`.
- MT5 log dizini:
  `%APPDATA%\MetaQuotes\Terminal\<hash>\MQL5\Logs\YYYYMMDD.log`

---

## Telemetri okuma

Daemon 30 saniyede bir basar:

```
[mt5-1] tick=1644 dom=0 emir=0 bar=111(mt5=0) | EA-kayip=0 birikmis=0
        | eslesme: bekleyen=0 gec=0 kimliksiz=0
        | gecmis: acik istek=1 bekleyen=0 eksik=0 hata=0 zamanasimi=0
```

| Alan | Anlamı | Alarm eşiği |
|---|---|---|
| `EA-kayip` | EA halkaya yazamadı = **kalıcı tick kaybı** | **> 0 ise sorun** |
| `birikmis` | okunmayı bekleyen tick — biz mi geride kalıyoruz | sürekli artıyorsa |
| `mt5=N` | MT5'ten gelen bar sayısı | 0 ise Service çalışmıyor |
| `kimliksiz` | hiçbir komuta bağlanamayan emir olayı | elle işlem yoksa > 0 sorun |
| `eksik` | eksik teslim edilen geçmiş isteği | bar halkası dolmuş olabilir |

EA de dakikada bir kendi log'una yazar; `halka-kayip` ve `timer-atlama`
oradadır.
