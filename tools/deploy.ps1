<#
.SYNOPSIS
    Sinyal köprüsünü bir MT5 terminaline kurar.

.DESCRIPTION
    Rust tarafını derler, üretilen DLL'i ve MQL5 kaynaklarını terminalin veri
    klasörüne kopyalar, isteğe bağlı olarak EA'yı derler.

    MT5'in "veri klasörü" kurulum klasöründen FARKLIDIR. Terminalde
    Dosya → Veri Klasörünü Aç ile bulunur; tipik olarak
    %APPDATA%\MetaQuotes\Terminal\<32-haneli-hex>\

.PARAMETER DataDir
    MT5 veri klasörü. Verilmezse %APPDATA%\MetaQuotes\Terminal altında
    tek bir aday varsa o kullanılır; birden fazlaysa hata verir (yanlış
    terminale kurmak sessiz bir hata olurdu).

.PARAMETER Compile
    EA'yı metaeditor64.exe ile derle. MetaEditor kurulu değilse atlanır.

.PARAMETER SkipBuild
    Rust derlemesini atla (DLL zaten güncelse).

.EXAMPLE
    .\tools\deploy.ps1 -Compile

.EXAMPLE
    .\tools\deploy.ps1 -DataDir "C:\MT5-ICMarkets" -Compile
#>
[CmdletBinding()]
param(
    [string]$DataDir,
    [switch]$Compile,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot

function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "!!  $msg" -ForegroundColor Yellow }

# --- 1. Veri klasörünü bul -------------------------------------------------

if (-not $DataDir) {
    $root = Join-Path $env:APPDATA 'MetaQuotes\Terminal'
    if (-not (Test-Path $root)) {
        throw "MT5 veri klasörü bulunamadı ($root). MT5 kurulu mu? -DataDir ile elle belirt."
    }
    # 32 haneli hex klasörler gerçek terminal örnekleridir; Common/Community değil.
    $candidates = Get-ChildItem $root -Directory |
        Where-Object { $_.Name -match '^[0-9A-F]{32}$' }

    if ($candidates.Count -eq 0) {
        throw "MT5 terminal veri klasörü bulunamadı. Terminali bir kez çalıştırıp tekrar dene, veya -DataDir ile belirt."
    }
    if ($candidates.Count -gt 1) {
        # Yanlış terminale kurmak sessiz bir hata olur — tahmin etmiyoruz.
        Write-Host "Birden fazla terminal veri klasörü bulundu:"
        $candidates | ForEach-Object { Write-Host "  $($_.FullName)  (son değişiklik: $($_.LastWriteTime))" }
        throw "Hangisine kurulacağını -DataDir ile belirt."
    }
    $DataDir = $candidates[0].FullName
}

if (-not (Test-Path (Join-Path $DataDir 'MQL5'))) {
    throw "'$DataDir' bir MT5 veri klasörüne benzemiyor (MQL5 alt klasörü yok)."
}
Write-Step "Hedef veri klasörü: $DataDir"

# --- 2. Rust tarafını derle ------------------------------------------------

if (-not $SkipBuild) {
    Write-Step 'Rust köprüsü derleniyor (release)'
    $cargo = Join-Path $env:USERPROFILE '.cargo\bin\cargo.exe'
    if (-not (Test-Path $cargo)) { $cargo = 'cargo' }

    Push-Location $repo
    try {
        & $cargo build --release -p sinyal-bridge
        if ($LASTEXITCODE -ne 0) { throw "cargo build başarısız (çıkış kodu $LASTEXITCODE)" }

        # MQL5 başlığını Rust tanımlarından YENİDEN ÜRET. Bu adım atlanırsa
        # Rust ile MQL5 yerleşimi sessizce kayabilir — EA açılışta
        # SinyalSizeof ile yakalar ama derleme zamanında yakalamak daha iyi.
        Write-Step 'Protocol.mqh yeniden üretiliyor'
        & $cargo run -q --release -p sinyal-proto --bin gen-mqh -- 'mql5/Include/Sinyal/Protocol.mqh'
        if ($LASTEXITCODE -ne 0) { throw "gen-mqh başarısız (çıkış kodu $LASTEXITCODE)" }
    }
    finally { Pop-Location }
}

$dll = Join-Path $repo 'target\release\sinyal_bridge.dll'
if (-not (Test-Path $dll)) {
    throw "DLL bulunamadı: $dll — önce -SkipBuild olmadan çalıştır."
}

# 64-bit doğrulaması: 32-bit DLL terminal tarafından "cannot load library" ile
# reddedilir ve sebebi ilk bakışta anlaşılmaz.
$bytes = [IO.File]::ReadAllBytes($dll)
$peOffset = [BitConverter]::ToInt32($bytes, 0x3C)
$machine = [BitConverter]::ToUInt16($bytes, $peOffset + 4)
if ($machine -ne 0x8664) {
    throw "DLL 64-bit değil (machine=0x{0:X4}). MT5 terminali 64-bit'tir." -f $machine
}
Write-Step "DLL doğrulandı: x64, $([math]::Round((Get-Item $dll).Length / 1KB)) KB"

# --- 3. Kopyala ------------------------------------------------------------

$targets = @{
    'Libraries'      = @(@{ Src = $dll; Name = 'sinyal_bridge.dll' })
    'Include\Sinyal' = @(
        @{ Src = Join-Path $repo 'mql5\Include\Sinyal\Protocol.mqh';    Name = 'Protocol.mqh' }
        @{ Src = Join-Path $repo 'mql5\Include\Sinyal\Bridge.mqh';      Name = 'Bridge.mqh' }
        @{ Src = Join-Path $repo 'mql5\Include\Sinyal\HistBridge.mqh';  Name = 'HistBridge.mqh' }
    )
    'Experts'        = @(@{ Src = Join-Path $repo 'mql5\Experts\SinyalCollector.mq5'; Name = 'SinyalCollector.mq5' })
    # Gecmis besleyici AYRI bir programdir (Service): CopyRates EA thread'ini
    # bloklar, o yuzden tick toplama yolunda olamaz.
    'Services'       = @(
        @{ Src = Join-Path $repo 'mql5\Services\SinyalHistory.mq5';  Name = 'SinyalHistory.mq5' }
        @{ Src = Join-Path $repo 'mql5\Services\SinyalBackfill.mq5'; Name = 'SinyalBackfill.mq5' }
    )
}

foreach ($sub in $targets.Keys) {
    $dest = Join-Path $DataDir "MQL5\$sub"
    if (-not (Test-Path $dest)) { New-Item -ItemType Directory -Path $dest -Force | Out-Null }
    foreach ($f in $targets[$sub]) {
        if (-not (Test-Path $f.Src)) {
            Write-Warn "atlandı (kaynak yok): $($f.Src)"
            continue
        }
        $target = Join-Path $dest $f.Name

        # İçerik aynıysa kopyalama. Bu sadece hız için değil: EA çalışırken
        # DLL kilitli olur ve aynı içeriği yeniden kopyalamaya çalışmak
        # gereksiz yere dağıtımı başarısız kılar.
        if (Test-Path $target) {
            $srcHash = (Get-FileHash $f.Src -Algorithm SHA256).Hash
            $dstHash = (Get-FileHash $target -Algorithm SHA256).Hash
            if ($srcHash -eq $dstHash) {
                Write-Host "    $sub\$($f.Name)  (degismemis, atlandi)"
                continue
            }
        }

        try {
            Copy-Item $f.Src $target -Force -ErrorAction Stop
            Write-Host "    $sub\$($f.Name)"
        }
        catch {
            if ($f.Name -like '*.dll') {
                # DLL'i MT5 sürecine yüklenmişken değiştiremeyiz.
                #
                # Bunu ölümcül hata yapmıyoruz: EA açılışta SinyalVerifyLayout
                # ile DLL'in protokol sürümünü ve yapı boyutlarını denetliyor.
                # Gerçekten uyumsuz bir DLL kalırsa EA başlamayı reddedip
                # sebebini yazar — yani eski DLL ile devam etmek sessiz bir
                # bozulmaya yol açamaz.
                Write-Warn "sinyal_bridge.dll guncellenemedi (terminal yuklemis durumda)."
                Write-Warn "EA ESKI DLL ile calismaya devam edecek."
                Write-Warn "Protokol degistiyse EA baslamayi reddeder ve Uzmanlar sekmesine yazar."
                Write-Warn "DLL'i gercekten yenilemek icin: EA'yi grafikten kaldirin, betigi tekrar calistirin."
                continue
            }
            throw
        }
    }
}
Write-Step 'Dosyalar kopyalandı'

# --- 4. EA'yı derle --------------------------------------------------------

if ($Compile) {
    $editor = $null
    foreach ($p in @(
        'C:\Program Files\MetaTrader 5\metaeditor64.exe',
        (Join-Path (Split-Path $DataDir -Parent) 'metaeditor64.exe')
    )) {
        if (Test-Path $p) { $editor = $p; break }
    }
    if (-not $editor) {
        $found = Get-ChildItem 'C:\Program Files','C:\Program Files (x86)' -Recurse -Depth 3 `
            -Filter 'metaeditor64.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found) { $editor = $found.FullName }
    }

    if (-not $editor) {
        Write-Warn 'metaeditor64.exe bulunamadı — EA derlenmedi.'
        Write-Warn 'EA''yı MetaEditor''de açıp F7 ile elle derleyebilirsin.'
    }
    else {
        # EA ve Service AYRI programlardır ve ikisi de derlenmelidir. Service
        # derlenmezse Navigator'da hiç görünmez ve tek belirtisi "geçmiş
        # gelmiyor" olur — sessiz bir başarısızlık.
        # Aynı oturumda ikinci kez çağrılırsa önceki koşudan sızmasın.
        $script:CompileFailed = $false
        $toCompile = @(
            @{ Path = Join-Path $DataDir 'MQL5\Experts\SinyalCollector.mq5'; Label = 'EA' }
            @{ Path = Join-Path $DataDir 'MQL5\Services\SinyalHistory.mq5';  Label = 'Service' }
            @{ Path = Join-Path $DataDir 'MQL5\Services\SinyalBackfill.mq5'; Label = 'Backfill' }
        )
        foreach ($item in $toCompile) {
            if (-not (Test-Path $item.Path)) {
                Write-Warn "$($item.Label) kaynağı bulunamadı: $($item.Path)"
                continue
            }
            Write-Step "$($item.Label) derleniyor ($editor)"
            $log = Join-Path $env:TEMP "sinyal-compile-$($item.Label).log"

            # BAYAT LOG TUZAĞI: metaeditor kaynağı hiç açamazsa log dosyasını
            # YAZMAZ. Önceki koşudan kalan log okunur ve "başarılı" sanılır.
            # Bu yüzden önce siliyoruz ve sonra "yeniden oluştu mu" diye
            # bakıyoruz.
            Remove-Item $log -Force -ErrorAction SilentlyContinue

            # `| Out-Null` metaeditor'ü BEKLEMEZ (GUI uygulaması hemen döner);
            # sabit `Start-Sleep` de bir tahmindi. -Wait gerçekten bekler.
            $proc = Start-Process -FilePath $editor `
                -ArgumentList "/compile:$($item.Path)", "/log:$log" `
                -Wait -PassThru -WindowStyle Hidden

            if (-not (Test-Path $log)) {
                Write-Warn "$($item.Label) DERLENMEDİ: metaeditor log yazmadı (çıkış=$($proc.ExitCode))."
                Write-Warn '  Kaynak dosya bulunamamış veya metaeditor başlatılamamış olabilir.'
                $script:CompileFailed = $true
                continue
            }

            $content = Get-Content $log -Encoding Unicode -ErrorAction SilentlyContinue
            if (-not $content) { $content = Get-Content $log -ErrorAction SilentlyContinue }
            $text = ($content -join "`n")

            $content | Where-Object { $_ -match 'error|warning|result' } | ForEach-Object {
                if ($_ -match '\b[1-9]\d*\s+errors?\b') { Write-Host "    $_" -ForegroundColor Red }
                else { Write-Host "    $_" }
            }

            # HATA SAYISINI OKU, metin ARAMA YAPMA.
            #
            # Önceki sürüm `-match '0 error'` kullanıyordu ve bu SESSİZ BİR
            # YALANDI: "10 errors" metni "0 error" desenini İÇERİR, yani 10
            # hatalı bir derleme "başarılı" diye raporlanırdı. Sayıyı
            # yakalayıp SIFIR MI diye bakmak tek doğru yol.
            if ($text -match '(?<n>\d+)\s+errors?\b') {
                if ([int]$Matches['n'] -eq 0) {
                    Write-Step "$($item.Label) derlemesi başarılı"
                }
                else {
                    Write-Warn "$($item.Label) DERLENEMEDİ: $($Matches['n']) hata — yukarıdaki loga bak."
                    $script:CompileFailed = $true
                }
            }
            else {
                Write-Warn "$($item.Label): derleme sonucu log'dan OKUNAMADI — başarılı SAYILMIYOR."
                $script:CompileFailed = $true
            }
        }
        # Service derlenmezse Navigator'da hiç görünmez ve tek belirtisi
        # "geçmiş gelmiyor" olur. Sessizce geçmek yerine çıkış kodu veriyoruz.
        if ($script:CompileFailed) {
            Write-Warn ''
            Write-Warn 'EN AZ BİR DERLEME BAŞARISIZ — kurulum EKSİK.'
            $global:LASTEXITCODE = 1
        }
    }
}

# --- 5. Sonraki adımlar ----------------------------------------------------

Write-Host ''
Write-Step 'Kurulum tamam. Terminalde yapılması gerekenler:'
Write-Host '  1. Araçlar → Seçenekler → Uzman Danışmanlar →'
Write-Host '     "Allow DLL imports" (DLL içe aktarmalarına izin ver) İŞARETLİ olmalı.'
Write-Host '  2. Navigator''da Uzman Danışmanlar''ı yenile (sağ tık → Yenile).'
Write-Host '  3. SinyalCollector''ı bir grafiğe sürükle.'
Write-Host '  4. EA açılışta yerleşim doğrulaması yapar; Uzmanlar sekmesindeki'
Write-Host '     logu kontrol et — uyumsuzluk varsa EA başlamaz ve sebebini yazar.'
Write-Host ''
