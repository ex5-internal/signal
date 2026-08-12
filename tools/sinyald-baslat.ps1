<#
.SYNOPSIS
  sinyald baslatici — oturum acilisinda calisir, TEK ORNEK garantisi verir.

.DESCRIPTION
  NEDEN WINDOWS SERVISI DEGIL

  Paylasilan bellek segmentleri `Local\` onekiyle olusturuluyor ve bu ad alani
  OTURUMA OZELDIR. MT5 etkilesimli oturumda calisir (ornegin SessionId=2),
  Windows servisleri ise SessionId=0'da. Oturum 0'daki bir servis MT5'in
  olusturdugu segmenti GOREMEZ; acilir ve sonsuza kadar "EA bekleniyor" der.

  `Global\` ad alanina gecmek de cozmez: oraya yazmak SeCreateGlobalPrivilege
  ister ve MT5 normal kullanici olarak calistigi icin o hakka sahip degildir.

  Dogru arac: kullanicinin OTURUMUNDA calisan bir Zamanlanmis Gorev.

  NEDEN TEK ORNEK

  Halkalar SPSC (tek yazar, tek okur). Ikinci bir sinyald ayni halkayi okumaya
  kalkarsa sozlesme kirilir ve IKISI DE bozuk veri gorur -- ustelik sessizce.
  Bu betik zaten calisan bir ornek varsa YENISINI BASLATMAZ.

.PARAMETER Force
  Calisan ornegi durdurup yenisini baslatir.
#>
[CmdletBinding()]
param(
    [string]$Exe      = (Join-Path $PSScriptRoot '..\target\release\sinyald.exe'),
    [string]$Instance = 'mt5-1',
    [string]$Bind     = '0.0.0.0:8787',
    [string]$PaperBind= '0.0.0.0:8789',
    [string]$DataDir  = (Join-Path $PSScriptRoot '..\veri'),
    [string]$LogDir   = (Join-Path $env:LOCALAPPDATA 'Sinyal\log'),
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$Exe = (Resolve-Path $Exe -ErrorAction SilentlyContinue)?.Path ?? $Exe

if (-not (Test-Path $Exe)) {
    Write-Error "sinyald bulunamadi: $Exe`nOnce derle: cargo build --release -p sinyal-core"
    exit 1
}

# --- tek ornek ---
$mevcut = @(Get-Process sinyald -ErrorAction SilentlyContinue)
if ($mevcut.Count -gt 0) {
    if (-not $Force) {
        Write-Host "sinyald ZATEN CALISIYOR (PID $($mevcut.Id -join ', ')) — yenisi baslatilmadi." -ForegroundColor Yellow
        Write-Host 'Ikinci bir ornek SPSC sozlesmesini kirar ve IKISI DE bozuk veri gorur.'
        Write-Host 'Yeniden baslatmak icin: -Force'
        exit 0
    }
    Write-Host "-Force: calisan ornek durduruluyor (PID $($mevcut.Id -join ', '))"
    $mevcut | Stop-Process -Force
    Start-Sleep -Milliseconds 800
}

# --- token ---
# Token KAYNAK KODA veya gorev tanimina GOMULMEZ: komut satiri makinedeki her
# kullaniciya `Get-CimInstance Win32_Process` ile gorunur. Ortam degiskeninden
# okunuyor; yoksa kimlik dogrulama KAPALI baslar ve bu ACIKCA soylenir.
$token = $env:SINYAL_TOKEN

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd'
$out = Join-Path $LogDir "sinyald-$stamp.out.log"
$err = Join-Path $LogDir "sinyald-$stamp.err.log"

$argv = @(
    '--instance', $Instance
    '--bind', $Bind
    '--enable-trading'
    '--record', $DataDir
    '--paper-bind', $PaperBind
)
if ($token) { $argv += @('--token', $token) }

Write-Host "sinyald baslatiliyor:"
Write-Host "  ikili   : $Exe"
Write-Host "  canli   : ws://$Bind"
Write-Host "  paper   : ws://$PaperBind"
Write-Host "  kayit   : $DataDir"
Write-Host "  log     : $LogDir"
if ($token) {
    Write-Host '  kimlik  : token gerekli (SINYAL_TOKEN ortam degiskeninden)'
} else {
    Write-Host '  kimlik  : YOK — SINYAL_TOKEN tanimli degil, emir yurutme HERKESE ACIK' -ForegroundColor Red
}

$p = Start-Process -FilePath $Exe -ArgumentList $argv `
        -RedirectStandardOutput $out -RedirectStandardError $err `
        -WindowStyle Hidden -PassThru

Start-Sleep -Seconds 4
if ($p.HasExited) {
    Write-Error "sinyald HEMEN CIKTI (kod $($p.ExitCode)). Son hata satirlari:"
    Get-Content $err -Tail 10 -ErrorAction SilentlyContinue
    exit 1
}

# Gercekten dinliyor mu — surecin ayakta olmasi yetmez.
$port = [int]($Bind -split ':')[-1]
$dinliyor = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
if (-not $dinliyor) {
    Write-Error "sinyald calisiyor (PID $($p.Id)) ama $port DINLEMIYOR. Log: $err"
    exit 1
}
Write-Host "TAMAM — PID $($p.Id), $port dinliyor." -ForegroundColor Green
