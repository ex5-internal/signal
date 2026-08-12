<#
.SYNOPSIS
  sinyald'i oturum acilisinda otomatik baslatan Zamanlanmis Gorev kurar.

.DESCRIPTION
  NEDEN WINDOWS SERVISI DEGIL — oturum izolasyonu

  Paylasilan bellek segmentleri `Local\` onekiyle olusturuluyor ve bu ad alani
  OTURUMA OZELDIR. Bu makinede olculdu:

      terminal64 (MT5)      SessionId = 2    <- etkilesimli oturum
      Windows servisleri    SessionId = 0

  Oturum 0'daki bir servis, oturum 2'de olusturulmus `Local\Sinyal.mt5-1.md`
  segmentini GOREMEZ. `sc create` ile kurulan bir servis acilir, hicbir sey
  bulamaz ve sonsuza kadar "EA bekleniyor" der.

  `Global\` ad alanina gecmek de cozmez: oraya YAZMAK SeCreateGlobalPrivilege
  ister ve MT5 normal kullanici olarak calistigi icin o hakka sahip degildir.

  Ayrica sinyald bir Windows servisi DEGIL: `StartServiceCtrlDispatcher`
  cagirmiyor, dolayisiyla `sc create` ile kaydedilse Servis Yoneticisi
  "zamaninda yanit vermedi" diyip oldururdu.

  Dogru arac: kullanicinin KENDI OTURUMUNDA calisan Zamanlanmis Gorev.

  YONETICI HAKKI GEREKMEZ — gorev kullanici duzeyinde kurulur.

.PARAMETER Token
  Kimlik dogrulama token'i. KULLANICI ORTAM DEGISKENINE yazilir, gorev
  tanimina GOMULMEZ: komut satiri makinedeki her kullaniciya
  `Get-CimInstance Win32_Process` ile gorunur olurdu.

.PARAMETER Kaldir
  Gorevi ve ortam degiskenini siler.

.EXAMPLE
  .\tools\gorev-kaydet.ps1 -Token 'pQZwo...'
  .\tools\gorev-kaydet.ps1 -Kaldir
#>
[CmdletBinding()]
param(
    [string]$Token,
    [string]$GorevAdi = 'Sinyal-Daemon',
    [string]$Betik = (Join-Path $PSScriptRoot 'sinyald-baslat.ps1'),
    [switch]$Kaldir
)

$ErrorActionPreference = 'Stop'
$me = "$env:USERDOMAIN\$env:USERNAME"

if ($Kaldir) {
    if (Get-ScheduledTask -TaskName $GorevAdi -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $GorevAdi -Confirm:$false
        Write-Host "Gorev silindi: $GorevAdi"
    } else {
        Write-Host "Gorev zaten yok: $GorevAdi"
    }
    [Environment]::SetEnvironmentVariable('SINYAL_TOKEN', $null, 'User')
    Write-Host 'SINYAL_TOKEN ortam degiskeni silindi.'
    Write-Host 'NOT: calisan sinyald sureci DURDURULMADI. Gerekirse:'
    Write-Host '  Get-Process sinyald | Stop-Process -Force'
    return
}

if (-not (Test-Path $Betik)) { throw "Baslatici bulunamadi: $Betik" }

if ($Token) {
    # HKCU\Environment — yalnizca bu kullanici okur. Gorev tanimina veya
    # komut satirina gommekten belirgin sekilde daha iyi, ama SIR DEGIL:
    # bu kullanici olarak calisan her sey okuyabilir.
    [Environment]::SetEnvironmentVariable('SINYAL_TOKEN', $Token, 'User')
    Write-Host 'SINYAL_TOKEN kullanici ortam degiskenine yazildi.'
} elseif (-not [Environment]::GetEnvironmentVariable('SINYAL_TOKEN', 'User')) {
    Write-Warning 'SINYAL_TOKEN tanimli DEGIL — sinyald kimlik dogrulama KAPALI baslar.'
    Write-Warning 'Uc aga acikken bu, emir yurutmeyi HERKESE acar.'
}

$pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source
if (-not $pwsh) { throw 'pwsh (PowerShell 7) bulunamadi. Betik PowerShell 5.1 ile calismaz.' }

$action = New-ScheduledTaskAction -Execute $pwsh `
    -Argument "-NoProfile -WindowStyle Hidden -File `"$Betik`"" `
    -WorkingDirectory (Split-Path $Betik -Parent)

# AtLogOn: gorev KULLANICI OTURUMUNDA calisir. AtStartup KULLANMA -- o,
# oturum 0'da calisir ve segmentleri goremez (yukaridaki gerekce).
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $me
$princ   = New-ScheduledTaskPrincipal -UserId $me -LogonType Interactive -RunLevel Limited
$set     = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999 `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask -TaskName $GorevAdi -Action $action -Trigger $trigger `
    -Principal $princ -Settings $set -Force | Out-Null

Write-Host ''
Write-Host "Gorev kuruldu: $GorevAdi" -ForegroundColor Green
Write-Host "  tetikleyici : oturum acilisi ($me)"
Write-Host "  calistirir  : $Betik"
Write-Host '  yeniden dene: her 1 dakikada, sinirsiz'
Write-Host ''
Write-Host 'Simdi test et (oturum kapatmadan):'
Write-Host "  Start-ScheduledTask -TaskName $GorevAdi"
Write-Host '  Get-NetTCPConnection -State Listen -LocalPort 8787,8789'
Write-Host ''
Write-Host 'Kaldirmak icin:'
Write-Host "  .\tools\gorev-kaydet.ps1 -Kaldir"
