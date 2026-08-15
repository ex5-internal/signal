<#
.SYNOPSIS
  sinyald nöbetçisi — SÜREKLİ ÇALIŞAN döngü. Görev Zamanlayıcı kullanmaz.

.DESCRIPTION
  NEDEN GÖREV ZAMANLAYICI DEĞİL

  Bu makinede Görev Zamanlayıcı etkileşimli görevleri ÇALIŞTIRMIYOR. Ölçüldü:
  `cmd.exe /c echo` kadar basit bir görev bile `LogonType Interactive` ile
  `0x800710E0` ("işleç veya yönetici isteği reddetti") veriyor ve eylemi hiç
  yürütmüyor. S4U biçimi kaydedilemiyor bile. Bir politika ya da güvenlik
  ürünü engelliyor.

  Bu yüzden `Sinyal-Daemon` görevi 12 Ağustos'ta kaydedilmiş olmasına rağmen
  bir kez bile çalışmadı — ve daemon öldüğünde onu kaldıran hiçbir şey yoktu.

  BU BETİĞİN YERİ

  Başlangıç klasöründen açılır (oturum açılışında Explorer başlatır), sonsuza
  kadar döner. Zamanlayıcıya hiç dokunmaz.

  ÖNEMLİ: bu betik GEÇİCİ BİR KABUKTAN başlatılmamalı. Windows iş nesnesi
  (Job Object) KILL_ON_JOB_CLOSE ile kurulmuşsa, başlatan kabuk kapanınca
  çocukları da izsiz gider — ne panik, ne olay kaydı. 14-15 Ağustos'ta
  daemon'un iki kez sessizce ölmesinin en güçlü adayı buydu. Elle başlatmak
  gerekirse Explorer üzerinden başlatın (bkz. `sinyald-nobet-baslat.cmd`).

  NEDEN WINDOWS SERVİSİ DEĞİL

  Olmaz. Paylaşılan bellek `Local\` önekiyle oluşuyor, bu ad alanı OTURUMA
  ÖZEL. MT5 etkileşimli oturumda, servisler Oturum 0'da; servis segmenti
  GÖREMEZ. NSSM de bunu değiştirmez — sorun sarmalayıcıda değil, oturum
  yalıtımında.
#>
[CmdletBinding()]
param(
    [int]$Port = 8787,
    [int]$AralikSn = 60,
    [string]$Exe = 'D:\Projeler\Sinyal\target\release\sinyald.exe',
    [string]$DataDir = 'D:\Projeler\Sinyal\veri',
    [string]$LogDir = (Join-Path $env:LOCALAPPDATA 'Sinyal\log')
)

$ErrorActionPreference = 'Continue'
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$nobet = Join-Path $LogDir 'nobetci.log'

function Yaz([string]$m) {
    $satir = '{0}  {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m
    Add-Content -Path $nobet -Value $satir -ErrorAction SilentlyContinue
}

# --- TEK ÖRNEK ---
#
# İki nöbetçi aynı anda dönerse ikisi de "ölü" görüp iki daemon başlatabilir.
# Halkalar SPSC (tek yazar, tek okur); ikinci bir okuyucu sözleşmeyi kırar ve
# İKİSİ DE bozuk veri görür — üstelik sessizce. Mutex bunu imkânsız kılar.
$mutex = New-Object System.Threading.Mutex($false, 'Global\SinyalNobetci')
if (-not $mutex.WaitOne(0)) {
    Yaz 'BASLAMADI: baska bir nobetci zaten calisiyor'
    exit 0
}

Yaz "NOBETCI BASLADI: PID $PID, port $Port, aralik ${AralikSn}sn"

try {
    while ($true) {
        try {
            $dinliyor = [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)

            if (-not $dinliyor) {
                # Süreç var ama dinlemiyorsa ZOMBİ: öldür, temiz başlat.
                # Sağlık ölçüsü portun dinlenmesidir, sürecin varlığı değil.
                $eski = @(Get-Process sinyald -ErrorAction SilentlyContinue)
                if ($eski.Count -gt 0) {
                    Yaz "ZOMBI: PID $($eski.Id -join ',') var ama $Port dinlemiyor — olduruluyor"
                    $eski | Stop-Process -Force -ErrorAction SilentlyContinue
                    Start-Sleep -Seconds 2
                }
                else {
                    Yaz "OLU: sinyald yok, $Port dinlenmiyor"
                }

                # Token komut satırına GÖMÜLMEZ: Win32_Process ile makinedeki
                # her kullanıcıya görünür. Ortamdan okunur; yoksa kimlik
                # doğrulama KAPALI başlar ve bu AÇIKÇA loglanır.
                $token = [Environment]::GetEnvironmentVariable('SINYAL_TOKEN', 'User')
                if (-not $token) { $token = $env:SINYAL_TOKEN }

                $argv = @(
                    '--instance', 'mt5-1'
                    '--bind', "0.0.0.0:$Port"
                    '--enable-trading'
                    '--record', $DataDir
                    '--paper-bind', '0.0.0.0:8789'
                )
                if ($token) { $argv += @('--token', $token) }
                else { Yaz 'UYARI: SINYAL_TOKEN yok — emir yurutme KIMLIK DOGRULAMASIZ aciliyor' }

                $stamp = Get-Date -Format 'yyyyMMdd'
                $p = Start-Process -FilePath $Exe -ArgumentList $argv `
                        -RedirectStandardOutput (Join-Path $LogDir "sinyald-$stamp.out.log") `
                        -RedirectStandardError  (Join-Path $LogDir "sinyald-$stamp.err.log") `
                        -WindowStyle Hidden -PassThru

                Start-Sleep -Seconds 5
                if ($p.HasExited) {
                    Yaz "BASLATILAMADI: hemen cikti, kod=$($p.ExitCode)"
                }
                else {
                    # Süreç ayakta olması yetmez — gerçekten dinliyor mu?
                    # "Başarı" ile "başarısızlık"ı ayırt edemeyen bir kontrol
                    # yazmak bu projede zaten bir kez pahalıya patladı.
                    $ok = [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
                    if ($ok) { Yaz "BASLATILDI: PID $($p.Id), $Port dinliyor" }
                    else     { Yaz "SORUNLU: PID $($p.Id) ayakta ama $Port hala dinlemiyor" }
                }
            }
        }
        catch {
            # Döngü ASLA ölmemeli: nöbetçinin kendisi düşerse geriye hiçbir
            # kurtarma katmanı kalmıyor.
            Yaz "DONGU HATASI (yutuldu): $($_.Exception.Message)"
        }

        Start-Sleep -Seconds $AralikSn
    }
}
finally {
    $mutex.ReleaseMutex()
    $mutex.Dispose()
}
