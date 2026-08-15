<#
.SYNOPSIS
  sinyald nöbetçisi — dakikada bir bakar, ölmüşse kaldırır.

.DESCRIPTION
  NEDEN VAR

  14-15 Ağustos 2026'da sinyald iki kez sessizce öldü: kendi stderr'inde iz
  yok, Windows olay günlüğünde kayıt yok. Otopsi:

    makine ayakta        80,7 saat  -> yeniden başlatma YOK
    kapanma/oturum olayı YOK
    Windows Update       yalnız Defender tanımı (restart istemez)
    MT5                  67,1 saat KESİNTİSİZ

  Yani ne makine ne terminal düştü; yalnız daemon gitti. Sebep KESİN olarak
  bulunamadı. En güçlü aday: süreç geçici bir kabuğun çocuğu olarak doğduğunda
  o kabuğun süreç ağacı temizlenince birlikte gidiyor (Windows Job Object
  KILL_ON_JOB_CLOSE bunu izsiz yapar — ne panik, ne olay kaydı). Desen bunu
  destekliyor: günlerce sorunsuz çalışan örnekler zamanlanmış görevden
  doğmuştu; ölenler elle bir kabuktan başlatılmıştı.

  NEDEN WINDOWS SERVİSİ DEĞİL

  Tekrar: OLMAZ. Paylaşılan bellek segmentleri `Local\` önekiyle oluşuyor ve
  bu ad alanı OTURUMA ÖZELDİR. MT5 etkileşimli oturumda, servisler Oturum
  0'da çalışır; servis segmenti GÖREMEZ ve sonsuza kadar "EA bekleniyor" der.
  NSSM de bunu değiştirmez — sorun sarmalayıcıda değil, oturum yalıtımında.

  BU BETİĞİN İŞİ

  Sebebi bulmak değil, sonucu ortadan kaldırmak. Dakikada bir port dinleniyor
  mu diye bakar; dinlenmiyorsa başlatır ve NEDEN başlattığını loglar. Sebep
  ne olursa olsun kesinti bir dakikayla sınırlanır.

.PARAMETER Port
  Dinlenmesi beklenen canlı port.
#>
[CmdletBinding()]
param(
    [int]$Port = 8787,
    [string]$Exe = 'D:\Projeler\Sinyal\target\release\sinyald.exe',
    [string]$DataDir = 'D:\Projeler\Sinyal\veri',
    [string]$LogDir = (Join-Path $env:LOCALAPPDATA 'Sinyal\log')
)

$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$nobet = Join-Path $LogDir 'nobetci.log'

function Yaz([string]$m) {
    $satir = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m
    Add-Content -Path $nobet -Value $satir
}

# --- port dinleniyor mu ---
#
# Süreç var mı diye bakmak YETMEZ: süreç ayakta olup soketi açmamış olabilir
# (zombi). Tek anlamlı sağlık ölçüsü portun dinlenmesi.
$dinliyor = [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
if ($dinliyor) { exit 0 }

$surec = @(Get-Process sinyald -ErrorAction SilentlyContinue)
if ($surec.Count -gt 0) {
    # Süreç var ama dinlemiyor: zombi. Öldür, temiz başlat.
    #
    # Sessizce ikinci bir örnek başlatmak SPSC sözleşmesini kırar (tek yazar,
    # tek okur) ve İKİSİ DE bozuk veri görür — o yüzden önce eskisi gider.
    Yaz "ZOMBI: surec var (PID $($surec.Id -join ',')) ama $Port dinlemiyor — olduruluyor"
    $surec | Stop-Process -Force
    Start-Sleep -Seconds 2
}
else {
    Yaz "OLU: sinyald sureci yok, $Port dinlenmiyor"
}

# Token komut satırına GÖMÜLMEZ: `Get-CimInstance Win32_Process` ile
# makinedeki her kullanıcıya görünür. Ortam değişkeninden okunur; yoksa
# kimlik doğrulama KAPALI başlar ve bu AÇIKÇA loglanır.
$token = $env:SINYAL_TOKEN
$argv = @(
    '--instance', 'mt5-1'
    '--bind', "0.0.0.0:$Port"
    '--enable-trading'
    '--record', $DataDir
    '--paper-bind', '0.0.0.0:8789'
)
if ($token) { $argv += @('--token', $token) }
else { Yaz "UYARI: SINYAL_TOKEN yok — emir yurutme KIMLIK DOGRULAMASIZ aciliyor" }

$stamp = Get-Date -Format 'yyyyMMdd'
$out = Join-Path $LogDir "sinyald-$stamp.out.log"
$err = Join-Path $LogDir "sinyald-$stamp.err.log"

$p = Start-Process -FilePath $Exe -ArgumentList $argv `
        -RedirectStandardOutput $out -RedirectStandardError $err `
        -WindowStyle Hidden -PassThru

Start-Sleep -Seconds 5
if ($p.HasExited) {
    Yaz "BASLATILAMADI: hemen cikti, kod=$($p.ExitCode)"
    exit 1
}

# Süreç ayakta olması yetmez — gerçekten dinliyor mu?
$ok = [bool](Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
if ($ok) { Yaz "BASLATILDI: PID $($p.Id), $Port dinliyor" }
else     { Yaz "SORUNLU: PID $($p.Id) ayakta ama $Port hala dinlemiyor" }
