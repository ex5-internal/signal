<#
.SYNOPSIS
  RECONCILE ölçümü — gerçekleşmiş `profit`/`commission`/`swap` telde geliyor mu.

.DESCRIPTION
  `OnTradeTransaction` SICAK YOLDUR: terminalin işlem kuyruğu 1024 elemanlı ve
  işleyici yavaşlarsa ESKİ olaylar sessizce ezilir. `HistoryDealGetDouble`
  orada çağrılamaz. Ama gerçekleşmiş sonuç YALNIZCA geçmişte vardır. Çözüm
  `OnTimer` içinde, sıcak yol dışında, seyrek bir mutabakat turu.

  Bu betik 0.01 GOLD açar, kapatır ve mutabakat olaylarını bekler.
  `reconciled: true` taşıyan olayda `profit`/`commission`/`swap` DOLU olmalı.

  Canlı olayda bu alanlar HİÇ görünmemeli: sıfır göndermek "kâr sıfırdı"
  dedirtirdi, oysa ölçüm hiç yapılmamıştır.
#>
[CmdletBinding()]
param(
    [string]$Url = 'ws://127.0.0.1:8787',
    [double]$Volume = 0.01,
    [int]$BeklemeSn = 60
)

$ErrorActionPreference = 'Stop'
$TOK = [Environment]::GetEnvironmentVariable('SINYAL_TOKEN', 'User')
if (-not $TOK) { $TOK = $env:SINYAL_TOKEN }
if (-not $TOK) { throw 'SINYAL_TOKEN yok' }

$ws = [System.Net.WebSockets.ClientWebSocket]::new()
$cts = [System.Threading.CancellationTokenSource]::new()
$ws.ConnectAsync([Uri]$Url, $cts.Token).GetAwaiter().GetResult()
$buf = [byte[]]::new(1048576); $seg = [ArraySegment[byte]]::new($buf); $script:pend = $null

function Al([int]$ms) {
    if ($null -eq $script:pend) { $script:pend = $ws.ReceiveAsync($seg, $cts.Token) }
    if (-not $script:pend.Wait($ms)) { return $null }
    $r = $script:pend.Result; $script:pend = $null
    [Text.Encoding]::UTF8.GetString($buf, 0, $r.Count)
}
function Yolla([string]$j) {
    $b = [Text.Encoding]::UTF8.GetBytes($j)
    [void]$ws.SendAsync([ArraySegment[byte]]::new($b), [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts.Token).GetAwaiter().GetResult()
}
$script:emirOlaylari = @()
function Pompala([int]$ms) {
    $m = Al $ms
    if ($null -eq $m) { return @() }
    $c = @()
    foreach ($ln in ($m -split "`n")) {
        if (-not $ln.Trim()) { continue }
        try { $o = $ln | ConvertFrom-Json } catch { continue }
        if ($o.t -eq 'order') { $script:emirOlaylari += $o }
        $c += $o
    }
    return $c
}
function Bekle([string]$want, [int]$sec = 15) {
    $end = (Get-Date).AddSeconds($sec)
    while ((Get-Date) -lt $end) {
        foreach ($o in (Pompala 700)) {
            if ($o.t -eq $want) { return $o }
            if ($o.t -eq 'error') { Write-Host "    HATA: $($o.msg)" -ForegroundColor Red }
        }
    }
    return $null
}
function Yaz($o) {
    $ek = ''
    if ($null -ne $o.reconciled) { $ek += " reconciled=$($o.reconciled)" }
    if ($null -ne $o.profit)     { $ek += " profit=$($o.profit)" }
    if ($null -ne $o.commission) { $ek += " commission=$($o.commission)" }
    if ($null -ne $o.swap)       { $ek += " swap=$($o.swap)" }
    "    kind={0,-9} order={1} deal={2} pos={3} vol={4} price={5}{6}" -f `
        $o.kind, $o.order, $o.deal, $o.position, $o.volume, $o.price, $ek
}

Write-Host "`n=== $Url ===" -ForegroundColor Cyan
$null = Bekle 'hello' 8
Yolla ('{"op":"auth","token":"' + $TOK + '"}')
$a = Bekle 'authed' 8
if ($null -eq $a) { throw 'auth basarisiz' }
Write-Host "  authed: $($a.level)"
Yolla '{"op":"subscribe","channels":["order"]}'
Start-Sleep -Seconds 1

# --- 1. AÇ --------------------------------------------------------------
$id = 'REC-' + (Get-Date -Format 'HHmmss')
Write-Host "`n  1) ACILIYOR: $Volume GOLD market buy  (id=$id)"
Yolla ('{"op":"order","id":"' + $id + '","symbol":"GOLD","side":"buy","type":"market","volume":' + $Volume + '}')

$poz = 0
$end = (Get-Date).AddSeconds(25)
while ((Get-Date) -lt $end -and $poz -eq 0) {
    foreach ($o in (Pompala 700)) {
        if ($o.t -eq 'order') { Yaz $o | Write-Host
            if ($o.kind -eq 'txn' -and $o.position -gt 0) { $poz = $o.position } }
    }
}
if ($poz -eq 0) { $ws.Dispose(); throw 'pozisyon acilmadi' }
Write-Host "     -> pozisyon $poz" -ForegroundColor Green

# --- 2. KAPAT -----------------------------------------------------------
Start-Sleep -Seconds 3
Write-Host "`n  2) KAPATILIYOR: ticket $poz"
Yolla ('{"op":"close","id":"' + $id + '-k","ticket":' + $poz + '}')

# --- 3. MUTABAKATI BEKLE ------------------------------------------------
# ReconcileSec varsayılan 5 sn; sıcak yol DIŞINDA çalışır, o yüzden gecikmeli.
Write-Host "`n  3) mutabakat bekleniyor ($BeklemeSn sn)..."
$end = (Get-Date).AddSeconds($BeklemeSn)
$mutabakat = @()
while ((Get-Date) -lt $end) {
    foreach ($o in (Pompala 700)) {
        if ($o.t -eq 'order') {
            Yaz $o | Write-Host
            if ($o.reconciled -eq $true) { $mutabakat += $o }
        }
    }
    if ($mutabakat.Count -ge 2) { break }   # giriş + çıkış deal'i
}

# --- SONUÇ ---------------------------------------------------------------
Write-Host "`n  SONUC:" -ForegroundColor Cyan
if ($mutabakat.Count -eq 0) {
    Write-Host "     MUTABAKAT OLAYI GELMEDI — profit/commission/swap telde YOK" -ForegroundColor Red
} else {
    $tp = ($mutabakat | Measure-Object -Property profit -Sum).Sum
    $tc = ($mutabakat | Measure-Object -Property commission -Sum).Sum
    $ts = ($mutabakat | Measure-Object -Property swap -Sum).Sum
    Write-Host ("     {0} mutabakat olayi" -f $mutabakat.Count) -ForegroundColor Green
    Write-Host ("     TOPLAM  profit={0}  commission={1}  swap={2}" -f $tp, $tc, $ts) -ForegroundColor Green
    Write-Host "     >>> Bu sayilari MT5 > Gecmis sekmesiyle BIREBIR karsilastir."
}

# Canlı olayda bu alanlar HİÇ görünmemeli.
$canliKirli = $script:emirOlaylari | Where-Object {
    $_.reconciled -ne $true -and ($null -ne $_.profit -or $null -ne $_.commission -or $null -ne $_.swap)
}
if ($canliKirli) {
    Write-Host "     KUSUR: canli olayda gerceklesmis alan gorundu — 'olcum yok' ile 'sifir' ayrimi bozuk" -ForegroundColor Red
} else {
    Write-Host "     canli olaylarda gerceklesmis alan YOK (dogru)" -ForegroundColor Green
}
$ws.Dispose()
