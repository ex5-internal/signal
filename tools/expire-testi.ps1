<#
.SYNOPSIS
  `expire_sn` ölçümü — tüketici sistemin 2026-08-17 tarihli a/b/c/d protokolü.

.DESCRIPTION
  Tüketici sistem `expire_sn`in sessizce düştüğünü ölçtü: gönderilen emir
  brokerda `expiration: null` ile duruyor ve süresiz bekliyordu.

  Bu betik iddiayı ÖLÇER, varsaymaz:
    a) piyasadan UZAK bir buy_limit, `expire_sn` ile
    b) hemen `orders` → `expiration` DOLU mu, broker saatine göre makul mü
    c) süre geçince `orders` → emir listede OLMAMALI, `expired` olayı gelmeli
    d) ölçülen GERÇEK ömür (saniye)

  Fiyat piyasadan uzak seçilir ki emir DOLMASIN: dolan bir emir süre dolumunu
  ölçemez ve "başarıyı başarısızlıktan ayıramayan" bir test olurdu.

.PARAMETER Url
  ws://127.0.0.1:8787 (canlı) ya da ws://127.0.0.1:8789 (paper).
#>
[CmdletBinding()]
param(
    [string]$Url = 'ws://127.0.0.1:8787',
    [int]$ExpireSn = 120,
    [int]$BeklemeSn = 300,
    # Replay hızlandırılmış oynatımda akış saati wall-clock'tan çok daha hızlı
    # akar; yoklama aralığı da ona göre kısalmalı yoksa ölüm anı kaçar.
    [int]$SorguSn = 20,
    [string]$Token
)

$ErrorActionPreference = 'Stop'
$TOK = if ($Token) { $Token } else { [Environment]::GetEnvironmentVariable('SINYAL_TOKEN', 'User') }
if (-not $TOK) { $TOK = $env:SINYAL_TOKEN }
if (-not $TOK) { throw 'SINYAL_TOKEN yok' }

$ws = [System.Net.WebSockets.ClientWebSocket]::new()
$cts = [System.Threading.CancellationTokenSource]::new()
$ws.ConnectAsync([Uri]$Url, $cts.Token).GetAwaiter().GetResult()
$buf = [byte[]]::new(1048576); $seg = [ArraySegment[byte]]::new($buf); $script:pend = $null

function Recv([int]$ms) {
    if ($null -eq $script:pend) { $script:pend = $ws.ReceiveAsync($seg, $cts.Token) }
    if (-not $script:pend.Wait($ms)) { return $null }
    $r = $script:pend.Result; $script:pend = $null
    [Text.Encoding]::UTF8.GetString($buf, 0, $r.Count)
}
function Send([string]$j) {
    $b = [Text.Encoding]::UTF8.GetBytes($j)
    [void]$ws.SendAsync([ArraySegment[byte]]::new($b), [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts.Token).GetAwaiter().GetResult()
}
# Gelen her satırı biriktir; emir olayları test boyunca KAYBOLMAMALI.
$script:olaylar = @()
function Pump([int]$ms) {
    $m = Recv $ms
    if ($null -eq $m) { return @() }
    $cikan = @()
    foreach ($ln in ($m -split "`n")) {
        if (-not $ln.Trim()) { continue }
        try { $o = $ln | ConvertFrom-Json } catch { continue }
        if ($o.t -eq 'order') { $script:olaylar += $o }
        $cikan += $o
    }
    return $cikan
}
function Wait4([string]$want, [int]$sec = 12) {
    $end = (Get-Date).AddSeconds($sec)
    while ((Get-Date) -lt $end) {
        foreach ($o in (Pump 800)) {
            if ($o.t -eq $want) { return $o }
            if ($o.t -eq 'error') { Write-Host "    HATA: $($o.msg)" -ForegroundColor Red }
        }
    }
    return $null
}

Write-Host "`n=== $Url ===" -ForegroundColor Cyan
$null = Wait4 'hello' 6
Send ('{"op":"auth","token":"' + $TOK + '"}')
$a = Wait4 'authed' 6
if ($null -eq $a) { throw 'auth basarisiz' }
Write-Host "  authed: $($a.level)"

Send '{"op":"subscribe","channels":["order","tick.GOLD"]}'

# --- broker saatini ve fiyatı TICK'ten oku -------------------------------
# Yerel saat KULLANILMAZ: ölçülmüş 3 saatlik ofset hatası tam da oradan gelir.
$tick = $null
$end = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $end -and $null -eq $tick) {
    foreach ($o in (Pump 800)) { if ($o.t -eq 'tick' -and $o.s -eq 'GOLD') { $tick = $o; break } }
}
if ($null -eq $tick) { throw 'GOLD tick gelmedi — piyasa kapali olabilir' }
$brokerSn0 = [int64]([double]$tick.ms / 1000)
Write-Host ("  broker saati: {0}  (bid {1} / ask {2})" -f $brokerSn0, $tick.b, $tick.a)

# --- a) piyasadan UZAK buy_limit ----------------------------------------
$id = 'EXP-' + (Get-Date -Format 'HHmmss')
$fiyat = [math]::Round([double]$tick.b - 15.0, 2)   # ~1500 point asagi: dolmaz
$emir = '{"op":"order","id":"' + $id + '","action":"pending","type":"buy_limit",' +
        '"symbol":"GOLD","volume":0.01,"price":' + $fiyat + ',"expire_sn":' + $ExpireSn + '}'
Write-Host "`n  a) GONDERILEN: $emir"
$gonderimSn = $brokerSn0
Send $emir
$q = Wait4 'order' 10
Write-Host ("     ilk cevap: kind=$($q.kind) retcode=$($q.retcode) $($q.msg)")

# --- b) hemen orders ------------------------------------------------------
Start-Sleep -Seconds 2
Send '{"op":"orders"}'
$ords = Wait4 'orders' 12
$benim = $ords.items | Where-Object { $_.symbol -eq 'GOLD' -and [double]$_.price -eq $fiyat }
Write-Host "`n  b) BROKERDAKI HALI:"
if ($null -eq $benim) {
    Write-Host "     EMIR BULUNAMADI (bekleyen: $(@($ords.items).Count))" -ForegroundColor Red
} else {
    $exp = $benim.expiration
    Write-Host ("     ticket={0} expiration={1} client_id={2}" -f $benim.ticket, $(if($null -eq $exp){'null'}else{$exp}), $benim.client_id)
    if ($null -eq $exp -or $exp -eq 0) {
        Write-Host "     >>> KUSUR: expiration BOS — alan uygulanmamis" -ForegroundColor Red
    } else {
        $omur = [int64]$exp - $brokerSn0
        $renk = if ($omur -ge $ExpireSn -and $omur -lt $ExpireSn + 120) { 'Green' } else { 'Red' }
        Write-Host ("     >>> istenen {0} sn, kurulan {1} sn sonrasi" -f $ExpireSn, $omur) -ForegroundColor $renk
    }
}

# --- c/d) sureyi bekle ----------------------------------------------------
Write-Host "`n  c) $BeklemeSn sn'ye kadar bekleniyor (expired olayi / listeden dusme)..."
$olduSn = $null; $expiredOlay = $null
$son = (Get-Date).AddSeconds($BeklemeSn)
$sonSorgu = Get-Date
while ((Get-Date) -lt $son) {
    foreach ($o in (Pump 500)) {
        if ($o.t -eq 'tick' -and $o.s -eq 'GOLD') { $script:sonTickSn = [int64]([double]$o.ms / 1000) }
        if ($o.t -eq 'order' -and $o.kind -eq 'expired') {
            $expiredOlay = $o
            if ($null -eq $olduSn) { $olduSn = $script:sonTickSn }
        }
    }
    if (((Get-Date) - $sonSorgu).TotalSeconds -ge $SorguSn) {
        $sonSorgu = Get-Date
        Send '{"op":"orders"}'
        $ords = Wait4 'orders' 8
        $hala = $ords.items | Where-Object { $_.symbol -eq 'GOLD' -and [double]$_.price -eq $fiyat }
        $gecen = $script:sonTickSn - $gonderimSn
        Write-Host ("     {0,4} sn: listede={1}  expired-olayi={2}" -f $gecen, $(if($hala){'EVET'}else{'hayir'}), $(if($expiredOlay){'geldi'}else{'yok'}))
        if ($null -eq $hala) { if ($null -eq $olduSn) { $olduSn = $script:sonTickSn }; break }
    }
}

Write-Host "`n  d) SONUC:" -ForegroundColor Cyan
if ($olduSn) {
    Write-Host ("     ISTENEN {0} sn — OLCULEN {1} sn" -f $ExpireSn, ($olduSn - $gonderimSn)) -ForegroundColor Green
} else {
    Write-Host ("     EMIR HALA YASIYOR — en az {0} sn (istenen {1})" -f ($script:sonTickSn - $gonderimSn), $ExpireSn) -ForegroundColor Red
}
Write-Host ("     expired olayi: {0}" -f $(if($expiredOlay){"kind=$($expiredOlay.kind) ticket=$($expiredOlay.order)"}else{'GELMEDI'}))

# --- temizlik -------------------------------------------------------------
#
# Bırakılan emir sonraki ölçümü kirletir. `cancel` BİLETLE çalışır; toplu bir
# op yok. Ve `cancel` DOLMUŞ bir emri geri almaz (2026-08-14'te ölçüldü,
# 42 pozisyonluk kaza) — o yüzden burada yalnız BEKLEYEN emirler iptal edilir
# ve sonuç sayılarak doğrulanır, "0 bekleyen" tek başına kanıt sayılmaz.
Send '{"op":"orders"}'
$ords = Wait4 'orders' 8
$kalan = @($ords.items | Where-Object { $_.symbol -eq 'GOLD' -and [double]$_.price -eq $fiyat })
foreach ($k in $kalan) {
    Write-Host "  temizlik: ticket $($k.ticket) iptal ediliyor"
    Send ('{"op":"cancel","id":"tmz-' + $id + '-' + $k.ticket + '","ticket":' + $k.ticket + '}')
    $null = Wait4 'order' 8
}
Start-Sleep -Seconds 2
Send '{"op":"orders"}'
$ords = Wait4 'orders' 8
$hala = @($ords.items | Where-Object { $_.symbol -eq 'GOLD' -and [double]$_.price -eq $fiyat })
Write-Host ("  temizlik sonrasi BU testin emri: {0} (toplam bekleyen: {1})" -f $hala.Count, @($ords.items).Count)
$ws.Dispose()
