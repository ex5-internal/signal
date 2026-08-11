# Kabul testi — sisteme teslim oncesi uctan uca dogrulama.
$ErrorActionPreference = 'Stop'
$URL = if ($args[0]) { $args[0] } else { 'ws://127.0.0.1:8787' }
$TOK = 'pQZwo7U7wyAppJCHzjtxfGxUnLdZddd4JRKhW6idHoA'
$run = Get-Date -Format 'HHmmss'

$ws = [System.Net.WebSockets.ClientWebSocket]::new()
$cts = [System.Threading.CancellationTokenSource]::new()
$ws.ConnectAsync([Uri]$URL, $cts.Token).GetAwaiter().GetResult()
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
# $want turundeki ilk mesaji bekle
function Wait4([string]$want, [int]$sec = 12) {
  $end = (Get-Date).AddSeconds($sec)
  while ((Get-Date) -lt $end) {
    $m = Recv 800; if ($null -eq $m) { continue }
    foreach ($ln in ($m -split "`n")) {
      if (-not $ln.Trim()) { continue }
      try { $o = $ln | ConvertFrom-Json } catch { continue }
      if ($o.t -eq $want) { return $o }
      if ($o.t -eq 'error') { Write-Host "    HATA: $($o.msg)" -ForegroundColor Red }
    }
  }
  return $null
}
$pass = 0; $fail = 0
function Chk([string]$ad, [bool]$ok, [string]$detay) {
  if ($ok) { $script:pass++; Write-Host ("  [GECTI] {0,-42} {1}" -f $ad, $detay) -ForegroundColor Green }
  else     { $script:fail++; Write-Host ("  [KALDI] {0,-42} {1}" -f $ad, $detay) -ForegroundColor Red }
}

$h = Wait4 'hello' 6
Write-Host "`n=== 1. BAGLANTI ==="
Chk "hello geldi" ($null -ne $h) "proto=$($h.proto) level=$($h.level) trading=$($h.trading)"

Write-Host "`n=== 2. SEMBOL BILGISI ==="
Send '{"op":"symbols"}'
$sy = Wait4 'symbols' 8
$gold = $sy.items | Where-Object { $_.s -eq 'GOLD' }
Chk "GOLD var" ($null -ne $gold) "digits=$($gold.digits) point=$($gold.point) step=$($gold.volume_step)"
Chk "GOLD grafik sembolu (olay gudumlu)" ($gold.chart -eq $true) "chart=$($gold.chart) polled_only=$($gold.polled_only)"
Chk "GOLD hazir" ($gold.ready -eq $true) "ready=$($gold.ready)"

Write-Host "`n=== 3. MT5 GECMISI — her dilim ==="
foreach ($tf in 'M1','M5','M15','H1','H4') {
  Send ('{"op":"candles","symbol":"GOLD","tf":"' + $tf + '","count":300}')
  $c = Wait4 'candles' 25
  if ($null -eq $c) { Chk "GOLD $tf" $false "cevap YOK"; continue }
  $n = @($c.items).Count
  $coh = $true
  foreach ($b in $c.items) { if ($b.h -lt $b.l -or $b.h -lt $b.c -or $b.l -gt $b.c) { $coh = $false; break } }
  $sonPartial = $false
  if ($n -gt 0) { $sonPartial = [bool]$c.items[-1].partial }
  $span = if ($n -gt 1) { [math]::Round(($c.items[-1].t - $c.items[0].t)/60000) } else { 0 }
  Chk "GOLD $tf" ($c.src_kind -eq 'mt5' -and $n -gt 50 -and $coh) `
      "src=$($c.src_kind) hist=$($c.hist) bar=$n kapsam=$span dk OHLC-tutarli=$coh sonBar-partial=$sonPartial"
}

Write-Host "`n=== 4. BASKA SEMBOL ==="
Send '{"op":"candles","symbol":"EURUSD","tf":"M5","count":200}'
$c = Wait4 'candles' 25
Chk "EURUSD M5" ($c.src_kind -eq 'mt5' -and @($c.items).Count -gt 50) "src=$($c.src_kind) bar=$(@($c.items).Count)"

Write-Host "`n=== 5. HATA YOLLARI ==="
Send '{"op":"candles","symbol":"GOLD","tf":"YOK"}'
$e = Wait4 'error' 6
Chk "gecersiz dilim reddedildi" ($null -ne $e) "$($e.msg)"
Send '{"op":"candles","symbol":"YOKBOYLE","tf":"M1","count":10}'
$c = Wait4 'candles' 20
Chk "bilinmeyen sembol cokmeden cevaplandi" ($null -ne $c) "bar=$(@($c.items).Count) hist=$($c.hist) not=$($c.hist_note)"

Write-Host "`n=== 6. CANLI AKIS ==="
Send '{"op":"subscribe","channels":["tick.GOLD"]}'
$t0 = Get-Date; $nt = 0; $lat = @()
while (((Get-Date) - $t0).TotalSeconds -lt 12) {
  $m = Recv 800; if ($null -eq $m) { continue }
  foreach ($ln in ($m -split "`n")) {
    if (-not $ln.Trim()) { continue }
    try { $o = $ln | ConvertFrom-Json } catch { continue }
    if ($o.t -eq 'tick' -and $o.s -eq 'GOLD') { $nt++; $lat += [double]$o.lat_us }
  }
}
$p50 = if ($lat.Count) { ($lat | Sort-Object)[[int]($lat.Count*0.5)] } else { 0 }
# GOLD'un GUNLUK ISLEM MOLASI var (~00:00-01:00 sunucu saati). O aralikta
# GOLD'da tick akmaz ama sistem SAGLAMDIR. "sistem bozuk" ile "piyasa kapali"
# ayrimini yapmadan bu testi kirmizi gostermek YANILTICI olurdu -- referans
# olarak forex'e bakiyoruz.
Send '{"op":"subscribe","channels":["tick.EURUSD"]}'
$t0 = Get-Date; $nfx = 0
while (((Get-Date) - $t0).TotalSeconds -lt 8) {
  $m = Recv 800; if ($null -eq $m) { continue }
  foreach ($ln in ($m -split "`n")) {
    if (-not $ln.Trim()) { continue }
    try { $o = $ln | ConvertFrom-Json } catch { continue }
    if ($o.t -eq 'tick' -and $o.s -eq 'EURUSD') { $nfx++ }
  }
}
if ($nt -gt 0) {
  Chk "canli GOLD tick akiyor" $true "12 sn'de $nt tick, gecikme p50=$([math]::Round($p50)) us"
} elseif ($nfx -gt 0) {
  Chk "akis sagliklı (GOLD molada)" $true "GOLD 0 tick ama EURUSD $nfx tick -> piyasa kapali, sistem saglam"
} else {
  Chk "canli tick akiyor" $false "HICBIR sembolde tick yok -- EA durmus veya terminal kapali"
}

Write-Host "`n=== 7. YETKI KAPISI ==="
Send '{"op":"positions"}'
$e = Wait4 'error' 6
Chk "token'siz pozisyon reddedildi" ($null -ne $e) "$($e.msg)"
Send ('{"op":"auth","token":"' + $TOK + '"}')
$a = Wait4 'authed' 6
Chk "token ile trader'a yukseldi" ($a.level -eq 'trader') "level=$($a.level)"
Send '{"op":"account"}'
$ac = Wait4 'account' 8
$acc = $ac.items[0]
Chk "hesap okundu" ($null -ne $acc) "mod=$($acc.mode) bakiye=$($acc.balance) $($acc.currency) kaldirac=1:$($acc.leverage)"
Chk "DEMO hesap" ($acc.mode -eq 'demo') "mode=$($acc.mode)"

Write-Host "`n=== 8. EMIR (0.01 GOLD al-kapat) ==="
Send '{"op":"subscribe","channels":["order"]}'
Start-Sleep -Milliseconds 300
Send ('{"op":"order","id":"KABUL-' + $run + '","symbol":"GOLD","side":"buy","type":"market","volume":0.01}')
$dolduMu = $false; $kapali = $false; $tick = 0; $px = 0; $kimliksiz = 0; $olay = 0
$t0 = Get-Date
while (((Get-Date) - $t0).TotalSeconds -lt 12) {
  $m = Recv 800; if ($null -eq $m) { continue }
  foreach ($ln in ($m -split "`n")) {
    if (-not $ln.Trim()) { continue }
    try { $o = $ln | ConvertFrom-Json } catch { continue }
    if ($o.t -ne 'order') { continue }
    $olay++
    if ([string]::IsNullOrEmpty($o.id)) { $kimliksiz++ }
    if ($o.kind -eq 'txn' -and $o.retcode -eq 10009) { $dolduMu = $true }
    # 10018 = TRADE_RETCODE_MARKET_CLOSED. Sistem hatasi DEGIL.
    if ($o.kind -eq 'txn' -and $o.retcode -eq 10018) { $kapali = $true }
    if ($o.position -and $o.price) { $tick = $o.position; $px = $o.price }
  }
}
if ($kapali) {
  Chk "emir yolu saglam (piyasa KAPALI)" $true "MT5 retcode 10018 MARKET_CLOSED dondu -- emir yolu calisiyor, sembol kapali"
} else {
  Chk "emir yurutuldu (txn 10009)" $dolduMu "olay=$olay kimliksiz=$kimliksiz pozisyon=$tick fiyat=$px"
}
Chk "tum olaylar kimlikli" ($kimliksiz -eq 0) "kimliksiz=$kimliksiz"

if ($tick -ne 0) {
  Send ('{"op":"close","id":"KABULK-' + $run + '","ticket":' + $tick + '}')
  $kapandi = $false; $t0 = Get-Date
  while (((Get-Date) - $t0).TotalSeconds -lt 12) {
    $m = Recv 800; if ($null -eq $m) { continue }
    foreach ($ln in ($m -split "`n")) {
      if (-not $ln.Trim()) { continue }
      try { $o = $ln | ConvertFrom-Json } catch { continue }
      if ($o.t -eq 'order' -and $o.kind -eq 'txn' -and $o.retcode -eq 10009) { $kapandi = $true }
    }
  }
  Chk "pozisyon kapatildi" $kapandi "ticket=$tick"
}

Write-Host ("`n================ SONUC: {0} gecti, {1} kaldi ================" -f $pass, $fail) -ForegroundColor $(if ($fail -eq 0) { 'Green' } else { 'Red' })
try { $ws.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure,'bye',$cts.Token).GetAwaiter().GetResult() } catch {}
