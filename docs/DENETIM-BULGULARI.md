# Denetim bulgulari — koruma yolu

Bu dosya **otomatik uretildi**: 37 ajanli salt-okunur adversaryal kod denetiminin
ciktisi. 5 ariza mercegi (dolum-SL penceresi, reddedilme yollari, bilesen olumleri,
kismi/coklu pozisyon, cifte tetik) + her iddiayi CURUTMEYE calisan ikinci tur.

Tarih: **2026-08-13** — 32 iddia denetlendi, **23 kaldi**, 9 curutuldu.

> ⚠️ Bu bulgular **kodda okundu**. Ikisi canlida dogrulandi (bkz. [OLCUMLER.md](OLCUMLER.md) §8a, §8b);
> geri kalani **olculmedi**. Duzeltmeye gecmeden once ilgili satirlari yeniden dogrulayin —
> denetim sirasinda API.md canli olarak duzenleniyordu, satir numaralari kaymis olabilir.

---

## 1. [KRITIK] Emirle BİRLİKTE SL gönderme yolu KODDA VAR ama API.md'de hiç yok — tüketici zorunlu olarak iki adımlı (korumasız pencereli) yolu kullanıyor

**Yer:** `D:\Projeler\Sinyal\API.md:490`  |  **Mercek:** zaman-penceresi

**Senaryo**

1) Sinyal sistemi API.md'yi okur; broker'a stop koymanın TEK yolu olarak `modify_sltp`i öğrenir (API.md:490, 502). 2) `{"op":"order",...}` gönderir — `sl` alanını hiç doldurmaz, çünkü var olduğunu bilmez. 3) Emir dolar; pozisyon SL=0 ile açılır. 4) Ancak dolum olayı (`txn`) gelip pozisyon bileti öğrenildikten SONRA `modify_sltp` gönderebilir. 5) Oysa `req.sl` doldurulmuş olsaydı MT5 emri ve stop'u TEK istekte işlerdi ve pencere hiç doğmazdı — üstelik geçersiz stop halinde MT5 tüm isteği 10016 ile reddeder, yani korumasız pozisyon da oluşmazdı. Belgelenmemiş olduğu için bu güvenli yol hiç kullanılmıyor.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — sadece API.md'ye yazi eklemek; KOD DEGISIKLIGI GEREKMIYOR.

(a) API.md §4, emir orneklerinin oldugu bloga (guncel satir 272-273'un hemen altina) tek satir ekle:

{"op":"order","id":"benim-6","symbol":"GOLD","side":"buy","type":"market","volume":0.01,"sl":4300,"tp":4400}

(b) Ornegin hemen altina kisa bir not bloku ekle (§4'te, "Stop broker tarafinda" basliginin ONUNE):

### Stop'u EMIRLE BIRLIKTE gonderin — korumasiz pencere hic dogmasin

`order` islemi `sl` ve `tp` alanlarini kabul eder (0 = konmadi). MT5 emri ve
stop'u TEK istekte isler; pozisyon SL ile DOGAR. Iki adimli
(`order` -> dolum bekle -> `modify_sltp`) yolda pozisyon, dolum olayi gelip
bilet ogrenilene kadar KORUMASIZDIR — kopru tam o araliкta zombi olursa
pozisyon saatlerce SL'siz kalir.

Bilinmesi gerekenler:
- Stop gecersizse (`stops_level` ihlali) MT5 TUM istegi `10016` ile reddeder:
  korumasiz pozisyon olusmaz. Iki adimli yolda ayni hata pozisyon ACIKKEN olur.
- **Bu yol icin `sltp_unverified` dogrulamasi KURULMAZ.** Dogrulama defteri
  yalnizca `modify_sltp` komutunda devreye girer. Emirle birlikte SL
  gonderdiyseniz dolumdan sonra `positions[].sl` degerini KENDINIZ okuyun.
- **Bu broker'da canli olculmedi.** `exec_mode` = 2 (MARKET) olan hesaplarda
  bazi broker'lar yerlestirme istegindeki SL/TP'yi kabul etmez ve emri `10016`
  ile reddeder. Once tek bir 0.01 lot emirle dogrulayin; reddedilirse
  `modify_sltp`e dusun (yedek stop'unuz her hâlükârda devrede kalsin).

(c) API.md §5b tablosunda (guncel satir 513) satiri ikiye ayir:

| Stop'u emirle BIRLIKTE koymak (pencere yok) | `order` icindeki `sl` / `tp` |
| Acik pozisyonun stop'unu SONRADAN degistirmek | `{"op":"modify_sltp","id":"..","ticket":N,"sl":..,"tp":..}` |

(d) API.md §5b'deki "> Stop'u koprude tutmayin" alintisinda (guncel 524-528)
"`modify_sltp` ile SL'i broker'a yazin" ifadesini
"SL'i tercihen EMIRLE BIRLIKTE (`order` icindeki `sl`), degistirirken
`modify_sltp` ile broker'a yazin" olarak duzelt.

NOT: API.md su anda canli olcum oturumu tarafindan duzenleniyor; satir
numaralari kaydi (iddiadaki 490/502 simdi 513/525). Duzeltme uygulanmadan
once satirlar yeniden dogrulanmali.
```

</details>

---

## 2. [KRITIK] Dolum ile SL kurulumu arasındaki pencerede HİÇBİR denetim yok: SltpGuard yapısı gereği bu pencereyi göremez

**Yer:** `D:\Projeler\Sinyal\crates\sinyal-core\src\server.rs:2049`  |  **Mercek:** zaman-penceresi

**Senaryo**

1) İstemci market emri gönderir, `queued` alır. 2) EA OrderSendAsync yapar, broker doldurur — pozisyon SL=0 ile AÇIK. 3) Tam bu noktada köprü zombileşir (WS ayakta, okuyucu thread/EA donmuş) ya da istemci düşer. 4) `modify_sltp` HİÇ gönderilmez. 5) `SltpGuard.pending` boş kaldığı için sweep_sltp ilk satırda döner; `sltp_unverified` üretilmez, `[stop] UYARI:` satırı (server.rs:577-581) hiç basılmaz, telemetri sayaçları (sent/verified/unverified/closed) sıfır kalır. 6) Pozisyon saatlerce korumasız durur ve sistem bunu 'sorun yok' olarak raporlar. Tüketicinin bildirdiği olay (8 saat zombi, 11 pozisyon 10+ saat korumasız) tam olarak bu boşluğa düşüyor: doğrulama defteri, korunması gereken pencerenin BİTİŞİNDEN sonra başlıyor.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Defteri `modify_sltp`de degil DOLUMDA kur. En kucuk hali: `submit_order` kabul edildiginde (dispatch `queued` dondugunde, server.rs:1531) ve `req.sl == 0.0` ise `SltpGuard`a "stop bekleniyor" kaydi ekle — anahtar bilet degil `wire`/`client_id` olsun (PositionInfo.client_id zaten magic'i tasiyor, wire.rs:496-497; server.rs:1512-1513 `magic: wire`). PendingSltp'ye tek bir `expect_any_sl: bool` alani yeter: `sweep_sltp` (server.rs:2158-2211) icinde eslesme `q.ticket == p.ticket` yerine bu kayitlar icin `q.client_id == p.wire` olur ve "dogrulandi" olcutu `stop_reached(...)` yerine `q.sl != 0.0` olur. Sonradan gelen `modify_sltp` ayni bileti armlayinca (ya da `sl != 0` gorulunce) kayit zaten dusuyor. Sure icin ayri bir sabit kullan (`SLTP_VERIFY_WINDOW` 3 sn cok kisa — istemcinin dolumu gorup `modify_sltp` gondermesine pay birakan ~10-15 sn'lik bir `FILL_SL_GRACE`), boylece normal iki adimli akista yanlis alarm cikmaz; pencere doldugu hâlde pozisyon acik ve `sl == 0` ise mevcut `FeedEvent::SltpUnverified` yolu ("dolum korumasiz: pozisyon acildi, stop kurulmadi" gerekcesiyle) ve main.rs:1294-1301 uyarisi kendiliginden calisir. `pend.is_empty()` erken donusu korunur — yeni kayitlar da ayni `pending` listesinde durdugu icin tarama maliyeti degismez.
```

</details>

---

## 3. [KRITIK] state_age_ms EA'nin yayin yasini DEGIL cekirdegin kopyalama anini olcuyor — zombi kopruda 'BAYAT' dali hic calismaz, gorulemeyen pozisyon SESSIZCE 'kapanmis' sayilir

**Yer:** `crates/sinyal-core/src/server.rs:2093`  |  **Mercek:** reddedilme

**Senaryo**

1) EA/terminal donuyor (zombi): OnTimer artik PublishState cagirmiyor, ama paylasilan bellekteki durum hucresi son yazilan haliyle duruyor ve Windows eslemesi cekirdek acikken yasamaya devam ediyor.
2) Okuyucu thread'i her saniye `session.state()` cagiriyor; `has_data()` hala true (generation > 0), ayni DONMUS goruntu doniyor, `set_state` ona `at = now` damgasi vuruyor.
3) `state_age_ms` bu yuzden SONSUZA KADAR ~0-1000 ms doner. `SLTP_STATE_STALE = 5s` esigi hicbir zaman asilmaz -> `trustworthy = true` KALICI olarak.
4) Istemci, donmus goruntude BULUNMAYAN bir bilet icin modify_sltp gonderir (pozisyon donma anindan sonra acildi, ya da donmus goruntu o bileti hic icermiyordu).
5) 3 sn sonra sweep: `items.iter().find(|q| q.ticket == p.ticket)` -> None; `trustworthy == true` -> `None if trustworthy` dalina duser.
6) `closed` sayaci artar, defter girdisi silinir, FeedEvent URETILMEZ, eprintln SATIRI BASILMAZ, tele hicbir sey gitmez.
7) Pozisyon saatlerce korumasiz kalir ve ne istemci ne daemon gunlugu haberdar olur — bildirilen olayin ta kendisi. 'a_position_we_cannot_see_says_so...' testi (server.rs:4032-4058) yalnizca `now`'u elle ileri sararak gecer; uretimde `s.at` her saniye tazelendigi icin o dal erisilemez.

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk ve dogru duzeltme: `at` damgasi "cekirdegin kopyalama ani" degil "EA'nin son YENI yayini" anlamina gelsin. Bunun icin `set_state`i yalnizca goruntu GERCEKTEN yenilendiginde cagir; karsilastirma olcutu EA'nin kendi damgasi olan `built_at_qpc` (monoton, her yayinla artar).

crates/sinyal-core/src/source.rs, reader_loop:
1) Dongu oncesi (satir ~236, `last_state_refresh` yaninda):
   // EA'nin SON yayin damgasi. Ayni damga = yeni yayin YOK: zombi kopruda
   // donmus hucreyi her saniye "taze" diye damgalamak, sltp dogrulamasinin
   // BAYAT dalini erisilemez kiliyordu.
   let mut last_built_qpc: u64 = 0;
2) satir 488-514'teki blokta `registry.set_state(&instance, st);` satirini sarmala:
   if st.built_at_qpc != last_built_qpc {
       last_built_qpc = st.built_at_qpc;
       registry.set_state(&instance, st);
   }
   (seed_from_positions/seed_from_orders ve truncated uyarisi oldugu gibi kalabilir; `st`i tasimak icin karsilastirmayi bu cagrilardan SONRA yap.)

Neden bu yeterli ve neden en kucuk:
- server.rs, state.rs, wire.rs ve proto'ya dokunulmaz; `state_age_ms`, `positions_now`, `SLTP_STATE_STALE` ve mevcut testlerin tamami degismeden dogru calismaya baslar (testler `registry.set_state`i dogrudan cagirdigi icin etkilenmez).
- Karsilastirma yalnizca ONCEKI degerle yapiliyor; QPC'yi yerel saate cevirme / broker-sunucu saat farki gibi yeni bir hata sinifi acilmaz.
- Ayni degisiklik `collect_accounts`taki `age_ms` (server.rs:2033) ve `sltp_unverified.state_age_ms` alanini da otomatik olarak dogru anlama kavusturur.
Istege bagli ufak sertlestirme (ayni satirda): EA yayini durdugunda `unstable`/kesinti farkindaligi icin `built_at_qpc == 0` gelen (replay/sim) durumda davranis degismez, cunku o yollarda `positions_now` zaten sim dalindan gecip `true` doner.
```

</details>

---

## 4. [KRITIK] sweep_sltp'in 'dogrulandi' dali trustworthy'ye BAKMIYOR: bayat/kirpilmis goruntudeki eski sl istenen degere esitse stop dogrulanmis sayilip sessizce defterden dusuyor

**Yer:** `crates/sinyal-core/src/server.rs:2161`  |  **Mercek:** reddedilme

**Senaryo**

1) Istemci trailing-stop / periyodik teyit dongusunde ayni bileti ayni SL degeriyle yeniden gonderiyor (idempotent tazeleme) — ya da pozisyonun SL'i onceki bir modify ile zaten istenen degerde.
2) Bu sirada durum listesi GUVENILMEZ: `truncated = true` (pozisyon sayisi kapasiteyi asti veya EA numaralandirma sirasinda slot atladi -> pos_total > pos_count) veya goruntu bayat.
3) sweep pozisyonu listede bulur, `q.sl` tesadufen `want_sl` ile bir tick_size icinde esittir.
4) Ilk dal calisir: `verified += 1`, girdi defterden silinir. `trustworthy == false` oldugu halde hicbir uyari uretilmez.
5) Gercekte broker o an ne yapti bilinmiyor (kirpilmis listede gorulen satir eski turdan kalma olabilir, ya da broker yeni komutu 10016 ile dusurmus olabilir). Sistem 'dogrulandi' der, telemetri `dogrulanan` sayacini artirir, korumasiz olabilecek pozisyon korunmus sayilir.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — basari dalini TAM `trustworthy` ile degil, yalnizca TAZELIK ile kapat (kirpilma bu dali bozmaz; onu da katmak yanlis alarm regresyonu olurdu).

server.rs, sweep_sltp (2150 civari), `positions_now`un yanina tazeligi ayrica hesapla:

    let (items, trustworthy) = positions_now(ctx, age);
    // Bayat satirdaki `sl` komuttan ONCEKI okumadir: "kuruldu" demek icin
    // delil degildir. KIRPILMA bunu bozmaz — listeye GIREN satir gercektir,
    // o yuzden burada `trustworthy` degil yalniz tazelik soruluyor.
    let fresh = ctx.sim().is_some()
        || age.is_some_and(|ms| ms <= SLTP_STATE_STALE.as_millis() as u64);

sonra 2161'deki dali:

    Some(q) if fresh && stop_reached(p.want_sl, q.sl, tick_size_of(ctx, &q.symbol)) => {
        ctx.sltp.verified.fetch_add(1, Ordering::Relaxed);
        false
    }

Davranis: bayat goruntude eslesme artik "dogrulandi" demiyor; girdi `_ if now < p.deadline => true` ile beklemeye devam ediyor (goruntu pencere icinde tazelenirse normal dogrulaniyor), pencere dolarsa mevcut 2168 dalindan BAYAT uyarisi cikiyor. Sifir yeni kod yolu.

ZORUNLU EK: 2183-2184'teki bayat metni artik yanlis onerme kuruyor ("sl istenen degerde degil AMA...") — cunku bu halde sl istenen degerde GORUNUYOR. Sunun gibi olmali:
    "stop DOGRULANAMADI: durum goruntusu BAYAT - gorulen sl komuttan ONCEKI olabilir (bkz. state_age_ms)"

TEST (mevcut yardimcilarla, yeni altyapi gerekmez): live_ctx(T, 1.09500) ile ayni degeri tekrar gonder (`modify("m1", T, 1.09500)`), sonra `sweep_sltp(&ctx, now + SLTP_STATE_STALE + SLTP_VERIFY_WINDOW)` cagir; eskiden `verified == 1` ve sifir olay uretiyordu, duzeltmeden sonra `verified == 0` ve BAYAT gerekceli tek uyari beklenmeli.

AYRI IS OLARAK ACILMALI (bu yama kapsaminda DEGIL): tazeligi cekirdegin okuma anindan degil EA'nin `built_at_msc`/`built_at_qpc` damgasindan olc (state.rs:141 + source.rs:511) ve `ReaderStats.connected`i (source.rs:228) EA sussunca dusur — zombi koprunun asil dedektoru budur.
```

</details>

---

## 5. [KRITIK] EA olurse state_age_ms YALAN soyler: bayatlik olcusu EA'nin YAZMA anini degil, koprunun OKUMA anini damgaliyor

**Yer:** `crates/sinyal-core/src/state.rs:141`  |  **Mercek:** olum-yollari

**Senaryo**

1) EA bir kez durum yayinlar, generation > 0 olur. 2) MT5 terminali kapanir / EA crash eder (SinyalClose yalnizca yerel tanitici slotunu birakir — ffi.rs:358-369 — segmente 'oldum' isareti YAZMAZ). 3) Paylasilan bellek sinyald'in tuttugu tanitici sayesinde YASAMAYA devam eder ve son goruntuyu aynen korur. 4) reader_loop saniyede bir session.state() cagirir; has_data() hala true oldugu icin DONMUS veri Some olarak doner. 5) set_state her saniye at = Instant::now() ile yeniden damgalar. 6) state_age_ms sonsuza kadar ~0-1000 ms doner. 7) SLTP_STATE_STALE (5 sn) ASLA tetiklenmez; positions_now trustworthy = true doner (server.rs:2078-2079). 8) sweep_sltp'de bilet donmus listede bulunursa 'guvenilir' dalina duser ve uyari BROKER'i suclar ('broker stop'u uygulamamis olabilir', server.rs:2178-2179) — hem de state_age_ms ~0 basarak kopruyu acikca AKLAR. 9) Bilet donmus listede yoksa (pozisyon donmadan SONRA acildi) 'None if trustworthy' dali calisir, sayac closed'a yazilir ve HICBIR uyari uretilmez (server.rs:2191-2194) — tuketicinin bildirdigi '11 pozisyon 10+ saat korumasiz, kimse uyarmadi' olayinin birebir kod karsiligi. Ayni kusur account.age_ms icin de gecerlidir (server.rs:2033 s.at.elapsed()).

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk dogru duzeltme — damgayi OKUMA anindan degil EA'nin YAZMA aninin kimliginden (built_at_qpc) turet. Tek dosya, tek fonksiyon:

crates/sinyal-core/src/state.rs (137-143 yerine):

    pub fn set_state(&self, instance: &str, snap: sinyal_proto::StateSnapshot) {
        let mut g = self.states.write().unwrap_or_else(|e| e.into_inner());
        // Damga EA'nin YAZMA anini temsil etmeli. Ayni goruntuyu (ayni
        // built_at_qpc) yeniden okumak TAZELIK DEGILDIR: EA olunce paylasilan
        // bellek donar ama okuma her saniye basarili olur ve yas ~0 kalirdi —
        // zombi kopru boylece "taze" gorunup SLTP dogrulamasini aklardi.
        let at = match g.get(instance) {
            Some(prev) if prev.snap.built_at_qpc == snap.built_at_qpc => prev.at,
            _ => std::time::Instant::now(),
        };
        g.insert(instance.to_owned(), InstanceSnapshot { snap, at });
    }

Neden bu yeterli ve neden yanlis alarm uretmez:
- built_at_qpc, yazma yolunda EA surecindeki kopru DLL'inde aliniyor (sinyal-bridge/src/session.rs:546, sinyal_shm::qpc()) — sinyald ile AYNI makine, monoton, alt-mikrosaniye cozunurluk; her yayin farkli deger uretir.
- EA saniyede en az bir kez yayin yapar (SinyalCollector.mq5:1224: `if(g_state_dirty || now_s != g_last_state_pub)` — olay-gudumlu + heartbeat), yani canliyken built_at_qpc >=1 Hz ilerler. SLTP_STATE_STALE 5 sn oldugundan 4-5 heartbeat'lik pay kalir.
- Tek satirlik bu degisiklik hem sweep_sltp'nin trustworthy kararini (server.rs:2078-2079, 2149-2150) hem de hesap listesindeki age_ms'i (server.rs:2033) ayni anda duzeltir; cagiranlarda degisiklik gerekmez.
- Regresyon testi onerisi: ayni StateSnapshot'i (ayni built_at_qpc) art arda set_state ile yaz, aradan >5 sn gectiginde state_age_ms'in >=5000 dondugunu ve sweep_sltp'nin "kopru zombi olabilir" uyarisi urettigini dogrula (mevcut server.rs:4040-4055 testinin gercek zombi karsiligi).
```

</details>

---

## 6. [KRITIK] Koprude YEDEK stop mantigi YOK — SltpGuard belgesi var oldugunu soyluyor, kodun geri kalani 'yedek istemcide' diyor

**Yer:** `crates/sinyal-core/src/server.rs:388`  |  **Mercek:** olum-yollari

**Senaryo**

1) Istemci pozisyon acar, ardindan modify_sltp gonderir. 2) Broker stops_level ihlali / requote / 10016 ile stop'u sessizce dusurur. 3) Kopru 3 saniye sonra TEK bir sltp_unverified uyarisi yayar. 4) Kopru baska HICBIR SEY yapmaz: stop'u yeniden gondermez, pozisyonu kapatmaz, fiyati izlemez. 5) SltpGuard belgesini okuyan bir operator/gelistirici 'kopruden de bir stop duruyor' sonucuna varir ve istemci tarafinda yedek stop kurmaz. 6) Boylece pozisyonu koruyan hicbir mekanizma kalmaz — ne brokerda, ne kopruda, ne istemcide. Ayni celiski, kopru olum senaryosunda da yanlis guven uretir: kopru olunce 'yedek de olur mu' sorusunun cevabi 'zaten hic yoktu'.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Kod degisikligi GEREKMIYOR; en kucuk dogru duzeltme belgedeki "kopru stop'u" ifadesini "ISTEMCI stop'u" ile degistirmek (API.md:337'deki kendi tablosuyla hizalamak). Somut: (a) server.rs:387-390'daki "# Kapsam" paragrafini su hale getir: "Bu **istemcinin kendi stop mantiginin yerine gecmez**; koprude YEDEK bir stop mantigi YOKTUR — bu defter yalniz GOZLEM yapar (arm/sweep/counts), mudahale etmez: stop'u yeniden gondermez, pozisyon kapatmaz, fiyat izlemez. Kullanicinin karari: broker stop'u ASIL koruma, ISTEMCININ kendi stop'u YEDEK — ikisi de durur. Ikisi ayni anda tetiklenirse ikinci kapatma 'pozisyon yok' alir; zararsiz ama loglanir." (b) API.md:330 basligini "Stop broker tarafinda, ISTEMCI stop'u YEDEK — ikisi de dursun" yap (wire.rs:650-651 bu basliga referans veriyor, ayni metne guncelle). (c) API.md:352 ve 524-526'daki "Koprudeki stop" ifadelerini "Istemci tarafindaki stop" yap ve 524'e "Koprude yedek stop mantigi YOKTUR" cumlesini ekle.
```

</details>

---

## 7. [KRITIK] sltp_unverified TEK ATISLIK: defter kaydi ilk taramada dusuruluyor, yeniden deneme ve surekli izleme yok

**Yer:** `crates/sinyal-core/src/server.rs:2158`  |  **Mercek:** olum-yollari

**Senaryo**

1) modify_sltp kabul edilir, defter kurulur, deadline = now + 3 sn. 2) 3 saniye sonra stop yerinde degilse TEK uyari yayilir ve kayit silinir. 3) O andan itibaren o bilet icin hicbir izleme kalmaz: kopru stop'u YENIDEN GONDERMEZ ve bir daha KONTROL ETMEZ. 4) Ayrica: uyari o an 'order' kanalina abone istemci yoksa yalnizca daemon stderr'ine duser (server.rs:577-581); defter kaydi silindigi icin istemci yeniden baglandiginda sorgulayabilecegi bir durum da kalmaz, uyari tekrar yayinlanmaz. SltpCounts sayaclari yalnizca 30 saniyede bir stdout'a basilir (main.rs:1282-1301) ve tel uzerinde HIC yayinlanmaz — ServerMsg'de karsiligi yok. 5) Simetrik acik: T+1 sn'de dogrulanmis bir stop, broker tarafinda T+1 saat'te kaldirilirsa (ya da EA/terminal yeniden baslatilirsa) bunu goren hicbir kod yoktur.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — stop'u YENIDEN GONDERME (o bilincli tercih dogru), sadece DEFTERI DUSURME. server.rs:2158-2211'de iki uyari dalinin (2168-2188 ve 2195-2209) 'false' donusunu, kaydi yeniden kurup 'true' donmeye cevir; kayit yalnizca (a) dogrulandiginda (2161-2164) ya da (b) GUVENILIR bir listede pozisyon kapandiginda (2191-2194) dusmeye devam etsin. Somut: PendingSltp'ye 'warned: bool' ekle; uyari uretilen dalda p.deadline = now + SLTP_REWARN (yeni sabit, ornegin 60 sn) yaz, warned = true yap ve true dondur. Boylece (i) o bilet, stop kurulana ya da pozisyon kapanana kadar SUREKLI izlenir; (ii) uyari 60 sn'de bir yeniden yayilir — 'order' kanalina gec abone olan ya da yeniden baglanan istemci bir sonraki tekrarda uyariyi ALIR; (iii) 250 ms'lik tarama sikligi (SLTP_SWEEP) uyari selini uretmez, cunku tekrar araligini deadline kontrol eder. Sayac ikilenmesin diye unverified.fetch_add'i yalnizca warned == false iken calistir (ilk tespit), tekrarlarda artirma. Bu, sizan tek sey olan 'kalici uyari durumu'nu geri getirir ve 15 satiri gecmez. Iddianin 5. maddesi (T+1 saatte broker'da kaldirilan stop) bununla KAPANMAZ; onun icin dogrulanmis kaydi da silmeyip 'izleme' listesine tasimak gerekir — o ayri ve daha buyuk bir degisikliktir, once yukaridaki minimal duzeltme yapilmalidir.
```

</details>

---

## 8. [KRITIK] SLTP_STATE_STALE bir kopru saglik gozcusu DEGIL: yalnizca modify_sltp sonrasi 3 saniyelik pencerede degerlendiriliyor

**Yer:** `crates/sinyal-core/src/server.rs:2144`  |  **Mercek:** olum-yollari

**Senaryo**

1) Kopru ayakta ama okuyucu thread'i olmus/donmus (bkz. asagidaki spawn_reader bulgusu) — set_state durur, state_age_ms buyumeye baslar. 2) O anda bekleyen bir modify_sltp dogrulamasi YOKSA sweep_sltp 2144'te aninda geri doner ve bayatlik HIC HESAPLANMAZ. 3) 11 pozisyon acik, hepsinin stop'u saatler once dogrulanmis ve defterden dusmus olsun: kopru 8 saat zombi kalir, sweep saniyede 4 kez calisir, her seferinde 2144'ten doner ve tek bir bayatlik kontrolu bile yapilmaz. 4) Yani belgede 'zombi bir kopruyu gizlemez' diye tanimlanan esik (server.rs:70-77), pratikte yalnizca modify_sltp komutunu izleyen 3 saniyelik pencerede canlidir; o pencerenin disinda zombi tespiti icin calisan HICBIR kod yoktur.

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk dogru duzeltme, sweep_sltp'i degistirmek yerine ONUN CAGIRILDIGI 250 ms'lik goreve (server.rs:568-586) ucuz bir bayatlik kapisi eklemek:

1) `Registry`'ye KLONSUZ bir yas erisimcisi ekle (mevcut `state_age_ms` `all_states()` ile tum pozisyon vektorlerini klonluyor; 4 Hz'te her turda cagrilamaz):
   `pub fn oldest_state_at(&self) -> Option<Instant>` — sadece `at` alanlarinin en eskisini dondursun.

2) Sweep gorevinde, `sweep_sltp` cagrisindan ONCE (yani pend bos olsa da calisan yerde):
   - yalniz CANLI kipte calissin (`ctx.sim().is_none()`) — paper/replay'de paylasimli bellek durumu yoktur, orada surekli yanlis alarm olurdu;
   - EA en az bir kez durum yayinlamis olsun (`oldest_state_at().is_some()`) — hic baglanmamis terminal "zombi" degildir, o hal zaten "[inst] EA bekleniyor..." ile basiliyor;
   - yas mevcut `SLTP_STATE_STALE`in acikca UZERINDE bir kopru-sagligi esigini (or. `const BRIDGE_STALE: Duration = 30s`) asarsa bir kez `eprintln!("[kopru] UYARI: durum yayini {age_ms} ms'dir gelmiyor — kopru zombi olabilir")` bas ve gorev icinde tutulan bir `warned: bool` ile TEKRARI BASTIR; yas esigin altina dondugunde `warned=false` yapip tek satirlik "kopru tazelendi (yas={} ms)" bas.
   - Ayni anda `FeedEvent`'e (source.rs:84 civari `SltpUnverified` komsulugunda) `BridgeStale { instance, state_age_ms, recovered: bool }` ekleyip `order` kanalindan yayinla (wire.rs'te `t:"order", kind:"bridge_stale"` olarak) ki tuketici `accounts` yoklamak zorunda kalmasin.

Neden bu kadar ve daha fazlasi degil: pozisyon listesi TOPLANMIYOR (2144'teki erken donusun asil gerekcesi olan maliyet korunur), `SLTP_STATE_STALE`in anlami degistirilmiyor (o esik "listede goremedik"i yorumlamaya ait kalir), ayri ve daha genis bir esik zaten seyrek olan bir olayi seyrek raporlar. Ek olarak belge duzeltmesi: API.md:399-406 "durum goruntusu bayat (>5 sn)" ifadesinin YANINA, bu esigin yalnizca modify_sltp dogrulamasi sirasinda degerlendirildigi (yeni bridge_stale uyarisi eklenene kadar surekli bir saglik gozcusu OLMADIGI) yazilmali.
```

</details>

---

## 9. [KRITIK] Istemci koprunun zombi oldugunu ANLAYAMAZ: ping canlilik kaniti degil, pozisyon listesinde yas alani yok, sunucu tarafli keepalive yok

**Yer:** `crates/sinyal-core/src/server.rs:1027`  |  **Mercek:** olum-yollari

**Senaryo**

1) Kopru zombi olur (EA olmus ya da okuyucu donmus). 2) Istemci saglik kontrolu icin {"op":"ping"} gonderir; tokio calisma zamani ayakta oldugu icin aninda ServerMsg::Pong doner — istemci 'kopru saglikli' sonucuna varir. 3) Istemci pozisyonlarini dogrulamak icin 'positions' cagirir; saatler once donmus liste, TAZE bir listeyle bit bit ayni bicimde doner (yas alani yok, truncated=false). Istemci 11 pozisyonun sl degerlerini okur, hepsi dolu gorunur ve 'stoplarim yerinde' sonucuna varir. 4) TCP yarim-olu kalirsa (ag kara deligi) sunucu tarafinda ne Ping ne okuma zaman asimi oldugu icin baglanti sonsuza kadar acik sayilir. 5) Istemcinin elinde kalan tek zombi isareti 'tick akmiyor'dur ve bu, sakin piyasa / hafta sonu ile ayirt edilemez.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Tazeligi okuma zamanina degil built_at_qpc'ye baglayin: state.rs:137-143'teki Registry::set_state icinde, gelen snap.built_at_qpc saklanan degerle AYNIYSA onceki `at` korunsun; yalnizca ilerlediginde Instant::now() damgalansin. Yaklasik 6 satir, protokol degisikligi yok; account.age_ms, state_age_ms, positions_now'un trustworthy karari ve tum SLTP defterini tek hamlede duzeltir.

built_at_msc DEGIL built_at_qpc kullanilmali: built_at_msc = TimeCurrent()*1000 (SinyalCollector.mq5:911), 1 sn cozunurluklu broker saati ve sakin bir hafta sonunda EA capcanliyken bile donabilir; built_at_qpc ise yayin aninda yerel yuksek cozunurluklu saatten damgalaniyor (session.rs:546) ve EA yasadigi her saniye ilerliyor.

Devaminda Positions cevabina age_ms/stale alani eklemek dogal bir takip adimi, ama deligi kapatan sey set_state degisikligi.
```

</details>

---

## 10. [KRITIK] modify_sltp/close/cancel komutu symbol_id ALANINI HIC DOLDURMUYOR — EA komutu alfabetik olarak ILK sembole uyguluyor

**Yer:** `crates/sinyal-core/src/server.rs:1432`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

1) EA GOLD dahil birden fazla sembol topluyor (SymbolList bos ise tum Market Watch); alfabetik ilk sembol ornegin AUDCAD. 2) Istemci GOLD'da pozisyon aciyor (bu emir DOGRU symbol_id ile gidiyor, server.rs:1520). 3) Istemci `modify_sltp {ticket: <GOLD pozisyonu>, sl: 3300}` gonderiyor. 4) Cekirdek `symbol_id=0` yaziyor; EA `req.symbol = "AUDCAD"`, `req.position = <GOLD bileti>` ile TRADE_ACTION_SLTP gonderiyor. 5a) EN IYI HAL: broker istegi reddediyor (sembol/pozisyon uyusmazligi) — GOLD pozisyonu KORUMASIZ kaliyor ve 3 saniye sonra gelen uyari sebebi yanlis gosteriyor ("broker stop'u uygulamamis olabilir", gercek sebep bizim yanlis sembol gondermemiz). 5b) NETTING hesapta pozisyon SEMBOL ile teshis edilir: SL/TP AUDCAD pozisyonuna yazilabilir — yani YANLIS POZISYONA stop. Ayni hata `close` yolunda daha da agir: mq5:1060-1076 `PositionSelectByTicket(cmd.ticket)` ile hacim/yon dogru pozisyondan aliniyor ama `req.symbol` yine g_name[0] ve `SymbolInfoTick(sym, tk)` ile fiyat YANLIS sembolden okunuyor; netting'de bu, kapatma yerine yanlis sembolde yeni pozisyon acabilir.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — yalniz cekirdek; EA'ya, tele ve protokole DOKUNMADAN (canli VPS'te EA yeniden derlenmesi gerekmez). Tek `symbol_id` alani dogru dolunca EA tarafinda `req.symbol`, `SymbolInfoTick` kaynagi ve close'un doldurma tablosu ucu birden kendiliginden duzelir, cunku hepsi ayni `idx`ten tureniyor.

`crates/sinyal-core/src/server.rs` icinde `submit_simple` (satir 1414-1446):

1) Bileti CANLI durum tablosundan coz. Gerekli veri zaten var: `collect_positions` (server.rs:2215-2243) ve `collect_orders` (server.rs:2245+) her satirda hem `src` (ornek adi) hem `symbol` tasiyor.

   let owner = if act == action::REMOVE {
       collect_orders(ctx).0.into_iter().find(|o| o.ticket == ticket).map(|o| (o.src, o.symbol))
   } else {
       collect_positions(ctx).0.into_iter().find(|p| p.ticket == ticket).map(|p| (p.src, p.symbol))
   };
   let Some((instance, sym)) = owner else {
       return rejected(ctx, id, "bilet durum goruntusunde yok - sembol cozulemedi");
   };
   let Some(symbol_id) = ctx.registry.resolve(&instance, &sym) else {
       return rejected(ctx, id, "biletin sembolu tabloda yok");
   };

2) 1429-1431'deki `ctx.cmd_tx.keys().next()` secimini SIL; komut biletin GERCEK sahibi olan `instance`a gitsin (bu ayni anda ikinci deligi de kapatir).

3) 1432-1443'teki literale `symbol_id,` alanini ekle:

   let mut cmd = Cmd { client_id: wire, magic: wire, ticket, volume, sl, tp, symbol_id, action: act, filling: filling::AUTO, type_time: type_time::GTC, ..Default::default() };

Neden "reddet" dogru taraf: yanlis sembolle gondermek sessizce yanlis pozisyona stop yazabilir; acik gerekceyle reddetmek istemciye tekrar deneme/alarm imkani birakir ve `SltpGuard` zaten korumasizligi rapor eder. Simule kip etkilenmez (o dal 1152-1177'de zaten ayrilmis).

REGRESYON TESTI (tek assert, mevcut kalibi kullanir — server.rs:3915 civari): iki sembollu bir kayit kur (0="AUDCAD", 3="GOLD"), GOLD pozisyonu icin `modify_sltp` gonder, `c_rx.recv()` ile alinan `Cmd` uzerinde `assert_eq!(cmd.symbol_id, 3, "stop biletin KENDI sembolune gitmeli")`. Bugun bu test 0 dondurur.

SAVUNMA DERINLIGI (istege bagli, EA yeniden derlenince): mq5 `ExecuteCmd` icinde SLTP/CLOSE_POSITION/CLOSE_BY dallarinda `PositionSelectByTicket(cmd.ticket)` sonrasi `sym = PositionGetString(POSITION_SYMBOL); req.symbol = sym;` ile idx'i yeniden turet — boylece bayat bir cekirdek tablosu bile yanlis sembol uretemez.
```

</details>

---

## 11. [KRITIK] Dogrulama defteri pozisyonu YALNIZ ticket ile ariyor; bilet degisirse pozisyon 'kapanmis' sayilip uyari SESSIZCE dusuruluyor

**Yer:** `crates/sinyal-core/src/server.rs:2159`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

MT5'te POSITION_TICKET sunucu tarafi servis islemlerinde (ornegin gecelik swap tahakkukunda pozisyonun yeniden acilmasi) DEGISIR, POSITION_IDENTIFIER degismez. 1) 23:59'da pozisyonun stop'u kurulu ve dogrulanmis. 2) Rollover'da bilet 942649399 -> 942700111 oluyor, pozisyon acik kaliyor. 3) Istemci elindeki ESKI bilet ile `modify_sltp` gonderiyor (trailing stop). 4) Broker bileti tanimiyor, komut dusuyor — stop guncellenmiyor. 5) `sweep_sltp` eski bileti listede bulamiyor; goruntu TAZE oldugu icin `trustworthy = true` dali calisiyor ve kayit `closed` sayaciyla SESSIZCE dusuyor (server.rs:2191). Hicbir uyari uretilmiyor, gunlukte tek satir yok; pozisyon acik ve stop'u eskimis halde saatlerce kaliyor. Ayni sessizlik, bilet degismeden pozisyon kapandiginda uretilen sessizlikten AYIRT EDILEMEZ.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Tek satir: server.rs:2159'daki aramayi `identifier` ile de esletir. Bayat bilet, yeniden acilmis pozisyonun `identifier`ina esit oldugu icin bu duzeltme senaryoyu tam olarak yakalar ve kayit `Some(q)` dalina duserek `SltpUnverified` (gercek_sl = eski stop) uretir.

- let found = items.iter().find(|q| q.ticket == p.ticket);
+ // Bilet, sunucu tarafi servis islemlerinde (or. swap tahakkukunda
+ // pozisyonun yeniden acilmasi) DEGISIR; `identifier` degismez ve ESKI
+ // bilete esittir. Yalniz bilete bakmak, ACIK duran pozisyonu "kapanmis"
+ // sayip uyariyi sessizce dusururdu.
+ let found = items.iter().find(|q| q.ticket == p.ticket || q.identifier == p.ticket);

Ek maliyet yok: `PendingSltp` degismiyor, `PositionInfo.identifier` zaten dolduruluyor (server.rs:2226) ve simule yolda `identifier == ticket` (server.rs:1918) oldugu icin sim/paper davranisi aynen korunuyor. Yanlis eslesme riski yok: bir pozisyonun `identifier`i kendi acilis emrinin biletidir, baska bir pozisyonun guncel biletiyle cakismaz (join.rs:96-97 zaten iki anahtari ayni tabloda tutuyor).

Regresyon testi olarak mevcut `a_position_that_simply_closed_raises_no_alarm` yaninda ikinci bir test: pozisyonu YENI biletle (ornegin 942700111) ve `identifier = 942649399` ile yayinla, ESKI bilete `modify` gonder, pencere sonrasi `sweep_sltp` tek bir `SltpUnverified` uretmeli ve `closed` sayaci ARTMAMALI.
```

</details>

---

## 12. [KRITIK] Defter TEK ATISLIK: dogrulandiktan sonra stop'un YERINDE KALDIGI bir daha hic kontrol edilmiyor (netting'de ikinci emir stop'u silse bile)

**Yer:** `crates/sinyal-core/src/server.rs:2158`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

NETTING hesap. 1) Istemci GOLD alis emri gonderiyor, `modify_sltp` ile stop kuruluyor, 3 sn icinde `verified` olup kayit defterden siliniyor. 2) Yarim saat sonra ayni yonde ikinci `order` gonderiliyor (piramitleme). Istekte `sl` alani yok -> `req.sl = 0.0` -> `cmd.sl = 0` -> EA `req.sl = 0` ile TRADE_ACTION_DEAL gonderiyor. 3) Netting'de bu emir AYNI pozisyonu buyutuyor ve islemin SL/TP'si pozisyona uygulaniyor: pozisyonun stop'u 0'a duser. 4) Kopruye gore her sey yolunda — defter bos oldugu icin `sweep_sltp` pozisyon listesine bakmiyor bile, telemetri `dogrulanan=1 dogrulanamayan=0` basmaya devam ediyor. 5) Pozisyon korumasiz, hicbir uyari yok. Ayni yapisal bosluk tuketicinin bildirdigi olayi da kapsamiyor: 'kopru 8 saat zombi, 11 pozisyon korumasiz' senaryosunda o sirada bekleyen bir `modify_sltp` yoksa bu defter TANIM GEREGI sifir uyari uretir.

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk dogru duzeltme, defteri "komut dogrulayici" olmaktan cikarip "koruma nobetcisi" yapmak — mevcut yapinin icinde kaliyor, yeni tarama dongusu/kanal gerekmiyor:

server.rs:366-375 — `PendingSltp`e iki bayrak ekle: `verified: bool` (baslangicta false) ve `alerted: bool`.

server.rs:2158-2164 — dogrulanan dalda kaydi SILME, defterde TUT:
~~~rust
Some(q) if stop_reached(p.want_sl, q.sl, tick_size_of(ctx, &q.symbol)) => {
    if !p.verified {
        ctx.sltp.verified.fetch_add(1, Ordering::Relaxed);  // sayac YALNIZCA ilk gecis
        p.verified = true;
    }
    p.deadline = now + SLTP_VERIFY_WINDOW;  // "pencere dolmadi" dali tekrar kullanilabilsin
    true                                     // <-- eskiden false
}
~~~
(`retain` yerine `retain_mut` gerekir.)

Boylece ayni `retain` govdesi her taramada (250 ms, SLTP_SWEEP) stop'un HALA yerinde oldugunu yeniden sinar; daha once dogrulanmis bir kaydin `sl`i kayarsa (ozellikle 0'a duserse) mevcut `Some(q)` dali zaten `SltpUnverified` uretir — yalnizca `why` metnine bu hali ayirt eden bir dal eklenmeli, or. `if p.verified { "daha once DOGRULANAN stop kayboldu - ikinci emir ya da elle mudahale silmis olabilir" }`. Uyari selini onlemek icin uyaran dogrulanmis kayit `alerted = true` ile susturulup defterde birakilir (ya da tek uyaridan sonra dusurulur). Kayit normalde YALNIZCA pozisyon GUVENILIR listede yokken (server.rs:2191, kapanis) dusmelidir.

Bu tek degisiklik `if pend.is_empty()` erken donusunu de dogal olarak etkisizlestirir: korunan pozisyon oldugu surece defter bos olmaz, dolayisiyla acik pozisyonlarin `sl`i her taramada gercekten okunur ve "kopru bayat/pozisyonu goremiyoruz" dali (server.rs:2195-2209) zombi senaryosunda da tetiklenir. Emir yolunu (`req.sl` varsayilani) degistirmeye gerek yok — TRADE_ACTION_DEAL'de "pozisyonun stop'una dokunma" diye bir deger zaten yok; dogru cozum gonderimi kisitlamak degil, kaybi TESPIT etmektir.
```

</details>

---

## 13. [KRITIK] Coklu terminalde bilet hangi ornege ait cozulmuyor: SLTP/kapatma komutu rastgele secilen ILK terminale gidiyor

**Yer:** `crates/sinyal-core/src/server.rs:1429`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

1) Iki terminal bagli: `icmarkets` ve `pepperstone`. 2) Pozisyon pepperstone'da acilmis (emir sembol uzerinden dogru terminale gitmisti). 3) Istemci `modify_sltp {ticket: 8814423}` gonderiyor. 4) Komut `cmd_tx.keys().next()` ile icmarkets'e gidiyor. 5a) Bilet orada yoksa komut dusuyor — pepperstone'daki pozisyon korumasiz. 5b) MT5 biletleri SUNUCU BASINA artan sayilardir; iki broker ayni sayiyi kolayca uretebilir. icmarkets'te 8814423 numarali BASKA bir pozisyon varsa stop O pozisyona yazilir — hedeflenen pozisyon korumasiz kalirken alakasiz bir pozisyonun stop'u degisir. Ayni yol `close` icin de gecerlidir (server.rs:1164), yani yanlis terminalde yanlis pozisyon KAPATILABILIR.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Bileti sahibine COZ, tahmin etme. `submit_simple` icinde (server.rs:1427-1431) `cmd_tx.keys().next()` yerine durum goruntusunden sahip ornegi bul:

1) `ctx.registry.all_states()` uzerinde don; SLTP/CLOSE_POSITION/CLOSE_BY icin `s.snap.positions`, REMOVE/MODIFY icin `s.snap.orders` icinde `ticket == p.ticket` olan ornek adlarini topla.
2) TAM BIR eslesme varsa o ornegi kullan.
3) SIFIR eslesme: yalnizca `ctx.cmd_tx.len() == 1` ise bugunku davranisi koru (tek ornekte belirsizlik yok), aksi halde `rejected(ctx, id, "bilet hangi ornege ait cozulemedi")`.
4) BIRDEN FAZLA eslesme (bilet carpismasi): kesinlikle gonderme — `rejected(ctx, id, "bilet birden fazla ornekte var; hedef belirsiz")`. Sessizce birini secmek, alakasiz bir pozisyonun stop'unu degistirmek/kapatmak demektir.
5) Goruntu BAYAT ise (bkz. SLTP_STATE_STALE, server.rs:77) cozumu guvenilir sayma; coklu ornekte reddet — zombi kopruda korumasiz kalindigini bilmek, yanlis terminale yazmaktan iyidir.

Ayni cozumlemenin iki yansimasi (kucuk, ayni yamada yapilmali):
- `gate()` (server.rs:1327) cozulen ornegi parametre olarak alsin; canli-hesap kilidi RASTGELE degil GERCEK hedef uzerinden denetlensin.
- `sweep_sltp` (server.rs:2158-2161) eslesmeyi `q.ticket == p.ticket && q.src == p.instance` yapsin — bunun icin `PendingSltp`e (server.rs:368) cozulen `instance: String` alani eklenip `SltpGuard::arm` ile tasinmasi yeter. Boylece carpisan bilet "verified" sanilip alarm susturulamaz ve uyarinin `instance` alani (server.rs:2154) gercek terminali gosterir.
```

</details>

---

## 14. [KRITIK] Cifte-tetigin ikinci kapatmasi KOPRUDE hicbir yere loglanmiyor — dokuman 'loglanir' diyor

**Yer:** `D:\Projeler\Sinyal\crates\sinyal-core\src\source.rs:594`  |  **Mercek:** cifte-tetik

**Senaryo**

1) Istemci pozisyonu acar, modify_sltp ile broker'a SL yazar (dogrulanir).
2) Fiyat stop'a deger. Broker SL tetiklenir VE istemcinin yedek stop mantigi ayni anda `{"op":"close",...}` gonderir.
3) EA komutu isledigi anda pozisyon broker'da kapanmistir → PositionSelectByTicket basarisiz → PushRejection("pozisyon yok"), retcode=0.
4) Bu red `FeedEvent::Order{kind:"rejected"}` olarak yalnizca yayin kanalina gider.
5) O anda `order` kanalina abone acik bir WebSocket YOKSA (tam da bu korumanin var olma sebebi olan kopmus/zombi istemci hali), olay broadcast kanalinda olur ve HICBIR IZ birakmaz: sinyald gunlugunde yok, MT5 Experts sekmesinde yok (VerboseLog kapaliysa toplu sayac bile yok).
6) Sonuc: kullanicinin "loglayin, sessiz gecmesin" karari kod tarafindan yerine getirilmiyor; cifte-tetik olaylari sayilamaz ve olayin gerceklestigi kanitlanamaz.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DUZELTME — D:\Projeler\Sinyal\crates\sinyal-core\src\source.rs, `emit_order` (satir 594). `sltp_unverified` icin server.rs:577'de zaten kullanilan kalibin aynisi, 6 satir, baska hicbir dosyaya dokunmadan:

fn emit_order(tx: &broadcast::Sender<FeedEvent>, inst: &Arc<str>, r: &sinyal_proto::Res) {
    let kind = order_kind(r);
    // Ret tel uzerinde gitse bile daemon gunlugunde de durmali: istemci
    // `order` kanalina abone degilse ya da dusmusse -- cift-tetik TAM DA
    // bu halde olur -- olayin hicbir yerde iz birakmamasi kabul edilemez.
    // Gerekce sltp_unverified ile ayni (bkz. server.rs:574-581).
    if kind == "rejected" {
        eprintln!(
            "[emir] RED client_id={} retcode={} - {}",
            r.client_id,
            r.retcode,
            sinyal_proto::read_fixed_str(&r.comment)
        );
    }
    let _ = tx.send(FeedEvent::Order { /* ... mevcut alanlar ... */ kind, /* ... */ });
}

(`kind: order_kind(r)` yerine yukarida hesaplanan `kind` degiskeni kullanilir; baska degisiklik yok.)

NEDEN BU:
- Sel riski yok: ret komut basina bir kez uretilir, tick yolunda degil.
- EA'ya dokunmaz (yeniden derleme/EA yeniden yukleme gerektirmez), paper/sim yolunu etkilemez (sim redleri server.rs:1645 `sim_rejected` ile dogrudan istegi yapan sokete doner).
- Cift-tetik ile diger red sebepleri `comment` alaninda ("pozisyon yok") ayirt edilebilir hale gelir; boylece olay hem sayilabilir hem kanitlanabilir olur.

ALTERNATIF (daha da kucuk ama vaadi karsilamaz): server.rs:389-390'daki "zararsiz ama loglanir" cumlesini gercege uyacak sekilde "zararsiz; ret `order` kanalindan `kind:\"rejected\"` olarak gider, ISTEMCI loglamalidir" diye duzeltmek. Kullanicinin "sessiz gecmesin" karari koprude karsilanmadigi icin tercih edilen duzeltme yukaridaki eprintln'dir.
```

</details>

---

## 15. [KRITIK] close / cancel / modify_sltp komutlari symbol_id=0 ile gidiyor; EA alfabetik olarak ILK sembole emir kuruyor

**Yer:** `D:\Projeler\Sinyal\crates\sinyal-core\src\server.rs:1432`  |  **Mercek:** cifte-tetik

**Senaryo**

Kurulum: EA `SymbolList=""` ile calisiyor, Market Watch'ta AUDCAD, EURUSD, GOLD var → alfabetik siralama sonrasi g_name[0] = "AUDCAD". Islem GOLD'da.
1) Istemci GOLD'da pozisyon acar (`op:"order"` yolu DOGRU calisir: submit_order server.rs:1454+1520'de `symbol_id`'yi resolve_any'den gercekten set eder).
2) modify_sltp ile broker stop'u yazilir → EA'ya symbol_id=0 gider → `req.symbol="AUDCAD"`, `req.position=<GOLD bileti>`, action=TRADE_ACTION_SLTP. Broker sembol/pozisyon uyusmazligini reddeder → GOLD pozisyonu KORUMASIZ kalir. SltpGuard uyariyi uretir ama teshisi "broker stop'u uygulamamis olabilir" der (server.rs:2178-2179) — yanlis yere baktirir.
3) Yedek katman devreye girer: istemcinin kendi stop'u `op:"close"` gonderir → EA yine `req.symbol="AUDCAD"`, `req.price = AUDCAD bid`, `req.volume = GOLD hacmi`, `req.type = ORDER_TYPE_SELL`.
4) Broker `req.position` ile `req.symbol` uyusmazligini reddederse: HER IKI stop katmani da devre disi, pozisyon korumasiz — ve bu red bulgu #1 yuzunden hicbir yere loglanmaz.
5) Broker `req.position` alanini gozetmeyip istegi duz bir piyasa emri olarak islerse: YANLIS SEMBOLDE (AUDCAD) GOLD hacminde yepyeni bir SHORT pozisyon acilir — kullanicinin sordugu "ikinci bir TERS pozisyon" tam olarak budur, ustelik baska bir enstrumanda.
Kod bu iki ihtimalden hangisinin gerceklestigini AYIRT ETMIYOR: OrderSendAsync sonrasi gercek retcode hicbir yerde denetlenmiyor.

<details><summary><b>Onerilen duzeltme</b></summary>

```
server.rs submit_simple icinde bileti durum goruntusunde cozup symbol_id'yi gercekten yaz (ve komutu biletin sahibi olan instance'a gonder): `let found = if act == action::REMOVE { collect_orders(ctx).0.iter().find(|o| o.ticket == ticket).map(|o| (o.src.clone(), o.symbol.clone())) } else { collect_positions(ctx).0.iter().find(|p| p.ticket == ticket).map(|p| (p.src.clone(), p.symbol.clone())) }; let Some((instance, symbol)) = found else { return rejected(ctx, id, "bilet durum goruntusunde yok - sembol cozulemedi"); }; let Some(symbol_id) = ctx.registry.resolve(&instance, &symbol) else { return rejected(ctx, id, "sembol kimligi cozulemedi"); }; let mut cmd = Cmd { client_id: wire, magic: wire, ticket, volume, sl, tp, action: act, symbol_id, filling: filling::AUTO, type_time: type_time::GTC, ..Default::default() };` — `Registry::resolve` (state.rs:111) zaten var, bugun #[allow(dead_code)]; bu duzeltme onu uretim yoluna alir ve `ctx.cmd_tx.keys().next()` ile rastgele instance secimini de kaldirir. Ikinci savunma (istege bagli, EA): SINYAL_ACTION_SLTP ve CLOSE_POSITION dallarinda PositionSelectByTicket sonrasi `sym = PositionGetString(POSITION_SYMBOL)` ile uzerine yaz ve idx'i bu adla ikili aramayla yeniden bul (doldurma modu ve tick de dogru sembolden cozulsun).
```

</details>

---

## 16. [ONEMLI] Emirle birlikte gönderilen SL, doğrulama defterinin KAPSAMI DIŞINDA — broker onu sessizce düşürürse `sltp_unverified` gelmez

**Yer:** `D:\Projeler\Sinyal\crates\sinyal-core\src\server.rs:1175`  |  **Mercek:** zaman-penceresi

**Senaryo**

1) (Bulgu 1'deki yol belgelenip kullanılmaya başlandığında) istemci `{"op":"order",...,"sl":4300}` gönderir. 2) MT5 emri kabul eder ama broker SL'i düşürür (bazı broker'lar MARKET execution modunda DEAL ile gelen SL'i yok sayıp emri yine de doldurur). 3) Pozisyon açılır, SL=0. 4) `SltpGuard` bu emir için hiç kurulmadığı için 3 saniyelik izleme çalışmaz; `sltp_unverified` üretilmez, sayaçlarda iz kalmaz. 5) Sonuç: iki adımlı yolda yakalanan sessiz düşürme, atomik yolda tamamen görünmez olur. Yani atomik yolu belgelemek tek başına yetmez — `submit_order` içinde `sl != 0` iken de `arm_sltp_verify` kurulmalı, aksi halde bir sessiz boşluk başka bir sessiz boşlukla takas edilmiş olur.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — defteri emrin GONDERILDIGI anda degil, DOLUM olayinda kur (bilet ancak orada bilinir):

1) `submit_order` (server.rs:1448-1532): `let sl = norm(req.sl);` hesaplandiktan sonra, `dispatch` KABUL ederse (`kind == "queued"`) ve `sl != 0.0` ise istenen degeri wire kimligiyle sakla — `SltpGuard`a kucuk bir alan: `want_on_fill: Mutex<HashMap<u64 /*wire client_id*/, f64>>`. (Reddedilen emirde kayit yapilmaz; `arm_sltp_verify`deki "queued" olcutuyle ayni kural.)

2) Dolum olayinda kur: `FeedEvent::Order` icinde `position != 0` gelen olay bizim emrimize aitse (`client_id` -> `ctx.orders.text_of`, server.rs:908'deki ayni cozum), `want_on_fill`ten `want_sl`i AL (remove) ve `ctx.sltp.arm(&id, position, want_sl, Instant::now())` cagir. Bu tek hook, `serve` icindeki mevcut gozcu gorevinin (server.rs:562-586) yanina bir `ctx.events.subscribe()` dinleyicisi olarak konmali — baglanti basina `to_wire`e KONMAMALI, yoksa her abone icin bir kez kurulur.

Boylece atomik yol da mevcut `sweep_sltp` mantigina girer: 3 sn sonra pozisyonun `sl`i istenen degere ulasmadiysa `sltp_unverified` gider, sayaclar (`sent/verified/unverified/closed`) tutar ve iki yol ayni garantiye sahip olur. Pending emirlerde de dogru calisir, cunku kurulum dolum anindadir.

IKINCIL (ayni asimetriyi kapatir, ayri commit olabilir): `submit_order` icinde `sl`/`tp` icin `sinyal_proto::respects_stops_level(...)` on kontrolunu ekleyip ihlalde `rejected` dondurmek — simule yolun 10016 davranisiyla esitlenir (bkz. test `broker_side_rejections_carry_the_real_mt5_retcode`, server.rs:3747).

TEST (mevcut desene uyar, server.rs:3908+ blogu): `live_ctx` ile `ClientMsg::Order { sl: 1.09500 }` gonder, `position=942649399` tasiyan bir `FeedEvent::Order` yayinla, `publish_position(ticket, 0.0)` ile SL'siz durum yayinla; `sweep_sltp(after_window())` TAM BIR `SltpUnverified` uretmeli ve `counts().sent == 1` olmali.
```

</details>

---

## 17. [ONEMLI] Canli yolda modify_sltp stops_level'a karsi ON DENETIM yapmiyor ve fiyati tick_size izgarasina oturtmuyor; simulator ikisini de yapiyor — paper'da aninda 'rejected' alan komut canlida 'queued' aliyor

**Yer:** `crates/sinyal-core/src/server.rs:1414`  |  **Mercek:** reddedilme

**Senaryo**

1) Sinyal sistemi paper kipinde gelistirilip test edilir. Fiyata cok yakin bir SL gonderdiginde motor 10016 ile ANINDA `kind:"rejected"` doner; strateji bunu gorup SL'i uzaklastirir.
2) Ayni kod canliya alinir. GOLD'da stops_level tipik olarak sifirdan buyuktur ve spread genisleyince istenen SL bu bandin icine duser.
3) Canli yolda hicbir on denetim yok: komut oldugu gibi MT5'e gider, istemci `kind:"queued"` alir ve stop'u kurulmus sayar.
4) Broker 10016 ile reddeder; bu tele yalnizca `kind:"txn", retcode:10016` olarak cikar (4. bulgu) — 'rejected' etiketi YOKTUR, yani paper'da calisan ret isleme mantigi canlida hic tetiklenmez.
5) En iyi ihtimalle 3 saniye sonra sltp_unverified uyarisi gelir; kotu ihtimalle 1./2. bulgudaki sessiz yollardan biri devreye girer ve hicbir uyari gelmez. Iki kip arasindaki bu ayrisma, projenin acikca kapatmaya calistigi 'test ile canli arasindaki fark'in ta kendisi.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Tek yerde, server.rs:1170-1177 canli dalda (`submit_simple` cagrisindan ONCE), sim yolunun kullandigi ayni veriyle ayni iki isi yap:

1) IZGARA (kosulsuz): bileti `collect_positions(ctx)` icinde bulup sembolu al, `ctx.registry.resolve_any(sym)` + `ctx.registry.symbol(...)` ile `SymbolEntry`yi cek ve `submit_order`daki `norm` kapanisinin AYNISINI uygula:
   let norm = |p: f64| if p == 0.0 { 0.0 } else { sinyal_proto::normalize_price(p, entry.tick_size, entry.digits) };
   `submit_simple`e `norm(sl)`/`norm(tp)` gecir. Sembol/pozisyon cozulemezse ham gonder (davranis bugunkuyle ayni kalir).

2) STOPS_LEVEL ON DENETIMI (yalnizca GORUNTU TAZEYKEN): `state_age_ms(ctx, now) <= SLTP_STATE_STALE` ve `ctx.registry.snapshot` son tick'i varsa, sim ile ayni semantikte denetle — referans fiyat pozisyon BUY ise `bid`, SELL ise `ask`; isaretli mesafe `(refp - nsl)` (buy) / `(nsl - refp)` (sell) icin `distance / point + POINT_TOLERANCE >= stops_level`. Ihlalde MT5'e hic gondermeden sim ile AYNI cifti don:
   ServerMsg::Order(OrderEvent { retcode: Some(10016), comment: "...stops_level...", ..event(ctx, id, "rejected", "") })
   Boylece `arm_sltp_verify` (queued aramadigi icin) zaten silahlanmaz ve sayac kirlenmez — paper'daki `sent == 1` testiyle ayni davranis.

IKI TUZAK:
- `sinyal_proto::respects_stops_level` (validate.rs:184-199) BURADA YETMEZ: mutlak mesafe kullaniyor ve `stops_level == 0` iken daima true donuyor; sim ise sifir stops_level'da bile YANLIS TARAF'i reddediyor (sim.rs:1402-1411). Iki kip ayni kalsin isteniyorsa isaretli mesafe kullanilmali (gerekirse `respects_gap` mantigini sinyal-proto'ya tasiyip iki taraftan da cagirin).
- Denetim MUTLAKA tazelik kapisinin arkasinda kalmali: bayat goruntuyle koruyucu bir stop'u reddetmek, brokerin kabul edecegi bir stop'u engellemek demektir ve bugunku halinden daha kotudur. Bayat/kor durumda: normalize et, gonder, dogrulamayi kur.

Ayrica server.rs:1641-1644'teki "canli `rejected` retcode tasimaz" yorumu bu on denetim icin guncellenmeli — kasitli olarak 10016 tasiyor.
```

</details>

---

## 18. [ONEMLI] Cok ornekli kurulumda modify_sltp RASTGELE bir ornege gonderiliyor (HashMap::keys().next()), bileti gercekten tutan ornege degil; dogrulama da tum orneklerin pozisyonlarini tek havuzda ariyor

**Yer:** `crates/sinyal-core/src/server.rs:1429`  |  **Mercek:** reddedilme

**Senaryo**

1) Iki broker baglidir (icmarkets, pepperstone). Pozisyon pepperstone'da acilmistir.
2) Istemci o pozisyon icin modify_sltp gonderir. `submit_simple` bileti sahiplenen ornegi aramaz; `cmd_tx.keys().next()` ile — cogu zaman icmarkets — YANLIS terminale komut yazar.
3) Yanlis terminalde o bilet yoktur; MT5 istegi reddeder (ve 4. bulgu geregi bu tele yalnizca ham retcode olarak cikar). Istemci ise `kind:"queued"` almistir.
4) 3 sn sonra sweep calisir: pozisyon pepperstone'un listesinde HALA vardir (birlesik havuzda bulunur). Eger `q.sl` istenen degere esitse (onceki bir modify'dan kalma / idempotent yeniden gonderim) 2. bulgudaki dal 'dogrulandi' der ve girdi sessizce duser.
5) Ek olarak: iki broker ayni bilet numarasini uretirse `find(|q| q.ticket == ...)` YANLIS brokerin pozisyonunu esler ve dogrulama tamamen anlamsiz bir satiri okur.

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk dogru duzeltme uc adim; ucu de mevcut veriyle mumkun, yeni protokol alani gerektirmez.

1) submit_simple hedefi bileti TUTAN ornek olsun (server.rs:1427-1431 yerine). Tek ornekte davranis aynen korunur (bayat/kirpik goruntude regresyon olmasin diye), coklu ornekte sahiplik sorulur:

   let instance = if ctx.cmd_tx.len() == 1 {
       ctx.cmd_tx.keys().next().cloned().unwrap()
   } else {
       // Bileti GERCEKTEN tutan ornegi bul; bulunamazsa REDDET.
       // Ilk ornege gondermek yanlis terminale yazmakti.
       match ctx.registry.all_states().into_iter().find(|(_, s)| {
           s.snap.positions.iter().any(|p| p.ticket == ticket)
               || s.snap.orders.iter().any(|o| o.ticket == ticket)
       }) {
           Some((inst, _)) => inst,
           None => return rejected(ctx, id, "bileti tutan instance bulunamadi"),
       }
   };

2) Yetki kapisi cozulen ornek icin YENIDEN calissin: `dispatch`ten hemen once
   `if let Err(why) = ctx.registry.trading_gate(&instance, ctx.allow_live) { return rejected(ctx, id, &why); }`
   (server.rs:1327'deki "ilk ornek" denetimi ancak kaba on-eleme olarak kalabilir; 1326'daki yanlis yorum da duzeltilmeli — submit_order icin de ayni ikinci denetim eklenmeli, bugun yok).

3) Defter ornegi tasisin: `PendingSltp`e `instance: String` alani eklenip `SltpGuard::arm`/`arm_sltp_verify` bu degeri gecirsin (`dispatch` zaten kabul edilen komutun ornegini `OrderEvent.src` icine basiyor, oradan okunabilir); sweep_sltp:2159 eslemesi `q.src == p.instance && q.ticket == p.ticket` olsun ve FeedEvent::SltpUnverified'in `instance` alani `all_states().first()` yerine `p.instance` ile doldurulsun (server.rs:2151-2156, 2171, 2198). Boylece hem bilet cakismasi hem yanlis ornek atfi biter.
```

</details>

---

## 19. [ONEMLI] sinyald'de canlilik gozcusu yok: connected bir kez true olup asla false olmuyor, okuyucu thread'i izlenmiyor, telemetri yalnizca kumulatif sayac basiyor

**Yer:** `crates/sinyal-core/src/source.rs:228`  |  **Mercek:** olum-yollari

**Senaryo**

1) Okuyucu thread'i panikler ya da 392. satirdan doner. 2) JoinHandle atilmis oldugu icin bunu fark eden hicbir kod yoktur; WebSocket sunucusu baglanti kabul etmeye ve pong dondurmeye devam eder. 3) stats.connected hala true oldugu icin telemetri 'EA bekleniyor...' YAZMAZ; onun yerine her 30 saniyede bir DONMUS bir tick sayacini normal bir satir olarak basar (ornegin 'tick=8412337' saatlerce sabit) ve tek bir UYARI satiri bile uretmez. 4) Operator loglara bakar, satirlar akiyor, hicbir uyari yok — kopru 8 saat 'saglikli gorunerek' zombi kalir. Bu bulgu, tuketicinin bildirdigi '8 saat zombi' suresinin neden kimse tarafindan fark edilmedigini aciklar.

<details><summary><b>Onerilen duzeltme</b></summary>

```
EN KUCUK DOGRU DUZELTME — tek dosya (`crates/sinyal-core/src/main.rs`), okuyucuya ve protokole DOKUNMADAN.

Dogru canlilik sinyali TICK DEGIL, DURUM GORUNTUSUNUN YASI. Sebep: hafta sonu/piyasa kapaliyken tick akmaz ve "tick donmus" kontrolu yanlis alarm uretir (bu depo zaten `server.rs:57` yorumunda yanlis alarmi en pahali uyari sayiyor). Oysa EA durumu `OnTimer` ile SANIYEDE BIR yayinliyor (`server.rs:70-72`) ve `registry.set_state`'i (`source.rs:511`) SADECE okuyucu thread'i cagiriyor — dolayisiyla `InstanceSnapshot.at`'in bayatlamasi hem OLU/PANIKLEMIS okuyucuyu hem DONMUS EA'yi ayni anda yakalar.

1) main.rs:929-942 — tutamagi tut ve stats ile birlikte sakla:
   `let rd = spawn_reader(...);`  ve `stats_all.push((inst.clone(), stats, rd));`
   (`stats_all`'in tipi `Vec<(String, Arc<Mutex<ReaderStats>>, std::thread::JoinHandle<()>)>` olur; `JoinHandle::is_finished` 1.61'den beri stabil, `rust-version = 1.77` yeterli.)

2) main.rs:1206 dongusunde, `if !s.connected` kontrolunun HEMEN ARDINDAN iki satir ekle (registry telemetriye `registry.clone()` ile tasinir, `candles` icin zaten yapilan sey):

~~~rust
if rd.is_finished() {
    println!("[{inst}] UYARI: OKUYUCU THREAD'I OLDU — akis DURDU, asagidaki sayaclar DONMUS. \
              Acik pozisyonlarin stop'u kopruden yonetilemez.");
}
let age = telemetry_registry.state_of(inst).map(|s| s.at.elapsed());
match age {
    Some(a) if a.as_secs() >= 10 => println!(
        "[{inst}] UYARI: KOPRU ZOMBI OLABILIR — durum goruntusu {} sn'dir tazelenmedi \
         (EA saniyede bir yayinlamali). Sayaclar bayat.", a.as_secs()),
    None => println!("[{inst}] UYARI: hic durum goruntusu gelmedi."),
    _ => {}
}
~~~

Esik 10 sn: `SLTP_STATE_STALE` 5 sn, 30 sn'lik rapor araligina karsi bir-iki turluk pay birakir. Yeni thread, yeni sayac, yeni protokol alani gerekmez; yalnizca zaten var olan iki bilgiden (thread durumu + goruntu yasi) 30 saniyede bir UYARI uretilir.
```

</details>

---

## 20. [ONEMLI] Dogrulama kaydi hangi terminale ait oldugunu tutmuyor; sweep tum terminallerin pozisyonlarinda bilet ariyor

**Yer:** `crates/sinyal-core/src/server.rs:368`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

Iki terminal bagli ve bilet numaralari cakisiyor (ayri sunucular, ayri sayac). 1) pepperstone'daki 8814423 icin stop komutu kuruluyor. 2) `sweep_sltp` birlesik listede once alfabetik olarak once gelen icmarkets'in 8814423'unu buluyor. 3) O pozisyonun sl'i tesadufen istenen degere yakinsa (`stop_reached` toleransi bir tick_size, server.rs:2114-2117) kayit `verified` sayilip DUSURULUYOR — gercekte hicbir stop kurulmamis olabilir: YANLIS DOGRULAMA. 4) Tersi durumda ise gercek pozisyonun stop'u kurulmus olsa bile yanlis terminalin sl'i uymadigi icin YANLIS ALARM uretilir ve uyari `instance` alaninda yanlis terminali gosterir.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Bileti ORNEGE bagla — dort kucuk degisiklik, imza degisikligi gerekmiyor cunku kabul olayi dogru ornegi zaten tasiyor.

1) server.rs:368 — `PendingSltp`'ye alan ekle:
   struct PendingSltp { id: String, src: String, ticket: u64, want_sl: f64, deadline: Instant }
   ve server.rs:423 `SltpGuard::arm` imzasina `src: &str` ekleyip `src: src.to_owned()` ile doldur.

2) server.rs:2049-2056 — `arm_sltp_verify` kabul edilen olaydan ornegi CIKARSIN (bool yerine ad):
   let accepted = out.iter().find_map(|m| match m {
       ServerMsg::Order(e) if e.kind == "queued" => Some(e.src.clone()),
       _ => None,
   });
   if let Some(src) = accepted {
       ctx.sltp.arm(id, &src, ticket, sl, std::time::Instant::now());
   }
   Bu dogru kaynak: canli yolda `dispatch` server.rs:1381 `event(ctx, id, "queued", instance)` ile komutun GERCEKTEN gonderildigi ornegi basiyor; simule yolda `sim_reply` server.rs:1722+1731 `s.src_of(Some(look.instance))` ile sembolden cozulen ornegi basiyor.

3) server.rs:2159 — eslesmeyi ornekle daralt:
   let found = items.iter().find(|q| q.ticket == p.ticket && q.src == p.src);

4) server.rs:2151-2156'daki global `src` hesabini KALDIR; server.rs:2171 ve 2198'deki `instance: src.clone()` yerine `instance: Arc::from(p.src.as_str())` kullan — uyari artik kaydin gercek sahibini gosterir.

NEDEN SIMULE KIPI BOZULMAZ: karsilastirmanin iki tarafi da ayni `resolve_any(symbol)`'dan turuyor — `sim_positions` server.rs:1916 `src: sim_src(ctx, s, &p.symbol)` (server.rs:1659-1661 `s.src_of(resolve_any(symbol))`) ile `sim_reply` server.rs:1722 `s.src_of(Some(look.instance))` (look.instance = server.rs:1598 `resolve_any(symbol).0`) ayni adi uretir.

IKINCIL (ayni kok, istege bagli): server.rs:2161'deki tolerans `tick_size_of(ctx, &q.symbol)` -> server.rs:2100 `resolve_any` de ornek-kor; `ctx.registry.resolve(&p.src, symbol)` (state.rs:111) ile ornege sabitlenmeli.

AYRI IS OLARAK ACILMALI (bu duzeltmenin kapsaminda degil): `submit_simple` server.rs:1429 ve `gate` server.rs:1327 hala `cmd_tx.keys().next()` kullaniyor; cok-ornekli canli kipte close/cancel/modify_sltp rastgele terminale gidiyor. Istemcinin `instance` belirtmesi ya da 2+ ornekte bilet-hedefli komutlarin reddedilmesi gerekir.
```

</details>

---

## 21. [ONEMLI] Ayni bilete ikinci modify_sltp (trailing stop) eski kaydi gecersiz kilmiyor — her stop hareketi sahte 'DOGRULANAMADI' uyarisi uretiyor

**Yer:** `crates/sinyal-core/src/server.rs:423`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

1) t=0'da `modify_sltp {ticket: T, sl: 1.0950}` -> kayit A kuruldu, deadline t+3sn. 2) t=1sn'de trailing stop ilerliyor: `modify_sltp {ticket: T, sl: 1.0960}` -> kayit B kuruldu. 3) Broker ikisini de uyguluyor; durum yayininda sl = 1.0960. 4) t=3.1sn'deki taramada kayit A icin `stop_reached(1.0950, 1.0960, tick)` false, `now >= deadline` -> `Some(q)` dali calisiyor: `unverified` sayaci artiyor, tel uzerine `sltp_unverified` gidiyor ve daemon gunlugune "[stop] UYARI: sltp DOGRULANAMADI" satiri basiliyor (server.rs:577-581). 5) Stop'u saniyede bir tasiyan bir strateji, stop'u DOGRU calisirken dakikada onlarca sahte uyari uretir; sayacin `dogrulanamayan` sutunu surekli sisar ve gercek bir korumasiz-pozisyon uyarisi bu gurultunun icinde okunmaz hale gelir.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Defteri KOMUT basina degil BILET basina tut; supersede ederken saati SIFIRLAMA ve pencere icinde istenmis TUM degerleri kabul et.

crates/sinyal-core/src/server.rs:

1) Kayit yapisi (368-375):
~~~rust
struct PendingSltp {
    id: String,          // pencerede istenen SON komutun kimligi
    ticket: u64,
    /// Bu pencerede bu bilete istenen TUM stop degerleri.
    /// Durum goruntusu saniyede bir ornekleniyor (source.rs:483-487);
    /// ara degerlerden HANGISI yakalanirsa yakalansin, broker komutlari
    /// uyguluyor demektir.
    wants: Vec<f64>,
    /// ILK doyurulmamis istegin zamanindan gelir; supersede SIFIRLAMAZ.
    deadline: std::time::Instant,
}
~~~

2) `arm` (423-431):
~~~rust
fn arm(&self, id: &str, ticket: u64, want_sl: f64, now: std::time::Instant) {
    self.sent.fetch_add(1, Ordering::Relaxed);
    let mut pend = self.pending.lock().unwrap_or_else(|e| e.into_inner());
    match pend.iter_mut().find(|p| p.ticket == ticket) {
        // Ayni bilete YENI komut: istemcinin guncel istegi budur.
        // Saat KASITLI olarak yenilenmez — yoksa penceresinden hizli
        // trailing yapan bir strateji, broker her stop'u dusurse bile
        // hicbir uyari almazdi.
        Some(p) => {
            p.id = id.to_owned();
            if p.wants.len() < 64 { p.wants.push(want_sl); }
            self.superseded.fetch_add(1, Ordering::Relaxed);
        }
        None => pend.push(PendingSltp {
            id: id.to_owned(),
            ticket,
            wants: vec![want_sl],
            deadline: now + SLTP_VERIFY_WINDOW,
        }),
    }
}
~~~

3) `sweep_sltp` (2161) — eslesme "herhangi bir istenen deger" olsun; uyari son isteneni tasisin:
~~~rust
Some(q) if p.wants.iter().any(|w| stop_reached(*w, q.sl, tick_size_of(ctx, &q.symbol))) => { ...verified... }
~~~
ve 2170-2186 / 2197-2207'de `want_sl: p.want_sl` yerine `want_sl: *p.wants.last().unwrap()`.

4) Sayac dogrulugu: `SltpGuard`e `superseded: AtomicU64`, `SltpCounts`e `pub superseded: u64` ekle; main.rs:1289-1290'daki "bekleyen" cikarmasina dahil et (`c.verified + c.unverified + c.closed + c.superseded`), yoksa "bekleyen" sutunu kalici olarak siser.

5) Iki test kilidi: (a) `modify(m1,T,1.0950)` + `modify(m2,T,1.0960)`, yayinda yalniz 1.0960 -> `sweep_sltp(after_window())` BOS, `verified == 1`, `superseded == 1`; (b) broker HIC uygulamiyorken (yayinda sl=0) pencereden hizli 1 sn araliklarla uc modify -> ilk deadline'da TEK bir uyari MUTLAKA cikmali (susma regresyonu testi).

(Tamamlayici, ucuz onlem: source.rs:483'teki hizli ornekleme kosuluna `|| sltp.pending_len() > 0` eklenirse dogrulama penceresi boyunca durum 2 ms'de bir okunur ve ara degerler zaten yakalanir; SltpGuard'in okuyucu thread'ine gecirilmesi gerekir.)
```

</details>

---

## 22. [ONEMLI] submit_order canli-para kilidini cozulen ornek icin YENIDEN denetlemiyor — yorum aksini soyluyor

**Yer:** `crates/sinyal-core/src/server.rs:1325`  |  **Mercek:** kismi-ve-coklu

**Senaryo**

1) Iki terminal: `demo-a` (demo hesap) ve `real-b` (gercek para). `--allow-live` VERILMEMIS. 2) Istemci yalnizca real-b'de kote edilen bir sembol icin emir gonderiyor. 3) `gate()` `cmd_tx.keys().next()` ile demo-a'yi denetliyor: hesap demo, `is_real_money()` false -> kapi ACILIYOR. 4) `submit_order` sembolu real-b'de cozuyor ve komutu real-b'ye gonderiyor. 5) Gercek para kilidi tamamen atlanmis oluyor; ayrica MT5/hesap izinleri (trade_allowed, terminal_trade_allowed) de yanlis terminal icin dogrulanmis olur. Ayni mantik hatasi `submit_simple` icin de gecerli ama orada komut zaten denetlenen ornege gittigi icin tutarli kaliyor.

<details><summary><b>Onerilen duzeltme</b></summary>

```
En kucuk dogru duzeltme: `submit_order` icinde ornek COZULDUKTEN hemen sonra, herhangi bir `Cmd` uretilmeden once denetimi TEKRARLA — yani yorumun soyledigi seyi gercekten yap. crates/sinyal-core/src/server.rs:1454-1456 arasina ekle:

    let Some((instance, symbol_id)) = ctx.registry.resolve_any(&req.symbol) else {
        return rejected(ctx, &req.id, "sembol bulunamadi");
    };
    // Kapi, komutun GERCEKTEN gidecegi ornek icin dogrulanmali: `gate()`
    // ornegi bilmedigi icin `cmd_tx`in ilk anahtarina bakar ve o baska bir
    // terminal olabilir (canli-para kilidi + hesap izinleri o zaman yanlis
    // hesapta dogrulanmis olur).
    if let Err(why) = ctx.registry.trading_gate(&instance, ctx.allow_live) {
        return rejected(ctx, &req.id, &why);
    }

Bu tek eklemeyle server.rs:1326'daki yorum dogru hale gelir ve `gate()` icindeki mevcut kontrol (ticket'li cancel/close/sltp gibi ornegi bilinmeyen komutlar icin) oldugu gibi kalabilir; `submit_simple` zaten ayni ornege gonderdigi icin tutarlidir. Not: reddetme `gate()`in `ctx.orders.register(id)` cagrisindan SONRA olur, ama bu submit_order'daki mevcut ret yollariyla (sembol bulunamadi, hacim, filling) ayni davranistir, yeni bir tutarsizlik dogurmaz. Regresyon testi olarak: iki ornek kur (`demo-a` demo state, `real-b` REAL state), sembolu yalnizca `real-b`ye tanit, `--allow-live` vermeden emir gonder ve `rejected` + "canli para kilidi" beklenmeli.
```

</details>

---

## 23. [KUCUK] SinyalHistory Service oldugunde telemetri 'gecmis: acik' demeye devam ediyor — hist bir kez baglandiktan sonra asla None'a donmuyor

**Yer:** `crates/sinyal-core/src/source.rs:527`  |  **Mercek:** olum-yollari

**Senaryo**

1) MQL5 SinyalHistory Service durur. 2) hist hala Some oldugu icin telemetri satiri 'gecmis: acik istek=... zamanasimi=N' basar — 'acik' etiketi YANLISTIR. 3) Operator etikete guvenip Service'i yeniden baslatmaz; yalnizca zamanasimi sayacinin arttigini fark ederse anlar. Koruma kaybi ACISINDAN bu bilesenin olumu en zararsizidir: stop dogrulamasi durum yayinindan beslendigi icin (server.rs:2071-2082) etkilenmez ve bar tazeligi tel uzerinde hist/hist_note alanlariyla durustce bildirilir (server.rs:1299-1312). Kayip yalnizca MT5 kaynakli bar sadakatidir; mumlar tick uretimine duser.

<details><summary><b>Onerilen duzeltme</b></summary>

```
Duzeltme main.rs telemetri dongusunde olmali, source.rs'te DEGIL: 'hist'i zaman asiminda None'a cekmek yanlis olur — hist.rs:606 testinin korudugu 'service yeniden baslarken okunmamis barlar kaybolmasin' davranisini bozar ve donmus bir Service'i (CopyRates 30-60 sn bloklayabiliyor) olmus saymaktir. En kucuk dogru duzeltme: main.rs:1197'deki telemetri gorevinde ornek basina bir onceki orneklemin (hist_timeout, hist_bars, hist_reqs) degerlerini kucuk bir HashMap'te tut ve 1226'daki etiketi kanittan turet: hist_ready false ise 'kapali'; true ve son 30 sn'de hist_timeout arttiysa 'cevapsiz (kanal bagli, Service yanit vermiyor)'; true ve hist_reqs hic artmadi/0 ise 'bagli (denenmedi)'; true ve bar/tamamlanan cevap geldiyse 'acik'. Ek olarak source.rs:249'daki 'gecmis kanali bagli — MT5 barlari acik' log'unun ikinci yarisi kaldirilmali (yalnizca 'gecmis kanali bagli'), cunku baglanti tek basina bar akisini kanitlamaz.
```

</details>

---

## Curutulen iddialar

Bunlar **sorun degil**. Tekrar gundeme gelirse yeniden arastirilmasin diye yaziliyor.

- **Pencere tek bir gidiş-dönüş değil: 16 ms'lik EA timer'ıyla kapılanmış 4 seri atlama + internet üzerinden istemci RTT'si; ve SL, pozisyon bileti gelmeden hiç konamıyor**
  - _Neden curutuldu:_ CURUTULDU. Iddianin tasiyici onermesi — "SL, pozisyon bileti gelmeden hic konamiyor" — kodla acikca yanlislaniyor: kopru, stop'u ACILIS EMRININ ICINDE brokera gonderebiliyor. Zincir uctan uca mevcut: wire.rs:145-148 `OrderReq` `sl`/`tp` alanlarini kabul ediyor; server.rs:1517-1518 bunlari sembol izgarasina yuvarlayip `Cmd`e yaziyor; SinyalCollector.mq5:981-982 `req.sl = cmd.sl; req.tp = cmd.tp;` satirlarini `switch`ten ONCE kuruyor, yani SINYAL_ACTION_DEAL dali bunlari ayni `MqlTradeRequest` ile mq5:1085 `OrderSendAsync`e tasiyor. Broker stop'u dolumla ayni anda uygular: bilet yok, `txn` olayi yok, istemci RTT'si yok, ikinci timer kapisi yok — iddia edilen t2-t6 penceresi bu yolda SIFIR uzunluktadir. Bu olu bir kod yolu da degil: sim motoru piyasa emrinde acilis SL'ini modelleyip dogruluyor (sim.rs:691-703 `check_sltp`). Dolayisiyla pencere koprunun bir kisiti degil, tuketicinin sectigi "once ac, sonra modify_sltp" dizisinin sonucudur. Iddianin ikinci onermesi "hicbir ust siniri YOK" da yanlis: sweep_sltp tarayicisi BAGLANTI BASINA DEGIL SUNUCU BASINA kuruluyor (server.rs:562-586, yorumu bunu ozellikle soyluyor: kopan istemcinin biraktigi korumasiz pozisyon tam da endise edilen durum), SLTP_VERIFY_WINDOW(3s) + SLTP_SWEEP(250ms) icinde karar veriyor ve zombi hali AYRI BIR DAL olarak isleniyor (server.rs:2195-2208): bayat/kirpilmis/hic gelmemis goruntude `actual_sl: None` ve "kopru zombi olabilir" gerekcesiyle SltpUnverified uretiliyor, ayrica eprintln ile daemon gunlugune dusuruluyor (server.rs:577-581). Kurulmus (armed) bir stop icin 8 saatlik sessiz zombi ulasilabilir degil. Iddianin sayisal/satir kanitlari dogru (mq5:1206-1214, :44, :499-501, :927-930, :1224-1229, join.rs:79-84, source.rs:483-487, server.rs:60 hepsi teyit edildi) ama bunlardan cikarilan "delik" sonucu, atomik acilis-SL yolu ve sunucu capinda tarayici tarafindan curutuluyor. Geriye kalan tek gercek eksiklik KOD DEGIL BELGE: API.md:272-276 ornekleri `order` isleminde `sl` alanini hic gostermiyor ve API.md:513 ile 524-528 `modify_sltp`i stop'u brokera tasimanin TEK yolu gibi sunuyor; tuketiciyi pencereli iki adimli yola iten sey budur.
- **Bağ kurulamazsa dolum olayı 5 saniye sonra `id: ""` ile yayımlanıyor; API.md'nin öğrettiği eşleştirme kuralı o olayla ASLA tutmaz ve istemci bileti hiç öğrenemez**
  - _Neden curutuldu:_ CURUTULDU. Iddianin 1., 2. ve 5. adimlari (kod satirlari) dogru, ama zinciri ayakta tutan 3. adim -- "durum yayini da gelmezse bag hic kurulmaz" -- kodda ulasilabilir bir hal degil; ve "hicbir uyari uretilmez" iddiasi da yanlis. 1) 5 saniye, baglama gecikmesi DEGIL; asil baglama yolu 2 ms'de bir yokluyor. D:\Projeler\Sinyal\crates\sinyal-core\src\source.rs:483-487 -- `let state_due = if join.pending_len() > 0 { last_state_refresh.elapsed() >= Duration::from_millis(2) } else { ... from_secs(1) }`. Yani bekleyen bir olay VARKEN durum blogu saniyede bir degil, 2 ms'de bir okunuyor; source.rs:494-501 `seed_from_positions` / `seed_from_orders` donusunu ANINDA yayimliyor. Baglama kaynagi (POSITION_MAGIC) her emirde yaziliyor: mql5\Experts\SinyalCollector.mq5:978 `req.magic = cmd.magic; // client_id burada tasinir (64 bit)` ve :850 `p.magic = PositionGetInteger(POSITION_MAGIC)`. EA `OnTrade`'de durumu kirli isaretleyip (:927-929 `g_state_dirty = true`) bir sonraki timer'da yayimliyor (:1224-1229). Pratik atif gecikmesi ~10-20 ms, pencere 5000 ms -- yaklasik 300x pay. Bu tam olarak join.rs:412-440 `seeding_flushes_already_pending_events` gerileme testinin civileledigi davranis (o hata BIR KEZ yasanmis ve duzeltilmis). 2) "Zombi EA / donmus timer" senaryosu dolum olayini da durdurur. EA TEK THREAD (dosya basligi, satir 7-13: "OnTick/OnTimer/OnBookEvent/OnTradeTransaction isleyicileri AYNI thread'de sirayla calisir"). Iddianin varsaydigi DEAL_ADD'i halkaya yazan el (:656 `OnTradeTransaction`) ile durumu yayimlayan el (:1192 `OnTimer`) ayni el. Timer donmussa islem isleyicisi de donmustur; cekirdege dolum olayi hic ulasmaz. "Dolum geldi ama durum hic gelmedi" icin terminalin, halkaya yazma ile bir sonraki timer arasindaki ~16 ms'lik aralikta donmasi VE 5 saniyeden uzun donuk kalmasi gerekir. Tam o halde iddianin gondermek istedigi `modify_sltp` de komut halkasinda islenmeden bekler -- bileti ogrenmek pozisyonu korumazdi. 3) Uc bagimsiz baglama yolu var, iddia yalnizca ikisinin dusmesini kurguluyor: (a) `SEND_ACK` -> `by_request` (EA:1099 `ack.request_id = (uint)res.request_id;` -- `OrderSendAsync` bunu doldurur), (b) TRADE_TRANSACTION_REQUEST olayindan `result.order` -> `by_order` (EA:677-690), (c) durum goruntusundeki magic -> `by_position`/`by_order` (join.rs:93-108). Ucunun ayni anda dusmesi gerekir. 4) "Hicbir uyari uretilmez" yanlis: main.rs:1214 telemetri satiri `eslesme: bekleyen={} gec={} kimliksiz={}` basiyor ve main.rs:1265-1270 acikca `"[{inst}] NOT: {} emir olayi hicbir komuta baglanamadi (elle islem yaptiysan normal)."` yaziyor. 5) Kurtarma yolu iddianin sundugundan daha guclu. Istemci kendi SAYISAL wire kimligini zaten hic ogrenmiyor (OrderTracker, server.rs:322-364), yani `positions[].client_id` tek basina ise yaramazdi; ama ayni kayit `comment` alanini da tasiyor ve orada istemcinin KENDI metin kimligi var: server.rs:1444 `write_fixed_str(&mut cmd.comment, &short(id))` ve :1528 `let c = if req.comment.is_empty() { short(&req.id) }`; EA :866 `POSITION_COMMENT`'i kayda yaziyor; wire.rs:509-510 tel uzerinde gonderiyor. Yani `{"op":"positions"}` ile bilet, istemcinin elinde zaten olan metin kimligiyle eslesip bulunabiliyor. Kalan gercek risk (delik degil, belgeleme borcu): terminal dolumdan hemen sonra 5+ sn donarsa kendi dolumumuz `id:""` ile gidebilir ve istemciye bu durum icin tel uzerinde ayri bir isaret yok -- ama o halde koprude islem zaten yurumuyor, gunluge dusuyor ve pozisyon `positions[].comment` ile geri bulunabiliyor. Iddianin cekirdegi ("bilet asla eline gecmez, telafi yolu yok") kodda dogrulanmadi.
- **Telemetri 'dolum → stop kuruldu' süresini hiç ölçmüyor; sayaçlar tamamen sağlıklı görünürken her pozisyon ölçülmemiş bir süre korumasız kalabilir**
  - _Neden curutuldu:_ Iddianin OLGULARI dogru ama SONUCU curutuldu. Dogrulanan olgular: `PendingSltp` (server.rs:368-375) gercekten sadece id/ticket/want_sl/deadline tasiyor; `arm()` saati komut kabul edildigi anda basliyor (server.rs:423-431 + 2049-2056, `Instant::now()`); `SltpCounts` dort sayaç (server.rs:406-419) ve main.rs:1277-1302 bunlardan baska hicbir sey basmiyor; dogrulama BASARILI olunca istemciye olay da gitmiyor (sweep_sltp server.rs:2161-2164 sessizce dusuyor). Yani "dolum → stop kuruldu" suresi hicbir yerde olculmuyor. (Kucuk hata: telemetri satiri API.md:392-393'te degil, API.md:415-418'de.) Neden delik degil: 1) SAYAÇLAR BU SOZU HIC VERMEDI. Defterin ilan edilen kapsami server.rs:377-396 ve API.md:363-418: "modify_sltp'in KABULU, stop'un KURULDUGU anlamina gelmez". Olculen sey komut→kurulum dogrulamasidir, maruziyet suresi degil. "Saglikli gorunuyor" elestirisi, sayaçlari hic verilmemis bir soze karsi olcuyor. 2) O PENCERE TELIN ISTEMCI TARAFINDA OLUSUYOR VE ISTEMCI ICIN OLCULEBILIR. Dolum olayi `{"t":"order","kind":"txn",...,"position":N}` (API.md:313-319) zaten `modify_sltp`i gonderen ayni kanaldan istemciye gidiyor; komutu da istemci kendisi uretiyor. Iki uç da istemcinin elinde. Ustelik `positions[]` hem `sl` hem `time_msc` tasiyor (server.rs:2233-2237), yani kurulum ani da sorgulanabiliyor. Bu gecikmenin KOPRU kaynakli kismi ise ayrica ölçülüyor: `eslesme: bekleyen=N gec=N` (main.rs:1214-1225; join.rs:37 PENDING_WINDOW, source.rs:524-525). 3) KOPRU SIFIR-PENCERELI YOLU ZATEN SUNUYOR. `OrderReq.sl/tp` (wire.rs:145-148) canli yolda dogrudan MT5 komutuna geciyor (server.rs:1517) — stop giris emriyle AYNI istekte broker'a yazilabiliyor. Dolum-sonrasi modify deseni koprunun dayattigi bir mimari degil, tuketicinin secimi. 4) O ARALIK "korumasiz" degil, TEK KATMANLI. Ilan edilen kural (API.md:330-337) istemcinin kendi stop mantigini HER ZAMAN devrede tutmasini sart kosuyor; broker stop'u ASIL, istemci stop'u YEDEK. Senaryodaki 4 saniyede yedek katman aciktir. 5) AYNI EKSENIN FELAKET UCU ZATEN OLCULU: `kapanmis` sayaci ve API.md:417-418 — "beklenmedik bicimde buyukse, stop komutlariniz pozisyonlar kapandiktan SONRA gidiyor demektir". Gec kalma ekseni farkedilmis ve olculebilir ucundan enstrumante edilmis. 6) ONERILEN DUZELTME YANLIS SAYI URETIRDI. `arm()` HER `modify_sltp`te calisiyor — trailing stop guncellemeleri dahil. `PendingSltp`e pozisyonun acilis anini koymak, 3 saat once stop'u kurulmus bir pozisyonun trailing guncellemesinde "3 saat korumasiz" basardi. Ayrica `time_msc` broker saatidir (docs/OLCUMLER.md:122-130'da olculmus +3 saat kayma); yerel `Instant` ile cikarilmasi anlamsiz bir sayi verir. Dogru olcum ancak "bilet basina ILK arm" + yerel saatte ilk-goruldu damgasi ile kurulabilir; iddianin sundugu duzeltme bu haliyle yeni bir yanlis alarm kaynagi olurdu. Ozet: gercek bir "eksik metrik" gozlemi, ama koprunun verdigi sozu bozan bir delik degil; olculmek istenen aralik tuketici tarafinda uretiliyor, tuketici tarafindan olculebiliyor, koprunun katkisi ayri sayaclarla gorunur ve sifir-pencereli alternatif kodda mevcut.
- **Komut halkasi doluysa modify_sltp SESSIZCE dusuyor: istemciye 'queued' denmisti, ret olayi hic uretilmiyor — sadece daemon stderr'ine bir satir**
  - _Neden curutuldu:_ CURUTULDU. Alintilanan satirlar birebir dogru (source.rs:378-394 sadece eprintln, `Res`/`FeedEvent` uretmiyor; source.rs:214-216 yorumun aksine yalnizca atiyor; cmd_push_failures() gercekten hic okunmuyor — source.rs:522 sadece tick_push_failures). AMA iddianin guvenlik sonucu ("stop ne gonderilir ne de eksikligi bildirilir") bu depoda ozellikle bu risk icin kurulmus bir telafi kontrolu tarafindan kapaniyor: 1) Dogrulama, KOMUTUN TESLIMINE DEGIL, "queued" etiketine baglaniyor. server.rs:1170-1177 -> arm_sltp_verify (2049-2055) `kind=="queued"` gorunce defteri kuruyor. push_cmd basarisiz olsa da defter KURULU kalir. Yani "halka dolu" hatasi, dogrulama defteri acisindan "broker emri dusurdu" ile AYNI gozlemi uretir. 2) Sweep gercekten calisiyor ve gorunur: server.rs:566-586'da serve() icinde 250 ms'de bir (SLTP_SWEEP), 3 sn pencere sonrasi (SLTP_VERIFY_WINDOW). Uyari uc yere birden gider — tel uzerinde `sltp_unverified` (order kanali, server.rs:553), daemon stderr (server.rs:577-581) ve telemetri sayaci (main.rs:1277-1290, "gonderilen/dogrulanan/dogrulanamayan/kapanmis/bekleyen" her 30 sn). Bu davranis testle de sabitlenmis: server.rs:3932 `a_stop_the_broker_silently_dropped_is_reported` — pozisyon gorunur, sl degismemis -> tam bir uyari. 3) Iddianin kacis yolu (sweep'in "None if trustworthy -> sessizce closed" dali, server.rs:2191-2194) senaryonun kendisiyle celisiyor: halkanin dolmasi icin EA'nin OnTimer'i tamamen durmus olmali (SinyalCollector.mq5:1210 timer basina 64 komut bosaltiyor, halka 8192 slot — capacity::CMDS = 1<<13). O donma anindan ONCE acilmis pozisyonlar EA'nin son yayinladigi durum goruntusunde ZATEN vardir (PublishState ayni OnTimer'in 3. adimi, mq5:1221-1229). Yani zombi kopruda pozisyon "listede yok" degil, "listede ama sl eski" gorunur ve sweep `Some(q)` daliyla (server.rs:2168-2188) uyariyi basar. "11 pozisyon 10+ saat korumasiz" olayi bu kodda sessiz kalamazdi. Sessiz kalmasi icin pozisyonun donmus goruntude HIC yer almamasi gerekir; bu da donmanin, pozisyonun acilisi ile bir sonraki durum yayini (~16 ms) arasindaki pencerede baslamasini VE ayni anda 8192 komutun birikmesini gerektirir — kodda kaniti olmayan, ayri bir bulguya zincirlenmis varsayim. Geriye kalan (ama iddia edilen delik degil): (a) cmd_push_failures() cekirdekte hic okunmadigi icin dusen komut telemetride gorunmuyor, yalnizca stderr'de; (b) source.rs:214-216'daki yorum "reddedilmelerini sagliyoruz" derken kod yalnizca atiyor — yorum/kod uyusmazligi; (c) order/close/cancel icin SltpGuard benzeri bir telafi yok. Bunlar tesnis/hijyen eksigi; modify_sltp'in "sessizce dusup hic bildirilmemesi" iddiasi ise curutulmustur.
- **Brokerin GERCEK reddi (10016 / 10004 / 10013) hicbir yerde yorumlanmiyor: kind alani retcode'a bakmaz, SltpGuard retcode'u hic okumaz, OrderSendAsync yerelde basarisiz olsa bile olay 'ack' etiketiyle gider**
  - _Neden curutuldu:_ CURUTULDU. Alintilanan satirlar dogru, ama iddianin tasiyici sonucu ("bu sessiz yollar SltpGuard'dan kacislari acar") yanlis: SltpGuard retcode yolundan HIC beslenmiyor, bu yuzden uc yolun ucu de onu devre disi birakamaz. 1) Kanit satirlari dogrulandi ama eksik okunmus. source.rs:575-591 gercekten retcode'a bakmiyor. Ancak retcode tel uzerinde AYNEN gidiyor: source.rs:599 -> server.rs:912 `retcode: Some(*retcode)` (0 bile bastirilmiyor, `Some` sabit). Sozlesme wire.rs:563-566'da acikca yazili: "Emri yalnizca `txn` + `retcode` 10009 geldiginde dolmus sayin." Yani 10016 istemciye gorunur; kopru sadece kendi etiket taksonomisini UYDURMUYOR. Cekirdegin kendi defterinde de retcode'u dolum sanan bir durum makinesi yok — join.rs yalnizca client_id atfi yapiyor, dolum/koruma durumu tutmuyor. 2) `sent == false` -> SEND_ACK kacisi acmiyor, cunku defter DAHA ONCE kuruluyor. arm_sltp_verify (server.rs:2049-2056) `kind == "queued"` uzerine kuruluyor; "queued" ise server.rs:1381'de komut paylasilan bellek kanalina yazilir yazilmaz uretiliyor — EA `OrderSendAsync`'i cagirmadan ONCE. Dolayisiyla terminal yerel dogrulamayi gecemese de (mq5:1085,1113-1114) kayit zaten bekleyen defterde duruyor ve 3 sn sonra taramada uyariya donusuyor. 3) Sonuc halkasi dolarsa da defter bosalmiyor. sweep_sltp (server.rs:2141-2213) YALNIZCA pozisyon goruntusunu okuyor (positions_now, 2071-2082); Res yoluna hic bakmiyor. `pending` alani tum dosyada sadece 3 yerde geciyor (399 tanim, 425 arm, 2143 sweep) — hicbir Res isleme yolu kayit silmiyor. Dusen bir Res kaydi sessizce dusurmez, suresi dolup UYARIYA doner. 4) Bildirilen olayin ta kendisi kodda ayri bir dal olarak var. SLTP_STATE_STALE (server.rs:77, 5 sn) + positions_now: goruntu bayat, kirpilmis ya da hic yoksa liste GUVENILMEZ sayiliyor ve "pozisyon listede yok" cevabi kapanis DEGIL korluk olarak yorumlanip uyari uretiliyor (2195-2209, why: "kopru zombi olabilir", state_age_ms ile birlikte). "Zombi kopru 8 saat" senaryosunda her modify_sltp 3 sn icinde sltp_unverified uretir. 5) Uyari sessiz kalamiyor: tarama baglanti basina degil SUNUCU basina, 250 ms'de bir calisiyor (server.rs:562-586); istemci dusse bile stderr'e basiliyor (577-581) ve main.rs:1277-1302'de 30 sn'de bir "UYARI: n stop komutunun KURULDUGU dogrulanamadi" satiri cikiyor. 6) Tasarim retcode yorumlamaktan DAHA GUCLU: broker 10009 ile onaylayip stop'u yine de uygulamazsa retcode tabanli bir kontrol bunu KACIRIRDI; goruntuden yeniden turetme yakalar (stop_reached, 2114-2117, izgara toleransli). Kalan ama BU iddiaya ait olmayan iki gozlem: session.rs:582 `res_push_failures()` gercekten cagrilmiyor (sonuc halkasi icin telemetri boslugu, ama defteri etkilemiyor), ve `sent == false` durumunda "ack" etiketi kozmetik olarak yanlis (retcode MT5'in yerel hata kodunu tasiyor). Ikisi de "gercek reddin hicbir yerde yakalanmadigi" iddiasini ayakta tutmuyor.
- **SltpGuard TEK ATIMLIK: 3 saniyelik pencerede bir kez karar verip defterden dusuyor; pencereden sonra gelen broker reddi ve sonradan silinen/tetiklenmeyen stop hicbir zaman yeniden denetlenmiyor**
  - _Neden curutuldu:_ SATIRLAR DOGRU, SONUC YANLIS. Dogrulanan kod olgulari (D:\Projeler\Sinyal\crates\sinyal-core\src\server.rs): - 60: SLTP_VERIFY_WINDOW = 3 sn. Dogru. - 2158-2211 pend.retain: dort daldan ucu false (verified/unverified/closed), yalniz `_ if now < p.deadline => true`. Dogru. - Defterin TEK besleme noktasi 1175 (arm_sltp_verify), o da yalniz ClientMsg::ModifySltp icinden. Grep ile dogruladim: `arm_sltp_verify` icin tum depoda tek cagri yeri 1175, `sltp.arm` icin tek yer 2054. - submit_order (1448-1531) `sl: norm(req.sl)` gonderir ama arm etmez. Dogru. - Daemon'daki TUM periyodik gorevler: server.rs:569 (sweep, 250 ms) ve main.rs:1198 (telemetri, 30 sn). Baska bir pozisyon gozcusu yok. Yani "surekli gozetim yok" LAFZEN dogru. Iddiayi curuten noktalar: 1) KAPSAM ILAN EDILMIS, ustu ortulmemis. Ozellik "stop KURULDU MU" sorusudur, "stop hala duruyor mu" degil; ve bu sinir tel/dokuman/telemetriye ayni sekilde yazilmis: server.rs:385-390 ("Kapsam... koprunun kendi stop mantiginin yerine gecmez"), wire.rs:642-655, API.md:369 ("koprü komuttan sonra 3 saniye boyunca ... izler"), API.md:336-337 (broker SL = ASIL koruma, istemcinin kendi stop mantigi = YEDEK, "Kaldirmayin"), main.rs:1294-1301 (telemetri uyarisi acikca "Istemci kendi yedek stop'unu devrede tutmali" der). Ust-iddia (overclaim) yok; dolayisiyla arayuzun soyledigi ile yaptigi arasinda bosluk yok. 2) TELAFI EDICI KONTROL VAR VE YAYINDA. Surekli denetimin ihtiyac duydugu yer gercegi zaten istemciye acik: positions[].sl, EA tarafindan saniyede bir tazelenen durum goruntusunden servis ediliyor (server.rs:1130-1136, collect_positions 2215+) ve API.md:514'te "Acik pozisyonun SL/TP'si -> positions[].sl" olarak ilan edilmis. Stop sonradan brokerda kalkarsa bunu goren veri istemcide MEVCUT; kopru risk yoneticisi degil, veri duzlemi olmayi bilincle secmis. 3) IDDIANIN 4. MADDESI FAIL-SAFE, SESSIZ DEGIL. Sweep retcode'a bakmaz cunku BROKER'IN SOZUNE degil YER GERCEGINE (durum yayinindaki gercek sl) bakar — bu bilincli (API.md:392, "retcode yoktur, bu bizim gozlemimiz"). Broker reddi 3 sn'den gec gelirse, o ana kadar sl istenen degere ULASMAMIS olur ve sweep zaten `unverified` basar (test: server.rs:3931-3944, 4060-4076). Yani gec ret "iki sistem birbirini gormez" degil, "erken uyari" uretir; hata yonu guvenli tarafta. 4) IDDIANIN 5. MADDESI KORUMASIZ POZISYON URETMIYOR. MT5'te TRADE_ACTION_DEAL istegindeki gecersiz SL emrin TAMAMINI dusurur (10016) — pozisyon hic acilmaz, dolayisiyla "korumasiz pozisyon" dogmaz. EA sl'i oldugu gibi req.sl'e koyup tek adimda gonderiyor (SinyalCollector.mq5:981, 1006-1018); iki adimli "ac sonra SL koy" yolu yok. Zaten belgelenen desen ac -> modify_sltp'tir ve O yol dogrulaniyor. 5) IDDIANIN 2. MADDESI SPEKULATIF. "Teminat tamamlama yan etkisi / stops_level degisikligi sonrasi sunucu tarafli duzeltme ile kurulmus SL'in kalkmasi" icin ne kodda ne olcumlerde tek bir kanit yok; stop-out pozisyonu KAPATIR, SL'i soymaz. 6) GOSTERILEN GERCEK OLAY BU OZELLIGIN CURUTULMESI DEGIL, VAROLUS SEBEBI. API.md:352-355 o olayi kaydediyor: zombi kalan sey KOPRUYDU ve basarisiz olan KOPRUDEKI stop mantigiydi (pozisyonlar kapanmadan durdu). Care broker tarafina SL yazmak; bu canli olculmus (API.md:339-350 ve docs\OLCUMLER.md: sinyald Stop-Process -Force ile oldurulmus, sl=4369.28 degismemis). Zombi bir kopru zaten hicbir sey "yeniden denetleyemez"; surekli sweep de o olayda calismaz. Sonuc: gosterilen satirlar dogru, ama bu bir sessiz delik degil — ilan edilmis bir kapsam siniri, yer gercegine bakan fail-safe bir karar kurali ve istemciye acikca birakilmis (ve veriyle desteklenmis) bir telafi kontrolu. CURUTULDU. Not (delik degil, kalite gozlemi): SltpCounts.verified kumulatif "kurulum dogrulandi" sayacidir; telemetri satirinda `dogrulanan=N` etiketi bunu "su an korunan N pozisyon" gibi okutabilir (main.rs:1282-1291). Bu bir raporlama netligi meselesi, koruma acigi degil.
- **Kismi dolumda tek emirden dogan EK pozisyonlar defterin gorus alani disinda; sim bu senaryoyu hic modellemiyor**
  - _Neden curutuldu:_ CURUTULDU. Alintilanan satirlar dogru ama iddianin dayandigi senaryo bu kod tabaninin (ve MT5'in) pozisyon modelinde olusmuyor, ustelik "koprude bunu soyleyen tek bir satir yok" kismi olgusal olarak yanlis. 1) Satirlar gercekten oyle: server.rs:2049-2056 `arm_sltp_verify` dogrulamayi TEK bilete bagliyor; server.rs:2158-2159 `pend.retain(... items.iter().find(|q| q.ticket == p.ticket))` yalniz o bileti ariyor; SIM_NOT_MODELED icinde "kismi dolum ve likidite derinligi" var (server.rs:181, iddiada 182 denmis) ve sim hesabi `margin_mode: "hedging"` ilan ediyor (server.rs:1896-1897). Buraya kadar dogru. 2) AMA "tek emir -> iki pozisyon (P1/P2, farkli bilet)" MT5 hedging'de olusan bir durum degil, ve depo bunu kendi modelinde acikca sabitlemis: sim.rs:894 `let position = o.ticket; // MT5: pozisyon kimligi = emrin bileti`, join.rs:98 ve join.rs:214 "Hedging'de acilis emrinin bileti pozisyon biletidir", join.rs:491-492 ayni cumleyi "gozlemimizde" diye canli hedging hesabina dayandiriyor (README.md:51 — XM Global hedging demo), docs/MIMARI.md:194 ayni sey. MT5'te bir emrin TUM deal'leri ayni DEAL_POSITION_ID'yi (= emrin bileti) tasir; kismi dolum yeni bilet dogurmaz, ayni pozisyonun HACMINI buyutur. SL/TP ise pozisyon duzeyinde bir ozelliktir — ikinci dolumun ekledigi hacim ayni SL'in altinda kalir. Yani "stop'suz P2" diye bir nesne dogmuyor. 3) Tek istek -> birden cok EMIR yolu da kapali: validate.rs:124-158 `normalize_volume` hacmi gondermeden once `volume_max`a kisiyor (snap_down), yani terminalin bolmek zorunda kalacagi bir hacim hic gitmiyor. 4) Farazi olarak ikinci bir pozisyon olsaydi bile koprü sessiz degil: her DEAL_ADD olayi kendi `position` biletini + `volume`unu tasiyor (EA: SinyalCollector.mq5:690 `r.position = trans.position`; sunucu: server.rs:915-916), ve kismi dolumu tam dolumdan ayirmanin belgelenmis alani `order_state: 3 = PARTIAL` telden gidiyor (wire.rs:616-620, API.md:475 ve API.md:520 "Kismi dolumu tam dolumdan ayirmak"). Ayrica her pozisyon kendi `sl`i ve `client_id`si ile yayimlaniyor (server.rs:2222-2239). Yani "ayni magic'e ait stop'suz pozisyon" istemcinin gorebilecegi iki ayri yuzeyde duruyor. 5) Defterin kapsami zaten "komut basina", "pozisyon basina" degil ve bu ilan edilmis: server.rs:377-396 (`SltpGuard` doc), wire.rs:638-655, API.md:363-418; telemetri satiri da komut sayiyor (main.rs:1283 "modify_sltp gonderilen=.. dogrulanan=.."). Dogrulanan tek komut icin `dogrulanan=1` yazmak yanlis beyan degil. Koprünün hangi pozisyonun stop'u olmasi gerektigine dair bir niyeti yok; elle acilmis/hedge bacagi olan her stop'suz pozisyona alarm uretmek, kodun bilerek kacindigi gurultuyu (server.rs:2133-2140) uretirdi. 6) "Sim bu vakayi uretemez" argumani da bir delik degil: modellenmeyenler gizlenmiyor, `hello` ile telden ilan ediliyor (server.rs:828-829, wire.rs:385-402) ve bir test bunu zorunlu kiliyor (server.rs:3446-3453). sim.rs:1028-1035'in kismi kapanista bileti korumasi da MT5 ile AYNI (kismi kapanis POSITION_TICKET'i degistirmez); MT5'te biletin degistigi bilinen vaka swap tahakkukunda sunucu tarafli yeniden acilistir — kismi dolumla ilgisi yok ve 3 saniyelik dogrulama penceresinin disinda kalir. Kod bu ayrimin farkinda: `ticket` ve `identifier` ayri tasiniyor ve join ikisini birden tohumluyor (state.rs:271-277, join.rs:96-102). Not (iddia disi, tek gercek zayif nokta): canli pozisyon olmayan bir bilete `modify_sltp` gonderilirse sweep bunu sessizce `kapanmis` sayiyor (server.rs:2191-2194) — ama bu sayac telemetride gorunuyor ve API.md:416-418 "kapanmis beklenmedik bicimde buyukse" diye yorumunu veriyor.
- **Dogrulama defteri yalnizca ESKI bileti ariyor: cifte-tetigin dogurdugu yeni/ters pozisyonu goremez, 'kapanmis' deyip sessiz duser**
  - _Neden curutuldu:_ CURUTULDU. Kodun literal kismi dogru, sonuc kismi dayanaksiz. 1) Literal dogru olan tek sey: server.rs:2159 gercekten `items.iter().find(|q| q.ticket == p.ticket)` ile SADECE bilet uzerinden eslesiyor ve defter kaydi yalnizca acik bir `modify_sltp` ile kuruluyor (server.rs:1170-1176 -> 2049-2056). Bu kadari dogru. 2) Ama iddianin bel kemigi olan T2 (cifte-tetigin dogurdugu yeni/ters pozisyon) BU DEPODA DOGAMAZ — iddia onu "bulgu #2/#4" diyerek disaridan ithal ediyor, kod yolu gostermiyor. Kapatma yolu ucundan uca kapali: - server.rs:1161-1166: `Close` -> `submit_simple(action::CLOSE_POSITION, ticket, ...)`. Kopru kendi basina HICBIR yerde pozisyon acmiyor; pozisyon yalnizca istemcinin acik `ClientMsg::Order` istegiyle acilir ve o da `OrderTracker::register` ile ayni id'de `duplicate` doner (server.rs:1332, 343-355) — cift emir sessizce gecmez. - SinyalCollector.mq5:1060-1063: `SINYAL_ACTION_CLOSE_POSITION` ONCE `PositionSelectByTicket(cmd.ticket)` yapiyor; pozisyon yoksa `PushRejection(..., "pozisyon yok")` ile ERKEN DONUYOR, broker'a hicbir sey gitmiyor. - Kontrol ile OrderSend arasindaki mikro-yaris bile T2 uretemez: mql5:1069-1075 `req.action = TRADE_ACTION_DEAL` ile birlikte `req.position = cmd.ticket` set ediliyor. Bu MT5'in "su pozisyonu kapat" bicimidir; bilet artik yoksa sunucu retcode hatasi dondurur, TERS BIR POZISYON ACMAZ. Yani "yedek stop'un close'u yeni pozisyon dogurur" adimi kodda karsiligi olmayan bir varsayim. 3) Iddianin 5. maddesi ("tek cikti kapanmis=1, ayirt edilemez") de yanlis: EA'nin "pozisyon yok" reddi `SINYAL_RES_REJECTED` + comment olarak tele ciker (mql5:935-951, join.rs:145-148, source.rs:587 -> `kind:"rejected"`). Cifte-tetigin ikinci kapatmasi `order` kanalinda sebebiyle birlikte GORUNUR; defterdeki `closed` sayaci tek sinyal degil. Kaldi ki bu senaryo server.rs:387-390'da acikca ongorulmus ve beklenen davranis olarak yazilmis. 4) Tuketicinin bildirdigi GERCEK olay (zombi kopru, korumasiz pozisyonlar) tam da defterin YAKALADIGI dal: `trustworthy` = (yas <= SLTP_STATE_STALE=5sn) && !truncated (server.rs:2071-2082). Zombi/bayat/hic gelmemis goruntude `None if trustworthy` CALISMAZ; `None =>` dali calisir, `SltpUnverified` ("kopru zombi olabilir") hem tele hem daemon gunlugune yazilir (server.rs:2195-2208, 572-584). Sayim sirasinda atlanan bir pozisyon da bu kor noktaya dusmez: EA atlayinca `unstable` isaretliyor ve pos_total > yazilan sayi oldugu icin state.rs:651-652'de TRUNCATED de kuruluyor, `truncated` ise guvenilirligi dusuruyor. 5) Geriye kalan "defter yalnizca kendi gonderdigi bileti izler" olgusu bir delik degil, ilan edilmis kapsam: SltpGuard'in isi "gonderdigim SL 3 sn icinde broker'da durdu mu" (SLTP_VERIFY_WINDOW=3sn, tek atislik). Hesaptaki her pozisyonun korumali olup olmadigini surekli denetleyen bir monitor oldugu hicbir yerde iddia edilmiyor (server.rs:377-396 kapsami acikca yaziyor) ve anlik korumasizlik zaten `positions` yuzeyinden her pozisyonun `sl` alaniyla gorulebiliyor. Kapsam genisletme talebi olabilir; kanitlanmis bir hata degil.
- **EA'daki 'pozisyon yok' kapisi TOCTOU; asenkron gonderimin GERCEK sonucu hicbir yerde denetlenmiyor**
  - _Neden curutuldu:_ CURUTULDU. Alintilanan satirlar dogru ama iddianin yuk tasiyan adimlari (4, 5, 6) kodda ve dokumanda karsiligi olmayan varsayimlar. 1) "Gercek sonuc hicbir yerde denetlenmiyor" YANLIS — sonuc tele TASINIYOR ve dogru istemciye BAGLANIYOR. - D:\Projeler\Sinyal\mql5\Experts\SinyalCollector.mq5:722 -> OnTradeTransaction ICINDE `r.retcode = (uint)result.retcode;` yaziliyor. Yani broker'in GERCEK cevabi (or. pozisyon zaten kapaliysa donen hata kodu) TRADE_TRANSACTION_REQUEST olayiyla halkaya giriyor. - D:\Projeler\Sinyal\crates\sinyal-core\src\server.rs:912 -> `retcode: Some(*retcode)` ile KOSULSUZ tele yaziliyor. Cekirdek retcode'u yutmuyor; kopru olarak ham broker kodunu geciriyor. `order_kind`in retcode'a bakmamasi bir kayip degil, alan zaten aynen aktarildigi icin politika istemcide. - D:\Projeler\Sinyal\crates\sinyal-core\src\join.rs:225-227 ve 243-247 -> kapatma emrinin sahibi ACIKCA belgelenmis yoldan bulunuyor: SEND_ACK -> `by_request[request_id] = client_id`, sonra TRADE_TRANSACTION_REQUEST olayi ayni `request_id`yi tasidigi icin `resolve` onu ayni `id`ye atfediyor. Yani basarisiz kapatma, istemcinin kendi `id`si altinda gercek retcode ile geliyor. 2) "Istemci basarisiz kapatma ile gerceklesmis kapatmayi ayirt edemez" YANLIS — ayirt kurali YAZILI SOZLESME. - D:\Projeler\Sinyal\crates\sinyal-core\src\wire.rs:563-566: "`kind` = `ack` emrin DOLDUGU anlamina GELMEZ ... Emri yalnizca `txn` + `retcode` 10009 geldiginde dolmus sayin." - D:\Projeler\Sinyal\API.md:313-325 ayni kurali ornekle veriyor; API.md:88 istemci filtresini bile `m.kind === "txn" && m.retcode === 10009` olarak yaziyor. Basarisiz kapatma `txn` + 10009-DISI retcode olarak gelir; kural "10009 degilse dolmadi" oldugu icin GUVENLI TARAFA duser. 3) "Cakisma icin yazilmis tek satir kod yok" YANLIS — cakisma ISIMLE belgelenmis ve beklenen davranis olarak tanimlanmis. - D:\Projeler\Sinyal\API.md:359-361: "Cift tetiklenme zararsizdir. Ikisi ayni anda tetiklenirse ikinci kapatma 'pozisyon yok' hatasi alir. Bu beklenen davranistir — ama loglayin, sessiz gecmesin." - Ayni karar server.rs:387-390'da SltpGuard'in kapsam notunda da yaziyor: "broker stop'u ASIL, kopru stop'u YEDEK ... ikisi ayni anda tetiklenirse ikinci kapatma 'pozisyon yok' alir; zararsiz ama loglanir." Yani bu bilinmeyen bir yaris degil, bilerek alinmis ve dokumante edilmis bir karar. 4) "API.md bu dizgeyi hicbir yerde belgelemiyor" YANLIS — "pozisyon yok" dizgesi API.md:360'ta birebir tirnak icinde gecer. `rejected` olayinin retcode tasimamasi da belgeli: API.md:436-439 "`queued` / `rejected` / `duplicate` koprunun kendi urettigi olaylardir" — bunlar broker cevabi degildir, dolayisiyla 0 "sahte basari" degil "broker kodu yok" demektir. Ustelik `kind:"rejected"` ile `kind:"txn"` telde zaten AYRI degerlerdir; istemci "kapatma emrim gitti" ile "reddedildi"yi kind alanindan ayirir. 5) Geriye kalan tek gercek unsur — `PositionSelectByTicket`in yerel goruntuye bakmasi — her MT5 istemcisinde kacinilmazdir (senkron `OrderSend`de bile ayni yaris vardir; otorite sunucudur) ve `req.position = cmd.ticket` (mq5:1070) tam olarak sunucunun bileti dogrulamasi icin konur; sunucunun cevabi da (1) ve (2)'deki yoldan istemciye ulasir. Ayrica pozisyon listesi saniyede bir yayinlanir ve `magic` alani client_id tasir (join.rs:93-103), yani ters bir pozisyon olussa dahi istemci kor kalmaz. Ozetle: pencere vardir, ama "sonuc denetlenmiyor / istemci ayirt edemiyor / hicbir sey yazilmamis / belgelenmemis" iddialarinin dordu de kodla ve API.md ile celisiyor. Delil yetersiz; iddia curutulmustur.

