# mini-tcp-tunnel

Ham Berkeley soketleri (`sys/socket.h`, `netinet/in.h`) ve `select()` ile, **hiçbir harici kütüphane kullanmadan** C++17'de yazılmış, eğitim amaçlı, çift yönlü (bi-directional) bir TCP tüneli / proxy'si.

```
 ┌────────┐    TCP     ┌──────────────────────────┐    TCP     ┌──────────────┐
 │ İstemci│ ─────────▶ │      mini-tcp-tunnel     │ ─────────▶ │ Hedef sunucu │
 │        │ ◀───────── │  (dinleme portu: 8080)   │ ◀───────── │ (ör. :9000)  │
 └────────┘            │  select() + XOR + loglar │            └──────────────┘
                       └──────────────────────────┘
```

Tünel, kendisine bağlanan her istemci için hedef sunucuya ayrı bir TCP bağlantısı açar. İki yöndeki baytları aktarırken her parçaya (chunk) isteğe bağlı bir **XOR maskesi** uygular ve parça boyutlarını loglar.

---

## İçindekiler

1. [Özellikler](#özellikler)
2. [Proje yapısı](#proje-yapısı)
3. [Mimari](#mimari)
4. [TCP proxy mantığı](#tcp-proxy-mantığı)
5. [`select()` ile I/O multiplexing](#select-ile-io-multiplexing)
6. [Bağlantı kapanışı: EOF, half-close ve hatalar](#bağlantı-kapanışı-eof-half-close-ve-hatalar)
7. [Ara katman: XOR dönüşümü](#ara-katman-xor-dönüşümü)
8. [Derleme](#derleme)
9. [Kullanım](#kullanım)
10. [Test senaryoları](#test-senaryoları)
11. [Log çıktısı](#log-çıktısı)
12. [Sınırlamalar ve geliştirme fikirleri](#sınırlamalar-ve-geliştirme-fikirleri)

---

## Özellikler

- **Tek thread, engelsiz (non-blocking) I/O:** Tüm soketler `O_NONBLOCK` modunda; tek bir `select()` döngüsü yüzlerce bağlantıyı yönetir.
- **Engelsiz `connect()`:** Hedef sunucuya bağlanırken döngü beklemez (`EINPROGRESS` + `SO_ERROR` kontrolü).
- **Çift yönlü aktarım:** Her oturumda iki bağımsız akış vardır: `client -> target` ve `target -> client`.
- **Geri basınç (back-pressure):** Her yön için sınırlı boyutlu bir tampon (buffer) kullanılır. Tampon dolunca kaynaktan okuma durur, böylece bellek kullanımı sınırsız büyümez.
- **Kısmi yazma (partial write) yönetimi:** `send()` verinin sadece bir kısmını gönderirse kalan baytlar tamponda bekler ve soket tekrar yazılabilir olduğunda gönderilir.
- **Half-close desteği:** Bir taraf yalnızca gönderme yönünü kapatırsa (`FIN`) bu durum karşı tarafa `shutdown(SHUT_WR)` ile iletilir; diğer yön çalışmaya devam eder.
- **Ara katman dönüşümü:** Her bayta isteğe bağlı XOR maskesi; verbose modda her parçanın boyutu ve hex önizlemesi (dönüşüm öncesi/sonrası).
- **RAII ile kaynak yönetimi:** Dosya tanımlayıcıları (fd) `UniqueFd` sınıfıyla yönetilir; hiçbir hata yolunda fd sızıntısı olmaz.
- **Düzgün kapanış:** `Ctrl+C` (`SIGINT`) / `SIGTERM` ile tüm oturumlar kapatılır ve özet istatistik basılır. `SIGPIPE` yok sayılır.
- **IPv4 ve IPv6:** Adresler `getaddrinfo()` / `getnameinfo()` ile çözülür ve biçimlendirilir.

---

## Proje yapısı

```
mini-tcp-tunnel/
├── CMakeLists.txt          # Modern CMake yapılandırması (C++17, uyarı bayrakları, sanitizer seçeneği)
├── include/
│   ├── TcpTunnel.hpp       # UniqueFd, ByteTransformer, TunnelConfig ve TcpTunnel sınıfı
│   └── Logger.hpp          # Zaman damgalı, seviyeli, header-only konsol logger'ı
├── src/
│   ├── TcpTunnel.cpp       # Event loop, oturum yaşam döngüsü, veri pompalama
│   └── main.cpp            # Komut satırı arayüzü (CLI) ve sinyal yönetimi
└── README.md
```

| Bileşen | Sorumluluk |
|---|---|
| `UniqueFd` | Bir fd'nin sahibi olan, sadece taşınabilen (move-only) RAII sarmalayıcı. Yıkıcısında `close()` çağırır. |
| `ByteTransformer` | Ara katman. Gelen her parçaya XOR maskesi uygular (anahtar yoksa hiçbir şey yapmaz). |
| `TunnelConfig` | Dinleme adresi/portu, hedef host/port, XOR anahtarı, oturum limiti, tampon boyutu, verbose. |
| `TcpTunnel` | Dinleme soketi, oturum tablosu ve `select()` döngüsü. |
| `TcpTunnel::Session` | Bir istemci bağlantısı + ona karşılık gelen hedef bağlantısı + iki yönlü akış durumu. |
| `detail::Flow` | Tek yönlü akış: kaynak soket → tampon → hedef soket; EOF bayrakları ve istatistikler. |
| `detail::ChannelBuffer` | Sabit kapasiteli bayt tamponu (okuma/yazma imleçleri ile). |

---

## Mimari

### Oturum (Session) modeli

Her kabul edilen (`accept`) istemci bağlantısı bir `Session` nesnesine dönüşür:

```
                         Session #N
        ┌──────────────────────────────────────────────┐
        │                upstream Flow                 │
clientFd│  recv() ─▶ [XOR] ─▶ ChannelBuffer ─▶ send()  │targetFd
 ◀────▶ │                                              │ ◀────▶
        │  send() ◀─ ChannelBuffer ◀─ [XOR] ◀─ recv()  │
        │               downstream Flow                │
        └──────────────────────────────────────────────┘
```

Dönüşüm, veri **okunduğu anda** (tampona yazılırken) bir kez uygulanır. Tamponda bekleyen veri her zaman "gönderilmeye hazır" haldedir; kısmi yazmalarda tekrar dönüşüm yapılmaz.

### Oturum durum makinesi

```
   accept()          connect() == 0
 ───────────▶ Connecting ─────────────────────▶ Established
                  │   │  (EINPROGRESS → soket yazılabilir   │
                  │   │   olunca SO_ERROR == 0)             │
                  │   └─────────────────────────────────────┤
                  │ SO_ERROR != 0                           │ iki yön de bitti /
                  │ (ECONNREFUSED, ETIMEDOUT...)            │ recv/send hatası /
                  ▼                                         ▼ tünel kapanıyor
                Closed ◀────────────────────────────────────┘
```

`Closed` durumundaki oturumlar, döngünün her turunun sonunda `reapClosedSessions()` ile tablodan silinir.

### Event loop'un bir turu

```
┌─▶ 1. fd_set'leri sıfırla (FD_ZERO)
│   2. İlgi bildir:
│        - Oturum limiti dolmadıysa dinleme soketi → readSet
│        - Her oturum için registerInterest() kuralları (aşağıda)
│   3. select(maxFd + 1, &readSet, &writeSet, nullptr, 500ms)
│   4. Hazır olan oturumlara hizmet et (serviceSession)
│   5. Kapanan oturumları sil (reapClosedSessions)
│   6. Dinleme soketi hazırsa yeni istemcileri kabul et (acceptPendingClients)
└── 7. stop() istenmediyse başa dön
```

Yeni istemcilerin **en son** kabul edilmesi bilinçli bir tercihtir: Bu turda kapatılan bir fd numarası, yeni açılan bir sokete yeniden atanabilir. Yeni oturumlar bu turun `fd_set`'lerine hiç bakmadığı için eski bitlerle karışmaz.

---

## TCP proxy mantığı

Bir TCP proxy'si, iki ayrı TCP bağlantısını birbirine "yapıştırır". Uygulama katmanındaki protokolü (HTTP, SSH, Redis, …) bilmez; sadece **bayt akışını** taşır.

1. **Dinleme:** `socket()` → `setsockopt(SO_REUSEADDR)` → `bind()` → `listen()` → `O_NONBLOCK`.
2. **Kabul:** Dinleme soketi okunabilir olduğunda `accept()` bekleyen tüm bağlantılar bitene kadar (`EAGAIN`) döngüyle çağrılır.
3. **Hedefe bağlanma:** Her istemci için yeni bir soket açılır ve engelsiz `connect()` çağrılır:
   - `0` dönerse (loopback'te olabilir) bağlantı hemen kurulmuştur.
   - `EINPROGRESS` dönerse TCP el sıkışması arka planda sürer. Soket **yazılabilir** olduğunda `getsockopt(SO_ERROR)` ile sonuç okunur.
4. **Aktarım:** İstemciden gelen baytlar hedefe, hedeften gelenler istemciye aktarılır. İstemci, hedef bağlantısı henüz kurulmadan veri göndermeye başlarsa bu veri tamponda bekletilir.
5. **Kapanış:** Bkz. [Bağlantı kapanışı](#bağlantı-kapanışı-eof-half-close-ve-hatalar).

### "Güvenli" aktarım ne demek?

TCP bir **akış** protokolüdür; mesaj sınırı yoktur. Bu nedenle:

- `recv()` 1 bayt da dönebilir, 64 KiB da. Tünel gelen parçanın boyutuna dair hiçbir varsayım yapmaz.
- `send()` istenen baytların **sadece bir kısmını** gönderebilir. Kalan kısım `ChannelBuffer` içinde tutulur ve kaybolmaz.
- `EAGAIN` / `EWOULDBLOCK` / `EINTR` hata değil, "şimdi olmaz, sonra tekrar dene" anlamına gelir.
- Gerçek hatalar (`ECONNRESET`, `EPIPE`, …) oturumu loglayarak kapatır; tüneli asla çökertmez.
- Linux'ta `send()` `MSG_NOSIGNAL` ile çağrılır, ayrıca `SIGPIPE` süreç genelinde yok sayılır. Böylece kopmuş bir sokete yazmak süreci öldürmez, sadece `EPIPE` döner.

### Geri basınç (back-pressure)

Hızlı bir gönderici ile yavaş bir alıcı arasında tünel sınırsız veri biriktirmemelidir. Her yönün tamponu `--buffer-size` kadardır (varsayılan 64 KiB):

- Tampon **doluysa** kaynak soket `readSet`'e eklenmez → tünel okumayı bırakır.
- Kaynağın çekirdek (kernel) alım tamponu dolar → TCP pencere boyutu (window) sıfıra iner → gönderici yavaşlar.

Aynı mantık dinleme soketi için de geçerlidir: `--max-sessions` dolduysa dinleme soketi izlenmez ve yeni bağlantılar çekirdeğin `listen` kuyruğunda bekler.

---

## `select()` ile I/O multiplexing

### Neden gerekli?

Engelleyen (blocking) soketlerle tek thread'de çalışan bir proxy, `recv(client)` çağrısında takılıp kalırken hedeften gelen veriyi kaçırır. Klasik çözümler:

| Yaklaşım | Artı | Eksi |
|---|---|---|
| Bağlantı başına thread | Basit kod | Bağlantı sayısıyla birlikte bellek ve context switch maliyeti artar |
| **`select()` / `poll()`** | Tek thread, taşınabilir (POSIX) | Her turda O(n) tarama |
| `epoll` / `kqueue` | O(1) ölçeklenme | Platforma özgü |

Bu proje eğitim amaçlı olduğu için en temel ve en taşınabilir mekanizma olan `select()` seçildi.

### `select()` nasıl çalışır?

```c
int select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout);
```

- `fd_set` her bitin bir fd'yi temsil ettiği bir bit dizisidir. `FD_ZERO`, `FD_SET`, `FD_ISSET` makrolarıyla yönetilir.
- `nfds`, izlenen **en büyük fd + 1** olmalıdır.
- `select()` çağrı sırasında kümeleri **yerinde değiştirir**: Dönüşte sadece hazır olan fd'lerin bitleri set kalır. Bu yüzden kümeler **her turda sıfırdan** kurulur.
- Dönüş değeri: hazır fd sayısı, zaman aşımında `0`, hatada `-1` (sinyal geldiyse `errno == EINTR`).

"Okunabilir" bir soket için `recv()` engellemez; ya veri döner, ya `0` (EOF), ya da hata. "Yazılabilir" bir soket için `send()` en az bir baytı kabul eder. Engelsiz `connect()` tamamlandığında soket yazılabilir hale gelir.

### Bu projede hangi fd hangi kümeye eklenir?

`TcpTunnel::registerInterest()` kuralları:

| Koşul | Eklenen fd | Küme |
|---|---|---|
| Oturum limiti dolmadı | dinleme soketi | `readSet` |
| İstemci EOF göndermedi **ve** upstream tamponu dolu değil | `clientFd` | `readSet` |
| Oturum `Connecting` durumunda | `targetFd` | `writeSet` |
| Hedef EOF göndermedi **ve** downstream tamponu dolu değil | `targetFd` | `readSet` |
| Upstream tamponunda gönderilecek veri var | `targetFd` | `writeSet` |
| Downstream tamponunda gönderilecek veri var | `clientFd` | `writeSet` |

> **Önemli:** Bir soketi gönderilecek veri yokken `writeSet`'e eklemek, `select()`'in sürekli anında dönmesine (busy loop, %100 CPU) yol açar. Bu yüzden yazma ilgisi sadece tampon boş değilken bildirilir.

### Zaman aşımı ve sinyaller

`select()` 500 ms zaman aşımıyla çağrılır. `Ctrl+C` geldiğinde sinyal işleyicisi sadece kilitsiz (lock-free) bir `std::atomic<bool>` bayrağını set eder (async-signal-safe). `sigaction` `SA_RESTART` olmadan kurulduğu için `select()` `EINTR` ile hemen döner ve döngü bayrağı görüp düzgünce kapanır.

### `FD_SETSIZE` sınırı

`fd_set` sabit boyutludur (Linux'ta genelde 1024). `FD_SETSIZE` değerine eşit veya büyük bir fd ile `FD_SET` çağırmak tanımsız davranıştır. Bu nedenle tünel:

- Bu sınırı aşan fd'leri reddeder ve loglar,
- `--max-sessions` değerini `(FD_SETSIZE - 16) / 2` ile sınırlar (her oturum 2 fd kullanır).

Daha fazla eşzamanlı bağlantı gerekiyorsa `poll()` veya `epoll` kullanılmalıdır.

---

## Bağlantı kapanışı: EOF, half-close ve hatalar

TCP'de bağlantının iki yönü **bağımsız** olarak kapatılabilir. Örneğin bir istemci isteğini gönderip `shutdown(SHUT_WR)` yapabilir ve hâlâ yanıtı okumayı bekleyebilir. İyi bir proxy bunu bozmamalıdır.

Her `Flow` iki bayrak tutar:

- `sourceEof`: Kaynaktan `recv()` → `0` geldi (karşı taraf `FIN` gönderdi).
- `sinkShutdown`: Bu `FIN`, hedef tarafa `shutdown(sinkFd, SHUT_WR)` ile iletildi.

Kurallar:

1. Kaynak EOF verdiğinde **tamponda kalan baytlar önce teslim edilir**, ancak tampon boşaldıktan sonra `FIN` iletilir (`propagateEof`).
2. Diğer yön bu sırada çalışmaya devam eder (half-close).
3. Her iki yönde de `sinkShutdown == true` olduğunda oturum `"both directions finished"` nedeniyle kapatılır.
4. `recv()`/`send()` gerçek bir hata döndürürse (`ECONNRESET`, `EPIPE`, `ETIMEDOUT`, …) oturum hemen kapatılır ve `WARN` seviyesinde loglanır.
5. Hedefe bağlantı başarısız olursa (`ECONNREFUSED` vb.) istemci soketi kapatılır; istemci bağlantının kapandığını görür.

```
İstemci                 Tünel                     Hedef
   │ ── "GET / ..." ──▶   │ ── "GET / ..." ──▶      │
   │ ── FIN ──────────▶   │ (tampon boş) ─ FIN ──▶  │   upstream bitti
   │                      │ ◀── "HTTP/1.0 200" ──── │
   │ ◀── "HTTP/1.0 200" ──│                         │
   │                      │ ◀── FIN ─────────────── │
   │ ◀── FIN ─────────────│                         │   downstream bitti → oturum kapanır
```

---

## Ara katman: XOR dönüşümü

`--xor-key` verildiğinde tünelden geçen **her bayt**, **her iki yönde** de anahtarla XOR'lanır:

```
çıktı[i] = girdi[i] ^ anahtar
```

XOR kendi tersidir (`x ^ k ^ k == x`). Bu nedenle aynı anahtarla çalışan **iki tünel art arda** konursa uçlardaki uygulamalar orijinal veriyi görürken, iki tünel arasındaki bağlantıda veri "karıştırılmış" olarak akar:

```
curl ──▶ Tünel A (:7000, XOR 0x5A) ══ karışık veri ══▶ Tünel B (:7001, XOR 0x5A) ──▶ HTTP sunucusu (:9000)
        düz metin                                                       düz metin
```

> ⚠️ **XOR şifreleme değildir.** Tek baytlık bir XOR anahtarı saniyeler içinde kırılır. Bu özellik yalnızca "proxy'nin içinden geçen veriyi değiştirebilir" fikrini göstermek içindir. Gerçek gizlilik için TLS kullanın.

Dönüşüm `ByteTransformer` sınıfında izole edilmiştir; farklı bir dönüşüm (ör. büyük/küçük harf çevirme, sayaçlı XOR, sıkıştırma) eklemek için sadece bu sınıfı değiştirmek yeterlidir.

---

## Derleme

### Gereksinimler

- Linux, macOS veya **Windows üzerinde WSL** (proje POSIX soketleri kullanır; yerel MSVC/Winsock desteklenmez)
- CMake ≥ 3.16
- C++17 destekleyen bir derleyici (GCC ≥ 8 veya Clang ≥ 7)

Ubuntu/Debian/WSL için:

```bash
sudo apt update
sudo apt install build-essential cmake netcat-openbsd curl python3
```

> **Windows kullanıcıları:** PowerShell'de `wsl --install` komutuyla Ubuntu kurup proje klasörüne `/mnt/c/...` yolundan erişebilirsiniz.

### Derleme adımları

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/mini-tcp-tunnel --help
```

### CMake seçenekleri

| Seçenek | Varsayılan | Açıklama |
|---|---|---|
| `CMAKE_BUILD_TYPE` | `Release` | `Debug`, `Release`, `RelWithDebInfo`, `MinSizeRel` |
| `TUNNEL_WARNINGS_AS_ERRORS` | `OFF` | Uyarıları hata olarak ele alır (`-Werror`) |
| `TUNNEL_ENABLE_SANITIZERS` | `OFF` | AddressSanitizer + UndefinedBehaviorSanitizer ile derler |

Geliştirme sırasında önerilen yapılandırma:

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug \
      -DTUNNEL_ENABLE_SANITIZERS=ON -DTUNNEL_WARNINGS_AS_ERRORS=ON
cmake --build build-debug -j
```

Kurulum (isteğe bağlı):

```bash
sudo cmake --install build      # /usr/local/bin/mini-tcp-tunnel
```

---

## Kullanım

```
mini-tcp-tunnel -l <port> -t <host> -p <port> [seçenekler]
```

| Seçenek | Açıklama | Varsayılan |
|---|---|---|
| `-l, --listen-port <port>` | İstemcilerin bağlanacağı yerel port (1-65535) | **zorunlu** |
| `-t, --target-host <host>` | Hedef sunucunun adı veya IP adresi | **zorunlu** |
| `-p, --target-port <port>` | Hedef sunucu portu (1-65535) | **zorunlu** |
| `-a, --listen-addr <ip>` | Bağlanılacak (bind) yerel IP adresi | `0.0.0.0` |
| `-x, --xor-key <byte>` | Her bayta uygulanacak XOR anahtarı (`90` veya `0x5A`, 1-255) | kapalı |
| `-m, --max-sessions <n>` | Eşzamanlı en fazla oturum sayısı | `64` |
| `-b, --buffer-size <bayt>` | Her yön için tampon boyutu (1 KiB - 16 MiB) | `65536` |
| `-v, --verbose` | Her parçayı boyut ve hex önizlemesiyle logla | kapalı |
| `-h, --help` | Yardımı göster | |

Çıkış kodları: `0` başarılı, `1` çalışma zamanı hatası (ör. port kullanımda), `2` hatalı komut satırı.

---

## Test senaryoları

Aşağıdaki örneklerde her komutu ayrı bir terminalde çalıştırın.

### 1. Etkileşimli metin (netcat)

```bash
# Terminal 1: hedef sunucu
nc -l 9000

# Terminal 2: tünel
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000 -v

# Terminal 3: istemci
nc 127.0.0.1 8080
```

Terminal 3'e yazdığınız satırlar Terminal 1'de, Terminal 1'e yazdıklarınız Terminal 3'te görünür. Tünel loglarında her parçanın boyutu ve hex içeriği görülür.

### 2. HTTP üzerinden

```bash
python3 -m http.server 9000                              # hedef
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000     # tünel
curl -v http://127.0.0.1:8080/                           # istemci
```

### 3. XOR dönüşümünü görmek

Tek tünelde XOR açıkken hedef sunucu bozuk baytlar alır. Bu beklenen davranıştır:

```bash
nc -l 9000 | xxd                                          # hedef: gelen baytları hex göster
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000 -x 0x20 -v
printf 'hello' | nc -N 127.0.0.1 8080
# 'h'(0x68) ^ 0x20 = 'H'(0x48) → hedef "HELLO" görür
```

### 4. Zincirleme tünel (XOR ile karıştırılmış ara bağlantı)

```bash
python3 -m http.server 9000
./build/mini-tcp-tunnel -l 7001 -t 127.0.0.1 -p 9000 -x 0x5A    # Tünel B (sunucu tarafı)
./build/mini-tcp-tunnel -l 7000 -t 127.0.0.1 -p 7001 -x 0x5A    # Tünel A (istemci tarafı)
curl http://127.0.0.1:7000/                                     # normal yanıt döner
```

7001 portundaki trafiği izleyerek verinin karıştırılmış olduğunu görebilirsiniz:

```bash
sudo tcpdump -i lo -X 'tcp port 7001'
```

### 5. Büyük veri ve bütünlük kontrolü

```bash
head -c 100M /dev/urandom > input.bin
nc -l 9000 > output.bin                                         # hedef
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000 -b 4096    # küçük tampon: kısmi yazma + geri basınç
nc -N 127.0.0.1 8080 < input.bin                                # istemci
sha256sum input.bin output.bin                                  # iki hash aynı olmalı
```

Aynı testi senaryo 4'teki gibi iki XOR tüneliyle de yapabilirsiniz; hash'ler yine aynı çıkmalıdır.

> `nc -N` (OpenBSD netcat) stdin bittiğinde `shutdown(SHUT_WR)` yapar. Bu, tünelin half-close iletimini test eder.

### 6. Hata durumları

```bash
# Hedef kapalıyken bağlanmak → "connect() ... failed: Connection refused"
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9999
nc 127.0.0.1 8080

# Port kullanımdayken başlatmak → çıkış kodu 1, "bind() ... failed: Address already in use"
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000

# Hatalı argüman → çıkış kodu 2
./build/mini-tcp-tunnel -l 70000 -t 127.0.0.1 -p 9000

# Oturum limiti: -m 1 ile ikinci istemci, ilki kapanana kadar listen kuyruğunda bekler
./build/mini-tcp-tunnel -l 8080 -t 127.0.0.1 -p 9000 -m 1
```

---

## Log çıktısı

Loglar zaman damgası ve seviye içerir. `INFO`/`DEBUG` stdout'a, `WARN`/`ERROR` stderr'e yazılır. `DEBUG` satırları sadece `-v` ile görünür. Senaryo 4'teki Tünel A'nın (`-l 7000 -p 7001 -x 0x5A -v`) bir oturumu şuna benzer:

```
14:02:11.512 [INFO ] tunnel ready: 0.0.0.0:7000 ==> 127.0.0.1:7001
14:02:11.512 [INFO ] transform: XOR mask 0x5a | max sessions: 64 | buffer: 64.00 KiB per direction | multiplexer: select() (FD_SETSIZE=1024)
14:02:11.512 [INFO ] press Ctrl+C to stop
14:02:15.037 [INFO ] [#1] accepted client 127.0.0.1:51544 (active sessions: 1/64)
14:02:15.037 [DEBUG] [#1] connecting to target 127.0.0.1:7001 ...
14:02:15.038 [INFO ] [#1] connected to target 127.0.0.1:7001 in 0 ms, tunnel established
14:02:15.038 [DEBUG] [#1] client -> target | chunk #1 | 78 bytes | in: 47 45 54 20 2f 20 48 54 54 50 2f 31 2e 31 0d 0a ... | out: 1d 1f 0e 7a 75 7a 12 0e 0e 0a 75 6b 74 6b 57 50 ...
14:02:15.041 [DEBUG] [#1] target -> client | chunk #1 | 1024 bytes | in: ...
14:02:15.041 [INFO ] [#1] target closed its sending side (EOF), 0 bytes still pending for client
14:02:15.041 [INFO ] [#1] forwarded EOF to client (target -> client finished, 1.00 KiB total)
14:02:15.042 [INFO ] [#1] client closed its sending side (EOF), 0 bytes still pending for target
14:02:15.042 [INFO ] [#1] forwarded EOF to target (client -> target finished, 78 B total)
14:02:15.042 [INFO ] [#1] session closed (both directions finished) | client -> target: 78 B in 1 chunks | target -> client: 1.00 KiB in 1 chunks | duration: 4 ms
^C
14:02:20.900 [INFO ] stop requested, shutting down
14:02:20.900 [INFO ] tunnel stopped | sessions served: 1 | client -> target: 78 B | target -> client: 1.00 KiB
```

Hata durumunda örnek:

```
14:05:42.101 [WARN ] [#2] session closed (connect() to 127.0.0.1:9999 failed: Connection refused) | client -> target: 0 B in 0 chunks | ...
```

---

## Sınırlamalar ve geliştirme fikirleri

- **`FD_SETSIZE` sınırı:** `select()` en fazla ~500 eşzamanlı oturuma izin verir. → `poll()` veya `epoll`/`kqueue`'ya geçiş iyi bir alıştırmadır; değişiklik `eventLoop()` ve `registerInterest()` ile sınırlıdır.
- **O(n) tarama:** Her turda tüm oturumlar dolaşılır. Binlerce bağlantı için `epoll` tercih edilmelidir.
- **Hedef çözümleme bir kez yapılır:** Hedef host adı başlangıçta çözülür ve sadece ilk adres kullanılır (yedek adreslere geçiş yok).
- **Boşta bekleme zaman aşımı yok:** Hiç veri göndermeyen bağlantılar süresiz açık kalabilir. → Oturum başına "son etkinlik" zamanı tutup idle timeout eklenebilir.
- **Engelsiz connect için zaman aşımı yok:** Çekirdeğin varsayılan TCP bağlantı zaman aşımı (Linux'ta ~2 dk) geçerlidir.
- **XOR şifreleme değildir:** Gerçek bir güvenli tünel için TLS (ör. OpenSSL) eklenmelidir.

Diğer fikirler: oturum başına bant genişliği limiti, hex dump'ı dosyaya kaydetme, SOCKS5 desteği, birim testleri (`ByteTransformer` ve `ChannelBuffer` için).
