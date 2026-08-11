//! # sinyal-bridge
//!
//! MT5 terminalinin süreç alanına yüklenen köprü DLL'i (`SinyalBridge.dll`).
//!
//! EA, `#import "SinyalBridge.dll"` ile bu kütüphaneyi çağırır; kütüphane
//! tick/derinlik verisini paylaşımlı belleğe yazar ve çekirdekten gelen emir
//! komutlarını EA'ya okutur. Süreç sınırını geçmek için soket veya named pipe
//! kullanmıyoruz: çağrı süreç içi olduğu için syscall maliyeti tamamen ortadan
//! kalkıyor.
//!
//! ## Katmanlar
//!
//! - [`session`] — saf Rust API. Testler ve `latency-bench` bunu kullanır.
//! - [`ffi`] — MQL5'in gördüğü C ABI. Her export `catch_unwind` ile sarılıdır.
//!
//! ## MT5 kurulumu
//!
//! `SinyalBridge.dll` terminalin `MQL5\Libraries\` klasörüne kopyalanır ve
//! terminalde **"Allow DLL imports"** açık olmalıdır. Kendi VPS'imizde bu bir
//! sorun değil; son kullanıcı dağıtımı gündeme gelirse engel oluşturur.
//!
//! DLL 64-bit olmalıdır (`x86_64-pc-windows-msvc`) — MT5 terminali 64-bit'tir
//! ve 32-bit DLL sessizce değil, "cannot load library" ile reddedilir.

// Dışa açık yüzey bilinçli olarak C/MQL5 tarzı PascalCase: `SinyalOpen`,
// `SinyalPushTick`... MQL5 `#import` bildirimleri bu adlarla birebir eşleşmek
// zorunda, bu yüzden Rust'ın snake_case kuralı burada geçerli değil.
#![allow(non_snake_case)]

pub mod ffi;
pub mod session;

pub use session::{Capacities, Session, SessionError};

// C ABI yüzeyini yeniden dışa aktar ki `rlib` olarak kullananlar (testler,
// latency-bench) sabitlere erişebilsin.
pub use ffi::{
    sizeof_what, SINYAL_ERR_ARGS, SINYAL_ERR_HANDLE, SINYAL_ERR_OTHER, SINYAL_ERR_PANIC, SINYAL_OK,
    SINYAL_WOULD_BLOCK,
};
