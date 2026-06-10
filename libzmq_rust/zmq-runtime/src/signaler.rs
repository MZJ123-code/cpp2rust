//! Cross-thread signaling mechanism for waking I/O threads.
//!
//! 1:1 translation of C++ `signaler_t`.
//!
//! Creates a TCP socketpair on localhost for cross-thread wake-up signals.
//! The writer sends a single byte to wake the reader, which is registered
//! with the poller for `Interest::READABLE`.
//!
//! Uses interior mutability (`parking_lot::Mutex`) so `send()` and `recv()`
//! can be called from different threads with `&self`.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use parking_lot::Mutex;

/// A cross-platform signaling pair for waking I/O threads.
///
/// Equivalent to C++ `signaler_t` which uses:
/// - `eventfd` on Linux
/// - `socketpair` / TCP pair on Windows
pub struct Signaler {
    /// Write end — sending a byte wakes the reader
    w: Mutex<TcpStream>,
    /// Read end — registered with the poller to detect wake-ups
    r: Mutex<TcpStream>,
}

impl Signaler {
    /// Create a new signaler pair using a TCP socketpair on localhost.
    pub fn new() -> io::Result<Self> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr)?;
        let bound_addr = listener.local_addr()?;

        // Writer side connects to the listener
        let w = TcpStream::connect(bound_addr)?;
        w.set_nonblocking(true)?;
        w.set_nodelay(true)?;

        // Accept the reader side
        listener.set_nonblocking(true)?;
        let (r, _peer_addr) = loop {
            match listener.accept() {
                Ok(pair) => break pair,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        };
        r.set_nonblocking(true)?;
        r.set_nodelay(true)?;

        Ok(Self {
            w: Mutex::new(w),
            r: Mutex::new(r),
        })
    }

    /// Send a wake-up signal. Thread-safe: can be called with `&self`.
    pub fn send(&self) -> io::Result<()> {
        let mut w = self.w.lock();
        let dummy: [u8; 1] = [0];
        match w.write(&dummy) {
            Ok(_) => Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Signal already pending — fine
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Consume a wake-up signal. Thread-safe: can be called with `&self`.
    pub fn recv(&self) -> io::Result<()> {
        let mut r = self.r.lock();
        let mut dummy = [0u8; 64];
        match r.read(&mut dummy) {
            Ok(_) => Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Non-blocking receive. Returns true if a signal was consumed.
    pub fn recv_failable(&self) -> io::Result<bool> {
        let mut r = self.r.lock();
        let mut dummy = [0u8; 64];
        match r.read(&mut dummy) {
            Ok(n) => Ok(n > 0),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Check if the signaler is valid (both sockets open).
    pub fn valid(&self) -> bool {
        self.w.lock().local_addr().is_ok() && self.r.lock().local_addr().is_ok()
    }

    /// Lock the reader and get mutable access (for mio registration).
    ///
    /// Use this to register the reader with `mio::Poll`:
    /// ```ignore
    /// let mut r = signaler.reader_lock();
    /// poll.registry().register(&mut *r, token, Interest::READABLE)?;
    /// ```
    pub fn reader_lock(&self) -> parking_lot::MutexGuard<'_, TcpStream> {
        self.r.lock()
    }

    /// Lock the writer for mio operations.
    pub fn writer_lock(&self) -> parking_lot::MutexGuard<'_, TcpStream> {
        self.w.lock()
    }

    /// Get the raw file descriptor of the reader (Unix only).
    #[cfg(unix)]
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        use std::os::unix::io::AsRawFd;
        self.r.lock().as_raw_fd()
    }
}

impl std::fmt::Debug for Signaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signaler")
            .field("w_addr", &self.w.lock().local_addr())
            .field("r_addr", &self.r.lock().local_addr())
            .finish()
    }
}

// ─── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_create_signaler() {
        let sig = Signaler::new().unwrap();
        assert!(sig.valid());
    }

    #[test]
    fn test_send_recv() {
        let sig = Signaler::new().unwrap();
        sig.send().unwrap();
        sig.recv().unwrap();
        // After recv, no more signals
        let got = sig.recv_failable().unwrap();
        assert!(!got);
    }

    #[test]
    fn test_recv_failable_empty() {
        let sig = Signaler::new().unwrap();
        let got = sig.recv_failable().unwrap();
        assert!(!got);
    }

    #[test]
    fn test_recv_failable_after_send() {
        let sig = Signaler::new().unwrap();
        sig.send().unwrap();
        assert!(sig.recv_failable().unwrap());
        assert!(!sig.recv_failable().unwrap());
    }

    #[test]
    fn test_multiple_sends() {
        let sig = Signaler::new().unwrap();
        sig.send().unwrap();
        sig.send().unwrap();
        sig.recv().unwrap();
        assert!(!sig.recv_failable().unwrap());
    }

    #[test]
    fn test_send_does_not_block() {
        let sig = Signaler::new().unwrap();
        for _ in 0..100 {
            sig.send().unwrap();
        }
    }

    #[test]
    fn test_send_from_another_thread() {
        let sig = Arc::new(Signaler::new().unwrap());
        let sig2 = sig.clone();

        let handle = thread::spawn(move || {
            sig2.send().unwrap();
        });
        handle.join().unwrap();

        assert!(sig.recv_failable().unwrap());
    }
}
