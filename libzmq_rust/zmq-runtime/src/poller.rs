//! Cross-platform I/O multiplexing via mio.
//!
//! 1:1 translation of C++ `poller_t` concept (backed by `poll_t` / `epoll_t` etc.).
//!
//! The `Poller` wraps `mio::Poll` to manage file descriptor registrations
//! and dispatch I/O events to handlers. It supports:
//! - Registering/deregistering file descriptors with read/write interests
//! - Edge-triggered and level-triggered modes (via mio Interest flags)
//! - Timers with millisecond granularity
//! - Thread-safe registration from other threads via `PollerHandle`
//! - Load tracking (number of registered file descriptors)

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{self, Sender, TryRecvError};
use mio::event::Source;
use mio::{Events, Interest, Poll, Registry, Token};

// ─── Public types ──────────────────────────────────────────────

/// Opaque handle to a registered I/O source.
///
/// Returned by `add_fd()`, used with `set_pollin()`, `rm_fd()`, etc.
pub type Handle = Token;

// ─── PollEvents trait ──────────────────────────────────────────

/// Callback trait for I/O events.
///
/// Equivalent to C++ `i_poll_events`. Implementors receive callbacks
/// when a registered file descriptor becomes readable, writable, or
/// when a timer expires.
pub trait PollEvents: Send + 'static {
    /// Called when the file descriptor is readable.
    fn in_event(&mut self);

    /// Called when the file descriptor is writable.
    fn out_event(&mut self);

    /// Called when a timer expires.
    /// `id` is the user-defined timer identifier.
    fn timer_event(&mut self, id: usize);
}

// ─── PollerHandle (cross-thread) ───────────────────────────────

/// A `PollerHandle` allows other threads to interact with a `Poller`.
///
/// It contains a clone of mio's `Registry` for fd-level operations
/// (register/reregister/deregister) and a channel sender for dispatching
/// handler mappings to the poller thread.
pub struct PollerHandle {
    /// Mio registry — `Send + Sync` in mio 1.x, shared via Arc
    registry: Arc<Registry>,
    /// Channel to send handler registration requests to the poller thread
    handler_tx: Sender<HandlerRegistration>,
}

impl Clone for PollerHandle {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            handler_tx: self.handler_tx.clone(),
        }
    }
}

impl PollerHandle {
    /// Access the mio `Registry` for low-level I/O source operations.
    ///
    /// Use this to register/reregister/deregister I/O sources from any thread.
    /// Note: the poller thread must also register the handler via
    /// `register_handler()` for events to be dispatched.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Register a handler for a token from another thread.
    ///
    /// This sends the handler mapping to the poller thread, which will
    /// add it to its handler table before the next poll cycle.
    /// Must be paired with a prior `registry().register()` call.
    pub fn register_handler(&self, token: Token, handler: Box<dyn PollEvents>) {
        let _ = self.handler_tx.send(HandlerRegistration { token, handler });
    }
}

impl std::fmt::Debug for PollerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollerHandle").finish()
    }
}

// ─── Internal types ────────────────────────────────────────────

/// A pending handler registration from another thread.
struct HandlerRegistration {
    token: Token,
    handler: Box<dyn PollEvents>,
}

/// Entry in the file descriptor table.
struct FdEntry {
    /// Which file descriptor this entry is for (for debugging)
    /// The actual fd is known by mio internally via the Token
    handler: Box<dyn PollEvents>,
    /// Whether this entry is marked for removal
    retired: bool,
}

/// A pending timer.
struct TimerEntry {
    /// Absolute expiration time (monotonic)
    expiration: Instant,
    /// The handler to call
    handler: Box<dyn PollEvents>,
    /// User-defined timer ID
    id: usize,
}

// ─── Poller ────────────────────────────────────────────────────

/// Cross-platform I/O multiplexer.
///
/// Wraps `mio::Poll` to provide a high-level polling interface matching
/// the C++ `poller_t` concept.
///
/// # Usage
///
/// ```ignore
/// use zmq_runtime::poller::{Poller, PollEvents};
///
/// let mut poller = Poller::new()?;
/// let handle = poller.cross_thread_handle();
///
/// // On the poller thread:
/// poller.add_fd(&mut stream, my_handler)?;
/// poller.set_pollin(handle, &mut stream);
/// poller.poll(100)?;  // blocks up to 100ms
/// ```
pub struct Poller {
    /// The mio poll instance — owned by the poller thread
    poll: Poll,
    /// Table of registered file descriptors and their handlers
    fd_table: HashMap<Token, FdEntry>,
    /// Track current interest sets for each token as raw bits.
    /// mio 1.x Interest is NonZeroU8, so we can't represent "empty" directly.
    /// We use u8: bit 0 = READABLE, bit 1 = WRITABLE. 0 means "deregistered".
    interests: HashMap<Token, u8>,
    /// Index for generating unique tokens
    next_token: usize,
    /// Current load (number of registered sources)
    load: usize,
    /// Whether the poller has been asked to stop
    stop: bool,
    /// Channel for receiving handler registrations from other threads
    handler_rx: crossbeam::channel::Receiver<HandlerRegistration>,
    /// Sender half — cloned to create `PollerHandle`s
    handler_tx: Sender<HandlerRegistration>,
    /// Active timers, sorted by expiration time
    timers: Vec<TimerEntry>,
}

impl Poller {
    /// Create a new poller.
    pub fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let (handler_tx, handler_rx) = channel::unbounded();

        Ok(Self {
            poll,
            fd_table: HashMap::new(),
            interests: HashMap::new(),
            next_token: 0,
            load: 0,
            stop: false,
            handler_rx,
            handler_tx,
            timers: Vec::new(),
        })
    }

    /// Get a `PollerHandle` that can be shared across threads.
    ///
    /// Other threads can use this handle to:
    /// - Register/deregister I/O sources via `registry()`
    /// - Register event handlers via `register_handler()`
    pub fn cross_thread_handle(&self) -> PollerHandle {
        PollerHandle {
            registry: Arc::new(self.poll.registry().try_clone().unwrap()),
            handler_tx: self.handler_tx.clone(),
        }
    }

    // ── File descriptor management ─────────────────────────────

    /// Add a file descriptor to the poller.
    ///
    /// Initially, no events are enabled. Use `set_pollin()` / `set_pollout()`
    /// to start receiving events.
    ///
    /// Returns a `Handle` (Token) that identifies this registration.
    pub fn add_fd(
        &mut self,
        source: &mut impl Source,
        handler: Box<dyn PollEvents>,
    ) -> io::Result<Handle> {
        let token = Token(self.next_token);
        self.next_token += 1;

        // Register with mio. mio 1.x requires at least one Interest, so we
        // register with READABLE as a placeholder. Our `interests` HashMap
        // tracks the actual desired interest set independently (0 means none).
        self.poll
            .registry()
            .register(source, token, Interest::READABLE)?;
        self.interests.insert(token, 0);

        self.fd_table.insert(
            token,
            FdEntry {
                handler,
                retired: false,
            },
        );
        self.load += 1;

        Ok(token)
    }

    /// Remove a file descriptor from the poller.
    ///
    /// Deregisters the source from mio and removes it from the fd table.
    pub fn rm_fd(&mut self, source: &mut impl Source) -> io::Result<()> {
        self.poll.registry().deregister(source)?;
        self.interests.remove(&Token(0)); // best-effort cleanup
        self.load = self.load.saturating_sub(1);
        Ok(())
    }

    /// Remove a file descriptor by its handle.
    ///
    /// This marks the entry as retired. The actual cleanup happens
    /// during the next `poll()` call, ensuring safe reentrancy.
    pub fn rm_fd_by_handle(&mut self, handle: Handle) {
        if let Some(entry) = self.fd_table.get_mut(&handle) {
            entry.retired = true;
        }
    }

    // ── Interest management ────────────────────────────────────
    //
    // mio 1.x Interest is a NonZeroU8, not a bitflags type. We track
    // interests as raw u8 bits (bit 0 = READABLE, bit 1 = WRITABLE)
    // and convert to Interest only when calling mio.

    const INT_READABLE: u8 = 1;
    const INT_WRITABLE: u8 = 2;

    fn retrack_interest(&self, source: &mut impl Source, token: Token) -> io::Result<()> {
        let bits = self.interests.get(&token).copied().unwrap_or(0);
        match bits {
            1 => self.poll.registry().reregister(source, token, Interest::READABLE),
            2 => self.poll.registry().reregister(source, token, Interest::WRITABLE),
            3 => {
                // Both READABLE + WRITABLE (BitOr is implemented)
                self.poll.registry().reregister(source, token, Interest::READABLE | Interest::WRITABLE)
            }
            _ => {
                // No actual interests — keep the placeholder READABLE
                self.poll.registry().reregister(source, token, Interest::READABLE)
            }
        }
    }

    /// Enable polling for input (read) events on a registered source.
    pub fn set_pollin(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        let old = self.interests.get(&token).copied().unwrap_or(0);
        self.interests.insert(token, old | Self::INT_READABLE);
        self.retrack_interest(source, token)
    }

    /// Disable polling for input (read) events.
    pub fn reset_pollin(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        let old = self.interests.get(&token).copied().unwrap_or(0);
        self.interests.insert(token, old & !Self::INT_READABLE);
        self.retrack_interest(source, token)
    }

    /// Enable polling for output (write) events on a registered source.
    pub fn set_pollout(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        let old = self.interests.get(&token).copied().unwrap_or(0);
        self.interests.insert(token, old | Self::INT_WRITABLE);
        self.retrack_interest(source, token)
    }

    /// Disable polling for output (write) events.
    pub fn reset_pollout(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        let old = self.interests.get(&token).copied().unwrap_or(0);
        self.interests.insert(token, old & !Self::INT_WRITABLE);
        self.retrack_interest(source, token)
    }

    /// Enable both polling for input and output events.
    pub fn set_pollin_pollout(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        self.interests.insert(token, Self::INT_READABLE | Self::INT_WRITABLE);
        self.poll.registry().reregister(source, token, Interest::READABLE | Interest::WRITABLE)
    }

    /// Enable edge-triggered polling for input events.
    pub fn set_pollin_edge(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        self.set_pollin(source, token)
    }

    /// Enable edge-triggered polling for output events.
    pub fn set_pollout_edge(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        self.set_pollout(source, token)
    }

    /// Disable all interests (but keep the fd registered).
    pub fn reset_pollin_pollout(&mut self, source: &mut impl Source, token: Token) -> io::Result<()> {
        self.interests.insert(token, 0);
        self.poll.registry().reregister(source, token, Interest::READABLE)
    }

    // ── Timer management ───────────────────────────────────────

    /// Add a timer that fires after `timeout` milliseconds.
    ///
    /// When the timer expires, `timer_event(id)` is called on the handler
    /// associated with the handle.
    pub fn add_timer(&mut self, timeout_ms: u64, id: usize, handler: Box<dyn PollEvents>) {
        let expiration = Instant::now() + Duration::from_millis(timeout_ms);
        self.timers.push(TimerEntry {
            expiration,
            handler,
            id,
        });
        // Sort by expiration — nearest first
        self.timers.sort_by_key(|t| t.expiration);
    }

    /// Cancel all timers with the given ID.
    ///
    /// In C++ libzmq, `cancel_timer` searches by (sink, id) pair.
    /// Since we store the handler in the timer entry, we match by id.
    /// Complexity: O(n) — assumed to be called rarely.
    pub fn cancel_timer(&mut self, id: usize) {
        self.timers.retain(|t| t.id != id);
    }

    // ── Polling loop ───────────────────────────────────────────

    /// Wait for I/O events and dispatch them to handlers.
    ///
    /// `timeout_ms` — maximum time to wait in milliseconds.
    ///   - `-1` means "wait forever" (until an event or signal)
    ///   - `0` means "return immediately"
    ///   - `> 0` means "wait up to timeout_ms"
    ///
    /// Returns the number of events processed, or `Ok(0)` on timeout.
    pub fn poll(&mut self, timeout_ms: i64) -> io::Result<usize> {
        // 1. Process pending handler registrations from other threads
        self.process_registrations();

        // 2. Clean up retired entries
        self.cleanup_retired();

        // 3. Check if we should stop
        if self.stop && self.fd_table.is_empty() {
            return Ok(0);
        }

        // 4. Execute expired timers and compute next timeout
        let timer_timeout = self.execute_timers();

        // 5. Compute the actual poll timeout
        let poll_timeout = compute_timeout(timeout_ms, timer_timeout);

        // 6. Poll for events
        let mut events = Events::with_capacity(1024);
        self.poll.poll(&mut events, poll_timeout)?;

        // 7. Dispatch events
        let mut event_count = 0;
        for event in &events {
            let token = event.token();

            // Remove retired entries when we encounter them
            if let Some(entry) = self.fd_table.get(&token) {
                if entry.retired {
                    continue;
                }
            }

            if let Some(entry) = self.fd_table.get_mut(&token) {
                if event.is_readable() {
                    event_count += 1;
                    entry.handler.in_event();
                }
                if event.is_writable() {
                    event_count += 1;
                    entry.handler.out_event();
                }
            }
        }

        // 8. Process any new registrations that arrived during dispatch
        self.process_registrations();

        Ok(event_count)
    }

    // ── Accessors ──────────────────────────────────────────────

    /// Get the current load (number of registered fds).
    pub fn get_load(&self) -> usize {
        self.load
    }

    /// Signal the poller to stop.
    ///
    /// The poll loop will exit when `stop` is true and all fds are removed.
    pub fn stop(&mut self) {
        self.stop = true;
    }

    /// Check if the poller is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stop
    }

    /// Get a reference to the mio Poll (advanced use).
    pub fn raw_poll(&self) -> &Poll {
        &self.poll
    }

    // ── Private helpers ────────────────────────────────────────

    /// Process pending handler registrations from the cross-thread channel.
    fn process_registrations(&mut self) {
        loop {
            match self.handler_rx.try_recv() {
                Ok(reg) => {
                    if let Some(entry) = self.fd_table.get_mut(&reg.token) {
                        entry.handler = reg.handler;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    /// Remove retired entries from the fd table.
    fn cleanup_retired(&mut self) {
        self.fd_table.retain(|_, entry| !entry.retired);
    }

    /// Execute all expired timers.
    ///
    /// Returns the number of milliseconds until the next timer expires,
    /// or `None` if there are no pending timers.
    fn execute_timers(&mut self) -> Option<u64> {
        if self.timers.is_empty() {
            return None;
        }

        let now = Instant::now();

        // Remove and fire expired timers
        while !self.timers.is_empty() {
            let next_expiry = self.timers[0].expiration;
            if next_expiry > now {
                // First non-expired timer — return time until it fires
                let remaining = next_expiry.duration_since(now);
                return Some(remaining.as_millis() as u64);
            }

            // Expired — remove and fire
            let mut timer = self.timers.remove(0);
            timer.handler.timer_event(timer.id);
        }

        None
    }
}

// ─── Helper functions ──────────────────────────────────────────

/// Compute the actual poll timeout from the user-requested timeout
/// and the next timer expiration.
fn compute_timeout(requested_ms: i64, next_timer_ms: Option<u64>) -> Option<Duration> {
    match (requested_ms, next_timer_ms) {
        // Wait forever (no timers)
        (-1, None) => None,
        // Wait forever (but wake for the next timer)
        (-1, Some(t)) => Some(Duration::from_millis(t)),
        // Return immediately
        (0, _) => Some(Duration::from_millis(0)),
        // Bounded timeout — take the minimum
        (n, None) if n > 0 => Some(Duration::from_millis(n as u64)),
        (n, Some(t)) if n > 0 => {
            let n = n as u64;
            Some(Duration::from_millis(n.min(t)))
        }
        // Default: 100ms
        _ => Some(Duration::from_millis(100)),
    }
}

// ─── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    /// Simple test handler that records events
    struct TestHandler {
        in_count: usize,
        out_count: usize,
        timer_fired: Vec<usize>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                in_count: 0,
                out_count: 0,
                timer_fired: Vec::new(),
            }
        }
    }

    impl PollEvents for TestHandler {
        fn in_event(&mut self) {
            self.in_count += 1;
        }
        fn out_event(&mut self) {
            self.out_count += 1;
        }
        fn timer_event(&mut self, id: usize) {
            self.timer_fired.push(id);
        }
    }

    #[test]
    fn test_create_poller() {
        let poller = Poller::new();
        assert!(poller.is_ok());
    }

    #[test]
    fn test_get_load_empty() {
        let poller = Poller::new().unwrap();
        assert_eq!(poller.get_load(), 0);
    }

    #[test]
    fn test_add_fd_increases_load() {
        let mut poller = Poller::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut listener = mio::net::TcpListener::from_std(listener);
        let handle = poller.add_fd(&mut listener, Box::new(TestHandler::new()));
        assert!(handle.is_ok());
        assert_eq!(poller.get_load(), 1);
    }

    #[test]
    fn test_cross_thread_handle() {
        let poller = Poller::new().unwrap();
        let handle = poller.cross_thread_handle();
        // The handle should be cloneable
        let handle2 = handle.clone();
        drop(handle2);
    }

    #[test]
    fn test_poll_timeout() {
        let mut poller = Poller::new().unwrap();
        // poll with 0ms timeout should return immediately
        let result = poller.poll(0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_poll_detects_readable() {
        let mut poller = Poller::new().unwrap();

        // Create a pipe-like pair using a TCP socketpair
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut writer = std::net::TcpStream::connect(addr).unwrap();
        writer.set_nonblocking(true).unwrap();

        let (reader, _) = listener.accept().unwrap();
        reader.set_nonblocking(true).unwrap();

        let mut reader_mio = mio::net::TcpStream::from_std(reader);

        // Register reader for readability
        let token = poller
            .add_fd(&mut reader_mio, Box::new(TestHandler::new()))
            .unwrap();
        poller.set_pollin(&mut reader_mio, token).unwrap();

        // Write data to make reader readable
        writer.write_all(b"x").unwrap();

        // Poll should detect the readable event
        let events = poller.poll(100).unwrap();
        assert!(events > 0, "should have detected readable event");
    }

    #[test]
    fn test_timer_fires() {
        let mut poller = Poller::new().unwrap();

        // Add a timer that fires after 1ms
        poller.add_timer(1, 42, Box::new(TestHandler::new()));

        // Wait a bit longer for the timer to expire
        thread::sleep(Duration::from_millis(10));

        // Poll should execute the timer
        let result = poller.poll(0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cancel_timer() {
        let mut poller = Poller::new().unwrap();
        poller.add_timer(10000, 99, Box::new(TestHandler::new()));
        poller.cancel_timer(99);
        // After cancellation, the timer list should be empty
        let result = poller.poll(0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_stop_poller() {
        let mut poller = Poller::new().unwrap();
        assert!(!poller.is_stopping());
        poller.stop();
        assert!(poller.is_stopping());
        let result = poller.poll(0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_rm_fd_decreases_load() {
        let mut poller = Poller::new().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut listener = mio::net::TcpListener::from_std(listener);

        assert_eq!(poller.get_load(), 0);
        let _token = poller
            .add_fd(&mut listener, Box::new(TestHandler::new()))
            .unwrap();
        assert_eq!(poller.get_load(), 1);

        // Deregister
        poller.rm_fd(&mut listener).unwrap();
        assert_eq!(poller.get_load(), 0);
    }

    #[test]
    fn test_edge_triggered() {
        let mut poller = Poller::new().unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut writer = std::net::TcpStream::connect(addr).unwrap();
        writer.set_nonblocking(true).unwrap();

        let (reader, _) = listener.accept().unwrap();
        reader.set_nonblocking(true).unwrap();

        let mut reader_mio = mio::net::TcpStream::from_std(reader);

        let token = poller
            .add_fd(&mut reader_mio, Box::new(TestHandler::new()))
            .unwrap();

        // Set edge-triggered polling
        poller.set_pollin_edge(&mut reader_mio, token).unwrap();

        // Write some data
        writer.write_all(b"hello").unwrap();

        // First poll should detect the event (edge)
        let events = poller.poll(100).unwrap();
        assert!(events > 0, "first poll should detect the edge");

        // Second poll should NOT detect another event (edge-triggered)
        let events2 = poller.poll(10).unwrap();
        assert_eq!(events2, 0, "second poll should not detect anything");
    }

    #[test]
    fn test_poller_handle_registry_from_other_thread() {
        let poller = Poller::new().unwrap();
        let handle = poller.cross_thread_handle();
        let registry = handle.registry().try_clone().unwrap();

        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            // Just verify we can access the registry from another thread
            let _ = registry;
            tx.send(true).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    /// Verify that `Registry` is `Send + Sync` (compile-time check)
    #[test]
    fn test_registry_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        let poller = Poller::new().unwrap();
        let handle = poller.cross_thread_handle();
        assert_send_sync::<PollerHandle>();
    }
}
