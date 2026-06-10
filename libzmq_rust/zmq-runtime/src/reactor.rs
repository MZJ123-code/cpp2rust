//! Reactor — I/O event loop for handling multiple connections.
//!
//! 1:1 translation of C++ `io_thread_t` + `poller_t`.
//!
//! The reactor polls for I/O events on registered file descriptors and
//! dispatches them to the appropriate session handlers.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use mio::event::Event;
use mio::{Events, Interest, Poll, Registry, Token};

/// A handle to a registered I/O object.
pub type ReactorHandle = Token;

/// Callback trait for I/O events — implemented by Sessions.
pub trait EventHandler: Send + 'static {
    /// Called when the file descriptor is readable.
    fn on_readable(&mut self) -> io::Result<()>;

    /// Called when the file descriptor is writable.
    fn on_writable(&mut self) -> io::Result<()>;
}

/// I/O reactor — polls file descriptors and dispatches events.
///
/// This is the Rust equivalent of `io_thread_t`. Each I/O thread runs
/// one reactor that manages all sessions bound to that thread.
pub struct Reactor {
    poll: Poll,
    /// Registered event handlers keyed by token
    handlers: HashMap<Token, Box<dyn EventHandler>>,
    next_token: usize,
    /// Whether the reactor should stop
    stop: bool,
}

impl Reactor {
    /// Create a new reactor.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poll: Poll::new()?,
            handlers: HashMap::new(),
            next_token: 0,
            stop: false,
        })
    }

    /// Get a registry handle for registering I/O sources.
    pub fn registry(&self) -> &Registry {
        self.poll.registry()
    }

    /// Register an I/O source with its event handler.
    pub fn register(
        &mut self,
        source: &mut impl mio::event::Source,
        interests: Interest,
        handler: Box<dyn EventHandler>,
    ) -> io::Result<Token> {
        let token = Token(self.next_token);
        self.next_token += 1;
        self.poll.registry().register(source, token, interests)?;
        self.handlers.insert(token, handler);
        Ok(token)
    }

    /// Reregister interests for an existing token.
    pub fn reregister(
        &mut self,
        source: &mut impl mio::event::Source,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        self.poll.registry().reregister(source, token, interests)
    }

    /// Deregister a source.
    pub fn deregister(&mut self, source: &mut impl mio::event::Source) -> io::Result<()> {
        self.poll.registry().deregister(source)
    }

    /// Run the event loop. Returns when `stop()` is called or all handlers are gone.
    pub fn run(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(1024);

        loop {
            if self.stop && self.handlers.is_empty() {
                break;
            }

            // Poll with a 100ms timeout (same as C++ poller)
            self.poll
                .poll(&mut events, Some(Duration::from_millis(100)))?;

            for event in &events {
                let token = event.token();
                if let Some(handler) = self.handlers.get_mut(&token) {
                    if event.is_readable() {
                        handler.on_readable()?;
                    }
                    if event.is_writable() {
                        handler.on_writable()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Signal the reactor to stop after processing current events.
    pub fn stop(&mut self) {
        self.stop = true;
    }
}
