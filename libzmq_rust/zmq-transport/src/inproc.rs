//! Inproc transport — in-process inter-thread communication.
//!
//! 1:1 translation of C++ inproc transport (built into `ctx.cpp` / `socket_base.cpp`).
//! Uses Tokio MPSC channels for zero-copy intra-process message passing.
//!
//! Each bound inproc endpoint creates a listener that queues incoming connections.
//! Connecting to an inproc endpoint creates a pair of channels connecting the two sockets.

use std::collections::HashMap;
use std::sync::Arc;

use std::sync::Mutex;
use tokio::sync::mpsc;
use zmq_core::error::{ZmqError, ZmqResult};

/// Data sent through an inproc connection.
#[derive(Debug)]
pub struct InprocData {
    /// Raw bytes of the message
    pub data: Vec<u8>,
}

/// One end of an inproc connection — a bidirectional channel pair.
pub struct InprocStream {
    /// Send data to the peer
    pub tx: mpsc::UnboundedSender<InprocData>,
    /// Receive data from the peer
    pub rx: mpsc::UnboundedReceiver<InprocData>,
}

impl InprocStream {
    /// Create a new connected pair of streams.
    pub fn pair() -> (Self, Self) {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        (
            Self { tx: tx1, rx: rx2 },
            Self { tx: tx2, rx: rx1 },
        )
    }
}

/// A waiting inproc listener (bound endpoint).
struct InprocListener {
    /// Sender to notify bound socket of new connections.
    /// Each incoming connection gets a new pair of streams.
    queue: Vec<InprocStream>,
}

/// Registry of all inproc endpoints in a context.
///
/// This is the Rust equivalent of the inproc endpoint map in `ctx_t`.
#[derive(Default)]
pub struct InprocRegistry {
    endpoints: HashMap<String, Arc<Mutex<InprocListener>>>,
}

impl InprocRegistry {
    pub fn new() -> Self {
        Self {
            endpoints: HashMap::new(),
        }
    }

    /// Bind to an inproc endpoint. Returns a receiver to accept connections.
    pub fn bind(&mut self, name: &str) -> ZmqResult<InprocBindResult> {
        if self.endpoints.contains_key(name) {
            return Err(ZmqError::AddressInUse);
        }
        let listener = Arc::new(Mutex::new(InprocListener {
            queue: Vec::new(),
        }));
        self.endpoints.insert(name.to_string(), listener.clone());
        Ok(InprocBindResult {
            listener,
            name: name.to_string(),
        })
    }

    /// Connect to an inproc endpoint. Returns a stream connected to the bound peer.
    pub fn connect(&mut self, name: &str) -> ZmqResult<InprocStream> {
        let listener = self
            .endpoints
            .get(name)
            .ok_or_else(|| ZmqError::Network(format!("no inproc endpoint: {}", name)))?;

        let (client_stream, server_stream) = InprocStream::pair();
        let mut guard = listener.lock().unwrap();
        guard.queue.push(server_stream);
        Ok(client_stream)
    }

    /// Unbind an inproc endpoint.
    pub fn unbind(&mut self, name: &str) {
        self.endpoints.remove(name);
    }
}

/// Result of binding an inproc endpoint.
pub struct InprocBindResult {
    listener: Arc<Mutex<InprocListener>>,
    /// The endpoint name
    #[allow(dead_code)]
    name: String,
}

impl InprocBindResult {
    /// Accept the next incoming connection. Returns `None` if no connections are pending.
    pub fn try_accept(&self) -> Option<InprocStream> {
        let mut guard = self.listener.lock().unwrap();
        if guard.queue.is_empty() {
            None
        } else {
            Some(guard.queue.remove(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inproc_bind_connect() {
        let mut registry = InprocRegistry::new();
        let bind = registry.bind("test-ep").unwrap();
        let client_stream = registry.connect("test-ep").unwrap();

        // Server accepts
        let mut server_stream = bind.try_accept().unwrap();

        // Client sends to server
        client_stream
            .tx
            .send(InprocData {
                data: b"hello".to_vec(),
            })
            .unwrap();

        // Server receives
        let received = server_stream.rx.blocking_recv().unwrap();
        assert_eq!(&received.data[..], b"hello");
    }

    #[test]
    fn test_bind_conflict() {
        let mut registry = InprocRegistry::new();
        registry.bind("conflict").unwrap();
        assert!(registry.bind("conflict").is_err());
    }

    #[test]
    fn test_connect_nonexistent() {
        let mut registry = InprocRegistry::new();
        assert!(registry.connect("missing").is_err());
    }
}
