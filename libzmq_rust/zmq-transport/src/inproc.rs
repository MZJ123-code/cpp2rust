//! Inproc transport — in-process inter-thread communication.
//!
//! Each bound inproc endpoint maintains a queue of pending pipe connections.
//! Connecting creates a pipe pair; one end attaches to the connecting socket,
//! the other end is queued for the bound socket to accept.

use std::collections::HashMap;
use std::sync::Arc;

use std::sync::Mutex;
use zmq_core::error::{ZmqError, ZmqResult};
use zmq_core::pipe::Pipe;

/// Registry of all inproc endpoints in a context.
///
/// Maps endpoint name → queue of pending pipe connections.
/// Supports connect-before-bind: if connects arrive before bind,
/// they are stored and delivered when bind occurs.
#[derive(Default)]
pub struct InprocRegistry {
    endpoints: HashMap<String, Arc<Mutex<InprocEndpoint>>>,
}

struct InprocEndpoint {
    /// Whether a socket has bound to this endpoint
    bound: bool,
    /// Pending pipe connections from connecting sockets
    pending_pipes: Vec<Arc<Pipe>>,
}

impl InprocRegistry {
    pub fn new() -> Self {
        Self {
            endpoints: HashMap::new(),
        }
    }

    /// Get or create the endpoint entry for a name.
    pub fn get_or_create(&mut self, name: &str) -> Arc<Mutex<InprocEndpoint>> {
        self.endpoints
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(InprocEndpoint {
                    bound: false,
                    pending_pipes: Vec::new(),
                }))
            })
            .clone()
    }

    /// Bind to an inproc endpoint. Returns pipes that were pending from
    /// connect-before-bind, and marks the endpoint as bound.
    pub fn bind(&mut self, name: &str) -> ZmqResult<Vec<Arc<Pipe>>> {
        let ep = self.get_or_create(name);
        let mut guard = ep.lock().unwrap();
        if guard.bound {
            return Err(ZmqError::AddressInUse);
        }
        guard.bound = true;
        let pending = std::mem::take(&mut guard.pending_pipes);
        Ok(pending)
    }

    /// Connect to a bound inproc endpoint. Creates a pipe pair,
    /// queues one end for the bound socket, and returns the other end
    /// for the connecting socket.
    ///
    /// If bind hasn't happened yet, the pipe is queued and will be
    /// delivered when bind occurs.
    pub fn connect(&mut self, name: &str) -> ZmqResult<Arc<Pipe>> {
        let ep = self.get_or_create(name);
        let (p1, p2) = Pipe::new_pair(0);
        let mut guard = ep.lock().unwrap();
        // p2 goes to the bound socket (or waits in queue)
        guard.pending_pipes.push(p2);
        // p1 is for the connecting socket
        Ok(p1)
    }

    /// Try to accept a pending connection on a bound endpoint.
    /// Returns None if no connections are pending.
    pub fn try_accept(&self, name: &str) -> Option<Arc<Pipe>> {
        let ep = self.endpoints.get(name)?;
        let mut guard = ep.lock().unwrap();
        if guard.pending_pipes.is_empty() {
            None
        } else {
            Some(guard.pending_pipes.remove(0))
        }
    }

    /// Unbind an inproc endpoint.
    pub fn unbind(&mut self, name: &str) {
        self.endpoints.remove(name);
    }

    /// Push a pipe to the pending queue for this endpoint (used by connect).
    /// If the endpoint is bound and a bound socket reference is available,
    /// the pipe should be delivered directly instead.
    pub fn queue_pipe(&mut self, name: &str, pipe: Arc<Pipe>) {
        let ep = self.get_or_create(name);
        let mut guard = ep.lock().unwrap();
        guard.pending_pipes.push(pipe);
    }

    /// Drain all pending pipes from this endpoint (used by bind).
    pub fn take_pending(&self, name: &str) -> Vec<Arc<Pipe>> {
        let ep = match self.endpoints.get(name) {
            Some(e) => e,
            None => return Vec::new(),
        };
        let mut guard = ep.lock().unwrap();
        std::mem::take(&mut guard.pending_pipes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inproc_bind_connect() {
        let mut registry = InprocRegistry::new();
        let pending = registry.bind("test-ep").unwrap();
        assert!(pending.is_empty());

        let client_pipe = registry.connect("test-ep").unwrap();
        let server_pipe = registry.try_accept("test-ep").unwrap();

        // Basic check: both pipes are valid
        assert!(client_pipe.is_active());
        assert!(server_pipe.is_active());
    }

    #[test]
    fn test_connect_before_bind() {
        let mut registry = InprocRegistry::new();

        let client_pipe = registry.connect("early").unwrap();
        let pending = registry.bind("early").unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].is_active());
    }

    #[test]
    fn test_bind_conflict() {
        let mut registry = InprocRegistry::new();
        registry.bind("conflict").unwrap();
        assert!(registry.bind("conflict").is_err());
    }

    #[test]
    fn test_multiple_connects() {
        let mut registry = InprocRegistry::new();
        let _ = registry.bind("multi").unwrap();

        let _c1 = registry.connect("multi").unwrap();
        let _c2 = registry.connect("multi").unwrap();
        let _c3 = registry.connect("multi").unwrap();

        assert!(registry.try_accept("multi").is_some());
        assert!(registry.try_accept("multi").is_some());
        assert!(registry.try_accept("multi").is_some());
        assert!(registry.try_accept("multi").is_none());
    }
}
