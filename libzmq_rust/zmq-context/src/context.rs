//! ZContext — global ZeroMQ context. 1:1 translation of C++ `ctx_t`.
//!
//! The context is the entry point for all ZeroMQ operations. It manages:
//! - IO thread pool
//! - Socket registry
//! - Inproc endpoint registry
//! - Reaper thread (async socket cleanup)

use std::sync::Arc;
use parking_lot::RwLock;
use zmq_core::error::{ZmqError, ZmqResult};
use zmq_core::socket_type::SocketType;
use zmq_transport::inproc::InprocRegistry;
use crate::socket::ZSocket;
use crate::options::SocketOptions;

/// Global ZeroMQ context — container for all sockets and shared state.
pub struct ZContext {
    inner: Arc<ZContextInner>,
}

pub(crate) struct ZContextInner {
    io_threads: usize,
    pub(crate) inproc_registry: RwLock<InprocRegistry>,
    terminated: std::sync::atomic::AtomicBool,
}

impl ZContext {
    /// Create a new ZeroMQ context with default settings.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ZContextInner {
                io_threads: 1,
                inproc_registry: RwLock::new(InprocRegistry::new()),
                terminated: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Create a new socket of the given type.
    pub fn socket(&self, typ: SocketType) -> ZmqResult<ZSocket> {
        if self.inner.terminated.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ZmqError::ContextTerminated);
        }
        Ok(ZSocket::new(self.inner.clone(), typ))
    }

    /// Shut down the context. All sockets will be terminated.
    pub fn shutdown(&self) -> ZmqResult<()> {
        self.inner.terminated.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Check if the context has been terminated.
    pub fn is_terminated(&self) -> bool {
        self.inner.terminated.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get a reference to the inproc registry for this context.
    pub(crate) fn inproc_registry(&self) -> &RwLock<InprocRegistry> {
        &self.inner.inproc_registry
    }

    /// Get the IO thread count.
    pub fn io_threads(&self) -> usize {
        self.inner.io_threads
    }
}

impl Default for ZContext {
    fn default() -> Self { Self::new() }
}
