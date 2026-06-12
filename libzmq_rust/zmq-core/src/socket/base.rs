//! Socket base trait — the common interface for all ZeroMQ socket types.
//!
//! 1:1 translation of C++ `socket_base_t` virtual interface.
//! Each socket type implements `Socket` and is responsible for its
//! specific messaging pattern behavior.

use std::sync::Arc;

use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;

/// Core socket trait — implemented by all 19 socket types.
///
/// Methods prefixed with `x` correspond to C++ virtual methods in `socket_base_t`.
/// Non-`x` methods are convenience wrappers.
pub trait Socket: Send + Sync {
    // ─── Core operations ──────────────────────────────────

    /// Send a message through the socket.
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()>;

    /// Receive a message from the socket.
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage>;

    /// Whether there is at least one message available for receiving.
    fn xhas_in(&self) -> bool;

    /// Whether at least one message can be sent.
    fn xhas_out(&self) -> bool;

    // ─── Pipe management ──────────────────────────────────

    /// Attach a pipe to this socket.
    /// `subscribe_to_all` — for SUB sockets, subscribe to all messages.
    /// `locally_initiated` — whether this side initiated the connection.
    fn attach_pipe(&mut self, pipe: Arc<Pipe>, subscribe_to_all: bool, locally_initiated: bool);

    /// A pipe was terminated — clean up references.
    fn pipe_terminated(&mut self, pipe: &Pipe);

    /// Data is available to read from a pipe.
    fn read_activated(&mut self, pipe: &Pipe);

    /// Buffer space is available on a pipe for writing.
    fn write_activated(&mut self, pipe: &Pipe);

    // ─── Identity ─────────────────────────────────────────

    /// The socket type.
    fn socket_type(&self) -> SocketType;

    // ─── Convenience ──────────────────────────────────────

    /// Whether the socket can send (by default, checks socket_type).
    fn can_send(&self) -> bool {
        self.socket_type().can_send()
    }

    /// Whether the socket can receive.
    fn can_recv(&self) -> bool {
        self.socket_type().can_recv()
    }

    /// Set subscriptions for filtering socket (e.g. SUB).
    /// Default implementation is a no-op.
    fn set_subscriptions(&mut self, _subs: &[Vec<u8>]) {}
}

/// A simpler, object-safe socket handle for use in generic contexts.
pub struct SocketHandle {
    inner: Box<dyn Socket>,
    socket_type: SocketType,
}

impl SocketHandle {
    pub fn new(socket: Box<dyn Socket>) -> Self {
        let st = socket.socket_type();
        Self {
            inner: socket,
            socket_type: st,
        }
    }
}

impl Socket for SocketHandle {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> { self.inner.xsend(msg) }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> { self.inner.xrecv() }
    fn xhas_in(&self) -> bool { self.inner.xhas_in() }
    fn xhas_out(&self) -> bool { self.inner.xhas_out() }
    fn attach_pipe(&mut self, p: Arc<Pipe>, sa: bool, li: bool) { self.inner.attach_pipe(p, sa, li) }
    fn pipe_terminated(&mut self, p: &Pipe) { self.inner.pipe_terminated(p) }
    fn read_activated(&mut self, p: &Pipe) { self.inner.read_activated(p) }
    fn write_activated(&mut self, p: &Pipe) { self.inner.write_activated(p) }
    fn socket_type(&self) -> SocketType { self.socket_type }
    fn set_subscriptions(&mut self, subs: &[Vec<u8>]) { self.inner.set_subscriptions(subs) }
}
