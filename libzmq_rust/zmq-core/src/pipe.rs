//! Internal pipe — bidirectional lock-free channel between Socket and Session.
//!
//! 1:1 translation of C++ `pipe_t`. Uses `YPipe` for each direction.
//!
//! A Pipe connects a Socket (application thread) to a Session (I/O thread).
//! Messages flow through lock-free SPSC queues in both directions.
//!
//! ## Pipe Pair Architecture
//!
//! When `new_pair()` is called, two Pipe objects share the same underlying
//! YPipe queues (via Arc). The SPSC discipline is maintained because:
//! - Only one peer writes to `to_session` (the socket/initiator)
//! - Only one peer reads from `to_session` (the session/acceptor)
//! - Only one peer writes to `to_socket` (the session/acceptor)
//! - Only one peer reads from `to_socket` (the socket/initiator)

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::data_structures::ypipe::YPipe;
use crate::message::ZmqMessage;

/// Opaque pipe identity.
pub type PipeId = usize;

/// Global pipe ID counter — ensures unique IDs across all pipe pairs.
static NEXT_PIPE_ID: AtomicUsize = AtomicUsize::new(1);

fn next_pipe_id() -> PipeId {
    NEXT_PIPE_ID.fetch_add(2, Ordering::Relaxed)
}

/// Bidirectional pipe between Socket and Session.
///
/// Contains two unidirectional YPipes (shared via Arc for pipe pairs):
/// - `to_session`: Socket → Session (outbound messages from app to network)
/// - `to_socket`: Session → Socket (inbound messages from network to app)
pub struct Pipe {
    pub(crate) id: PipeId,
    /// Whether this pipe has been terminated
    pub(crate) terminated: AtomicBool,
    /// Whether to delay termination until a delimiter is received (REQ/REP)
    pub(crate) delay_termination: AtomicBool,
    /// Messages from Socket to Session (app → network)
    pub(crate) to_session: Arc<parking_lot::Mutex<YPipe<ZmqMessage>>>,
    /// Messages from Session to Socket (network → app)
    pub(crate) to_socket: Arc<parking_lot::Mutex<YPipe<ZmqMessage>>>,
}

impl Pipe {
    /// Create a single stand-alone pipe (for cases where only one direction is needed).
    pub fn new(id: PipeId) -> Self {
        Self {
            id,
            terminated: AtomicBool::new(false),
            delay_termination: AtomicBool::new(false),
            to_session: Arc::new(parking_lot::Mutex::new(YPipe::new())),
            to_socket: Arc::new(parking_lot::Mutex::new(YPipe::new())),
        }
    }

    /// Create a connected pipe pair with crossed queues and unique IDs.
    ///
    /// Pipe A writes to `to_session` → Pipe B reads from `to_socket`.
    /// Pipe B writes to `to_session` → Pipe A reads from `to_socket`.
    ///
    /// This matches the C++ libzmq pattern where the session layer connects
    /// each pipe's outgoing queue to the other pipe's incoming queue.
    /// - A's outgoing (to_session) = B's incoming (to_socket)
    /// - B's outgoing (to_session) = A's incoming (to_socket)
    pub fn new_pair(_id: PipeId) -> (Arc<Pipe>, Arc<Pipe>) {
        let id = next_pipe_id();
        // Create two queues: a_to_b (A sends data to B) and b_to_a (B sends data to A)
        let a_to_b: Arc<parking_lot::Mutex<YPipe<ZmqMessage>>> =
            Arc::new(parking_lot::Mutex::new(YPipe::new()));
        let b_to_a: Arc<parking_lot::Mutex<YPipe<ZmqMessage>>> =
            Arc::new(parking_lot::Mutex::new(YPipe::new()));

        let a = Arc::new(Pipe {
            id,
            terminated: AtomicBool::new(false),
            delay_termination: AtomicBool::new(false),
            to_session: Arc::clone(&a_to_b),  // A's outgoing → a_to_b
            to_socket: Arc::clone(&b_to_a),    // A's incoming ← b_to_a
        });

        let b = Arc::new(Pipe {
            id: id + 1,
            terminated: AtomicBool::new(false),
            delay_termination: AtomicBool::new(false),
            to_session: b_to_a,   // B's outgoing → b_to_a
            to_socket: a_to_b,     // B's incoming ← a_to_b
        });

        (a, b)
    }

    /// Whether the pipe is still active.
    pub fn is_active(&self) -> bool {
        !self.terminated.load(Ordering::Acquire)
    }

    /// Mark the pipe as terminated.
    pub fn terminate(&self) {
        self.terminated.store(true, Ordering::Release);
    }

    /// Get the pipe's unique ID.
    pub fn id(&self) -> PipeId {
        self.id
    }

    /// Write a message from Socket to Session.
    pub fn write_to_session(&self, msg: ZmqMessage, incomplete: bool) {
        let mut pipe = self.to_session.lock();
        pipe.write(msg, incomplete);
    }

    /// Flush pending writes from Socket to Session. Returns true if reader is active.
    pub fn flush_to_session(&self) -> bool {
        let mut pipe = self.to_session.lock();
        pipe.flush()
    }

    /// Check if there's a message from Session to Socket.
    pub fn check_read_from_session(&self) -> bool {
        let mut pipe = self.to_socket.lock();
        pipe.check_read()
    }

    /// Read a message from Session to Socket.
    pub fn read_from_session(&self) -> Option<ZmqMessage> {
        let mut pipe = self.to_socket.lock();
        pipe.read()
    }

    /// Write a message from Session to Socket.
    pub fn write_to_socket(&self, msg: ZmqMessage, incomplete: bool) {
        let mut pipe = self.to_socket.lock();
        pipe.write(msg, incomplete);
    }

    /// Flush pending writes from Session to Socket.
    pub fn flush_to_socket(&self) -> bool {
        let mut pipe = self.to_socket.lock();
        pipe.flush()
    }

    /// Check if there's a message from Socket to Session.
    pub fn check_read_from_socket(&self) -> bool {
        let mut pipe = self.to_session.lock();
        pipe.check_read()
    }

    /// Read a message from Socket to Session.
    pub fn read_from_socket(&self) -> Option<ZmqMessage> {
        let mut pipe = self.to_session.lock();
        pipe.read()
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipe_lifecycle() {
        let (p1, p2) = Pipe::new_pair(1);
        assert!(p1.is_active());
        assert!(p2.is_active());
        // p1 and p2 have different IDs (p1=1, p2=2 in the pair)
        p1.terminate();
        assert!(!p1.is_active());
    }

    #[test]
    fn test_pipe_send_recv() {
        let (p1, p2) = Pipe::new_pair(1);
        let msg = ZmqMessage::from_slice(b"hello");

        // p1 writes to session (to_session = a_to_b queue), p2 reads from session
        // (to_socket = a_to_b queue) — crossed: p1's outgoing = p2's incoming
        p1.write_to_session(msg, false);
        p1.flush_to_session();
        assert!(p2.check_read_from_session());
        let received = p2.read_from_session().unwrap();
        assert_eq!(received.data(), b"hello");
    }

    #[test]
    fn test_pipe_bidirectional() {
        let (p1, p2) = Pipe::new_pair(1);

        // p1 sends to p2: p1 writes to_session → a_to_b, p2 reads to_socket → a_to_b
        p1.write_to_session(ZmqMessage::from_slice(b"req"), false);
        p1.flush_to_session();
        assert!(p2.check_read_from_session());
        assert_eq!(p2.read_from_session().unwrap().data(), b"req");

        // p2 sends to p1: p2 writes to_session → b_to_a, p1 reads to_socket → b_to_a
        p2.write_to_session(ZmqMessage::from_slice(b"rep"), false);
        p2.flush_to_session();
        assert!(p1.check_read_from_session());
        assert_eq!(p1.read_from_session().unwrap().data(), b"rep");
    }
}
