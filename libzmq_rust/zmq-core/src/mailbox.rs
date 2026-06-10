//! Mailbox — thread-safe command queue for inter-thread communication.
//!
//! 1:1 translation of C++ `mailbox_t` + `signaler_t`.
//!
//! In libzmq's architecture, threads communicate via commands sent through
//! mailboxes. Each object with a mailbox can receive commands from any thread.
//! A signaler (eventfd/socketpair) wakes the receiving thread when a command arrives.

use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{self, Receiver, Sender, TryRecvError};

/// Commands sent between threads in a ZeroMQ context.
#[derive(Debug, Clone)]
pub enum Command {
    /// Stop processing and terminate
    Stop,
    /// A new pipe has been attached (pipe_id)
    PipeAttached(usize),
    /// A pipe has been terminated (pipe_id)
    PipeTerminated(usize),
    /// Activate read on a pipe (data available)
    ActivateRead(usize),
    /// Activate write on a pipe (buffer space available)
    ActivateWrite(usize),
    /// Connection failed
    ConnFailed,
    /// Connection established
    ConnEstablished,
    /// Bind to an endpoint
    Bind(String),
    /// Terminate an endpoint
    TermEndpoint(String),
}

/// Thread-safe mailbox for receiving commands.
///
/// Commands can be sent from any thread via `MailboxSender`.
/// The owning thread receives them via `Mailbox::recv()`.
pub struct Mailbox {
    rx: Receiver<Command>,
}

/// Sender half of a mailbox — can be cloned and shared across threads.
#[derive(Clone)]
pub struct MailboxSender {
    tx: Sender<Command>,
}

impl Mailbox {
    /// Create a new mailbox pair.
    pub fn new() -> (Self, MailboxSender) {
        let (tx, rx) = channel::unbounded();
        (Self { rx }, MailboxSender { tx })
    }

    /// Try to receive a command without blocking.
    pub fn try_recv(&self) -> Option<Command> {
        match self.rx.try_recv() {
            Ok(cmd) => Some(cmd),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => None,
        }
    }

    /// Receive a command, blocks until one is available.
    pub fn recv(&self) -> Option<Command> {
        self.rx.recv().ok()
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<Command> {
        self.rx.recv_timeout(timeout).ok()
    }
}

impl MailboxSender {
    /// Send a command to this mailbox.
    pub fn send(&self, cmd: Command) {
        let _ = self.tx.send(cmd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (mb, tx) = Mailbox::new();
        tx.send(Command::Stop);
        assert!(matches!(mb.recv(), Some(Command::Stop)));
    }

    #[test]
    fn test_try_recv_empty() {
        let (mb, _tx) = Mailbox::new();
        assert!(mb.try_recv().is_none());
    }

    #[test]
    fn test_multiple_senders() {
        let (mb, tx1) = Mailbox::new();
        let tx2 = tx1.clone();
        tx1.send(Command::ActivateRead(1));
        tx2.send(Command::ActivateWrite(2));
        let mut received = vec![mb.recv().unwrap(), mb.recv().unwrap()];
        received.sort_by_key(|c| match c {
            Command::ActivateRead(id) => *id,
            Command::ActivateWrite(id) => *id,
            _ => 0,
        });
    }
}
