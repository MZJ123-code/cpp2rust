//! Test utilities — 1:1 translation of C++ `testutil.hpp` + `testutil.cpp`.
#![allow(dead_code)]
use std::time::Duration;
use zmq_context::ZContext;
pub use zmq_context::socket::{SendFlags, RecvFlags};
pub use zmq_core::error::ZmqError;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

pub const SETTLE_TIME: Duration = Duration::from_millis(300);

/// Marker value to signal end of sequence in s_send_seq / s_recv_seq.
pub const SEQ_END: Option<&str> = None;

pub struct TestContext {
    pub ctx: ZContext,
}

impl TestContext {
    pub fn new() -> Self { Self { ctx: ZContext::new() } }
    pub fn socket(&self, typ: SocketType) -> zmq_context::ZSocket {
        self.ctx.socket(typ).expect("create test socket")
    }
    pub fn bind_inproc(&self, socket: &zmq_context::ZSocket, name: &str) -> String {
        let ep = format!("inproc://{}", name);
        socket.bind(&ep).expect("bind_inproc");
        ep
    }
    pub fn connect_inproc(&self, socket: &zmq_context::ZSocket, name: &str) -> String {
        let ep = format!("inproc://{}", name);
        socket.connect(&ep).expect("connect_inproc");
        ep
    }
    pub fn bounce(&self, server: &zmq_context::ZSocket, client: &zmq_context::ZSocket) {
        send_string_(client, "Hello", SendFlags::NONE);
        recv_string_assert(server, "Hello", RecvFlags::NONE);
        send_string_(server, "World", SendFlags::NONE);
        recv_string_assert(client, "World", RecvFlags::NONE);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) { let _ = self.ctx.shutdown(); }
}

// ── Basic send/recv helpers ──────────────────────────────────────

pub fn send_string_(socket: &zmq_context::ZSocket, s: &str, flags: SendFlags) {
    socket.send(ZmqMessage::from_slice(s.as_bytes()), flags).expect("send failed");
}

pub fn recv_string_assert(socket: &zmq_context::ZSocket, expected: &str, flags: RecvFlags) {
    let msg = socket.recv(flags).expect("recv failed");
    assert_eq!(msg.data(), expected.as_bytes(), "recv data mismatch");
}

/// Send a sequence of string parts, terminated by None.
pub fn s_send_seq(socket: &zmq_context::ZSocket, parts: &[Option<&str>]) {
    for (i, part) in parts.iter().enumerate() {
        match part {
            None => break,
            Some(s) => {
                let is_last = i + 1 >= parts.len() || parts[i + 1].is_none();
                let flags = if is_last { SendFlags::NONE } else { SendFlags::SNDMORE };
                if s.is_empty() {
                    socket.send(ZmqMessage::new(), flags).expect("send delimiter");
                } else {
                    socket.send(ZmqMessage::from_slice(s.as_bytes()), flags).expect("send part");
                }
            }
        }
    }
}

/// Receive and verify a sequence of string parts.
pub fn s_recv_seq(socket: &zmq_context::ZSocket, flags: RecvFlags, expected: &[Option<&str>]) {
    for exp in expected {
        match exp {
            None => break,
            Some(expected_str) => {
                let msg = socket.recv(flags).expect("recv failed");
                if expected_str.is_empty() {
                    assert!(msg.data().is_empty(), "expected empty frame, got {:?}", msg.data());
                } else {
                    assert_eq!(msg.data().as_slice(), expected_str.as_bytes(),
                        "data mismatch: exp={:?}", expected_str);
                }
            }
        }
    }
}

/// Send an empty frame.
pub fn send_empty(socket: &zmq_context::ZSocket, flags: SendFlags) {
    socket.send(ZmqMessage::new(), flags).expect("send empty");
}

/// Send a byte array.
pub fn send_array(socket: &zmq_context::ZSocket, data: &[u8], flags: SendFlags) {
    socket.send(ZmqMessage::from_slice(data), flags).expect("send_array failed");
}

/// Receive and assert a byte array match.
pub fn recv_array_assert(socket: &zmq_context::ZSocket, expected: &[u8], flags: RecvFlags) {
    let msg = socket.recv(flags).expect("recv_array failed");
    assert_eq!(msg.data(), expected, "recv data mismatch");
}

/// Receive a string and assert match.
pub fn recv_string_expect_success(socket: &zmq_context::ZSocket, expected: &str, flags: RecvFlags) {
    recv_string_assert(socket, expected, flags);
}

/// Poll-like sleep.
pub fn msleep(ms: u64) {
    std::thread::sleep(Duration::from_millis(ms));
}

/// Create an inproc endpoint.
pub fn ep_inproc(name: &str) -> String {
    format!("inproc://{}", name)
}

/// Get events from a socket (ZMQ_EVENTS equivalent).
pub fn get_events(socket: &zmq_context::ZSocket) -> i32 {
    let mut events: i32 = 0;
    if socket.has_out() { events |= 1; } // ZMQ_POLLOUT
    if socket.has_in() { events |= 2; } // ZMQ_POLLIN
    events
}
