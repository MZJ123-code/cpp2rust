//! Test utilities — 1:1 translation of C++ `testutil.hpp` + `testutil.cpp`.
#![allow(dead_code)]
use std::time::Duration;
use zmq_context::ZContext;
use zmq_context::socket::{SendFlags, RecvFlags};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

pub const SETTLE_TIME: Duration = Duration::from_millis(300);

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
    pub fn bounce(&self, server: &zmq_context::ZSocket, client: &zmq_context::ZSocket) {
        send_string_expect_success(client, "Hello", SendFlags::NONE);
        recv_string_expect_success(server, "Hello", RecvFlags::NONE);
        send_string_expect_success(server, "World", SendFlags::NONE);
        recv_string_expect_success(client, "World", RecvFlags::NONE);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) { let _ = self.ctx.shutdown(); }
}

pub fn send_string_expect_success(socket: &zmq_context::ZSocket, s: &str, flags: SendFlags) {
    socket.send(ZmqMessage::from_slice(s.as_bytes()), flags).expect("send failed");
}

pub fn recv_string_expect_success(socket: &zmq_context::ZSocket, expected: &str, flags: RecvFlags) {
    let msg = socket.recv(flags).expect("recv failed");
    assert_eq!(msg.data(), expected.as_bytes(), "recv data mismatch");
}
