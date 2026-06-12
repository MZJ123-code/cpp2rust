//! 1:1 translation of C++ `tests/test_xpub_welcome_msg.cpp`.
mod common;

use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    pub_sock.set_xpub_welcome_msg(b"W").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.subscribe(b"W").unwrap();
    sub_sock.connect("inproc://soname").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &[1, b'W']);

    let msg = sub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"W");
}
