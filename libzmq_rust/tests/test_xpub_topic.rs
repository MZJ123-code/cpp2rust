//! 1:1 translation of C++ `tests/test_xpub_topic.cpp`.
//! Tests long topic subscription/cancel with XPUB.
mod common;

use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

const SHORT_TOPIC: &[u8] = b"ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDE";

const LONG_TOPIC: &[u8] = b"ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEF";

fn test_subscribe_cancel(
    pub_sock: &zmq_context::ZSocket,
    sub_sock: &zmq_context::ZSocket,
    topic: &[u8],
) {
    sub_sock.subscribe(topic).unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data()[0], 1);
    assert_eq!(msg.data().len(), topic.len() + 1);
    assert_eq!(&msg.data()[1..], topic);

    sub_sock.unsubscribe(topic).unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data()[0], 0);
    assert_eq!(msg.data().len(), topic.len() + 1);
    assert_eq!(&msg.data()[1..], topic);
}

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xpub_subscribe_long_topic() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    test_subscribe_cancel(&pub_sock, &sub_sock, SHORT_TOPIC);
    test_subscribe_cancel(&pub_sock, &sub_sock, LONG_TOPIC);
}
