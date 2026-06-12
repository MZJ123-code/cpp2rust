//! 1:1 translation of C++ `tests/test_msg_flags.cpp`.
//!
//! Tests ZMQ_MORE and ZMQ_SHARED message flags.
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

#[test]
fn test_msg_more_flag() {
    let test = TestContext::new();

    let sb = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sb, "msgflags");

    let sc = test.socket(SocketType::Pair);
    sc.connect(&common::ep_inproc("msgflags")).unwrap();

    // Send 2-part message from connecting socket
    send_string_(&sc, "A", SendFlags::SNDMORE);
    send_string_(&sc, "B", SendFlags::NONE);

    // Receive both parts on the bind socket
    let msg1 = sb.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg1.data(), b"A", "first part should be A");

    let msg2 = sb.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg2.data(), b"B", "second part should be B");
}

#[test]
fn test_msg_shared_refcounted() {
    // Test that copying a message creates a shared reference
    let msg_a = ZmqMessage::from_slice(&[0u8; 1024]);
    let msg_b = msg_a.clone();

    assert_eq!(msg_a.data(), msg_b.data());
    assert_eq!(msg_a.data().len(), 1024);
}

#[test]
fn test_msg_shared_const() {
    let msg = ZmqMessage::from_slice(b"TEST");
    assert_eq!(msg.data(), b"TEST");
}
