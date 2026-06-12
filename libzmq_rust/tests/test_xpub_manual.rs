//! 1:1 translation of C++ `tests/test_xpub_manual.cpp`.
mod common;

use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

#[test]
#[ignore = "XPUB socket not yet implemented"]
fn test_basic() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.set_xpub_manual(true).unwrap();
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Xsub);
    sub_sock.connect("inproc://soname").unwrap();

    let subscription = [1, b'A'];
    sub_sock
        .send(ZmqMessage::from_slice(&subscription), SendFlags::NONE)
        .unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &subscription);

    pub_sock.subscribe(b"B").unwrap();

    pub_sock
        .send(ZmqMessage::from_slice(b"A"), SendFlags::NONE)
        .unwrap();
    pub_sock
        .send(ZmqMessage::from_slice(b"B"), SendFlags::NONE)
        .unwrap();

    let msg = sub_sock.recv(RecvFlags::DONTWAIT).unwrap();
    assert_eq!(msg.data(), b"B");
}

#[test]
#[ignore = "XPUB socket not yet implemented"]
fn test_unsubscribe_manual() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.set_xpub_manual(true).unwrap();
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Xsub);
    sub_sock.connect("inproc://soname").unwrap();

    let subscription_a = [1, b'A'];
    let subscription_b = [1, b'B'];
    sub_sock
        .send(ZmqMessage::from_slice(&subscription_a), SendFlags::NONE)
        .unwrap();
    sub_sock
        .send(ZmqMessage::from_slice(&subscription_b), SendFlags::NONE)
        .unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &subscription_a);
    pub_sock.subscribe(b"XA").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &subscription_b);
    pub_sock.subscribe(b"XB").unwrap();

    let unsubscription_a = [0, b'A'];
    sub_sock
        .send(ZmqMessage::from_slice(&unsubscription_a), SendFlags::NONE)
        .unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &unsubscription_a);
    pub_sock.unsubscribe(b"XA").unwrap();

    pub_sock
        .send(ZmqMessage::from_slice(b"XA"), SendFlags::NONE)
        .unwrap();
    pub_sock
        .send(ZmqMessage::from_slice(b"XB"), SendFlags::NONE)
        .unwrap();

    let msg = sub_sock.recv(RecvFlags::DONTWAIT).unwrap();
    assert_eq!(msg.data(), b"XB");

    drop(sub_sock);

    let unsubscription_b = [0, b'B'];
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &unsubscription_b);
    pub_sock.unsubscribe(b"XB").unwrap();
}

#[test]
fn test_user_message() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Xsub);
    sub_sock.connect("inproc://soname").unwrap();

    let subscription = [2, b'A'];
    sub_sock
        .send(ZmqMessage::from_slice(&subscription), SendFlags::NONE)
        .unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), &subscription);
}
