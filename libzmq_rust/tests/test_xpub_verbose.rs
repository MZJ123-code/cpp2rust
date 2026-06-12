//! 1:1 translation of C++ `tests/test_xpub_verbose.cpp`.
mod common;

use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

const SUBSCRIBE_A: &[u8] = &[1, b'A'];
const SUBSCRIBE_B: &[u8] = &[1, b'B'];
const UNSUBSCRIBE_A: &[u8] = &[0, b'A'];

fn create_xpub_with_2_subs(
    ctx: &TestContext,
) -> (zmq_context::ZSocket, zmq_context::ZSocket, zmq_context::ZSocket) {
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub0 = ctx.socket(SocketType::Sub);
    sub0.connect("inproc://soname").unwrap();

    let sub1 = ctx.socket(SocketType::Sub);
    sub1.connect("inproc://soname").unwrap();

    (pub_sock, sub0, sub1)
}

fn create_duplicate_subscription(
    pub_sock: &zmq_context::ZSocket,
    sub0: &zmq_context::ZSocket,
    sub1: &zmq_context::ZSocket,
) {
    sub0.subscribe(b"A").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub1.subscribe(b"A").unwrap();

    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());
}

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xpub_verbose_one_sub() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock.subscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock.subscribe(b"B").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_B);

    sub_sock.subscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    pub_sock.set_xpub_verbose(true).unwrap();

    sub_sock.subscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    pub_sock
        .send(ZmqMessage::from_slice(b"A"), SendFlags::NONE)
        .unwrap();
    pub_sock
        .send(ZmqMessage::from_slice(b"B"), SendFlags::NONE)
        .unwrap();

    let msg = sub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");
    let msg = sub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"B");
}

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xpub_verbose_two_subs() {
    let ctx = TestContext::new();
    let (pub_sock, sub0, sub1) = create_xpub_with_2_subs(&ctx);
    create_duplicate_subscription(&pub_sock, &sub0, &sub1);

    sub0.subscribe(b"B").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_B);

    pub_sock.set_xpub_verbose(true).unwrap();

    sub1.subscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    pub_sock
        .send(ZmqMessage::from_slice(b"A"), SendFlags::NONE)
        .unwrap();
    pub_sock
        .send(ZmqMessage::from_slice(b"B"), SendFlags::NONE)
        .unwrap();

    let msg = sub0.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");
    let msg = sub1.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");
    let msg = sub0.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"B");
}

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xpub_verboser_one_sub() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock.unsubscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub_sock.subscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock.subscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub_sock.unsubscribe(b"A").unwrap();
    sub_sock.unsubscribe(b"A").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub_sock.unsubscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    pub_sock.set_xpub_verboser(true).unwrap();

    sub_sock.subscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    pub_sock
        .send(ZmqMessage::from_slice(b"A"), SendFlags::NONE)
        .unwrap();
    let msg = sub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");

    sub_sock.unsubscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub_sock.unsubscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());
}

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xpub_verboser_two_subs() {
    let ctx = TestContext::new();
    let (pub_sock, sub0, sub1) = create_xpub_with_2_subs(&ctx);
    create_duplicate_subscription(&pub_sock, &sub0, &sub1);

    sub0.unsubscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub1.unsubscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    pub_sock.set_xpub_verboser(true).unwrap();

    sub0.subscribe(b"A").unwrap();
    sub1.subscribe(b"A").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    pub_sock
        .send(ZmqMessage::from_slice(b"A"), SendFlags::NONE)
        .unwrap();

    let msg = sub0.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");
    let msg = sub1.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"A");

    sub1.unsubscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub0.unsubscribe(b"A").unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub1.unsubscribe(b"A").unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());
}
