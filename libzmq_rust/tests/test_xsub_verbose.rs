//! 1:1 translation of C++ `tests/test_xsub_verbose.cpp`.
mod common;

use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

const SUBSCRIBE_A: &[u8] = &[1, b'A'];
const UNSUBSCRIBE_A: &[u8] = &[0, b'A'];

#[test]
#[ignore = "XPUB/XSUB sockets not yet implemented"]
fn test_xsub_verbose_unsubscribe() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Xpub);
    pub_sock.bind("inproc://soname").unwrap();
    pub_sock.set_xpub_verboser(true).unwrap();

    let sub_sock = ctx.socket(SocketType::Xsub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();

    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub_sock
        .send(ZmqMessage::from_slice(SUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(SUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let res = pub_sock.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err());

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub_sock.set_xsub_verbose_unsubscribe(true).unwrap();

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(SUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(SUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), SUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);

    sub_sock
        .send(ZmqMessage::from_slice(UNSUBSCRIBE_A), SendFlags::NONE)
        .unwrap();
    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), UNSUBSCRIBE_A);
}
