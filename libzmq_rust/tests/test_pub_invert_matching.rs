//! 1:1 translation of C++ `tests/test_pub_invert_matching.cpp`.
//! Tests basic subscription matching (invert_matching not yet implemented).
mod common;

use common::{msleep, TestContext};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

#[test]
fn test_basic_subscription_filtering() {
    let ctx = TestContext::new();

    let pub_sock = ctx.socket(SocketType::Pub);
    pub_sock.bind("inproc://sotest").unwrap();

    let sub1 = ctx.socket(SocketType::Sub);
    sub1.connect("inproc://sotest").unwrap();

    let sub2 = ctx.socket(SocketType::Sub);
    sub2.connect("inproc://sotest").unwrap();

    sub1.subscribe(b"prefix1").unwrap();
    sub2.subscribe(b"p2").unwrap();

    msleep(300);

    // pub sends "prefix1" — only sub1 should receive it
    pub_sock
        .send(ZmqMessage::from_slice(b"prefix1"), SendFlags::NONE)
        .unwrap();
    msleep(300);

    let msg = sub1.recv(RecvFlags::DONTWAIT).unwrap();
    assert_eq!(msg.data(), b"prefix1");

    let res = sub2.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err(), "sub2 should not receive 'prefix1'");

    // pub sends "p2" — only sub2 should receive it
    pub_sock
        .send(ZmqMessage::from_slice(b"p2"), SendFlags::NONE)
        .unwrap();
    msleep(300);

    let msg = sub2.recv(RecvFlags::DONTWAIT).unwrap();
    assert_eq!(msg.data(), b"p2");

    let res = sub1.recv(RecvFlags::DONTWAIT);
    assert!(res.is_err(), "sub1 should not receive 'p2'");
}
