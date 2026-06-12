//! 1:1 translation of C++ `tests/test_sub_forward.cpp`.
//! Uses inproc instead of TCP for the intermediate device.
mod common;

use common::{msleep, TestContext};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{RecvFlags, SendFlags};

fn ep_inproc(name: &str) -> String {
    format!("inproc://{}", name)
}

#[test]
#[ignore = "PUB/SUB socket not yet implemented"]
fn test() {
    let ctx = TestContext::new();

    let xpub = ctx.socket(SocketType::Xpub);
    let xpub_ep = ep_inproc("xpub");
    xpub.bind(&xpub_ep).unwrap();

    let xsub = ctx.socket(SocketType::Xsub);
    let xsub_ep = ep_inproc("xsub");
    xsub.bind(&xsub_ep).unwrap();

    let pub_sock = ctx.socket(SocketType::Pub);
    pub_sock.connect(&xsub_ep).unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect(&xpub_ep).unwrap();

    sub_sock.subscribe(b"").unwrap();

    let msg = xpub.recv(RecvFlags::NONE).unwrap();
    let sub_data = msg.data().to_vec();
    xsub
        .send(ZmqMessage::from_slice(&sub_data), SendFlags::NONE)
        .unwrap();

    msleep(300);

    pub_sock
        .send(ZmqMessage::from_slice(b""), SendFlags::NONE)
        .unwrap();

    let msg = xsub.recv(RecvFlags::NONE).unwrap();
    let data = msg.data().to_vec();
    xpub
        .send(ZmqMessage::from_slice(&data), SendFlags::NONE)
        .unwrap();

    let msg = sub_sock.recv(RecvFlags::NONE).unwrap();
    assert!(msg.data().is_empty());
}
