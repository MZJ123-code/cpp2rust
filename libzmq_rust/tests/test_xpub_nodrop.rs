//! 1:1 translation of C++ `tests/test_xpub_nodrop.cpp`.
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

    let hwm = 2000;
    pub_sock.set_sndhwm(hwm).unwrap();
    pub_sock.set_xpub_nodrop(true).unwrap();
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock.subscribe(b"").unwrap();

    let msg = pub_sock.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"\x01");

    let hwmlimit = hwm - 1;
    let mut send_count = 0;

    for _ in 0..hwmlimit {
        pub_sock
            .send(ZmqMessage::new(), SendFlags::NONE)
            .unwrap();
        send_count += 1;
    }

    let mut recv_count = 0;
    loop {
        let rc = sub_sock.recv(RecvFlags::NONE);
        match rc {
            Ok(_) => {
                recv_count += 1;
                if recv_count == 1 {
                    sub_sock.set_rcvtimeo(250).unwrap();
                }
            }
            Err(_) => break,
        }
    }

    assert_eq!(send_count, recv_count);

    pub_sock.set_sndtimeo(0).unwrap();

    send_count = 0;
    recv_count = 0;

    while pub_sock
        .send(ZmqMessage::new(), SendFlags::NONE)
        .is_ok()
    {
        send_count += 1;
    }

    if send_count > 0 {
        sub_sock.recv(RecvFlags::NONE).unwrap();
        recv_count += 1;

        while sub_sock.recv(RecvFlags::DONTWAIT).is_ok() {
            recv_count += 1;
        }
    }

    assert_eq!(send_count, recv_count);
}
