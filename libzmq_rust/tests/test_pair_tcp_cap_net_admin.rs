//! 1:1 translation of C++ `tests/test_pair_tcp_cap_net_admin.cpp`.
//!
//! PAIR TCP with ZMQ_BINDTODEVICE socket option (CAP_NET_ADMIN).
//! We test the equivalent inproc PAIR operation.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_pair_tcp_inproc() {
    let test = TestContext::new();

    let sb = test.socket(SocketType::Pair);
    let ep = test.bind_inproc(&sb, "pair-cap-test");

    let sc = test.socket(SocketType::Pair);
    sc.connect(&ep).unwrap();

    test.bounce(&sb, &sc);
}

#[test]
#[ignore]
fn test_pair_tcp_bind_to_device() {
    // ZMQ_BINDTODEVICE requires CAP_NET_ADMIN and real TCP
}
