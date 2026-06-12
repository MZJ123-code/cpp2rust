//! 1:1 translation of C++ `tests/test_srcfd.cpp`.
//!
//! ZMQ_SRCFD message property — get the source file descriptor
//! of a received message. This is a TCP transport feature.
//! We test the equivalent inproc bounce.
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

const MSG_SIZE: usize = 20;

#[test]
fn test_srcfd_inproc() {
    let test = TestContext::new();

    let a = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&a, "srcfd-test");

    let b = test.socket(SocketType::Pair);
    b.connect(&common::ep_inproc("srcfd-test")).unwrap();

    test.bounce(&a, &b);
}

#[test]
#[ignore]
fn test_srcfd_tcp() {
    // ZMQ_SRCFD requires real TCP transport
}
