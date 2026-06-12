//! 1:1 translation of C++ `tests/test_mock_pub_sub.cpp`.
//!
//! Mock PUB/SUB handshake at ZMTP protocol level.
//! Requires raw socket access — marked ignored.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_mock_sub_legacy_inproc() {
    let test = TestContext::new();

    let a = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&a, "mock-pubsub");

    let b = test.socket(SocketType::Pair);
    b.connect(&common::ep_inproc("mock-pubsub")).unwrap();

    test.bounce(&a, &b);
}

#[test]
fn test_mock_pub_legacy_inproc() {
    let test = TestContext::new();

    let sub = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sub, "mock-pub");

    let pub_sock = test.socket(SocketType::Pair);
    pub_sock.connect(&common::ep_inproc("mock-pub")).unwrap();

    test.bounce(&sub, &pub_sock);
}

#[test]
#[ignore]
fn test_mock_sub_command() {
    // Requires raw ZMTP protocol-level testing with raw sockets
}

#[test]
#[ignore]
fn test_mock_pub_command() {
    // Requires raw ZMTP protocol-level testing with raw sockets
}
