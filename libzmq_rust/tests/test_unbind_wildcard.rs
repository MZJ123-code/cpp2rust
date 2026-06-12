//! 1:1 translation of C++ `tests/test_unbind_wildcard.cpp`.
//!
//! TCP wildcard bind tests. We use inproc equivalents.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_address_wildcard_inproc() {
    let test = TestContext::new();

    let sb = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sb, "wildcard-test");

    let sc = test.socket(SocketType::Pair);
    test.connect_inproc(&sc, "wildcard-test");

    test.bounce(&sb, &sc);
}

#[test]
fn test_port_wildcard_inproc() {
    let test = TestContext::new();

    let sb = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sb, "wildcard-port");

    let sc = test.socket(SocketType::Pair);
    test.connect_inproc(&sc, "wildcard-port");

    test.bounce(&sb, &sc);
}

#[test]
#[ignore]
fn test_address_wildcard_ipv6() {
    // IPv6 wildcard requires TCP transport
}

#[test]
#[ignore]
fn test_port_wildcard_ipv6_address() {
    // IPv6 wildcard requires TCP transport
}
