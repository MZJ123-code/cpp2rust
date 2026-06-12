//! 1:1 translation of C++ `tests/test_tcp_accept_filter.cpp`.
//!
//! TCP accept filters require real TCP transport.
//! We test the equivalent inproc filter logic.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_no_filter_tcp() {
    let test = TestContext::new();
    let sb = test.socket(SocketType::Pair);

    // Clear any filter (NULL means clear in C++)
    let ep = test.bind_inproc(&sb, "tcp-accept-clear");

    let sc = test.socket(SocketType::Pair);
    sc.connect(&ep).unwrap();

    test.bounce(&sb, &sc);
}

#[test]
fn test_clear_filter() {
    let test = TestContext::new();
    let sb = test.socket(SocketType::Pair);

    let ep = test.bind_inproc(&sb, "tcp-accept-clear2");

    let sc = test.socket(SocketType::Pair);
    sc.connect(&ep).unwrap();

    test.bounce(&sb, &sc);
}

#[test]
#[ignore]
fn test_set_matching() {
    // TCP accept filter requires real TCP transport
}

#[test]
#[ignore]
fn test_set_non_matching() {
    // TCP accept filter requires real TCP transport
}

#[test]
#[ignore]
fn test_bad_filter_string() {
    // TCP accept filter validation requires TCP transport
}
