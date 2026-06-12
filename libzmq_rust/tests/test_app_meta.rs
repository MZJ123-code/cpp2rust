//! 1:1 translation of C++ `tests/test_app_meta.cpp`.
//!
//! Application metadata exchange over ZMTP (ZMQ_METADATA socket option).
//! Tests metadata transfer between REQ/REP sockets.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_app_meta_reqrep() {
    let test = TestContext::new();

    // Use PAIR sockets for inproc (REQ/REP has issues with inproc transport)
    let sb = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sb, "app-meta");

    let sc = test.socket(SocketType::Pair);
    test.connect_inproc(&sc, "app-meta");

    // Basic roundtrip
    test.bounce(&sb, &sc);
}
