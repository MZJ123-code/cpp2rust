//! 1:1 translation of C++ `tests/test_ipc_wildcard.cpp`.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_ipc_wildcard() {
    let test = TestContext::new();
    let sb = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&sb, "ipc-wildcard");

    let sc = test.socket(SocketType::Pair);
    test.connect_inproc(&sc, "ipc-wildcard");

    test.bounce(&sb, &sc);
}
