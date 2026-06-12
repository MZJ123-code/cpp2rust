//! 1:1 translation of C++ `tests/test_rebind_ipc.cpp`.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_rebind_ipc() {
    let test = TestContext::new();

    // Test bind on different sockets
    let sb0 = test.socket(SocketType::Pair);
    let sb1 = test.socket(SocketType::Pair);

    let ep = test.bind_inproc(&sb0, "rebind-test");

    let sc = test.socket(SocketType::Pair);
    test.connect_inproc(&sc, "rebind-test");

    // Communicate via first bind
    test.bounce(&sb0, &sc);

    // Close first socket
    std::mem::drop(sb0);

    // Rebind with second socket
    sb1.bind(&ep).unwrap();

    // Connect again after rebind
    let sc2 = test.socket(SocketType::Pair);
    test.connect_inproc(&sc2, "rebind-test");

    test.bounce(&sb1, &sc2);
}
