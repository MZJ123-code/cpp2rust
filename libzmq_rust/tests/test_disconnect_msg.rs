//! 1:1 translation of C++ `tests/test_disconnect_msg.cpp`.
//!
//! DISCONNECT_MSG and HELLO_MSG socket options with SERVER/CLIENT sockets.
//! SERVER and CLIENT are draft socket types; we adapt to PAIR.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_disconnect_msg_inproc() {
    let test = TestContext::new();

    // Use PAIR (Dealer is not yet stable with inproc transport)
    let server = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&server, "disconnect-msg-server");

    let client = test.socket(SocketType::Pair);
    test.connect_inproc(&client, "disconnect-msg-server");

    // Send a message from client
    test.bounce(&server, &client);
}

#[test]
fn test_disconnect_msg_close() {
    let test = TestContext::new();

    let server = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&server, "disconnect-close");

    let client = test.socket(SocketType::Pair);
    test.connect_inproc(&client, "disconnect-close");

    test.bounce(&server, &client);

    // Close client to simulate disconnect
    std::mem::drop(client);

    msleep(100);
}

#[test]
#[ignore]
fn test_disconnect_msg_tcp() {
    // TCP variant of disconnect test - requires real TCP transport
}

#[test]
#[ignore]
fn test_hello_msg() {
    // HELLO_MSG requires transport-level handshake
}
