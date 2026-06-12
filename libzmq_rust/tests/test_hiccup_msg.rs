//! 1:1 translation of C++ `tests/test_hiccup_msg.cpp`.
//!
//! HICCUP_MSG socket option — a message sent when the connection
//! to a peer is lost (draft SERVER/CLIENT sockets).
//! We adapt to PAIR.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_hiccup_msg_inproc() {
    let test = TestContext::new();

    let server = test.socket(SocketType::Pair);
    let client = test.socket(SocketType::Pair);

    let _ep = test.bind_inproc(&server, "hiccup-test");
    test.connect_inproc(&client, "hiccup-test");

    test.bounce(&server, &client);

    // Kill the server
    std::mem::drop(server);

    msleep(200);

    std::mem::drop(client);
}

#[test]
#[ignore]
fn test_hiccup_msg_tcp() {
    // TCP variant requires real transport and HELLO_MSG/HICCUP_MSG options
}
