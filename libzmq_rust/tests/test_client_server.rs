//! 1:1 translation of C++ `tests/test_client_server.cpp`
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

fn create_inproc_client_server_pair(ctx: &TestContext) -> (zmq_context::ZSocket, zmq_context::ZSocket) {
    let server = ctx.socket(SocketType::Server);
    let client = ctx.socket(SocketType::Client);

    ctx.bind_inproc(&server, "test-client-server");
    ctx.connect_inproc(&client, "test-client-server");

    (server, client)
}

fn send_sndmore_expect_failure(socket: &zmq_context::ZSocket) {
    let msg = ZmqMessage::from_slice(b"X");
    let result = socket.send(msg, SendFlags::SNDMORE);
    assert!(result.is_err(), "expected SNDMORE to fail");
}

#[test]
#[ignore = "Client socket rejects SNDMORE - not yet implemented"]
fn test_client_sndmore_fails() {
    let ctx = TestContext::new();
    let (server, client) = create_inproc_client_server_pair(&ctx);

    send_sndmore_expect_failure(&client);

    drop(server);
    drop(client);
}

#[test]
#[ignore = "Server socket rejects SNDMORE - not yet implemented"]
fn test_server_sndmore_fails() {
    let ctx = TestContext::new();
    let (server, client) = create_inproc_client_server_pair(&ctx);

    send_sndmore_expect_failure(&server);

    drop(server);
    drop(client);
}

#[test]
#[ignore = "Client/Server routing ID feature not yet implemented"]
fn test_routing_id() {
    let ctx = TestContext::new();
    let (server, client) = create_inproc_client_server_pair(&ctx);

    // Client sends a message
    send_string_(&client, "X", SendFlags::NONE);

    // Server receives with a routing id set by the system
    let msg = server.recv(RecvFlags::NONE).expect("server recv");
    assert_eq!(msg.data(), b"X", "received data mismatch");

    let routing_id = msg.routing_id();
    assert!(routing_id.is_some(), "expected routing id to be set");
    assert_ne!(routing_id.unwrap(), 0, "routing id should be non-zero");

    // Server sends a reply back using the routing id
    let mut reply = ZmqMessage::from_slice(b"\x02");
    reply.set_routing_id(routing_id.unwrap());
    server.send(reply, SendFlags::NONE).expect("server send reply");

    // Client receives the reply — its routing id should be cleared
    let reply_msg = client.recv(RecvFlags::NONE).expect("client recv");
    assert_eq!(reply_msg.data(), b"\x02", "reply data mismatch");
    assert_eq!(reply_msg.routing_id(), None, "client should not see routing id");

    drop(server);
    drop(client);
}
