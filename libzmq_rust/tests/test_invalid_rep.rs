mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_invalid_rep` — verify that REP properly handles
/// invalid replies (missing envelope delimiter).
fn invalid_rep(ctx: &TestContext) {
    let router_socket = ctx.socket(SocketType::Router);
    let req_socket = ctx.socket(SocketType::Req);

    router_socket.set_linger(0).unwrap();
    req_socket.set_linger(0).unwrap();

    router_socket.bind(&common::ep_inproc("invalid_rep")).unwrap();
    req_socket.connect(&common::ep_inproc("invalid_rep")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Initial request
    common::send_string_(&req_socket, "r", SendFlags::NONE);

    // Receive the request on the ROUTER
    let addr = router_socket.recv(RecvFlags::NONE).unwrap();
    let addr_data = addr.data().clone();
    assert!(!addr_data.is_empty(), "routing id should not be empty");

    let bottom = router_socket.recv(RecvFlags::NONE).unwrap();
    assert!(bottom.data().is_empty(), "delimiter should be empty");

    common::recv_string_assert(&router_socket, "r", RecvFlags::NONE);

    // Send invalid reply: just the routing ID without delimiter or data
    router_socket
        .send(ZmqMessage::from_slice(&addr_data), SendFlags::NONE)
        .unwrap();

    // Send valid reply: routing_id + SNDMORE, delimiter + SNDMORE, body
    router_socket
        .send(ZmqMessage::from_slice(&addr_data), SendFlags::SNDMORE)
        .unwrap();
    router_socket
        .send(ZmqMessage::new(), SendFlags::SNDMORE)
        .unwrap();
    common::send_string_(&router_socket, "b", SendFlags::NONE);

    // Check that we got the valid reply
    common::recv_string_assert(&req_socket, "b", RecvFlags::NONE);

    router_socket.set_linger(0).unwrap();
    req_socket.set_linger(0).unwrap();
}

#[test]
#[ignore = "REQ/ROUTER socket state machine not yet implemented"]
fn test_invalid_rep_inproc() {
    let ctx = TestContext::new();
    invalid_rep(&ctx);
}
