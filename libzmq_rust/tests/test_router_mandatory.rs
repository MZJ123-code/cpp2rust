mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_basic` — router mandatory routing.
/// Without mandatory: unknown peers silently drop.
/// With mandatory: unknown peers return EHOSTUNREACH.
fn basic(ctx: &TestContext) {
    let router = ctx.socket(SocketType::Router);
    router.bind(&common::ep_inproc("mandatory_basic")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send to unknown peer WITHOUT mandatory (default) — silently dropped
    common::s_send_seq(&router, &[Some("UNKNOWN"), Some("DATA"), common::SEQ_END]);

    // Send to unknown peer WITH mandatory — should fail
    router.set_router_mandatory(true).unwrap();
    assert!(
        router.send(ZmqMessage::from_slice(b"UNKNOWN"), SendFlags::SNDMORE).is_err(),
        "expected error sending to unknown peer with mandatory set"
    );

    // Create dealer "X" and connect
    let dealer = ctx.socket(SocketType::Dealer);
    dealer.set_routing_id(b"X").unwrap();
    dealer.connect(&common::ep_inproc("mandatory_basic")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Get message from dealer to know connection is ready
    common::s_send_seq(&dealer, &[Some("Hello"), common::SEQ_END]);
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // Send to connected dealer "X" — should work
    common::s_send_seq(&router, &[Some("X"), Some("Hello"), common::SEQ_END]);
    common::recv_string_expect_success(&dealer, "Hello", RecvFlags::NONE);

    router.set_linger(0).unwrap();
    dealer.set_linger(0).unwrap();
}

/// Corresponds to C++ `test_get_peer_state` — requires draft API
/// `zmq_socket_get_peer_state` which is not implemented.
#[test]
#[ignore = "zmq_socket_get_peer_state not implemented (draft API)"]
fn test_get_peer_state() {
    // This test requires the draft API zmq_socket_get_peer_state
    // which is not yet implemented in this Rust port.
}

/// Corresponds to C++ `test_get_peer_state_corner_cases` — requires draft API.
#[test]
#[ignore = "zmq_socket_get_peer_state not implemented (draft API)"]
fn test_get_peer_state_corner_cases() {
    // This test requires the draft API zmq_socket_get_peer_state
    // which is not yet implemented in this Rust port.
}

#[test]
#[ignore = "ROUTER state machine not yet implemented"]
fn test_basic_inproc() {
    let ctx = TestContext::new();
    basic(&ctx);
}
