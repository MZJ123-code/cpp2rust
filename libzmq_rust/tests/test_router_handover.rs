mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_with_handover` — ROUTER with handover enabled,
/// second DEALER with same routing ID replaces the first.
fn with_handover(ctx: &TestContext) {
    let router = ctx.socket(SocketType::Router);
    router.bind(&common::ep_inproc("handover_on")).unwrap();
    router.set_router_handover(true).unwrap();

    // Create dealer "X" and connect
    let dealer_one = ctx.socket(SocketType::Dealer);
    dealer_one.set_routing_id(b"X").unwrap();
    dealer_one.connect(&common::ep_inproc("handover_on")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send from dealer_one to ensure connection is established
    common::s_send_seq(&dealer_one, &[Some("Hello"), common::SEQ_END]);
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // Create second dealer "X" (same routing ID)
    let dealer_two = ctx.socket(SocketType::Dealer);
    dealer_two.set_routing_id(b"X").unwrap();
    dealer_two.connect(&common::ep_inproc("handover_on")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send from dealer_two to ensure connection is established
    common::s_send_seq(&dealer_two, &[Some("Hello"), common::SEQ_END]);
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // Send a message to 'X'. With handover, dealer_two should receive it.
    common::s_send_seq(&router, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // dealer_one should NOT receive (handover replaced it)
    dealer_one.set_rcvtimeo(common::SETTLE_TIME.as_millis() as i32).unwrap();
    assert!(
        dealer_one.recv(RecvFlags::DONTWAIT).is_err(),
        "dealer_one should not receive (handover replaced it)"
    );

    // dealer_two SHOULD receive
    common::recv_string_expect_success(&dealer_two, "Hello", RecvFlags::NONE);

    dealer_one.set_linger(0).unwrap();
    dealer_two.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
}

/// Corresponds to C++ `test_without_handover` — ROUTER without handover,
/// first DEALER with routing ID "X" keeps the spot.
fn without_handover(ctx: &TestContext) {
    let router = ctx.socket(SocketType::Router);
    router.bind(&common::ep_inproc("handover_off")).unwrap();

    // Create dealer "X" and connect
    let dealer_one = ctx.socket(SocketType::Dealer);
    dealer_one.set_routing_id(b"X").unwrap();
    dealer_one.connect(&common::ep_inproc("handover_off")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send from dealer_one
    common::s_send_seq(&dealer_one, &[Some("Hello"), common::SEQ_END]);
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // Create second dealer "X"
    let dealer_two = ctx.socket(SocketType::Dealer);
    dealer_two.set_routing_id(b"X").unwrap();
    dealer_two.connect(&common::ep_inproc("handover_off")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Second dealer's message should be ignored (no handover)
    common::s_send_seq(&dealer_two, &[Some("Hello"), common::SEQ_END]);

    // Router should NOT receive from dealer_two
    router.set_rcvtimeo(common::SETTLE_TIME.as_millis() as i32).unwrap();
    assert!(
        router.recv(RecvFlags::DONTWAIT).is_err(),
        "router should not receive from dealer_two (no handover)"
    );

    // Send to 'X'. Should go to dealer_one (original).
    common::s_send_seq(&router, &[Some("X"), Some("Hello"), common::SEQ_END]);

    // dealer_two should NOT receive
    dealer_two.set_rcvtimeo(common::SETTLE_TIME.as_millis() as i32).unwrap();
    assert!(
        dealer_two.recv(RecvFlags::DONTWAIT).is_err(),
        "dealer_two should not receive (no handover)"
    );

    // dealer_one SHOULD receive
    common::recv_string_expect_success(&dealer_one, "Hello", RecvFlags::NONE);

    dealer_one.set_linger(0).unwrap();
    dealer_two.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
}

#[test]
#[ignore = "ROUTER state machine not yet implemented"]
fn test_with_handover_inproc() {
    let ctx = TestContext::new();
    with_handover(&ctx);
}

#[test]
#[ignore = "ROUTER state machine not yet implemented"]
fn test_without_handover_inproc() {
    let ctx = TestContext::new();
    without_handover(&ctx);
}
