mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_roundtrip` — manual ROUTER/DEALER device.
/// REQ → ROUTER → (device) → DEALER → REP roundtrip.
fn roundtrip(ctx: &TestContext) {
    let dealer = ctx.socket(SocketType::Dealer);
    dealer.bind(&common::ep_inproc("device_dealer")).unwrap();

    let router = ctx.socket(SocketType::Router);
    router.bind(&common::ep_inproc("device_router")).unwrap();

    // Create a worker (REP connects to DEALER)
    let rep = ctx.socket(SocketType::Rep);
    rep.connect(&common::ep_inproc("device_dealer")).unwrap();

    // Create a client (REQ connects to ROUTER)
    let req = ctx.socket(SocketType::Req);
    req.connect(&common::ep_inproc("device_router")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // ── Send a request through the device ──
    common::s_send_seq(&req, &[Some("ABC"), Some("DEF"), common::SEQ_END]);

    // Forward request: ROUTER → DEALER (4 frames: routing-id + empty + "ABC" + "DEF")
    for _ in 0..4 {
        let msg = router.recv(RecvFlags::NONE).unwrap();
        let more = msg.more();
        let flags = if more { SendFlags::SNDMORE } else { SendFlags::NONE };
        dealer.send(msg, flags).unwrap();
    }

    // Receive the request on REP
    common::recv_string_expect_success(&rep, "ABC", RecvFlags::NONE);
    let msg = rep.recv(RecvFlags::NONE).unwrap();
    assert!(msg.more(), "expected RCVMORE after 'ABC'");
    common::recv_string_expect_success(&rep, "DEF", RecvFlags::NONE);
    let msg = rep.recv(RecvFlags::NONE).unwrap();
    assert!(!msg.more(), "expected no RCVMORE after 'DEF'");

    // ── Send the reply through the device ──
    common::s_send_seq(&rep, &[Some("GHI"), Some("JKL"), common::SEQ_END]);

    // Forward reply: DEALER → ROUTER (4 frames: routing-id + empty + "GHI" + "JKL")
    for _ in 0..4 {
        let msg = dealer.recv(RecvFlags::NONE).unwrap();
        let more = msg.more();
        let flags = if more { SendFlags::SNDMORE } else { SendFlags::NONE };
        router.send(msg, flags).unwrap();
    }

    // Receive the reply on REQ
    common::recv_string_expect_success(&req, "GHI", RecvFlags::NONE);
    let msg = req.recv(RecvFlags::NONE).unwrap();
    assert!(msg.more(), "expected RCVMORE after 'GHI'");
    common::recv_string_expect_success(&req, "JKL", RecvFlags::NONE);
    let msg = req.recv(RecvFlags::NONE).unwrap();
    assert!(!msg.more(), "expected no RCVMORE after 'JKL'");

    req.set_linger(0).unwrap();
    rep.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
    dealer.set_linger(0).unwrap();
}

#[test]
#[ignore = "ROUTER/DEALER state machine not yet implemented"]
fn test_roundtrip_inproc() {
    let ctx = TestContext::new();
    roundtrip(&ctx);
}
