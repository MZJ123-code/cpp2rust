mod common;
use common::{TestContext, SEQ_END};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

fn round_robin_out(ctx: &TestContext) {
    let req = ctx.socket(SocketType::Req);
    req.bind(&common::ep_inproc("req_rr")).unwrap();

    const SERVICES: usize = 5;
    let mut reps = Vec::new();
    for _ in 0..SERVICES {
        let rep = ctx.socket(SocketType::Rep);
        rep.connect(&common::ep_inproc("req_rr")).unwrap();
        reps.push(rep);
    }
    common::msleep(300 * SERVICES as u64);

    for peer in 0..SERVICES {
        common::s_send_seq(&req, &[Some("ABC"), SEQ_END]);
        common::s_recv_seq(&reps[peer], RecvFlags::NONE, &[Some("ABC"), SEQ_END]);
        common::s_send_seq(&reps[peer], &[Some("DEF"), SEQ_END]);
        common::s_recv_seq(&req, RecvFlags::NONE, &[Some("DEF"), SEQ_END]);
    }

    req.set_linger(0).unwrap();
    for rep in &reps {
        rep.set_linger(0).unwrap();
    }
}

fn req_message_format(ctx: &TestContext) {
    let req = ctx.socket(SocketType::Req);
    let router = ctx.socket(SocketType::Router);

    req.bind(&common::ep_inproc("req_fmt")).unwrap();
    router.connect(&common::ep_inproc("req_fmt")).unwrap();

    common::msleep(300);

    // Send a multi-part request
    common::s_send_seq(&req, &[Some("ABC"), Some("DEF"), SEQ_END]);

    // Receive peer routing id
    let peer_id = router.recv(RecvFlags::NONE).unwrap();
    assert!(!peer_id.data().is_empty(), "routing id should not be empty");
    let peer_id_data = peer_id.data();

    // Receive delimiter
    let delim = router.recv(RecvFlags::NONE).unwrap();
    assert!(delim.data().is_empty(), "delimiter should be empty");

    // Receive "ABC", "DEF"
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some("ABC"), Some("DEF"), SEQ_END]);

    // Send back a single-part reply
    router.send(ZmqMessage::from_slice(&peer_id_data), SendFlags::SNDMORE).unwrap();
    common::s_send_seq(&router, &[Some(""), Some("GHI"), SEQ_END]);

    // Receive reply
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("GHI"), SEQ_END]);

    req.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
}

#[test]
#[ignore = "REQ socket not yet implemented"]
fn test_round_robin_out_inproc() {
    let ctx = TestContext::new();
    round_robin_out(&ctx);
}

#[test]
#[ignore = "REQ socket not yet implemented"]
fn test_req_message_format_inproc() {
    let ctx = TestContext::new();
    req_message_format(&ctx);
}

#[test]
fn test_block_on_send_no_peers() {
    let ctx = TestContext::new();
    let sc = ctx.socket(SocketType::Req);

    sc.set_sndtimeo(250).unwrap();

    assert!(sc.send(ZmqMessage::new(), SendFlags::DONTWAIT).is_err());
    assert!(sc.send(ZmqMessage::new(), SendFlags::NONE).is_err());
}
