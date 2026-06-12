mod common;
use common::{TestContext, SEQ_END};
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

fn fair_queue_in(ctx: &TestContext) {
    let rep = ctx.socket(SocketType::Rep);
    rep.bind(&common::ep_inproc("rep_fq")).unwrap();

    const SERVICES: usize = 5;
    let mut reqs = Vec::new();
    for _ in 0..SERVICES {
        let req = ctx.socket(SocketType::Req);
        req.connect(&common::ep_inproc("rep_fq")).unwrap();
        reqs.push(req);
    }
    common::msleep(300);

    common::s_send_seq(&reqs[0], &[Some("A"), SEQ_END]);
    common::s_recv_seq(&rep, RecvFlags::NONE, &[Some("A"), SEQ_END]);
    common::s_send_seq(&rep, &[Some("A"), SEQ_END]);
    common::s_recv_seq(&reqs[0], RecvFlags::NONE, &[Some("A"), SEQ_END]);

    common::s_send_seq(&reqs[0], &[Some("A"), SEQ_END]);
    common::s_recv_seq(&rep, RecvFlags::NONE, &[Some("A"), SEQ_END]);
    common::s_send_seq(&rep, &[Some("A"), SEQ_END]);
    common::s_recv_seq(&reqs[0], RecvFlags::NONE, &[Some("A"), SEQ_END]);

    rep.set_linger(0).unwrap();
    for req in &reqs {
        req.set_linger(0).unwrap();
    }
}

fn envelope(ctx: &TestContext) {
    let rep = ctx.socket(SocketType::Rep);
    rep.bind(&common::ep_inproc("rep_env")).unwrap();

    let dealer = ctx.socket(SocketType::Dealer);
    dealer.connect(&common::ep_inproc("rep_env")).unwrap();

    common::msleep(300);

    // minimal envelope
    common::s_send_seq(&dealer, &[Some(""), Some("A"), SEQ_END]);
    common::s_recv_seq(&rep, RecvFlags::NONE, &[Some("A"), SEQ_END]);
    common::s_send_seq(&rep, &[Some("A"), SEQ_END]);
    common::s_recv_seq(&dealer, RecvFlags::NONE, &[Some(""), Some("A"), SEQ_END]);

    // big envelope
    common::s_send_seq(&dealer, &[Some("X"), Some("Y"), Some(""), Some("A"), SEQ_END]);
    common::s_recv_seq(&rep, RecvFlags::NONE, &[Some("A"), SEQ_END]);
    common::s_send_seq(&rep, &[Some("A"), SEQ_END]);
    common::s_recv_seq(&dealer, RecvFlags::NONE, &[Some("X"), Some("Y"), Some(""), Some("A"), SEQ_END]);

    rep.set_linger(0).unwrap();
    dealer.set_linger(0).unwrap();
}

#[test]
fn test_fair_queue_in_inproc() {
    let ctx = TestContext::new();
    fair_queue_in(&ctx);
}

#[test]
#[ignore = "DEALER socket xrecv not yet implemented"]
fn test_envelope_inproc() {
    let ctx = TestContext::new();
    envelope(&ctx);
}
