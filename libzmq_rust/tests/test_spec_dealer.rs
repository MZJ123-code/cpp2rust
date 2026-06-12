//! 1:1 translation of C++ `tests/test_spec_dealer.cpp`.
mod common;
use common::{TestContext, SEQ_END};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// SHALL route outgoing messages to available peers using a round-robin strategy.
fn round_robin_out(ctx: &TestContext) {
    let dealer = ctx.socket(SocketType::Dealer);
    dealer.bind(&common::ep_inproc("dealer_rr")).unwrap();

    const SERVICES: usize = 5;
    let mut reps = Vec::new();
    for _ in 0..SERVICES {
        let rep = ctx.socket(SocketType::Rep);
        rep.connect(&common::ep_inproc("dealer_rr")).unwrap();
        reps.push(rep);
    }

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send all requests (DEALER sends empty-frame + "ABC" to each peer)
    for _ in 0..SERVICES {
        common::s_send_seq(&dealer, &[Some(""), Some("ABC"), SEQ_END]);
    }

    // Expect every REP got one message
    for rep in &reps {
        common::s_recv_seq(rep, RecvFlags::NONE, &[Some("ABC"), SEQ_END]);
    }

    dealer.set_linger(0).unwrap();
    for rep in &reps {
        rep.set_linger(0).unwrap();
    }
}

/// SHALL receive incoming messages from its peers using a fair-queuing strategy.
fn fair_queue_in(ctx: &TestContext) {
    let receiver = ctx.socket(SocketType::Dealer);
    receiver.bind(&common::ep_inproc("dealer_fq")).unwrap();

    const SERVICES: usize = 5;
    let mut senders = Vec::new();
    for _ in 0..SERVICES {
        let sender = ctx.socket(SocketType::Dealer);
        sender.connect(&common::ep_inproc("dealer_fq")).unwrap();
        senders.push(sender);
    }

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    common::s_send_seq(&senders[0], &[Some("A"), SEQ_END]);
    common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("A"), SEQ_END]);

    common::s_send_seq(&senders[0], &[Some("A"), SEQ_END]);
    common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("A"), SEQ_END]);

    // Send from all
    for peer in 0..SERVICES {
        common::s_send_seq(&senders[peer], &[Some("B"), SEQ_END]);
    }

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Receive all
    for _ in 0..SERVICES {
        common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("B"), SEQ_END]);
    }

    receiver.set_linger(0).unwrap();
    for sender in &senders {
        sender.set_linger(0).unwrap();
    }
}

/// SHALL block on sending when it has no connected peers.
fn block_on_send_no_peers(ctx: &TestContext) {
    let sc = ctx.socket(SocketType::Dealer);

    sc.set_sndtimeo(250).unwrap();

    assert!(
        sc.send(ZmqMessage::new(), SendFlags::DONTWAIT).is_err(),
        "expected error on DONTWAIT with no peers"
    );
    assert!(
        sc.send(ZmqMessage::new(), SendFlags::NONE).is_err(),
        "expected error on blocking send with no peers"
    );
}

/// SHALL create a double queue when a peer connects. On disconnect,
/// destroy queue and discard messages. (Disabled in C++ too.)
fn destroy_queue_on_disconnect(ctx: &TestContext) {
    let a = ctx.socket(SocketType::Dealer);
    a.bind(&common::ep_inproc("dealer_dq")).unwrap();

    let b = ctx.socket(SocketType::Dealer);
    b.connect(&common::ep_inproc("dealer_dq")).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send in both directions
    common::s_send_seq(&a, &[Some("ABC"), SEQ_END]);
    common::s_send_seq(&b, &[Some("DEF"), SEQ_END]);

    // b.close() would disconnect
    b.set_linger(0).unwrap();
    drop(b);

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Should not be able to send/receive
    assert!(
        a.send(ZmqMessage::new(), SendFlags::DONTWAIT).is_err(),
        "expected error after disconnect"
    );

    a.set_linger(0).unwrap();
}

#[test]
#[ignore = "DEALER socket not yet implemented"]
fn test_round_robin_out_inproc() {
    let ctx = TestContext::new();
    round_robin_out(&ctx);
}

#[test]
#[ignore = "DEALER socket not yet implemented"]
fn test_fair_queue_in_inproc() {
    let ctx = TestContext::new();
    fair_queue_in(&ctx);
}

#[test]
fn test_block_on_send_no_peers_inproc() {
    let ctx = TestContext::new();
    block_on_send_no_peers(&ctx);
}

#[test]
#[ignore = "TODO libzmq does this properly; uncomment when DEALER destroy queue on disconnect is verified"]
fn test_destroy_queue_on_disconnect_inproc() {
    let ctx = TestContext::new();
    destroy_queue_on_disconnect(&ctx);
}
