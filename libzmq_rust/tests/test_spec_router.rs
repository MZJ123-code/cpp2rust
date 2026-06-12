//! 1:1 translation of C++ `tests/test_spec_router.cpp`.
mod common;
use common::{TestContext, SEQ_END};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// SHALL receive incoming messages from its peers using a fair-queuing strategy.
fn fair_queue_in(ctx: &TestContext) {
    let receiver = ctx.socket(SocketType::Router);
    receiver.bind(&common::ep_inproc("router_fq")).unwrap();

    const SERVICES: usize = 5;
    let mut senders = Vec::new();
    for peer in 0..SERVICES {
        let sender = ctx.socket(SocketType::Dealer);
        // Routing IDs: "A\0", "B\0", "C\0", "D\0", "E\0" (matching C++ style)
        let id = vec![b'A' + peer as u8, 0u8];
        sender.set_routing_id(&id).unwrap();
        sender.connect(&common::ep_inproc("router_fq")).unwrap();
        senders.push(sender);
    }

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    // Send from first peer, verify routing id + data
    common::s_send_seq(&senders[0], &[Some("M"), SEQ_END]);

    // Receive routing-id frame
    let id_msg = receiver.recv(RecvFlags::NONE).unwrap();
    assert_eq!(id_msg.data().len(), 2, "routing id should be 2 bytes");
    assert_eq!(id_msg.data()[0], b'A');
    // Receive data frame
    common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("M"), SEQ_END]);

    common::s_send_seq(&senders[0], &[Some("M"), SEQ_END]);

    let id_msg = receiver.recv(RecvFlags::NONE).unwrap();
    assert_eq!(id_msg.data().len(), 2);
    assert_eq!(id_msg.data()[0], b'A');
    common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("M"), SEQ_END]);

    // Send from all peers, verify fair-queuing by checking routing IDs
    let mut sum: i32 = 0;
    for peer in 0..SERVICES {
        common::s_send_seq(&senders[peer], &[Some("M"), SEQ_END]);
        sum += (b'A' + peer as u8) as i32;
    }

    let expected_sum = (SERVICES as i32 * b'A' as i32 + SERVICES as i32 * (SERVICES as i32 - 1) / 2);
    assert_eq!(sum, expected_sum);

    for _ in 0..SERVICES {
        let id_msg = receiver.recv(RecvFlags::NONE).unwrap();
        assert_eq!(id_msg.data().len(), 2);
        sum -= id_msg.data()[0] as i32;
        common::s_recv_seq(&receiver, RecvFlags::NONE, &[Some("M"), SEQ_END]);
    }

    assert_eq!(sum, 0);

    receiver.set_linger(0).unwrap();
    for sender in &senders {
        sender.set_linger(0).unwrap();
    }
}

/// SHALL create a double queue when a peer connects. On disconnect,
/// destroy queue and discard messages. (Disabled in C++ too.)
fn destroy_queue_on_disconnect(_ctx: &TestContext) {
    // Leaving implementation placeholder as in C++ (test is commented out)
}

#[test]
#[ignore = "ROUTER socket not yet implemented"]
fn test_fair_queue_in_inproc() {
    let ctx = TestContext::new();
    fair_queue_in(&ctx);
}

#[test]
#[ignore = "TODO commented out until libzmq implements this properly (matching C++ upstream)"]
fn test_destroy_queue_on_disconnect_inproc() {
    let ctx = TestContext::new();
    destroy_queue_on_disconnect(&ctx);
}
