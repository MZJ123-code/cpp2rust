mod common;
use common::{TestContext, SEQ_END};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

#[test]
#[ignore = "REQ/ROUTER correlate state machine not yet implemented"]
fn test_req_correlate() {
    let ctx = TestContext::new();
    let req = ctx.socket(SocketType::Req);
    req.set_req_correlate(true).unwrap();

    let router = ctx.socket(SocketType::Router);

    router.bind(&common::ep_inproc("req_corr")).unwrap();
    req.connect(&common::ep_inproc("req_corr")).unwrap();

    common::msleep(300);

    // Send a multi-part request
    common::s_send_seq(&req, &[Some("ABC"), Some("DEF"), SEQ_END]);

    // Receive peer routing id
    let peer_id_msg = router.recv(RecvFlags::NONE).unwrap();
    assert!(!peer_id_msg.data().is_empty(), "routing id should not be empty");
    let peer_id_data = peer_id_msg.data();

    // Receive request id
    let req_id_msg = router.recv(RecvFlags::NONE).unwrap();
    assert_eq!(req_id_msg.data().len(), 4, "request id should be uint32 (4 bytes)");
    let req_id_bytes = req_id_msg.data();
    let req_id = u32::from_ne_bytes(req_id_bytes[..4].try_into().unwrap());

    // Receive the rest: delimiter, "ABC", "DEF"
    common::s_recv_seq(&router, RecvFlags::NONE, &[Some(""), Some("ABC"), Some("DEF"), SEQ_END]);

    let bad_req_id = req_id.wrapping_add(1);
    let bad_req_id_bytes = bad_req_id.to_ne_bytes();

    // Send back a bad reply: wrong req id, delimiter, data
    router.send(ZmqMessage::from_slice(&peer_id_data), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::from_slice(&bad_req_id_bytes), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::new(), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::from_slice(b"DATA"), SendFlags::NONE).unwrap();

    // Send back a good reply: correct req id, delimiter, data
    router.send(ZmqMessage::from_slice(&peer_id_data), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::from_slice(&req_id_bytes), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::new(), SendFlags::SNDMORE).unwrap();
    router.send(ZmqMessage::from_slice(b"GHI"), SendFlags::NONE).unwrap();

    // Receive the good reply. If the bad reply got through, we wouldn't see "GHI".
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("GHI"), SEQ_END]);

    req.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
}
