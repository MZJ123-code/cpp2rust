mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Stream socket can be created; Stream uses SocketType::Stream (value 11).
#[test]
fn test_stream_socket_create() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    assert_eq!(s.socket_type(), SocketType::Stream);
}

/// Stream socket can bind and connect inproc (basic operation).
#[test]
fn test_stream_bind_connect_inproc() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let ep = ctx.bind_inproc(&s, "stream-bind");
    let c = ctx.socket(SocketType::Stream);
    ctx.connect_inproc(&c, "stream-bind");
    // Connected — no crash
}

/// Stream to Dealer: send/receive a message pair.
#[test]
fn test_stream_to_dealer_simple() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let d = ctx.socket(SocketType::Dealer);

    ctx.bind_inproc(&s, "s2d");
    ctx.connect_inproc(&d, "s2d");

    s_send_seq(&d, &[Some("Hello"), None]);
    s_send_seq(&s, &[Some("World"), None]);
}

/// Stream to Pair: send/receive (pair acts as streaming peer).
#[test]
fn test_stream_to_pair_send_recv() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let p = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&s, "s2p");
    ctx.connect_inproc(&p, "s2p");

    ctx.bounce(&s, &p);
}

/// Stream with ZMQ_STREAM_NOTIFY (not implemented yet).
#[test]
#[ignore = "ZMQ_STREAM_NOTIFY option not yet implemented"]
fn test_stream_notify() {
    let _ctx = TestContext::new();
    // Would set stream_notify and verify connect notification frames
}

/// Stream-to-stream request/response with routing id.
#[test]
#[ignore = "Stream routing-id metadata not yet implemented"]
fn test_stream_to_stream_request_response() {
    let _ctx = TestContext::new();
    // Would test two stream sockets exchanging messages with routing frames
}
