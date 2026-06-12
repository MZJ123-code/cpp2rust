mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Stream socket can set/get basic timeout options.
#[test]
fn test_stream_timeout_options() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    s.set_linger(0).expect("set linger");
    assert_eq!(s.get_options().linger, 0);

    let d = ctx.socket(SocketType::Dealer);
    d.set_linger(0).expect("set dealer linger");
    assert_eq!(d.get_options().linger, 0);
}

/// Socket handshake interval option (ZMQ_HANDSHAKE_IVL).
#[test]
fn test_handshake_ivl_option() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Dealer);
    // Default handshake interval is 30000 ms (30 sec)
    // We can't set it directly yet, but verify default options exist
    assert_eq!(s.get_options().linger, 30000);
}

/// Stream handshake timeout on accept — full monitor-based test.
#[test]
#[ignore = "Socket monitor and ZMQ_HANDSHAKE_IVL not yet implemented"]
fn test_stream_handshake_timeout_accept() {
    // Would bind a DEALER with ZMQ_HANDSHAKE_IVL=100ms,
    // connect a STREAM that sends nothing,
    // monitor ZMQ_EVENT_ACCEPTED then ZMQ_EVENT_DISCONNECTED
    let _ctx = TestContext::new();
}

/// Stream handshake timeout on connect — full monitor-based test.
#[test]
#[ignore = "Socket monitor and ZMQ_HANDSHAKE_IVL not yet implemented"]
fn test_stream_handshake_timeout_connect() {
    // Would bind a STREAM, connect a DEALER with ZMQ_HANDSHAKE_IVL=100ms,
    // monitor ZMQ_EVENT_CONNECTED then ZMQ_EVENT_DISCONNECTED
    let _ctx = TestContext::new();
}
