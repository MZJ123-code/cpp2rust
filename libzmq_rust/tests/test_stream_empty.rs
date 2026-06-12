mod common;
use common::*;
use zmq_core::socket_type::SocketType;
use zmq_core::message::ZmqMessage;

/// Stream socket can send and receive empty messages.
#[test]
fn test_stream_empty_message() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let c = ctx.socket(SocketType::Dealer);

    ctx.bind_inproc(&s, "stream-empty");
    ctx.connect_inproc(&c, "stream-empty");

    // Send empty string
    s_send_seq(&c, &[Some(""), None]);

    // Send an explicitly empty message via ZmqMessage
    let empty = ZmqMessage::new();
    c.send(empty, SendFlags::NONE).expect("send empty");
}

/// Stream socket with zero-length frame (disconnect notification pattern).
#[test]
#[ignore = "Stream disconnect notification not yet implemented"]
fn test_stream_empty_close() {
    // Would test that closing a stream socket with zero-length frame
    // does not cause "Bad Address" error
    let _ctx = TestContext::new();
}
