//! 1:1 translation of C++ `tests/test_channel.cpp`
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

#[test]
fn test_roundtrip() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Channel);
    let sc = ctx.socket(SocketType::Channel);

    ctx.bind_inproc(&sb, "test-channel");
    ctx.connect_inproc(&sc, "test-channel");

    send_string_(&sb, "HELLO", SendFlags::NONE);
    recv_string_assert(&sc, "HELLO", RecvFlags::NONE);

    send_string_(&sc, "WORLD", SendFlags::NONE);
    recv_string_assert(&sb, "WORLD", RecvFlags::NONE);
}

#[test]
fn test_sndmore_fails() {
    let ctx = TestContext::new();
    let sc = ctx.socket(SocketType::Channel);

    // Channel does not support multipart messages
    let msg = ZmqMessage::from_slice(b"X");
    let result = sc.send(msg, SendFlags::SNDMORE);
    assert!(result.is_err(), "expected error for SNDMORE on Channel");
}
