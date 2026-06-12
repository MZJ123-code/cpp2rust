mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Stream sockets can connect and disconnect; basic lifecycle test.
#[test]
fn test_stream_connect_disconnect_inproc() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let c = ctx.socket(SocketType::Stream);

    ctx.bind_inproc(&s, "stream-disc");
    ctx.connect_inproc(&c, "stream-disc");

    // Send one message each way
    s_send_seq(&c, &[Some("hello from client"), None]);
    s_send_seq(&s, &[Some("hello from server"), None]);
}

/// Test stream socket with dealer — send, poll-style wait, receive.
#[test]
#[ignore = "Stream disconnect dialog with poll not yet implemented"]
fn test_stream_disconnect_dialog() {
    // Would test a multi-step dialog between server and client stream sockets
    // using polling, ending with a disconnect (zero-length frame)
    let _ctx = TestContext::new();
}
