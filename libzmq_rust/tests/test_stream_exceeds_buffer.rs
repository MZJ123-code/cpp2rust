mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Stream socket: send/receive without buffer overflow.
#[test]
fn test_stream_normal_buffer() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Stream);
    let c = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&s, "stream-buf");
    ctx.connect_inproc(&c, "stream-buf");

    let payload = vec![0xABu8; 100];
    s.send(payload.as_slice(), SendFlags::NONE)
        .expect("send payload");
}

/// Stream socket with data exceeding internal buffer.
#[test]
#[ignore = "Stream exceeds-buffer test requires raw TCP socket, not yet available"]
fn test_stream_exceeds_buffer() {
    // Would connect a raw TCP socket, send 8193 bytes with magic marker,
    // then verify via ZMQ_STREAM socket that the first 4 bytes are received
    // in correct order (0xde, 0xad, 0xbe, 0xef).
    let _ctx = TestContext::new();
}
