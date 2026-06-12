mod common;
use common::{TestContext, SendFlags, RecvFlags};
use zmq_core::socket_type::SocketType;

#[test]
fn test_roundtrip() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Rep);
    sb.bind(&common::ep_inproc("a")).unwrap();

    let sc = ctx.socket(SocketType::Req);
    sc.connect(&common::ep_inproc("a")).unwrap();

    common::send_string_(&sc, "Hello", SendFlags::NONE);
    let msg = sb.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"Hello");
    common::send_string_(&sb, "World", SendFlags::NONE);
    let msg = sc.recv(RecvFlags::NONE).unwrap();
    assert_eq!(msg.data(), b"World");
}
