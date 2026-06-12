mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_bind_after_connect() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    ctx.connect_inproc(&sc, "bind-after-connect");

    s_send_seq(&sc, &[Some("foobar"), None]);
    s_send_seq(&sc, &[Some("baz"), None]);
    s_send_seq(&sc, &[Some("buzz"), None]);

    ctx.bind_inproc(&sb, "bind-after-connect");

    s_recv_seq(&sb, RecvFlags::NONE, &[Some("foobar"), None]);
    s_recv_seq(&sb, RecvFlags::NONE, &[Some("baz"), None]);
    s_recv_seq(&sb, RecvFlags::NONE, &[Some("buzz"), None]);
}
