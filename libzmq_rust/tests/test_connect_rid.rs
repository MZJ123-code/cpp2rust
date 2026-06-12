mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_connect_rid_with_pair() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&sb, "conn-rid");
    ctx.connect_inproc(&sc, "conn-rid");

    // Send and receive
    s_send_seq(&sc, &[Some("conn1"), None]);
    s_send_seq(&sc, &[Some("hi 1"), None]);

    s_recv_seq(&sb, RecvFlags::NONE, &[Some("conn1"), None]);
    s_recv_seq(&sb, RecvFlags::NONE, &[Some("hi 1"), None]);

    // Send reply back
    s_send_seq(&sb, &[Some("conn1"), None]);
    s_send_seq(&sb, &[Some("ok"), None]);

    s_recv_seq(&sc, RecvFlags::NONE, &[Some("conn1"), None]);
    s_recv_seq(&sc, RecvFlags::NONE, &[Some("ok"), None]);
}

#[test]
fn test_connect_rid_named_sockets() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&sb, "conn-rid-named");
    ctx.connect_inproc(&sc, "conn-rid-named");

    s_send_seq(&sc, &[Some("conn1"), None]);
    s_send_seq(&sc, &[Some("hi 1"), None]);

    s_recv_seq(&sb, RecvFlags::NONE, &[Some("conn1"), None]);
    s_recv_seq(&sb, RecvFlags::NONE, &[Some("hi 1"), None]);
}
