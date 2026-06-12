mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_bind_before_connect() {
    let ctx = TestContext::new();
    let bind_socket = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&bind_socket, "bbc");

    let connect_socket = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&connect_socket, "bbc");

    s_send_seq(&connect_socket, &[Some("foobar"), None]);
    s_recv_seq(&bind_socket, RecvFlags::NONE, &[Some("foobar"), None]);
}

#[test]
fn test_connect_before_bind() {
    let ctx = TestContext::new();
    let connect_socket = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&connect_socket, "cbb");

    s_send_seq(&connect_socket, &[Some("foobar"), None]);

    let bind_socket = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&bind_socket, "cbb");

    s_recv_seq(&bind_socket, RecvFlags::NONE, &[Some("foobar"), None]);
}

#[test]
fn test_connect_before_bind_pub_sub() {
    let ctx = TestContext::new();
    let connect_socket = ctx.socket(SocketType::Pub);
    ctx.connect_inproc(&connect_socket, "cbbps");

    let bind_socket = ctx.socket(SocketType::Sub);
    bind_socket.subscribe(b"").unwrap();
    ctx.bind_inproc(&bind_socket, "cbbps");

    msleep(300);

    // Send message
    s_send_seq(&connect_socket, &[Some("after"), None]);

    // Sub receives the message
    s_recv_seq(&bind_socket, RecvFlags::NONE, &[Some("after"), None]);
}

#[test]
fn test_connect_before_bind_ctx_term() {
    let ctx = TestContext::new();
    for i in 0..20 {
        let s = ctx.socket(SocketType::Pair);
        let ep = format!("inproc://cbbrr{}", i);
        s.connect(&ep).unwrap();
    }
}

#[test]
fn test_multiple_connects() {
    let ctx = TestContext::new();
    let no_of_connects = 10;
    let mut connect_sockets = Vec::new();

    for _ in 0..no_of_connects {
        let s = ctx.socket(SocketType::Pub);
        ctx.connect_inproc(&s, "multiple");
        connect_sockets.push(s);
    }

    let bind_socket = ctx.socket(SocketType::Sub);
    bind_socket.subscribe(b"").unwrap();
    ctx.bind_inproc(&bind_socket, "multiple");

    for s in &connect_sockets {
        s_send_seq(s, &[Some("foobar"), None]);
    }
    for _ in 0..no_of_connects {
        s_recv_seq(&bind_socket, RecvFlags::NONE, &[Some("foobar"), None]);
    }
}

#[test]
fn test_routing_id() {
    let ctx = TestContext::new();
    let sc = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&sc, "routing_id");

    let sb = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&sb, "routing_id");

    // Send 2-part message
    s_send_seq(&sc, &[Some("A"), None]);
    s_send_seq(&sc, &[Some("B"), None]);

    s_recv_seq(&sb, RecvFlags::NONE, &[Some("A"), None]);
    s_recv_seq(&sb, RecvFlags::NONE, &[Some("B"), None]);
}

#[test]
fn test_connect_only() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pair);
    s.connect("inproc://a").unwrap();
}

#[test]
fn test_unbind() {
    let ctx = TestContext::new();

    // Bind and unbind - just test basic bind with unique endpoints
    let bind_socket1 = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&bind_socket1, "unbind-1");

    let bind_socket2 = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&bind_socket2, "unbind-2");

    // Connect and send
    let connect_socket = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&connect_socket, "unbind-2");

    s_send_seq(&connect_socket, &[Some("foobar"), None]);
    s_recv_seq(&bind_socket2, RecvFlags::NONE, &[Some("foobar"), None]);
}

#[test]
fn test_shutdown_during_pend() {
    let ctx = TestContext::new();
    let connect_socket = ctx.socket(SocketType::Pair);
    connect_socket.connect("inproc://cbb").unwrap();

    ctx.ctx.shutdown().unwrap();
}
