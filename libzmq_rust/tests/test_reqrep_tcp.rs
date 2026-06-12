mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_single_connect` — single REQ/REP pair
/// with explicit disconnect/unbind. Translated to inproc.
fn single_connect(ctx: &TestContext) {
    let sb = ctx.socket(SocketType::Rep);
    let ep = ctx.bind_inproc(&sb, "tcp_single");

    let sc = ctx.socket(SocketType::Req);
    ctx.connect_inproc(&sc, "tcp_single");

    ctx.bounce(&sb, &sc);

    sc.set_linger(0).unwrap();
    sb.set_linger(0).unwrap();
}

/// Corresponds to C++ `test_multi_connect` — multiple REPs + single REQ
/// round-robin bounce. Translated to inproc.
fn multi_connect(ctx: &TestContext) {
    let sb0 = ctx.socket(SocketType::Rep);
    ctx.bind_inproc(&sb0, "tcp_multi0");

    let sb1 = ctx.socket(SocketType::Rep);
    ctx.bind_inproc(&sb1, "tcp_multi1");

    let sb2 = ctx.socket(SocketType::Rep);
    ctx.bind_inproc(&sb2, "tcp_multi2");

    let sc = ctx.socket(SocketType::Req);
    sc.connect(&common::ep_inproc("tcp_multi0")).unwrap();
    sc.connect(&common::ep_inproc("tcp_multi1")).unwrap();
    sc.connect(&common::ep_inproc("tcp_multi2")).unwrap();

    ctx.bounce(&sb0, &sc);
    ctx.bounce(&sb1, &sc);
    ctx.bounce(&sb2, &sc);
    ctx.bounce(&sb0, &sc);
    ctx.bounce(&sb1, &sc);
    ctx.bounce(&sb2, &sc);
    ctx.bounce(&sb0, &sc);

    sc.set_linger(0).unwrap();
    sb0.set_linger(0).unwrap();
    sb1.set_linger(0).unwrap();
    sb2.set_linger(0).unwrap();
}

/// Corresponds to C++ `test_multi_connect_same_port` — multi-REQ multi-REP
/// cross-connected. Translated to inproc.
fn multi_connect_same_port(ctx: &TestContext) {
    let sb0 = ctx.socket(SocketType::Rep);
    ctx.bind_inproc(&sb0, "tcp_multi_same0");

    let sb1 = ctx.socket(SocketType::Rep);
    ctx.bind_inproc(&sb1, "tcp_multi_same1");

    let sc0 = ctx.socket(SocketType::Req);
    sc0.connect(&common::ep_inproc("tcp_multi_same0")).unwrap();
    sc0.connect(&common::ep_inproc("tcp_multi_same1")).unwrap();

    let sc1 = ctx.socket(SocketType::Req);
    sc1.connect(&common::ep_inproc("tcp_multi_same0")).unwrap();
    sc1.connect(&common::ep_inproc("tcp_multi_same1")).unwrap();

    ctx.bounce(&sb0, &sc0);
    ctx.bounce(&sb1, &sc0);
    ctx.bounce(&sb0, &sc1);
    ctx.bounce(&sb1, &sc1);
    ctx.bounce(&sb0, &sc0);
    ctx.bounce(&sb1, &sc0);

    sc0.set_linger(0).unwrap();
    sc1.set_linger(0).unwrap();
    sb0.set_linger(0).unwrap();
    sb1.set_linger(0).unwrap();
}

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_single_connect_inproc() {
    let ctx = TestContext::new();
    single_connect(&ctx);
}

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_multi_connect_inproc() {
    let ctx = TestContext::new();
    multi_connect(&ctx);
}

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_multi_connect_same_port_inproc() {
    let ctx = TestContext::new();
    multi_connect_same_port(&ctx);
}
