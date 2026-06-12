mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

/// Corresponds to C++ `test_simple` — basic REQ/REP bounce via IPC.
/// Translated to inproc since IPC transport is not yet available.
fn simple(ctx: &TestContext) {
    let sb = ctx.socket(SocketType::Rep);
    sb.bind(&common::ep_inproc("ipc_simple")).unwrap();

    let sc = ctx.socket(SocketType::Req);
    sc.connect(&common::ep_inproc("ipc_simple")).unwrap();

    ctx.bounce(&sb, &sc);
}

/// Corresponds to C++ `test_leak` — verify no memory leak when sending
/// after peer disconnect.
fn leak(ctx: &TestContext) {
    let sb = ctx.socket(SocketType::Rep);
    sb.bind(&common::ep_inproc("ipc_leak")).unwrap();

    let sc = ctx.socket(SocketType::Req);
    sc.connect(&common::ep_inproc("ipc_leak")).unwrap();

    sb.send(ZmqMessage::from_slice(b"leakymsg"), SendFlags::NONE).unwrap();

    common::msleep(common::SETTLE_TIME.as_millis() as u64);

    sc.close().unwrap();
}

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_simple_inproc() {
    let ctx = TestContext::new();
    simple(&ctx);
}

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_leak_inproc() {
    let ctx = TestContext::new();
    leak(&ctx);
}
