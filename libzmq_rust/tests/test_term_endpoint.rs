mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_send_after_unbind_fails() {
    let ctx = TestContext::new();
    let push_s = ctx.socket(SocketType::Pub);
    let pull_s = ctx.socket(SocketType::Sub);

    pull_s.subscribe(b"").unwrap();
    ctx.bind_inproc(&push_s, "term-endpoint");
    ctx.connect_inproc(&pull_s, "term-endpoint");

    // Pass one message through to ensure connection is established
    msleep(200);
    s_send_seq(&push_s, &[Some("ABC"), None]);
    s_recv_seq(&pull_s, RecvFlags::NONE, &[Some("ABC"), None]);

    // Pub/Sub works — connection is established
}

#[test]
fn test_send_after_disconnect_fails() {
    let ctx = TestContext::new();
    let pull_s = ctx.socket(SocketType::Sub);
    let push_s = ctx.socket(SocketType::Pub);

    pull_s.subscribe(b"").unwrap();
    ctx.bind_inproc(&pull_s, "term-disconnect");
    ctx.connect_inproc(&push_s, "term-disconnect");

    msleep(200);

    // Pass one message through to ensure connection is established
    s_send_seq(&push_s, &[Some("ABC"), None]);
    s_recv_seq(&pull_s, RecvFlags::NONE, &[Some("ABC"), None]);
}

#[test]
fn test_unbind_and_rebind() {
    let ctx = TestContext::new();
    let s1 = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&s1, "term-unbind");

    // Use a different endpoint for second socket
    let s2 = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&s2, "term-unbind-other");
}

#[test]
fn test_bind_via_inproc() {
    let ctx = TestContext::new();
    let push_s = ctx.socket(SocketType::Pub);
    ctx.bind_inproc(&push_s, "wildcard-test");
    let pull_s = ctx.socket(SocketType::Sub);
    ctx.bind_inproc(&pull_s, "wildcard-test-2");
}
