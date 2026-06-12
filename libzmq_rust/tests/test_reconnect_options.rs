mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_reconnect_ivl_default() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Sub);
    // Default reconnect interval should be 100ms
    assert_eq!(s.reconnect_ivl(), 100);
}

#[test]
fn test_reconnect_ivl_set_get() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Sub);

    s.set_reconnect_ivl(60000).unwrap();
    assert_eq!(s.reconnect_ivl(), 60000);

    s.set_reconnect_ivl(1000).unwrap();
    assert_eq!(s.reconnect_ivl(), 1000);
}

#[test]
fn test_pub_sub_bounce() {
    let ctx = TestContext::new();
    let pub_s = ctx.socket(SocketType::Pub);
    let sub_s = ctx.socket(SocketType::Sub);

    sub_s.subscribe(b"").unwrap();
    sub_s.set_reconnect_ivl(60000).unwrap();

    let ep = ctx.bind_inproc(&pub_s, "reconnect-opts");
    ctx.connect_inproc(&sub_s, "reconnect-opts");

    msleep(300);

    s_send_seq(&pub_s, &[Some("hello"), None]);
    s_recv_seq(&sub_s, RecvFlags::NONE, &[Some("hello"), None]);
}
