mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_proxy_pub_sub() {
    let ctx = TestContext::new();
    let pub_s = ctx.socket(SocketType::Pub);
    let sub_s = ctx.socket(SocketType::Sub);

    sub_s.subscribe(b"topic").unwrap();
    ctx.bind_inproc(&pub_s, "proxy-pub");
    ctx.connect_inproc(&sub_s, "proxy-pub");

    msleep(200);
    s_send_seq(&pub_s, &[Some("topic_data"), None]);

    s_recv_seq(&sub_s, RecvFlags::NONE, &[Some("topic_data"), None]);
}

#[test]
fn test_proxy_pair_roundtrip() {
    let ctx = TestContext::new();
    let s1 = ctx.socket(SocketType::Pair);
    let s2 = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&s1, "proxy-pair");
    ctx.connect_inproc(&s2, "proxy-pair");

    ctx.bounce(&s1, &s2);
}
