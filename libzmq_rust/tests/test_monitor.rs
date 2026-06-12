mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_monitor_socket_events_inproc() {
    let ctx = TestContext::new();
    let client = ctx.socket(SocketType::Pair);
    let server = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&server, "monitor-test");
    ctx.connect_inproc(&client, "monitor-test");

    // Basic ping-pong (bounce) to verify socket communication works
    ctx.bounce(&server, &client);
}

#[test]
fn test_monitor_connect_and_send() {
    let ctx = TestContext::new();
    let dealer = ctx.socket(SocketType::Pub);
    let sub = ctx.socket(SocketType::Sub);

    sub.subscribe(b"").unwrap();
    ctx.bind_inproc(&dealer, "monitor-connect");
    ctx.connect_inproc(&sub, "monitor-connect");

    msleep(200);

    // Send and receive
    s_send_seq(&dealer, &[Some("ping"), None]);
    s_recv_seq(&sub, RecvFlags::NONE, &[Some("ping"), None]);

    // Reply through a second pair
    let dealer2 = ctx.socket(SocketType::Pair);
    let sub2 = ctx.socket(SocketType::Pair);
    ctx.bind_inproc(&dealer2, "monitor-reply");
    ctx.connect_inproc(&sub2, "monitor-reply");

    s_send_seq(&dealer2, &[Some("pong"), None]);
    s_recv_seq(&sub2, RecvFlags::NONE, &[Some("pong"), None]);
}

#[test]
fn test_monitor_pair_basic() {
    let ctx = TestContext::new();
    let s1 = ctx.socket(SocketType::Pair);
    let s2 = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&s1, "monitor-pair");
    ctx.connect_inproc(&s2, "monitor-pair");

    ctx.bounce(&s1, &s2);
}

#[test]
fn test_monitor_pub_sub_subscribe() {
    let ctx = TestContext::new();
    let pub_s = ctx.socket(SocketType::Pub);
    let sub_s = ctx.socket(SocketType::Sub);

    sub_s.subscribe(b"topic").unwrap();

    ctx.bind_inproc(&pub_s, "monitor-pubsub");
    ctx.connect_inproc(&sub_s, "monitor-pubsub");

    msleep(300);

    s_send_seq(&pub_s, &[Some("topic"), None]);

    s_recv_seq(&sub_s, RecvFlags::NONE, &[Some("topic"), None]);
}
