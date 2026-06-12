mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_steerable_proxy_pub_sub() {
    let ctx = TestContext::new();
    let pub_s = ctx.socket(SocketType::Pub);
    let sub_s = ctx.socket(SocketType::Sub);

    sub_s.subscribe(b"").unwrap();
    ctx.bind_inproc(&pub_s, "steerable-pub");
    ctx.connect_inproc(&sub_s, "steerable-pub");

    msleep(300);
    s_send_seq(&pub_s, &[Some("test"), None]);
    s_recv_seq(&sub_s, RecvFlags::NONE, &[Some("test"), None]);
}

#[test]
fn test_steerable_proxy_pair() {
    let ctx = TestContext::new();
    let frontend = ctx.socket(SocketType::Pair);
    let backend = ctx.socket(SocketType::Pair);

    ctx.bind_inproc(&frontend, "steerable-frontend");
    ctx.bind_inproc(&backend, "steerable-backend");

    let client = ctx.socket(SocketType::Pair);
    let worker = ctx.socket(SocketType::Pair);

    ctx.connect_inproc(&client, "steerable-frontend");
    ctx.connect_inproc(&worker, "steerable-backend");

    msleep(200);

    // Client -> Frontend -> Backend -> Worker
    s_send_seq(&client, &[Some("Ping"), None]);
    s_recv_seq(&frontend, RecvFlags::NONE, &[Some("Ping"), None]);
    s_send_seq(&backend, &[Some("Ping"), None]);
    s_recv_seq(&worker, RecvFlags::NONE, &[Some("Ping"), None]);

    // Worker -> Backend -> Frontend -> Client
    s_send_seq(&worker, &[Some("Pong"), None]);
    s_recv_seq(&backend, RecvFlags::NONE, &[Some("Pong"), None]);
    s_send_seq(&frontend, &[Some("Pong"), None]);
    s_recv_seq(&client, RecvFlags::NONE, &[Some("Pong"), None]);
}
