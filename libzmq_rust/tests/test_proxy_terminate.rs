mod common;
use common::*;
use zmq_core::socket_type::SocketType;
use std::sync::Arc;
use std::thread;

#[test]
fn test_proxy_terminate_basic() {
    let ctx = Arc::new(TestContext::new());

    let ctx_for_server = ctx.clone();
    let server_handle = thread::spawn(move || {
        let server = ctx_for_server.socket(SocketType::Pair);
        ctx_for_server.bind_inproc(&server, "proxy-term-server");
        // Wait for client to connect and send
        msleep(300);
        s_recv_seq(&server, RecvFlags::NONE, &[Some("This is a test"), None]);
        s_recv_seq(&server, RecvFlags::NONE, &[Some("This is a test"), None]);
        s_recv_seq(&server, RecvFlags::NONE, &[Some("This is a test"), None]);
        drop(ctx_for_server);
    });

    let client = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&client, "proxy-term-server");

    msleep(100);
    s_send_seq(&client, &[Some("This is a test"), None]);
    msleep(50);
    s_send_seq(&client, &[Some("This is a test"), None]);
    msleep(50);
    s_send_seq(&client, &[Some("This is a test"), None]);

    server_handle.join().unwrap();
}

#[test]
fn test_proxy_pub_sub_basic() {
    let ctx = Arc::new(TestContext::new());
    let ctx_for_server = ctx.clone();

    let server_handle = thread::spawn(move || {
        let server = ctx_for_server.socket(SocketType::Pair);
        ctx_for_server.bind_inproc(&server, "proxy-term-pubsub");
        msleep(300);
        s_recv_seq(&server, RecvFlags::NONE, &[Some("Hello from proxy"), None]);
        drop(ctx_for_server);
    });

    let client = ctx.socket(SocketType::Pair);
    ctx.connect_inproc(&client, "proxy-term-pubsub");

    msleep(100);
    s_send_seq(&client, &[Some("Hello from proxy"), None]);

    server_handle.join().unwrap();
}
