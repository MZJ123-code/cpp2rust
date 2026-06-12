//! 1:1 translation of C++ `tests/test_pubsub_topics_count.cpp`.
mod common;

use common::{get_events, msleep, TestContext};
use zmq_core::socket_type::SocketType;

fn settle_subscriptions(socket: &zmq_context::ZSocket) {
    msleep(300);
    let _ = get_events(socket);
}

fn get_subscription_count(socket: &zmq_context::ZSocket) -> i32 {
    settle_subscriptions(socket);
    socket.topics_count()
}

#[test]
fn test_independent_topic_prefixes() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Pub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock.subscribe(b"topicprefix1").unwrap();
    sub_sock.subscribe(b"topicprefix2").unwrap();
    sub_sock.subscribe(b"topicprefix3").unwrap();

    assert_eq!(get_subscription_count(&sub_sock), 3);

    sub_sock.unsubscribe(b"topicprefix3").unwrap();
    assert_eq!(get_subscription_count(&sub_sock), 2);

    sub_sock.unsubscribe(b"topicprefix1").unwrap();
    sub_sock.unsubscribe(b"topicprefix2").unwrap();
    assert_eq!(get_subscription_count(&sub_sock), 0);
}

#[test]
fn test_nested_topic_prefixes() {
    let ctx = TestContext::new();
    let pub_sock = ctx.socket(SocketType::Pub);
    pub_sock.bind("inproc://soname").unwrap();

    let sub_sock = ctx.socket(SocketType::Sub);
    sub_sock.connect("inproc://soname").unwrap();

    sub_sock.subscribe(b"a").unwrap();
    sub_sock.subscribe(b"ab").unwrap();
    sub_sock.subscribe(b"abc").unwrap();

    assert_eq!(get_subscription_count(&sub_sock), 3);

    sub_sock.subscribe(b"xyz").unwrap();
    sub_sock.subscribe(b"xy").unwrap();
    sub_sock.subscribe(b"x").unwrap();

    assert_eq!(get_subscription_count(&sub_sock), 6);
}
