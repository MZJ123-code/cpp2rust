//! 1:1 translation of C++ `tests/test_setsockopt.cpp`.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

#[test]
fn test_default_options() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Push);
    let opts = s.get_options();
    assert_eq!(opts.linger, 30000);
    assert_eq!(opts.sndhwm, 1000);
    assert_eq!(opts.rcvhwm, 1000);
    assert!(!opts.ipv6);
}

#[test]
fn test_subscribe_unsubscribe() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Sub);
    s.subscribe(b"topicA").unwrap();
    s.subscribe(b"topicB").unwrap();
    s.unsubscribe(b"topicA").unwrap();
}
