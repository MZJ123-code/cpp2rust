//! Smoke tests — verify the public API works end-to-end.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

#[test]
fn test_context_create_and_shutdown() {
    let ctx = TestContext::new();
    // Context creates and shuts down cleanly via Drop
}

#[test]
fn test_all_socket_types_create() {
    let ctx = TestContext::new();
    let types = [
        SocketType::Pair, SocketType::Pub, SocketType::Sub,
        SocketType::Req, SocketType::Rep, SocketType::Dealer, SocketType::Router,
        SocketType::Pull, SocketType::Push, SocketType::Xpub, SocketType::Xsub,
    ];
    for t in &types {
        let s = ctx.socket(*t);
        assert_eq!(s.socket_type(), *t);
    }
}

#[test]
fn test_connect_and_bind() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Push);
    assert!(s.connect("tcp://localhost:5555").is_ok());
    assert!(s.bind("tcp://127.0.0.1:5556").is_ok());
}

#[test]
fn test_options() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Push);
    let opts = s.get_options();
    assert_eq!(opts.linger, 30000);
    assert_eq!(opts.sndhwm, 1000);
    assert_eq!(opts.rcvhwm, 1000);
}

#[test]
fn test_subscribe_unsubscribe() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Sub);
    s.subscribe(b"topic").unwrap();
    s.unsubscribe(b"topic").unwrap();
}
