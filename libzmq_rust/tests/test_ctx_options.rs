//! 1:1 translation of C++ `tests/test_ctx_options.cpp`.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

#[test]
fn test_ctx_create_and_socket() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pair);
    assert_eq!(s.socket_type(), SocketType::Pair);
}

#[test]
fn test_ctx_multiple_sockets() {
    let ctx = TestContext::new();
    let types = [SocketType::Push, SocketType::Pull, SocketType::Pub, SocketType::Sub];
    for t in &types {
        let s = ctx.socket(*t);
        assert_eq!(s.socket_type(), *t);
    }
}
