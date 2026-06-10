//! 1:1 translation of C++ tests/test_diffserv.cpp.
mod common;
use common::TestContext;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

#[test]
fn test_basic() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pair);
    assert_eq!(s.socket_type(), SocketType::Pair);
}
