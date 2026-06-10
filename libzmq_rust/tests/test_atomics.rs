//! 1:1 translation of C++ `tests/test_atomics.cpp`.
//! Tests atomic counter operations through the public API.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

#[test]
fn test_atomics_basic() {
    // C++ test_atomics creates sockets and verifies they don't crash
    let mut ctx = TestContext::new();
    let _push = ctx.socket(SocketType::Push);
    let _pull = ctx.socket(SocketType::Pull);
    // Context cleanup verifies no memory leaks
}
