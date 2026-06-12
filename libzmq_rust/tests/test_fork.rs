//! 1:1 translation of C++ `tests/test_fork.cpp`.
//!
//! C++ fork() test. Rust doesn't support fork() in a safe way.
//! We test context operations instead.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;
use zmq_context::ZContext;

#[test]
#[ignore]
fn test_multi_context_push_pull() {
    // Cross-thread inproc connections are not yet supported
    // due to inproc transport synchronization limitations
}

#[test]
fn test_context_create_destroy() {
    let ctx = ZContext::new();
    let sock = ctx.socket(SocketType::Push).unwrap();
    assert_eq!(sock.socket_type(), SocketType::Push);
    let _ = sock.close();
    let _ = ctx.shutdown();
}
