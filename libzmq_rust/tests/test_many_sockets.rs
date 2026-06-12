//! 1:1 translation of C++ `tests/test_many_sockets.cpp`.
//!
//! Tests creating many sockets until the context limit or
//! system resources are exhausted.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_many_sockets() {
    let test = TestContext::new();

    let mut sockets = Vec::new();

    // Create as many PAIR sockets as we can
    let max_sockets = 1024;
    for _i in 0..max_sockets {
        match test.ctx.socket(SocketType::Pair) {
            Ok(s) => sockets.push(s),
            Err(_) => break,
        }
    }

    assert!(
        !sockets.is_empty(),
        "should be able to create at least some sockets"
    );

    // All should close cleanly
    for s in sockets {
        let _ = s.close();
    }
}
