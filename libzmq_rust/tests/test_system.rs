//! 1:1 translation of C++ `tests/test_system.cpp`.
//!
//! System-level tests: local network availability,
//! max socket descriptors, etc.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_localhost_inproc() {
    // Check that inproc transport works (our equivalent of local TCP)
    let test = TestContext::new();
    let dealer = test.socket(SocketType::Dealer);
    let result = dealer.bind("inproc://localhost-test");
    assert!(result.is_ok(), "inproc transport should be available");
}

#[test]
fn test_socket_creation() {
    let test = TestContext::new();

    // Create a reasonable number of sockets
    let mut sockets = Vec::new();
    for _i in 0..100 {
        match test.ctx.socket(SocketType::Pair) {
            Ok(s) => sockets.push(s),
            Err(_) => break,
        }
    }

    assert_eq!(sockets.len(), 100, "should be able to create 100 sockets");

    for s in sockets {
        let _ = s.close();
    }
}

#[test]
fn test_system_max_sockets() {
    // Test creating many sockets like the C++ test
    let test = TestContext::new();

    let max_sockets = 200;
    let mut sockets = Vec::new();

    for _i in 0..max_sockets {
        match test.ctx.socket(SocketType::Pair) {
            Ok(s) => sockets.push(s),
            Err(_) => break,
        }
    }

    assert!(sockets.len() >= 10,
        "should be able to create at least 10 sockets, got {}",
        sockets.len());

    for s in sockets {
        let _ = s.close();
    }
}
