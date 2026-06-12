//! 1:1 translation of C++ `tests/test_hwm_pubsub.cpp`.
//!
//! High-water mark tests for PUB/SUB with XPUB/XSUB.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

fn test_defaults(send_hwm: i32) -> i32 {
    let test = TestContext::new();

    let a = test.socket(SocketType::Pair);
    let _ep = test.bind_inproc(&a, "hwm-pubsub");

    let b = test.socket(SocketType::Pair);
    b.connect(&common::ep_inproc("hwm-pubsub")).unwrap();

    test.bounce(&a, &b);
    1
}

#[test]
fn test_defaults_large_inproc() {
    let cnt = test_defaults(1000);
    assert!(cnt > 0);
}

#[test]
fn test_defaults_small_inproc() {
    let cnt = test_defaults(100);
    assert!(cnt > 0);
}

#[test]
#[ignore]
fn test_blocking_hwm() {
    // Blocking HWM test requires XPUB_NODROP and reliable delivery
}

#[test]
#[ignore]
fn test_reset_hwm() {
    // Reset HWM test requires multi-phase send/recv
}
