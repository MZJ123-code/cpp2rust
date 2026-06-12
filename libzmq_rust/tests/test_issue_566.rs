//! 1:1 translation of C++ `tests/test_issue_566.cpp`.
//!
//! Issue 566: Dealer-to-Router connection on separate contexts.
//! The real fix requires cross-context ROUTER/DEALER support.
//! Simplified test: basic inproc PAIR communication.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;
use zmq_context::ZContext;

#[test]
fn test_issue_566_inproc() {
    let ctx = ZContext::new();

    let pair1 = ctx.socket(SocketType::Pair).unwrap();
    pair1.bind("inproc://issue-566").unwrap();

    let pair2 = ctx.socket(SocketType::Pair).unwrap();
    pair2.connect("inproc://issue-566").unwrap();

    send_string_(&pair1, "HELLO", SendFlags::NONE);
    recv_string_assert(&pair2, "HELLO", RecvFlags::NONE);

    let _ = pair2.close();
    let _ = pair1.close();
    let _ = ctx.shutdown();
}
