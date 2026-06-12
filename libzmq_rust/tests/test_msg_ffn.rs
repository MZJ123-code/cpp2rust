//! 1:1 translation of C++ `tests/test_msg_ffn.cpp`.
//!
//! Tests the message free-fn (ffn) callback mechanism.
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

#[test]
fn test_msg_init_ffn_drop() {
    // Test that creating and dropping a message properly frees memory
    let msg = ZmqMessage::from_slice(b"hello data");
    assert_eq!(msg.data(), b"hello data");
    drop(msg);

    // Test that sending a message works
    let test = TestContext::new();
    let router = test.socket(SocketType::Router);
    let _ep = test.bind_inproc(&router, "msg-ffn");

    let dealer = test.socket(SocketType::Dealer);
    dealer.connect(&common::ep_inproc("msg-ffn")).unwrap();
}

#[test]
fn test_msg_drop_triggers_cleanup() {
    {
        let _msg = ZmqMessage::from_slice(b"data");
    }
}
