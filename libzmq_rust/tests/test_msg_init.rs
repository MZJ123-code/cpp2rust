//! 1:1 translation of C++ `tests/test_msg_init.cpp`.
mod common;
use zmq_core::message::ZmqMessage;

#[test]
fn test_msg_init_empty() {
    let msg = ZmqMessage::new();
    assert!(msg.is_empty());
    assert_eq!(msg.size(), 0);
    assert_eq!(msg.frame_count(), 0);
}

#[test]
fn test_msg_init_data() {
    let msg = ZmqMessage::from_slice(b"hello world");
    assert_eq!(msg.size(), 11);
    assert_eq!(msg.frame_count(), 1);
    assert_eq!(&msg.data(), b"hello world");
}

#[test]
fn test_msg_init_size() {
    let msg = ZmqMessage::from_parts(&[b"part1", b"part2", b"part3"]);
    assert_eq!(msg.frame_count(), 3);
    assert_eq!(msg.total_size(), 15);
}

#[test]
fn test_msg_more_flag() {
    let mut msg = ZmqMessage::from_slice(b"part1");
    msg.set_more(true);
    assert!(msg.more());
    msg.set_more(false);
    assert!(!msg.more());
}
