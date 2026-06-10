//! 1:1 translation of C++ `tests/test_spec_pushpull.cpp`.
mod common;
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::message::ZmqMessage;

#[test]
fn test_pushpull_round_robin() {
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    for i in 0..3 {
        pipe.write(ZmqMessage::from_slice(format!("msg{}", i).as_bytes()), false);
        pipe.flush();
        assert!(pipe.check_read());
        assert_eq!(pipe.read().unwrap().data(), format!("msg{}", i).as_bytes());
    }
}

#[test]
fn test_pushpull_multipart() {
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    pipe.write(ZmqMessage::from_slice(b"frame1"), true);
    pipe.write(ZmqMessage::from_slice(b"frame2"), false);
    pipe.flush();
    assert!(pipe.check_read());
    assert_eq!(pipe.read().unwrap().data(), b"frame1");
    assert!(pipe.check_read());
    assert_eq!(pipe.read().unwrap().data(), b"frame2");
}

#[test]
fn test_pushpull_empty() {
    let mut pull = YPipe::<ZmqMessage, 256>::new();
    assert!(!pull.check_read());
    assert!(pull.read().is_none());
}
