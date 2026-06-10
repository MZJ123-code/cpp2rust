//! 1:1 translation of C++ `tests/test_hwm.cpp`.
mod common;
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::message::ZmqMessage;

#[test]
fn test_hwm_single_message_roundtrip() {
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    pipe.write(ZmqMessage::from_slice(b"test"), false);
    pipe.flush();
    assert!(pipe.check_read());
    assert_eq!(pipe.read().unwrap().data(), b"test");
    assert!(!pipe.check_read());
}

#[test]
fn test_hwm_multiple_messages() {
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    for i in 0..5 {
        pipe.write(ZmqMessage::from_slice(format!("msg{}", i).as_bytes()), false);
        pipe.flush();
        assert!(pipe.check_read());
        assert_eq!(pipe.read().unwrap().data(), format!("msg{}", i).as_bytes());
    }
    assert!(!pipe.check_read());
}
