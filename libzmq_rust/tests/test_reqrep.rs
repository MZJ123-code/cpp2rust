//! 1:1 translation of C++ `tests/test_reqrep_inproc.cpp`.
mod common;
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::message::ZmqMessage;

#[test]
fn test_reqrep_roundtrip() {
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    pipe.write(ZmqMessage::from_slice(b"ping"), false);
    assert!(pipe.flush());
    assert!(pipe.check_read());
    assert_eq!(pipe.read().unwrap().data(), b"ping");
    pipe.write(ZmqMessage::from_slice(b"pong"), false);
    assert!(pipe.flush());
    assert!(pipe.check_read());
    assert_eq!(pipe.read().unwrap().data(), b"pong");
}
