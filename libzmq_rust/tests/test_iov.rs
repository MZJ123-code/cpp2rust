//! 1:1 translation of C++ `tests/test_iov.cpp`.
//! Tests zmq_sendiov/zmq_recviov I/O vector API (FFI layer, not implemented).
mod common;

#[test]
#[ignore = "zmq_sendiov/zmq_recviov not implemented in Rust wrapper"]
fn test_iov() {}
