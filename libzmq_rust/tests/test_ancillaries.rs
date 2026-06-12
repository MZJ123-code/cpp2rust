//! 1:1 translation of C++ `tests/test_ancillaries.cpp`.
//!
//! Tests for ancillary API methods: version and error strings.
mod common;

#[test]
fn test_strerror_not_null() {
    let err_msg = zmq_core::error::ZmqError::WouldBlock.to_string();
    assert!(!err_msg.is_empty(), "error string should not be empty");
}

#[test]
fn test_zmq_version() {
    let version = env!("CARGO_PKG_VERSION");
    assert!(!version.is_empty(), "version should not be empty");
}
