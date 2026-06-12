//! 1:1 translation of C++ `tests/test_busy_poll.cpp`
mod common;

// BUSY_POLL test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "ZMQ_BUSY_POLL option not yet exposed on ZSocket"]
fn test_busy_poll() {
    // C++ tests setting ZMQ_BUSY_POLL on a DEALER socket and binding to TCP
}
