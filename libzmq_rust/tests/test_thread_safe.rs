//! 1:1 translation of C++ `tests/test_thread_safe.cpp`
mod common;

// THREAD_SAFE test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "ZSocket does not implement Clone for thread sharing yet"]
fn test_thread_safe() {
    // C++ tests:
    // 1. Create SERVER socket, bind
    // 2. Create CLIENT socket, connect
    // 3. Spawn 2 threads sharing the CLIENT socket, each sending 15000 messages
    // 4. SERVER receives until both threads signal done
}

#[test]
#[ignore = "ZMQ_THREAD_SAFE sockopt not yet exposed on ZSocket"]
fn test_client_getsockopt_thread_safe() {
    // C++ tests that CLIENT socket has ZMQ_THREAD_SAFE = 1
}

#[test]
#[ignore = "ZMQ_THREAD_SAFE sockopt not yet exposed on ZSocket"]
fn test_server_getsockopt_thread_safe() {
    // C++ tests that SERVER socket has ZMQ_THREAD_SAFE = 1
}
