mod common;

/// TIPC transport is not available in this Rust implementation.
/// Corresponds to C++ `test_reqrep_tipc.cpp` which checks `is_tipc_available()`.
/// In the C++ test, -1 exit code 77 is returned when TIPC is unavailable.
#[test]
#[ignore = "TIPC transport not implemented in this Rust port"]
fn test_tipc_unavailable() {
    // TIPC transport (tipc://) is not implemented in zmq-transport.
    // The C++ test (test_reqrep_tipc.cpp) skips with exit 77 when TIPC is unavailable.
    // This stub preserves the test entry point.
}
