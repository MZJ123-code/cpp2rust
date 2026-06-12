//! 1:1 translation of C++ `tests/test_shutdown_stress_tipc.cpp`.
//! Shutdown stress test over TIPC.
mod common;

#[test]
#[ignore = "TIPC transport not implemented"]
fn test_shutdown_stress_tipc() {}
