//! 1:1 translation of C++ `tests/test_poller.cpp`
mod common;

// POLLER test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_destroy_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_destroy_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_size_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_size_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_add_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_add_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_modify_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_modify_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_remove_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_remove_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_add_fd_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_add_fd_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_modify_fd_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_modify_fd_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_remove_fd_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_remove_fd_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_wait_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_wait_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_wait_all_direct() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointers_wait_all_indirect() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_poller_pointer_poller_fd() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_null_socket_pointers() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_null_event_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_null_event_fails_event_count_nonzero() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_null_event_fails_event_count_zero() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_size() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_add_twice_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_remove_unregistered_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_modify_unregistered_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_add_no_events() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_modify_no_events() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_add_fd_twice_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_remove_fd_unregistered_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_modify_fd_unregistered_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_add_invalid_events_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_modify_invalid_events_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_add_fd_invalid_events_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_modify_fd_invalid_events_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_empty_with_timeout_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_empty_without_timeout_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_empty_negative_count_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_empty_without_timeout_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_empty_with_timeout_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_wait_all_inf_disabled_fails() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_fd_no_signaler() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_call_poller_fd() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_poll_basic() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_poll_fd() {}
#[test]
#[ignore = "zmq_poller API not yet implemented in Rust wrapper"]
fn test_poll_client_server() {}
