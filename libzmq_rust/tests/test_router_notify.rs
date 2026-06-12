mod common;

/// All tests in test_router_notify.cpp require ZMQ_ROUTER_NOTIFY socket option,
/// which is not yet implemented in the Rust port (no set_router_notify API).

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_sockopt_router_notify() {
    // Requires zmq_setsockopt/zmq_getsockopt for ZMQ_ROUTER_NOTIFY.
    // Not yet implemented in ZSocket API.
}

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_router_notify_connect() {
    // Requires zmq_setsockopt ZMQ_ROUTER_NOTIFY = ZMQ_NOTIFY_CONNECT.
}

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_router_notify_disconnect() {
    // Requires zmq_setsockopt ZMQ_ROUTER_NOTIFY = ZMQ_NOTIFY_DISCONNECT.
}

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_router_notify_both() {
    // Requires ZMQ_NOTIFY_CONNECT | ZMQ_NOTIFY_DISCONNECT.
}

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_handshake_fail() {
    // Requires ZMQ_ROUTER_NOTIFY and ZMQ_STREAM socket.
}

#[test]
#[ignore = "ZMQ_ROUTER_NOTIFY socket option not implemented"]
fn test_error_during_multipart() {
    // Requires ZMQ_ROUTER_NOTIFY and ZMQ_MAXMSGSIZE.
}
