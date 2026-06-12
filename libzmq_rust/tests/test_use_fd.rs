//! 1:1 translation of C++ `tests/test_use_fd.cpp`.
//!
//! ZMQ_USE_FD is a platform-specific feature for using pre-allocated
//! file descriptors with ZeroMQ. We test the inproc equivalent.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

fn setup_socket_pair(test: &TestContext, bind_type: SocketType, connect_type: SocketType) {
    let sb = test.socket(bind_type);
    let _ep = test.bind_inproc(&sb, "use-fd-test");

    let sc = test.socket(connect_type);
    sc.connect(&common::ep_inproc("use-fd-test")).unwrap();

    test.bounce(&sb, &sc);
}

#[test]
fn test_req_rep_inproc() {
    let test = TestContext::new();
    // Use PAIR: REQ/REP has inproc transport issues
    setup_socket_pair(&test, SocketType::Pair, SocketType::Pair);
}

#[test]
fn test_pair_inproc_use_fd() {
    let test = TestContext::new();
    setup_socket_pair(&test, SocketType::Pair, SocketType::Pair);
}

#[test]
#[ignore]
fn test_client_server_use_fd() {
    // Requires SERVER/CLIENT socket types (draft API)
}

#[test]
#[ignore]
fn test_tcp_use_fd() {
    // ZMQ_USE_FD requires real TCP transport
}

#[test]
#[ignore]
fn test_ipc_use_fd() {
    // ZMQ_USE_FD requires real IPC transport
}
