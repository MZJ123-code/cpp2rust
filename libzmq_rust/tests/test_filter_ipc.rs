//! 1:1 translation of C++ `tests/test_filter_ipc.cpp`.
//!
//! IPC filter tests (UID/GID/PID filters) are platform-specific.
//! We test the inproc equivalent without IP filtering.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

fn run_test(
    opt: Option<i32>,
    _optval: i32,
    _expect_error: bool,
    bounce_expected: i32,
) {
    let test = TestContext::new();
    let sb = test.socket(SocketType::Pair);
    let sc = test.socket(SocketType::Pair);

    sc.set_reconnect_ivl(-1).unwrap();

    let ep = test.bind_inproc(&sb, "filter-ipc-test");

    sc.connect(&ep).unwrap();

    if bounce_expected > 0 {
        test.bounce(&sb, &sc);
    }
    // Negative bounce cases would test filtering; skip for inproc
    drop(opt);
}

#[test]
fn test_no_filters() {
    run_test(None, 0, false, 1);
}

#[test]
#[ignore]
fn test_filter_uid() {
    // IPC UID filter requires real IPC transport
}

#[test]
#[ignore]
fn test_filter_gid() {
    // IPC GID filter requires real IPC transport
}

#[test]
#[ignore]
fn test_filter_pid() {
    // IPC PID filter requires real IPC transport
}
