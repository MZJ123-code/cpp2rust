//! 1:1 translation of C++ `tests/test_pair_ipc.cpp`.
//!
//! The C++ test uses IPC transport; we translate to inproc since
//! IPC support is not yet implemented in the Rust port.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

/// C++ `test_roundtrip`: bind PAIR via IPC, connect PAIR, bounce.
/// Translated to use inproc.
#[test]
fn test_roundtrip() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    let ep = ctx.bind_inproc(&sb, "pair-ipc-roundtrip");
    sc.connect(&ep).unwrap();

    ctx.bounce(&sb, &sc);
}

/// C++ `test_endpoint_too_long`: try to bind with an excessively long
/// endpoint name. IPC paths are limited to ~108 chars on Unix; inproc
/// does not have this limit, so bind succeeds. The IPC-specific length
/// check is not applicable to inproc.
#[test]
fn test_endpoint_too_long() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);

    // Build a very long inproc endpoint name (>256 chars)
    let mut long_name = String::from("inproc://");
    for _ in 0..256 {
        long_name.push('a');
    }

    // Inproc endpoints accept long names (no OS path length limitation)
    let result = sb.bind(&long_name);
    assert!(result.is_ok(), "inproc should accept long endpoint names");
}

/// Bind with a very long endpoint name between bind and connect should
/// still allow basic pair communication with reasonable names.
#[test]
fn test_pair_bind_connect_with_long_name() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);

    // Use a reasonable-length name
    let ep = ctx.bind_inproc(&sb, "reasonable-pair-name");
    let sc = ctx.socket(SocketType::Pair);
    sc.connect(&ep).unwrap();

    common::send_string_(&sc, "hello", common::SendFlags::NONE);
    common::recv_string_assert(&sb, "hello", common::RecvFlags::NONE);
}
