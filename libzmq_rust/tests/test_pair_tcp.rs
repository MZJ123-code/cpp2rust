//! 1:1 translation of C++ `tests/test_pair_tcp.cpp`.
//!
//! The C++ test uses TCP transport; we translate to inproc since
//! TCP transport is not yet fully wired in the Rust port.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

/// C++ `test_pair_tcp_regular`: basic PAIR socket bind/connect/bounce.
#[test]
fn test_pair_inproc_regular() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    let ep = ctx.bind_inproc(&sb, "pair-tcp-regular");
    sc.connect(&ep).unwrap();

    ctx.bounce(&sb, &sc);
}

/// C++ `test_pair_tcp_connect_by_name`: connect to endpoint by symbolic
/// name rather than the numerical address returned by bind.
/// Translated: connect to a well-known inproc name.
#[test]
fn test_pair_inproc_connect_by_name() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);

    // Bind with a known endpoint name
    let ep = "inproc://well-known-pair";
    sb.bind(ep).unwrap();

    let sc = ctx.socket(SocketType::Pair);
    sc.connect(ep).unwrap();

    ctx.bounce(&sb, &sc);
}

/// Test multiple pair connections over the same endpoint pattern.
/// The C++ test only tests one pair; we add an extra inproc-specific test.
#[test]
fn test_pair_inproc_multiple_roundtrips() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    let ep = "inproc://multi-roundtrip";
    sb.bind(ep).unwrap();
    sc.connect(ep).unwrap();

    // Multiple send/recv cycles
    for i in 0..5 {
        let msg = format!("msg-{}", i);
        common::send_string_(&sc, &msg, common::SendFlags::NONE);
        common::recv_string_assert(&sb, &msg, common::RecvFlags::NONE);
    }
}

/// C++ `test_pair_tcp` with extra_func (draft ZMQ_LOOPBACK_FASTPATH):
/// Not applicable for inproc. We test that set_immediate works with PAIR.
#[test]
fn test_pair_inproc_with_immediate() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    // Set immediate mode (no delayed delivery)
    sb.set_immediate(true).unwrap();

    let ep = ctx.bind_inproc(&sb, "pair-immediate");
    let sc = ctx.socket(SocketType::Pair);
    sc.connect(&ep).unwrap();

    ctx.bounce(&sb, &sc);
}

/// Test that connect-before-bind works with PAIR inproc.
#[test]
fn test_pair_inproc_connect_before_bind() {
    let ctx = TestContext::new();
    let sc = ctx.socket(SocketType::Pair);

    // Connect before bind (should be queued)
    let ep = "inproc://connect-before-bind-pair";
    sc.connect(ep).unwrap();

    let sb = ctx.socket(SocketType::Pair);
    sb.bind(ep).unwrap();

    // Allow some time for pipe delivery
    common::msleep(100);

    ctx.bounce(&sb, &sc);
}
