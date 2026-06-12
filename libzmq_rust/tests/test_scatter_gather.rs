//! 1:1 translation of C++ `tests/test_scatter_gather.cpp`
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;

#[test]
#[ignore = "SCATTER/GATHER socket types not yet implemented"]
fn test_scatter_gather_multipart_fails() {
    let ctx = TestContext::new();
    let scatter = ctx.socket(SocketType::Scatter);
    let gather = ctx.socket(SocketType::Gather);

    ctx.bind_inproc(&scatter, "test-scatter-gather");
    ctx.connect_inproc(&gather, "test-scatter-gather");

    // Multipart is not supported on Scatter
    let msg = ZmqMessage::from_slice(b"1");
    let result = scatter.send(msg, SendFlags::SNDMORE);
    assert!(result.is_err(), "expected SNDMORE to fail on Scatter");

    drop(scatter);
    drop(gather);
}

#[test]
#[ignore = "SCATTER/GATHER socket types not yet implemented"]
fn test_scatter_gather() {
    let ctx = TestContext::new();
    let scatter = ctx.socket(SocketType::Scatter);
    let gather = ctx.socket(SocketType::Gather);
    let gather2 = ctx.socket(SocketType::Gather);

    ctx.bind_inproc(&scatter, "test-scatter-gather-2");
    ctx.connect_inproc(&gather, "test-scatter-gather-2");
    ctx.connect_inproc(&gather2, "test-scatter-gather-2");

    send_string_(&scatter, "1", SendFlags::NONE);
    send_string_(&scatter, "2", SendFlags::NONE);

    recv_string_assert(&gather, "1", RecvFlags::NONE);
    recv_string_assert(&gather2, "2", RecvFlags::NONE);

    drop(scatter);
    drop(gather);
    drop(gather2);
}
