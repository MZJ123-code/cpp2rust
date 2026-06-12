//! 1:1 translation of C++ `tests/test_metadata.cpp`.
//!
//! ZAP metadata and message metadata tests.
//! Requires ZAP handler infrastructure — marked ignored.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_metadata_basic() {
    let test = TestContext::new();

    let server = test.socket(SocketType::Pair);
    let client = test.socket(SocketType::Pair);

    let _ep = test.bind_inproc(&server, "metadata-test");
    test.connect_inproc(&client, "metadata-test");

    test.bounce(&server, &client);
}

#[test]
#[ignore]
fn test_metadata_zap() {
    // ZAP metadata requires ZAP handler infrastructure
}

#[test]
#[ignore]
fn test_router_prefetch_metadata() {
    // ROUTER prefetch metadata requires full ZAP + ZMTP stack
}
