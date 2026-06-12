mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Connect with empty endpoint should fail.
#[test]
#[ignore]
fn test_connect_empty_string() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let result = s.connect("");
    assert!(result.is_err(), "connect with empty endpoint should fail");
}

/// Connect with invalid endpoint should fail.
#[test]
#[ignore]
fn test_connect_invalid_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let result = s.connect("not-a-valid-endpoint");
    assert!(result.is_err(), "connect with invalid endpoint should fail");
}

/// Connect with garbage bytes should not crash.
#[test]
fn test_connect_garbage_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let garbage = vec![0u8, 0xff, 0xfe, 0x01, 0x00, 0x7f];
    let ep = String::from_utf8_lossy(&garbage);
    let _result = s.connect(&ep);
}

/// Connect with very long endpoint should not crash.
#[test]
#[ignore]
fn test_connect_very_long_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let long_ep = "tcp://".to_string() + &"a".repeat(4096) + ":9999";
    let result = s.connect(&long_ep);
    assert!(result.is_err(), "connect with very long endpoint should fail");
}

/// Connect with malformed TCP address.
#[test]
#[ignore]
fn test_connect_malformed_tcp() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);

    let cases = [
        "tcp://",
        "tcp://:",
        "tcp://:9999",
        "tcp://256.256.256.256:5555",
        "tcp://localhost:-1",
        "tcp://localhost:99999",
        "tcp://[::1]:-1",
        "tcp://[::1]:99999",
    ];
    for ep in &cases {
        let result = s.connect(ep);
        assert!(result.is_err(), "connect with '{}' should fail", ep);
    }
}

/// Connect with unsupported transport.
#[test]
#[ignore]
fn test_connect_unsupported_transport() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);

    let cases = ["pgm://localhost:5555", "epgm://localhost:5555", "norm://localhost:5555"];
    for ep in &cases {
        let result = s.connect(ep);
        assert!(result.is_err(), "connect with '{}' should fail", ep);
    }
}
