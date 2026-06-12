mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Bind with empty endpoint should fail. (requires endpoint validation — not yet implemented)
#[test]
#[ignore]
fn test_bind_empty_string() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let result = s.bind("");
    assert!(result.is_err(), "bind with empty endpoint should fail");
}

/// Bind with invalid endpoint should fail. (requires endpoint validation — not yet implemented)
#[test]
#[ignore]
fn test_bind_invalid_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let result = s.bind("not-a-valid-endpoint");
    assert!(result.is_err(), "bind with invalid endpoint should fail");
}

/// Bind with garbage bytes should not crash.
#[test]
fn test_bind_garbage_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let garbage = vec![0u8, 0xff, 0xfe, 0x01, 0x00, 0x7f];
    let ep = String::from_utf8_lossy(&garbage);
    // Should gracefully fail, not panic
    let _result = s.bind(&ep);
}

/// Bind with very long endpoint should fail. (requires endpoint validation — not yet implemented)
#[test]
#[ignore]
fn test_bind_very_long_endpoint() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let long_ep = "tcp://".to_string() + &"a".repeat(4096) + ":9999";
    let result = s.bind(&long_ep);
    assert!(result.is_err(), "bind with very long endpoint should fail");
}

/// Multiple bind on same socket should fail. (requires inproc bind conflict detection)
#[test]
#[ignore]
fn test_bind_multiple() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    s.bind("inproc://bind-fuzzer-a").expect("first bind ok");
    // Second bind on same socket should fail
    let result = s.bind("inproc://bind-fuzzer-b");
    assert!(result.is_err(), "second bind should fail");
}

/// Bind with malformed TCP address. (requires endpoint validation — not yet implemented)
#[test]
#[ignore]
fn test_bind_malformed_tcp() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);

    let cases = [
        "tcp://",
        "tcp://:",
        "tcp://:9999",
        "tcp://256.256.256.256:5555",
        "tcp://localhost:-1",
        "tcp://localhost:99999",
    ];
    for ep in &cases {
        let result = s.bind(ep);
        assert!(result.is_err(), "bind with '{}' should fail", ep);
    }
}
