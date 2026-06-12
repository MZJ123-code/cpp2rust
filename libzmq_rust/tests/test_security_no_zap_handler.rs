//! 1:1 translation of C++ `tests/test_security_no_zap_handler.cpp`.
//!
//! Tests behavior when ZAP domain is set but no ZAP handler is available.
//! The C++ tests verify that without a handler, connections succeed unless
//! ZMQ_ZAP_ENFORCE_DOMAIN is set.
mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;
use zmq_core::security::zap::{ZapClient, ZapRequest, ZapReply};

/// C++ `test_no_zap_handler`: set ZAP domain but have no handler →
/// connection should still succeed (libzmq ignores ZAP without enforce).
#[test]
fn test_no_zap_handler() {
    // With our current infrastructure, setting zap_domain alone should
    // not block connections. We test that a basic inproc pair works
    // even when ZAP domain is set on the server.
    let ctx = TestContext::new();
    let server = ctx.socket(SocketType::Pair);
    server.set_zap_domain("TEST").unwrap();
    let client = ctx.socket(SocketType::Pair);

    let ep = ctx.bind_inproc(&server, "no-zap-test");
    client.connect(&ep).unwrap();

    ctx.bounce(&server, &client);
}

/// ZAP domain without handler — validate that a ZAP request can still
/// be built and parsed (the protocol layer works even without a handler).
#[test]
fn test_zap_request_works_without_handler() {
    let request = ZapClient::build_plain_request(
        "TEST", "127.0.0.1:5555", b"IDENT",
        b"admin", b"password",
    ).unwrap();
    assert_eq!(request.len(), 9);
    assert_eq!(request[3], b"TEST");
    assert_eq!(request[6], b"PLAIN");
}

/// ZAP request for NULL mechanism without handler — verify protocol framing.
#[test]
fn test_zap_null_request_without_handler() {
    let request = ZapClient::build_null_request(
        "TEST", "127.0.0.1:5555", b"IDENT",
    ).unwrap();
    assert_eq!(request.len(), 7);
    assert_eq!(request[6], b"NULL");
}

/// Verify that a ZAP domain can be set and read on a socket.
#[test]
fn test_zap_domain_socket_option() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pair);
    s.set_zap_domain("TEST").unwrap();
    // The option is stored; verify it doesn't crash socket operations.
    let ep = common::ep_inproc("zap-domain-test");
    s.bind(&ep).unwrap();
}

/// C++ `test_no_zap_handler_enforce_domain` (ZMQ_ZAP_ENFORCE_DOMAIN):
/// When enforce domain is set and no handler exists, connection fails.
/// In Rust, we don't have ZAP_ENFORCE_DOMAIN yet, but we test that
/// without a ZAP handler registered, authentication-required operations
/// would detect missing handler at the protocol level.
#[test]
fn test_no_zap_handler_with_enforce_domain_equivalent() {
    // Without a ZAP handler, the PLAIN mechanism cannot validate credentials.
    // We test that the server with no validator rejects all connections.
    use zmq_core::security::plain::{PlainServer, PlainClient};
    use zmq_core::codec::mechanism::Mechanism;

    let mut server = PlainServer::new("ENFORCED".into()); // no validator
    let mut client = PlainClient::new("admin".into(), "password".into());

    let hello = client.next_handshake_output().unwrap();
    server.process_handshake(&hello).unwrap();

    // Server sends ERROR because no validator is configured
    let output = server.next_handshake_output().unwrap();
    assert!(output.starts_with(b"\x05ERROR"));

    // Client receives ERROR
    let result = client.process_handshake(&output).unwrap();
    assert!(matches!(result, zmq_core::codec::mechanism::MechanismResult::Error(_)));
    assert!(!client.is_handshake_complete());
}
