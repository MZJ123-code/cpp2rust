//! 1:1 translation of C++ `tests/test_security_plain.cpp`.
//!
//! Tests PLAIN security mechanism at the protocol level, since full socket-level
//! security handshake integration is not yet wired. Matches the C++ test's
//! authentication scenarios using PlainClient/PlainServer + StaticCredentialValidator.
mod common;
use zmq_core::codec::mechanism::{Mechanism, MechanismResult, SecurityMechanism};
use zmq_core::security::plain::{
    PlainClient, PlainServer, StaticCredentialValidator, ValidationResult,
};

const USERNAME: &str = "admin";
const PASSWORD: &str = "password";

fn make_validator() -> StaticCredentialValidator {
    StaticCredentialValidator::new(vec![
        (USERNAME.to_string(), PASSWORD.to_string()),
    ])
}

/// C++ `test_plain_success`: correct username/password → full handshake succeeds.
#[test]
fn test_plain_success() {
    let mut server = PlainServer::with_validator("test".into(), Box::new(make_validator()));
    let mut client = PlainClient::new(USERNAME.into(), PASSWORD.into());

    // Client → Server: HELLO
    let hello = client.next_handshake_output().unwrap();
    assert!(hello.starts_with(b"\x05HELLO"));
    let s_result = server.process_handshake(&hello).unwrap();
    assert!(matches!(s_result, MechanismResult::NeedMore(_)));

    // Server → Client: WELCOME
    let welcome = server.next_handshake_output().unwrap();
    assert_eq!(&welcome, b"\x07WELCOME");
    let c_result = client.process_handshake(&welcome).unwrap();
    assert!(matches!(c_result, MechanismResult::NeedMore(_)));

    // Client → Server: INITIATE
    let initiate = client.next_handshake_output().unwrap();
    assert!(initiate.starts_with(b"\x08INITIATE"));
    let s_result = server.process_handshake(&initiate).unwrap();
    assert!(matches!(s_result, MechanismResult::NeedMore(_)));

    // Server → Client: READY
    let ready = server.next_handshake_output().unwrap();
    assert!(ready.starts_with(b"\x05READY"));
    let c_result = client.process_handshake(&ready).unwrap();
    assert!(matches!(c_result, MechanismResult::Success { user_id: None }));

    assert!(client.is_handshake_complete());
    assert!(server.is_handshake_complete());
    assert_eq!(server.user_id(), Some(USERNAME));
}

/// C++ `test_plain_wrong_credentials_fails`: wrong credentials → server sends ERROR.
#[test]
fn test_plain_wrong_credentials_fails() {
    let mut server = PlainServer::with_validator("test".into(), Box::new(make_validator()));
    let mut client = PlainClient::new("wronguser".into(), "wrongpass".into());

    // Client sends HELLO
    let hello = client.next_handshake_output().unwrap();
    server.process_handshake(&hello).unwrap();

    // Server sends ERROR (auth failure)
    let error = server.next_handshake_output().unwrap();
    assert!(error.starts_with(b"\x05ERROR"));

    // Client receives ERROR
    let c_result = client.process_handshake(&error).unwrap();
    assert!(matches!(c_result, MechanismResult::Error(_)));
    assert!(!client.is_handshake_complete());
    assert!(!server.is_handshake_complete());
}

/// C++ `test_plain_client_as_server_fails`: client misconfigured as PLAIN server.
/// At the protocol level this means a PlainServer on the client side sends
/// WELCOME instead of HELLO, confusing the server.
#[test]
fn test_plain_client_as_server_fails() {
    // If the "client" side acts like a plain server (sends WELCOME-like behavior),
    // the real server waiting for HELLO should reject it.
    let mut server = PlainServer::with_validator("test".into(), Box::new(make_validator()));

    // Send garbage/WELCOME prefix instead of HELLO
    let result = server.process_handshake(b"\x07WELCOME");
    assert!(result.is_err());
}

/// After a failed handshake, no further messages should be exchanged.
#[test]
fn test_plain_no_messages_after_auth_failure() {
    let mut server = PlainServer::with_validator("test".into(), Box::new(make_validator()));
    let mut client = PlainClient::new("bad".into(), "creds".into());

    let hello = client.next_handshake_output().unwrap();
    server.process_handshake(&hello).unwrap();

    let error = server.next_handshake_output().unwrap();
    client.process_handshake(&error).unwrap();

    // Neither side should produce more output
    assert!(server.next_handshake_output().is_none());
    assert!(client.next_handshake_output().is_none());
}

/// Vanilla (no mechanism) connections should not be processed by PLAIN server.
/// At the protocol level, this means the server rejects data that is not a valid HELLO.
#[test]
fn test_plain_vanilla_socket_rejected() {
    let mut server = PlainServer::new("test".into());
    // Send raw ZMTP greeting-like data instead of HELLO
    let result = server.process_handshake(b"\x01\x00");
    assert!(result.is_err());
}
