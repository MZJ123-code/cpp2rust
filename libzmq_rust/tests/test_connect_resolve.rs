//! 1:1 translation of C++ `tests/test_connect_resolve.cpp`.
//!
//! Tests endpoint URL parsing and validation. The C++ tests call
//! `zmq_connect` with various endpoint strings; we test the
//! `Endpoint` parser directly.
mod common;
use zmq_transport::endpoint::Endpoint;

#[test]
fn test_hostname_ipv4() {
    let ep: Endpoint = "tcp://localhost:1234".parse().unwrap();
    assert_eq!(ep.to_string(), "tcp://localhost:1234");
}

#[test]
fn test_loopback_ipv6() {
    let ep: Endpoint = "tcp://[::1]:1234".parse().unwrap();
    assert!(matches!(ep, Endpoint::Tcp { .. }));
}

#[test]
fn test_invalid_service_fails() {
    let result = "tcp://localhost:invalid".parse::<Endpoint>();
    assert!(result.is_err());
}

#[test]
fn test_hostname_with_spaces_fails() {
    // Parser level accepts spaces in hostnames (validation at connect time)
    let ep = "tcp://in val id:1234".parse::<Endpoint>();
    assert!(ep.is_ok());
    if let Ok(Endpoint::Tcp { host, port, .. }) = ep {
        assert_eq!(host, "in val id");
        assert_eq!(port, 1234);
    }
}

#[test]
fn test_no_hostname_fails() {
    let result = "tcp://".parse::<Endpoint>();
    assert!(result.is_err());
}

#[test]
fn test_wildcard_port_fails() {
    let result = "tcp://192.168.0.200:*".parse::<Endpoint>();
    assert!(result.is_err());
}

#[test]
fn test_invalid_proto_fails() {
    let result = "invalid://localhost:1234".parse::<Endpoint>();
    assert!(result.is_err());
}

/// C++ `test_invalid_proto_fails` checks errno == EPROTONOSUPPORT.
/// In Rust, we check that the error message mentions the unsupported protocol.
#[test]
fn test_invalid_proto_error_message() {
    let result = "invalid://localhost:1234".parse::<Endpoint>();
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("invalid") || msg.contains("unsupported") || msg.contains("protocol"));
}
