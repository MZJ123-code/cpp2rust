//! 1:1 translation of C++ `tests/test_connect_resolve.cpp`.
mod common;
use zmq_transport::endpoint::Endpoint;

#[test]
fn test_resolve_tcp_localhost() {
    let ep: Endpoint = "tcp://127.0.0.1:5555".parse().unwrap();
    assert_eq!(ep.to_string(), "tcp://127.0.0.1:5555");
}

#[test]
fn test_resolve_inproc() {
    let ep: Endpoint = "inproc://test".parse().unwrap();
    assert!(matches!(ep, Endpoint::Inproc { .. }));
    assert_eq!(ep.to_string(), "inproc://test");
}

#[test]
fn test_resolve_ipc() {
    let ep: Endpoint = "ipc:///tmp/test".parse().unwrap();
    assert!(matches!(ep, Endpoint::Ipc { .. }));
    assert_eq!(ep.to_string(), "ipc:///tmp/test");
}

#[test]
fn test_resolve_wildcard() {
    let ep: Endpoint = "tcp://*:5555".parse().unwrap();
    assert!(matches!(ep, Endpoint::Tcp { is_wildcard: true, .. }));
}

#[test]
fn test_resolve_invalid() {
    assert!("invalid://foo".parse::<Endpoint>().is_err());
}
