//! 1:1 translation of C++ `tests/test_security_null.cpp`.
mod common;
use zmq_core::codec::mechanism::{Mechanism, SecurityMechanism};
use zmq_core::security::null::NullMechanism;

const READY_COMMAND: &[u8] = b"\x05READY";

#[test]
fn test_null_mechanism_type() {
    let mut mech = NullMechanism::new();
    assert_eq!(mech.mechanism_type(), SecurityMechanism::Null);
}

#[test]
fn test_null_mechanism_not_complete_initially() {
    let mech = NullMechanism::new();
    // Not complete until both sides exchange READY
    assert!(!mech.is_handshake_complete());
}

#[test]
fn test_null_mechanism_first_output_is_ready() {
    let mut mech = NullMechanism::new();
    let output = mech.next_handshake_output().unwrap();
    assert_eq!(&output, READY_COMMAND);
}

#[test]
fn test_null_mechanism_handshake() {
    let mut mech = NullMechanism::new();

    // Get our READY
    let _our_ready = mech.next_handshake_output().unwrap();

    // Process peer's READY
    let result = mech.process_handshake(READY_COMMAND).unwrap();
    match result {
        zmq_core::codec::mechanism::MechanismResult::Success { user_id } => {
            assert_eq!(user_id, None);
        }
        _ => panic!("expected Success after receiving peer READY"),
    }

    assert!(mech.is_handshake_complete());
}

#[test]
fn test_null_mechanism_error_on_bad_data() {
    let mut mech = NullMechanism::new();
    // sending random data should fail
    let result = mech.process_handshake(b"garbage");
    assert!(result.is_err());
}
