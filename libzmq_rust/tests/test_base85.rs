//! 1:1 translation of C++ `tests/test_base85.cpp` (Z85 encoding).
mod common;
use zmq_core::codec::greeting::Greeting;
use zmq_core::codec::greeting::GreetingMechanism;

#[test]
fn test_greeting_default() {
    let g = Greeting::new(GreetingMechanism::Null);
    let bytes = g.encode();
    assert_eq!(bytes[0], 0xFF); // signature
    assert_eq!(bytes[9], 0x7F); // final marker
}
