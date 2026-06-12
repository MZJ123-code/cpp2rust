//! 1:1 translation of C++ `tests/test_dgram.cpp`
mod common;

// DGRAM test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "UDP/DGRAM transport not yet implemented in Rust wrapper"]
fn test_connect_fails() {
    // C++ tests that connecting DGRAM to TCP fails with ENOCOMPATPROTO
}

#[test]
#[ignore = "UDP/DGRAM transport not yet implemented in Rust wrapper"]
fn test_roundtrip() {
    // C++ tests DGRAM roundtrip with address-based send/recv
}
