//! 1:1 translation of C++ `tests/test_pair_vsock.cpp`.
//! VSOCK transport not supported.
mod common;

#[test]
#[ignore = "VSOCK transport not implemented"]
fn test_pair_vsock() {}
