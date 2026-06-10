//! Binary large object. Replaces C++ `blob.hpp`. Stub — Phase 1.
#[derive(Debug, Clone, Default)]
pub struct Blob(Vec<u8>);
impl Blob { pub fn new(data: Vec<u8>) -> Self { Self(data) } }
