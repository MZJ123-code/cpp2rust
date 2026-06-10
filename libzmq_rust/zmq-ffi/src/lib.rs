//! # zmq-ffi
//!
//! C API compatibility layer — `#[no_mangle] extern "C"` wrappers
//! that mirror the `zmq.h` public API for drop-in replacement.

#![allow(dead_code)]
#![allow(non_camel_case_types)]

// Stub: will be populated in Phase 6 after zmq-context is complete.
// Maps zmq_ctx_new, zmq_socket, zmq_send, zmq_recv, etc.
