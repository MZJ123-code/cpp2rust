//! ZMTP protocol constants and version information.

/// ZMTP major version implemented by this library
pub const ZMTP_MAJOR_VERSION: u8 = 3;

/// ZMTP minor version
pub const ZMTP_MINOR_VERSION: u8 = 0;

/// ZMTP greeting signature byte
pub const ZMTP_SIGNATURE: u8 = 0xFF;

/// Greeting total length in bytes
pub const GREETING_LENGTH: usize = 64;

/// Maximum Very Small Message (VSM) size — messages ≤ this size are stored inline
pub const ZMQ_MAX_VSM_SIZE: usize = 30;

/// Total size of `zmq_msg_t` in bytes (for C FFI compatibility)
pub const ZMQ_MSG_T_SIZE: usize = 64;

/// Default High Water Mark for send/receive queues
pub const DEFAULT_HWM: i32 = 1000;

/// Default number of I/O threads
pub const DEFAULT_IO_THREADS: i32 = 1;

/// Default linger period (ms) — time to wait for pending messages on close
pub const DEFAULT_LINGER: i32 = 30000;

/// Settle time for inproc connections (ms)
pub const SETTLE_TIME_MS: u64 = 300;

/// Chunk size for yqueue (number of elements per chunk)
pub const YQUEUE_CHUNK_SIZE: usize = 256;

/// Write queue batch size
pub const WRITE_QUEUE_BATCH_SIZE: usize = 128;

/// Maximum endpoint string length
pub const MAX_ENDPOINT_LENGTH: usize = 256;

/// ZMQ version reported via zmq_version()
pub const ZMQ_VERSION_MAJOR: i32 = 4;
pub const ZMQ_VERSION_MINOR: i32 = 3;
pub const ZMQ_VERSION_PATCH: i32 = 6;
