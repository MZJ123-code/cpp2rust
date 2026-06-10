//! ZAP (ZeroMQ Authentication Protocol) handler.
//!
//! ZAP is the standard way to authenticate PLAIN and CURVE connections
//! in ZeroMQ. It uses multi-part messages over an inproc pipe.
//!
//! # Protocol
//!
//! ZAP request (multi-part message from mechanism to ZAP handler):
//! ```text
//! Frame 0: empty delimiter frame
//! Frame 1: "1.0" (version)
//! Frame 2: "1" (request sequence)
//! Frame 3: domain (e.g., "global")
//! Frame 4: address (peer IP:port)
//! Frame 5: identity (peer routing ID)
//! Frame 6: mechanism name ("NULL", "PLAIN", "CURVE")
//! Frame 7+: credentials (depends on mechanism)
//! ```
//!
//! ZAP reply (multi-part message back from ZAP handler to mechanism):
//! ```text
//! Frame 0: empty delimiter frame
//! Frame 1: "1.0" (version)
//! Frame 2: "1" (request sequence)
//! Frame 3: status code ("200", "300", "400", or "500")
//! Frame 4: status text (human-readable)
//! Frame 5: user ID (empty on failure)
//! Frame 6: metadata (ZAP properties)
//! ```
//!
//! # Status codes
//!
//! - `200` — Success
//! - `300` — Temporary failure (client should retry, silently disconnect)
//! - `400` — Authentication failure (invalid credentials)
//! - `500` — Internal error (configuration problem)
//!
//! References:
//! - C++ `zap_client.cpp` / `zap_client.hpp`
//! - ZAP specification (RFC 27)

use crate::error::{ZmqError, ZmqResult};

// ============================================================================
// ZAP protocol constants
// ============================================================================

/// ZAP protocol version.
const ZAP_VERSION: &str = "1.0";
/// ZAP request ID (always "1").
const ZAP_REQUEST_ID: &str = "1";
/// Minimum number of frames in a ZAP request (delimiter + version + seq + domain + addr + id + mechanism).
const ZAP_MIN_REQUEST_FRAMES: usize = 7;
/// Exact number of frames in a ZAP reply.
const ZAP_REPLY_FRAME_COUNT: usize = 7;

// ============================================================================
// Status codes
// ============================================================================

/// ZAP reply status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZapStatusCode {
    /// 200: Authentication successful.
    Success = 200,
    /// 300: Temporary failure (silent disconnect, per CURVEZMQ RFC).
    TemporaryFailure = 300,
    /// 400: Authentication failed (invalid credentials).
    AuthenticationFailure = 400,
    /// 500: Internal error (ZAP handler misconfiguration).
    InternalError = 500,
}

impl ZapStatusCode {
    /// Get the three-digit string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            ZapStatusCode::Success => "200",
            ZapStatusCode::TemporaryFailure => "300",
            ZapStatusCode::AuthenticationFailure => "400",
            ZapStatusCode::InternalError => "500",
        }
    }

    /// Parse from a three-digit status code string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "200" => Some(ZapStatusCode::Success),
            "300" => Some(ZapStatusCode::TemporaryFailure),
            "400" => Some(ZapStatusCode::AuthenticationFailure),
            "500" => Some(ZapStatusCode::InternalError),
            _ => None,
        }
    }

    /// Whether this status indicates success.
    pub fn is_success(&self) -> bool {
        matches!(self, ZapStatusCode::Success)
    }

    /// Whether the client should be silently disconnected (300).
    pub fn is_silent_disconnect(&self) -> bool {
        matches!(self, ZapStatusCode::TemporaryFailure)
    }
}

// ============================================================================
// Type-safe ZAP structures
// ============================================================================

/// A parsed ZAP request from the mechanism.
#[derive(Debug, Clone)]
pub struct ZapRequest {
    /// Protocol version (should be "1.0").
    pub version: String,
    /// Request sequence ID (typically "1").
    pub sequence: String,
    /// ZAP domain for filtering.
    pub domain: String,
    /// Peer address (e.g., "127.0.0.1:5555").
    pub address: String,
    /// Peer routing identity.
    pub identity: Vec<u8>,
    /// Mechanism name ("NULL", "PLAIN", "CURVE").
    pub mechanism: String,
    /// Mechanism-specific credentials.
    /// - PLAIN: [username, password]
    /// - CURVE: [client_key, ...]
    /// - NULL: []
    pub credentials: Vec<Vec<u8>>,
}

/// A ZAP reply to send back to the mechanism.
#[derive(Debug, Clone)]
pub struct ZapReply {
    /// Protocol version ("1.0").
    pub version: String,
    /// Matching request sequence ID.
    pub sequence: String,
    /// Status code.
    pub status_code: ZapStatusCode,
    /// Human-readable status text.
    pub status_text: String,
    /// Authenticated user ID (empty string on failure).
    pub user_id: String,
    /// ZAP metadata properties (can be empty).
    pub metadata: Vec<u8>,
}

impl ZapReply {
    /// Whether this reply indicates success.
    pub fn is_success(&self) -> bool {
        self.status_code.is_success()
    }
}

// ============================================================================
// ZAP Client — builds requests, parses replies
// ============================================================================

/// ZAP protocol client — builds and parses ZAP messages.
///
/// This is a Sans-I/O handler: it takes frames in, produces frames out.
/// The actual I/O (sending over inproc) is done by the context/transport layer.
pub struct ZapClient;

impl ZapClient {
    /// Create a new ZAP client.
    pub fn new() -> Self {
        Self
    }

    // ==================== Request construction ====================

    /// Build a ZAP request as a sequence of frame payloads.
    ///
    /// Use this to construct the multi-part message to send over the
    /// ZAP inproc pipe.
    pub fn build_request(
        mechanism: &str,
        domain: &str,
        address: &str,
        identity: &[u8],
        credentials: &[Vec<u8>],
    ) -> ZmqResult<Vec<Vec<u8>>> {
        let total_frames = ZAP_MIN_REQUEST_FRAMES + credentials.len();
        let mut frames = Vec::with_capacity(total_frames);

        // Frame 0: delimiter (empty frame)
        frames.push(Vec::new());
        // Frame 1: version
        frames.push(ZAP_VERSION.as_bytes().to_vec());
        // Frame 2: sequence (request ID)
        frames.push(ZAP_REQUEST_ID.as_bytes().to_vec());
        // Frame 3: domain
        frames.push(domain.as_bytes().to_vec());
        // Frame 4: address
        frames.push(address.as_bytes().to_vec());
        // Frame 5: identity (routing ID)
        frames.push(identity.to_vec());
        // Frame 6: mechanism name
        frames.push(mechanism.as_bytes().to_vec());
        // Frames 7+: credentials
        for cred in credentials {
            frames.push(cred.clone());
        }

        Ok(frames)
    }

    /// Build a ZAP request for the NULL mechanism (no credentials).
    pub fn build_null_request(domain: &str, address: &str, identity: &[u8]) -> ZmqResult<Vec<Vec<u8>>> {
        Self::build_request("NULL", domain, address, identity, &[])
    }

    /// Build a ZAP request for the PLAIN mechanism.
    pub fn build_plain_request(
        domain: &str,
        address: &str,
        identity: &[u8],
        username: &[u8],
        password: &[u8],
    ) -> ZmqResult<Vec<Vec<u8>>> {
        Self::build_request("PLAIN", domain, address, identity, &[username.to_vec(), password.to_vec()])
    }

    // ==================== Request parsing ====================

    /// Parse a ZAP request from raw frame data.
    pub fn parse_request(frames: &[Vec<u8>]) -> ZmqResult<ZapRequest> {
        if frames.len() < ZAP_MIN_REQUEST_FRAMES {
            return Err(ZmqError::Protocol(format!(
                "ZAP request: expected at least {} frames, got {}",
                ZAP_MIN_REQUEST_FRAMES,
                frames.len()
            )));
        }

        // Frame 0: delimiter (must be empty)
        if !frames[0].is_empty() {
            return Err(ZmqError::Protocol(
                "ZAP request: frame 0 must be empty delimiter".into(),
            ));
        }

        // Frame 1: version
        let version = String::from_utf8_lossy(&frames[1]).to_string();
        if version != ZAP_VERSION {
            return Err(ZmqError::Protocol(format!(
                "ZAP request: unsupported version '{}', expected '{}'",
                version, ZAP_VERSION
            )));
        }

        // Frame 2: sequence
        let sequence = String::from_utf8_lossy(&frames[2]).to_string();

        // Frame 3: domain
        let domain = String::from_utf8_lossy(&frames[3]).to_string();

        // Frame 4: address
        let address = String::from_utf8_lossy(&frames[4]).to_string();

        // Frame 5: identity
        let identity = frames[5].clone();

        // Frame 6: mechanism
        let mechanism = String::from_utf8_lossy(&frames[6]).to_string();

        // Frames 7+: credentials
        let credentials: Vec<Vec<u8>> = frames[7..].to_vec();

        Ok(ZapRequest {
            version,
            sequence,
            domain,
            address,
            identity,
            mechanism,
            credentials,
        })
    }

    // ==================== Reply construction ====================

    /// Build a ZAP reply from a request and response fields.
    pub fn build_reply(
        request: &ZapRequest,
        status_code: ZapStatusCode,
        status_text: &str,
        user_id: &str,
        metadata: &[u8],
    ) -> ZapReply {
        ZapReply {
            version: ZAP_VERSION.to_string(),
            sequence: request.sequence.clone(),
            status_code,
            status_text: status_text.to_string(),
            user_id: user_id.to_string(),
            metadata: metadata.to_vec(),
        }
    }

    /// Build a success (200) reply.
    pub fn success_reply(request: &ZapRequest, user_id: &str) -> ZapReply {
        Self::build_reply(request, ZapStatusCode::Success, "OK", user_id, &[])
    }

    /// Build a temporary failure (300) reply.
    pub fn temporary_failure_reply(request: &ZapRequest, reason: &str) -> ZapReply {
        Self::build_reply(request, ZapStatusCode::TemporaryFailure, reason, "", &[])
    }

    /// Build an authentication failure (400) reply.
    pub fn auth_failure_reply(request: &ZapRequest, reason: &str) -> ZapReply {
        Self::build_reply(request, ZapStatusCode::AuthenticationFailure, reason, "", &[])
    }

    /// Build an internal error (500) reply.
    pub fn internal_error_reply(request: &ZapRequest, reason: &str) -> ZapReply {
        Self::build_reply(request, ZapStatusCode::InternalError, reason, "", &[])
    }

    // ==================== Reply serialization ====================

    /// Serialize a `ZapReply` to wire-format frames.
    pub fn serialize_reply(reply: &ZapReply) -> Vec<Vec<u8>> {
        vec![
            Vec::new(),                                 // Frame 0: delimiter
            reply.version.as_bytes().to_vec(),           // Frame 1: version
            reply.sequence.as_bytes().to_vec(),          // Frame 2: sequence
            reply.status_code.as_str().as_bytes().to_vec(), // Frame 3: status
            reply.status_text.as_bytes().to_vec(),       // Frame 4: text
            reply.user_id.as_bytes().to_vec(),           // Frame 5: user ID
            reply.metadata.clone(),                      // Frame 6: metadata
        ]
    }

    // ==================== Reply parsing ====================

    /// Parse a ZAP reply from raw frame data.
    ///
    /// Validates all fields per the ZAP specification (RFC 27).
    pub fn parse_reply(frames: &[Vec<u8>]) -> ZmqResult<ZapReply> {
        if frames.len() != ZAP_REPLY_FRAME_COUNT {
            return Err(ZmqError::Protocol(format!(
                "ZAP reply: expected {} frames, got {}",
                ZAP_REPLY_FRAME_COUNT,
                frames.len()
            )));
        }

        // Frame 0: delimiter (must be empty)
        if !frames[0].is_empty() {
            return Err(ZmqError::Protocol(
                "ZAP reply: frame 0 must be empty delimiter".into(),
            ));
        }

        // Frame 1: version
        let version = String::from_utf8_lossy(&frames[1]);
        if version != ZAP_VERSION {
            return Err(ZmqError::Protocol(format!(
                "ZAP reply: bad version '{}', expected '{}'",
                version, ZAP_VERSION
            )));
        }

        // Frame 2: sequence (must match request ID "1")
        let sequence = String::from_utf8_lossy(&frames[2]);
        if sequence != ZAP_REQUEST_ID {
            return Err(ZmqError::Protocol(format!(
                "ZAP reply: bad request ID '{}', expected '{}'",
                sequence, ZAP_REQUEST_ID
            )));
        }

        // Frame 3: status code — must be 3 chars: [2345]00
        let status_str = String::from_utf8_lossy(&frames[3]);
        let status_code = validate_and_parse_status_code(&status_str)?;

        // Frame 4: status text
        let status_text = String::from_utf8_lossy(&frames[4]).to_string();

        // Frame 5: user ID
        let user_id = String::from_utf8_lossy(&frames[5]).to_string();

        // Frame 6: metadata
        let metadata = frames[6].clone();

        Ok(ZapReply {
            version: version.to_string(),
            sequence: sequence.to_string(),
            status_code,
            status_text,
            user_id,
            metadata,
        })
    }
}

impl Default for ZapClient {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ZAP Handler trait — for implementing server-side ZAP validation
// ============================================================================

/// A ZAP request handler — implements server-side credential validation.
///
/// Applications implement this trait to provide custom authentication logic.
/// The handler receives parsed `ZapRequest`s and returns `ZapReply`s.
pub trait ZapHandler: Send + Sync {
    /// Handle a ZAP request and produce a reply.
    fn handle_request(&self, request: &ZapRequest) -> ZapReply;
}

/// A simple ZAP handler that validates PLAIN credentials against a static list.
///
/// This is useful for testing and simple deployments. For production,
/// implement `ZapHandler` yourself (e.g., querying a database).
pub struct StaticZapHandler {
    domain: String,
    /// List of allowed (username, password) pairs.
    allowed_credentials: Vec<(String, String)>,
}

impl StaticZapHandler {
    /// Create a new static ZAP handler.
    ///
    /// # Arguments
    /// - `domain`: only requests matching this domain are processed.
    /// - `allowed_credentials`: list of valid (username, password) pairs.
    pub fn new(domain: String, allowed_credentials: Vec<(String, String)>) -> Self {
        Self {
            domain,
            allowed_credentials,
        }
    }

    /// Get the domain this handler is responsible for.
    pub fn domain(&self) -> &str {
        &self.domain
    }
}

impl ZapHandler for StaticZapHandler {
    fn handle_request(&self, request: &ZapRequest) -> ZapReply {
        // Check domain match
        if request.domain != self.domain {
            return ZapClient::auth_failure_reply(
                request,
                &format!("Domain mismatch: expected '{}', got '{}'", self.domain, request.domain),
            );
        }

        // Route by mechanism
        match request.mechanism.as_str() {
            "PLAIN" => self.handle_plain(request),
            "NULL" => {
                // NULL has no credentials to validate — always succeed
                ZapClient::success_reply(request, "")
            }
            mechanism => ZapClient::internal_error_reply(
                request,
                &format!("Unsupported mechanism: {}", mechanism),
            ),
        }
    }
}

impl StaticZapHandler {
    /// Handle PLAIN-specific credential validation.
    fn handle_plain(&self, request: &ZapRequest) -> ZapReply {
        if request.credentials.len() < 2 {
            return ZapClient::auth_failure_reply(
                request,
                "PLAIN mechanism requires at least 2 credentials (username and password)",
            );
        }

        let username = String::from_utf8_lossy(&request.credentials[0]);
        let password = String::from_utf8_lossy(&request.credentials[1]);

        for (allowed_user, allowed_pass) in &self.allowed_credentials {
            if username.as_ref() == allowed_user.as_str()
                && password.as_ref() == allowed_pass.as_str()
            {
                return ZapClient::success_reply(request, &username);
            }
        }

        ZapClient::auth_failure_reply(request, "Invalid username or password")
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Validate and parse a ZAP status code string.
///
/// Per the ZAP spec, valid codes are 3 characters: [2345]00.
fn validate_and_parse_status_code(s: &str) -> ZmqResult<ZapStatusCode> {
    if s.len() != 3 {
        return Err(ZmqError::Protocol(format!(
            "ZAP status code must be 3 characters, got '{}' (len={})",
            s,
            s.len()
        )));
    }

    let bytes = s.as_bytes();
    // First digit must be '2', '3', '4', or '5'
    if bytes[0] < b'2' || bytes[0] > b'5' {
        return Err(ZmqError::Protocol(format!(
            "ZAP status code must start with 2-5, got '{}'",
            s
        )));
    }
    // Second and third digits must be '0'
    if bytes[1] != b'0' || bytes[2] != b'0' {
        return Err(ZmqError::Protocol(format!(
            "ZAP status code must end with '00', got '{}'",
            s
        )));
    }

    ZapStatusCode::from_str(s).ok_or_else(|| {
        ZmqError::Protocol(format!("Invalid ZAP status code: '{}'", s))
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Request building tests ====================

    #[test]
    fn test_build_zap_plain_request() {
        let frames = ZapClient::build_request(
            "PLAIN",
            "global",
            "127.0.0.1:5555",
            b"",
            &[b"alice".to_vec(), b"secret".to_vec()],
        )
        .unwrap();

        assert_eq!(frames.len(), 9); // 7 fixed + 2 credentials
        assert!(frames[0].is_empty());         // delimiter
        assert_eq!(frames[1], b"1.0");         // version
        assert_eq!(frames[2], b"1");           // sequence
        assert_eq!(frames[3], b"global");      // domain
        assert_eq!(frames[4], b"127.0.0.1:5555"); // address
        assert!(frames[5].is_empty());         // identity
        assert_eq!(frames[6], b"PLAIN");       // mechanism
        assert_eq!(frames[7], b"alice");       // username
        assert_eq!(frames[8], b"secret");      // password
    }

    #[test]
    fn test_build_zap_null_request() {
        let frames = ZapClient::build_null_request("global", "192.168.1.1:9999", b"\x01\x02").unwrap();

        assert_eq!(frames.len(), 7); // No credentials
        assert!(frames[0].is_empty());
        assert_eq!(frames[1], b"1.0");
        assert_eq!(frames[6], b"NULL");
    }

    #[test]
    fn test_build_zap_request_with_identity() {
        let frames = ZapClient::build_plain_request(
            "mydomain",
            "10.0.0.1:5000",
            b"peer-id-123",
            b"bob",
            b"pass",
        )
        .unwrap();

        assert_eq!(frames[5], b"peer-id-123");
        assert_eq!(frames[7], b"bob");
        assert_eq!(frames[8], b"pass");
    }

    // ==================== Request parsing tests ====================

    #[test]
    fn test_parse_valid_zap_request() {
        let frames = vec![
            vec![],                                    // 0: delimiter
            b"1.0".to_vec(),                           // 1: version
            b"1".to_vec(),                             // 2: sequence
            b"global".to_vec(),                        // 3: domain
            b"127.0.0.1:5555".to_vec(),                // 4: address
            vec![1, 2, 3],                             // 5: identity
            b"PLAIN".to_vec(),                         // 6: mechanism
            b"alice".to_vec(),                         // 7: credential 1
            b"secret".to_vec(),                        // 8: credential 2
        ];

        let req = ZapClient::parse_request(&frames).unwrap();
        assert_eq!(req.version, "1.0");
        assert_eq!(req.sequence, "1");
        assert_eq!(req.domain, "global");
        assert_eq!(req.address, "127.0.0.1:5555");
        assert_eq!(req.identity, vec![1, 2, 3]);
        assert_eq!(req.mechanism, "PLAIN");
        assert_eq!(req.credentials.len(), 2);
        assert_eq!(req.credentials[0], b"alice");
        assert_eq!(req.credentials[1], b"secret");
    }

    #[test]
    fn test_parse_zap_request_null_mechanism() {
        let frames = vec![
            vec![],
            b"1.0".to_vec(),
            b"1".to_vec(),
            b"global".to_vec(),
            b"::1:5555".to_vec(),
            vec![],
            b"NULL".to_vec(),
        ];

        let req = ZapClient::parse_request(&frames).unwrap();
        assert_eq!(req.mechanism, "NULL");
        assert!(req.credentials.is_empty());
    }

    #[test]
    fn test_parse_zap_request_too_few_frames() {
        let frames = vec![vec![], b"1.0".to_vec()];
        let result = ZapClient::parse_request(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zap_request_non_empty_delimiter() {
        let frames = vec![
            vec![42], // should be empty
            b"1.0".to_vec(),
            b"1".to_vec(),
            b"global".to_vec(),
            b"addr".to_vec(),
            vec![],
            b"NULL".to_vec(),
        ];
        let result = ZapClient::parse_request(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zap_request_bad_version() {
        let frames = vec![
            vec![],
            b"2.0".to_vec(), // bad version
            b"1".to_vec(),
            b"global".to_vec(),
            b"addr".to_vec(),
            vec![],
            b"NULL".to_vec(),
        ];
        let result = ZapClient::parse_request(&frames);
        assert!(result.is_err());
    }

    // ==================== Reply serialization tests ====================

    #[test]
    fn test_serialize_success_reply() {
        let request = ZapRequest {
            version: "1.0".into(),
            sequence: "1".into(),
            domain: "global".into(),
            address: "127.0.0.1:5555".into(),
            identity: vec![],
            mechanism: "PLAIN".into(),
            credentials: vec![b"alice".to_vec(), b"secret".to_vec()],
        };

        let reply = ZapClient::success_reply(&request, "alice-user");
        let frames = ZapClient::serialize_reply(&reply);

        assert_eq!(frames.len(), 7);
        assert!(frames[0].is_empty());         // delimiter
        assert_eq!(frames[1], b"1.0");         // version
        assert_eq!(frames[2], b"1");           // sequence
        assert_eq!(frames[3], b"200");         // status
        assert_eq!(frames[4], b"OK");          // text
        assert_eq!(frames[5], b"alice-user");  // user ID
        assert!(frames[6].is_empty());         // metadata
    }

    #[test]
    fn test_serialize_error_replies() {
        let request = create_test_request();

        let auth_fail = ZapClient::auth_failure_reply(&request, "Bad password");
        let frames = ZapClient::serialize_reply(&auth_fail);
        assert_eq!(frames[3], b"400");

        let internal_err = ZapClient::internal_error_reply(&request, "DB down");
        let frames = ZapClient::serialize_reply(&internal_err);
        assert_eq!(frames[3], b"500");

        let temp_fail = ZapClient::temporary_failure_reply(&request, "Overloaded");
        let frames = ZapClient::serialize_reply(&temp_fail);
        assert_eq!(frames[3], b"300");
    }

    // ==================== Reply parsing tests ====================

    #[test]
    fn test_parse_valid_zap_reply_success() {
        let frames = vec![
            vec![],              // delimiter
            b"1.0".to_vec(),     // version
            b"1".to_vec(),       // sequence
            b"200".to_vec(),     // status
            b"OK".to_vec(),      // text
            b"alice".to_vec(),   // user ID
            vec![],              // metadata
        ];

        let reply = ZapClient::parse_reply(&frames).unwrap();
        assert_eq!(reply.status_code, ZapStatusCode::Success);
        assert_eq!(reply.status_text, "OK");
        assert_eq!(reply.user_id, "alice");
        assert!(reply.is_success());
    }

    #[test]
    fn test_parse_valid_zap_reply_auth_failure() {
        let frames = vec![
            vec![],
            b"1.0".to_vec(),
            b"1".to_vec(),
            b"400".to_vec(),
            b"Invalid credentials".to_vec(),
            b"".to_vec(),
            vec![],
        ];

        let reply = ZapClient::parse_reply(&frames).unwrap();
        assert_eq!(reply.status_code, ZapStatusCode::AuthenticationFailure);
        assert!(!reply.is_success());
        assert!(reply.user_id.is_empty());
    }

    #[test]
    fn test_parse_zap_reply_bad_status_code() {
        let frames = vec![
            vec![],
            b"1.0".to_vec(),
            b"1".to_vec(),
            b"999".to_vec(),     // invalid
            b"OK".to_vec(),
            b"".to_vec(),
            vec![],
        ];

        let result = ZapClient::parse_reply(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zap_reply_wrong_frame_count() {
        let frames = vec![vec![], b"1.0".to_vec()];
        let result = ZapClient::parse_reply(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zap_reply_bad_version() {
        let frames = vec![
            vec![],
            b"2.0".to_vec(),     // bad version
            b"1".to_vec(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"".to_vec(),
            vec![],
        ];

        let result = ZapClient::parse_reply(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zap_reply_bad_sequence() {
        let frames = vec![
            vec![],
            b"1.0".to_vec(),
            b"99".to_vec(),      // bad sequence
            b"200".to_vec(),
            b"OK".to_vec(),
            b"".to_vec(),
            vec![],
        ];

        let result = ZapClient::parse_reply(&frames);
        assert!(result.is_err());
    }

    // ==================== Status code validation tests ====================

    #[test]
    fn test_validate_status_code_valid() {
        assert!(validate_and_parse_status_code("200").is_ok());
        assert!(validate_and_parse_status_code("300").is_ok());
        assert!(validate_and_parse_status_code("400").is_ok());
        assert!(validate_and_parse_status_code("500").is_ok());
    }

    #[test]
    fn test_validate_status_code_invalid() {
        assert!(validate_and_parse_status_code("100").is_err()); // 1xx not valid
        assert!(validate_and_parse_status_code("600").is_err()); // 6xx not valid
        assert!(validate_and_parse_status_code("201").is_err()); // not ending in 00
        assert!(validate_and_parse_status_code("20").is_err());  // too short
        assert!(validate_and_parse_status_code("2000").is_err()); // too long
        assert!(validate_and_parse_status_code("").is_err());    // empty
    }

    // ==================== ZapStatusCode tests ====================

    #[test]
    fn test_status_code_roundtrip() {
        assert_eq!(
            ZapStatusCode::from_str("200"),
            Some(ZapStatusCode::Success)
        );
        assert_eq!(
            ZapStatusCode::from_str("300"),
            Some(ZapStatusCode::TemporaryFailure)
        );
        assert_eq!(
            ZapStatusCode::from_str("400"),
            Some(ZapStatusCode::AuthenticationFailure)
        );
        assert_eq!(
            ZapStatusCode::from_str("500"),
            Some(ZapStatusCode::InternalError)
        );
        assert_eq!(ZapStatusCode::from_str("999"), None);
    }

    #[test]
    fn test_status_code_as_str() {
        assert_eq!(ZapStatusCode::Success.as_str(), "200");
        assert_eq!(ZapStatusCode::TemporaryFailure.as_str(), "300");
        assert_eq!(ZapStatusCode::AuthenticationFailure.as_str(), "400");
        assert_eq!(ZapStatusCode::InternalError.as_str(), "500");
    }

    #[test]
    fn test_status_code_is_success() {
        assert!(ZapStatusCode::Success.is_success());
        assert!(!ZapStatusCode::TemporaryFailure.is_success());
        assert!(!ZapStatusCode::AuthenticationFailure.is_success());
        assert!(!ZapStatusCode::InternalError.is_success());
    }

    // ==================== StaticZapHandler tests ====================

    #[test]
    fn test_static_zap_handler_success() {
        let handler = StaticZapHandler::new("global".into(), vec![
            ("alice".into(), "secret".into()),
            ("bob".into(), "pass".into()),
        ]);

        let request = create_plain_request("global", "alice", "secret");
        let reply = handler.handle_request(&request);

        assert!(reply.is_success());
        assert_eq!(reply.user_id, "alice");
    }

    #[test]
    fn test_static_zap_handler_auth_failure() {
        let handler = StaticZapHandler::new("global".into(), vec![
            ("alice".into(), "secret".into()),
        ]);

        let request = create_plain_request("global", "eve", "hacker");
        let reply = handler.handle_request(&request);

        assert!(!reply.is_success());
        assert_eq!(reply.status_code, ZapStatusCode::AuthenticationFailure);
    }

    #[test]
    fn test_static_zap_handler_domain_mismatch() {
        let handler = StaticZapHandler::new("myapp".into(), vec![
            ("alice".into(), "secret".into()),
        ]);

        let request = create_plain_request("other-app", "alice", "secret");
        let reply = handler.handle_request(&request);

        assert!(!reply.is_success());
    }

    #[test]
    fn test_static_zap_handler_null_mechanism() {
        let handler = StaticZapHandler::new("global".into(), vec![]);

        let mut request = create_test_request();
        request.mechanism = "NULL".into();
        request.credentials = vec![];

        let reply = handler.handle_request(&request);
        assert!(reply.is_success());
    }

    #[test]
    fn test_static_zap_handler_unsupported_mechanism() {
        let handler = StaticZapHandler::new("global".into(), vec![]);

        let mut request = create_test_request();
        request.mechanism = "UNKNOWN".into();

        let reply = handler.handle_request(&request);
        assert_eq!(reply.status_code, ZapStatusCode::InternalError);
    }

    #[test]
    fn test_static_zap_handler_missing_credentials() {
        let handler = StaticZapHandler::new("global".into(), vec![
            ("alice".into(), "secret".into()),
        ]);

        let mut request = create_test_request();
        request.mechanism = "PLAIN".into();
        request.credentials = vec![b"alice".to_vec()]; // only 1 credential

        let reply = handler.handle_request(&request);
        assert_eq!(reply.status_code, ZapStatusCode::AuthenticationFailure);
    }

    // ==================== Round-trip tests ====================

    #[test]
    fn test_zap_request_reply_roundtrip() {
        // Build a request
        let request_frames = ZapClient::build_plain_request(
            "global",
            "10.0.0.1:9999",
            b"peer-1",
            b"alice",
            b"secret",
        )
        .unwrap();

        // Parse the request
        let request = ZapClient::parse_request(&request_frames).unwrap();

        // Build a success reply
        let reply = ZapClient::success_reply(&request, "alice");
        let reply_frames = ZapClient::serialize_reply(&reply);

        // Parse the reply
        let parsed_reply = ZapClient::parse_reply(&reply_frames).unwrap();
        assert!(parsed_reply.is_success());
        assert_eq!(parsed_reply.user_id, "alice");
    }

    // ==================== Edge cases ====================

    #[test]
    fn test_zap_request_empty_credentials() {
        let frames = ZapClient::build_null_request("global", "addr", b"").unwrap();
        assert_eq!(frames.len(), 7);
        assert_eq!(frames[6], b"NULL");
    }

    #[test]
    fn test_zap_reply_with_metadata() {
        let request = create_test_request();
        let reply = ZapClient::build_reply(
            &request,
            ZapStatusCode::Success,
            "OK",
            "user1",
            b"key=value",
        );

        let frames = ZapClient::serialize_reply(&reply);
        assert_eq!(frames[6], b"key=value");
    }

    #[test]
    fn test_zap_reply_non_empty_delimiter() {
        let frames = vec![
            vec![42],            // non-empty delimiter
            b"1.0".to_vec(),
            b"1".to_vec(),
            b"200".to_vec(),
            b"OK".to_vec(),
            b"".to_vec(),
            vec![],
        ];

        let result = ZapClient::parse_reply(&frames);
        assert!(result.is_err());
    }

    #[test]
    fn test_zap_request_utf8_in_credentials() {
        let frames = ZapClient::build_request(
            "PLAIN",
            "global",
            "addr",
            b"",
            &["café".as_bytes().to_vec(), "passwörd".as_bytes().to_vec()],
        )
        .unwrap();

        let req = ZapClient::parse_request(&frames).unwrap();
        assert_eq!(req.credentials[0], "café".as_bytes());
        assert_eq!(req.credentials[1], "passwörd".as_bytes());
    }

    // ==================== Test helpers ====================

    fn create_test_request() -> ZapRequest {
        ZapRequest {
            version: "1.0".into(),
            sequence: "1".into(),
            domain: "global".into(),
            address: "127.0.0.1:5555".into(),
            identity: vec![],
            mechanism: "PLAIN".into(),
            credentials: vec![b"alice".to_vec(), b"secret".to_vec()],
        }
    }

    fn create_plain_request(domain: &str, username: &str, password: &str) -> ZapRequest {
        ZapRequest {
            version: "1.0".into(),
            sequence: "1".into(),
            domain: domain.into(),
            address: "127.0.0.1:5555".into(),
            identity: vec![],
            mechanism: "PLAIN".into(),
            credentials: vec![username.as_bytes().to_vec(), password.as_bytes().to_vec()],
        }
    }
}
