//! PLAIN security mechanism — username/password authentication.
//!
//! The PLAIN mechanism provides simple username/password authentication
//! as defined in the ZMTP/PLAIN specification.
//!
//! # Protocol
//!
//! ```text
//! Client                                    Server
//!   |  \x05HELLO[len_u][username][len_p][pw] → |
//!   |                                         | (validates via ZAP)
//!   |                   ← \x07WELCOME          |
//!   |  \x08INITIATE[properties]             → |
//!   |                                         | (validates CONNECT compat)
//!   |                   ← \x05READY[props]     |
//! ```
//!
//! On authentication failure, the server sends:
//! ```text
//!   |  ← \x05ERROR[len][reason]               |
//! ```
//!
//! References:
//! - C++ `plain_client.cpp` and `plain_server.cpp`
//! - ZMTP/PLAIN specification (RFC 23)

use crate::codec::mechanism::{Mechanism, MechanismResult, SecurityMechanism};
use crate::error::{ZmqError, ZmqResult};

// ============================================================================
// Wire format constants (matching C++ plain_common.hpp)
// ============================================================================

/// HELLO command: client sends username/password to server.
const HELLO_PREFIX: &[u8] = b"\x05HELLO";
const HELLO_PREFIX_LEN: usize = 6;

/// WELCOME command: server acknowledges HELLO receipt.
const WELCOME_PREFIX: &[u8] = b"\x07WELCOME";
const WELCOME_PREFIX_LEN: usize = 8;

/// INITIATE command: client signals readiness after receiving WELCOME.
const INITIATE_PREFIX: &[u8] = b"\x08INITIATE";
const INITIATE_PREFIX_LEN: usize = 9;

/// READY command: server signals handshake completion.
const READY_PREFIX: &[u8] = b"\x05READY";
const READY_PREFIX_LEN: usize = 6;

/// ERROR command: server signals authentication failure.
const ERROR_PREFIX: &[u8] = b"\x05ERROR";
const ERROR_PREFIX_LEN: usize = 6;

// ============================================================================
// State machines
// ============================================================================

/// PLAIN client handshake states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainClientState {
    /// Initial state: need to send HELLO with credentials.
    SendingHello,
    /// HELLO sent; waiting for server's WELCOME.
    WaitingForWelcome,
    /// WELCOME received; need to send INITIATE.
    SendingInitiate,
    /// INITIATE sent; waiting for server's READY.
    WaitingForReady,
    /// READY received; handshake complete.
    Ready,
    /// ERROR received from server.
    ErrorReceived,
}

/// PLAIN server handshake states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainServerState {
    /// Initial state: waiting for client's HELLO.
    WaitingForHello,
    /// HELLO received and validated; need to send WELCOME.
    SendingWelcome,
    /// WELCOME sent; waiting for client's INITIATE.
    WaitingForInitiate,
    /// INITIATE received; need to send READY.
    SendingReady,
    /// Authentication failed; need to send ERROR.
    SendingError,
    /// READY sent; handshake complete.
    Ready,
    /// ERROR sent; handshake failed.
    ErrorSent,
}

// ============================================================================
// Credential validation
// ============================================================================

/// Result of credential validation (modeled after ZAP responses).
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Status code: "200", "300", "400", or "500"
    pub status_code: String,
    /// Human-readable status text
    pub status_text: String,
    /// Authenticated user ID (set on success)
    pub user_id: Option<String>,
}

impl ValidationResult {
    /// Create a success result with the given user ID.
    pub fn success(user_id: String) -> Self {
        Self {
            status_code: "200".into(),
            status_text: "OK".into(),
            user_id: Some(user_id),
        }
    }

    /// Create a temporary failure result (client should retry).
    pub fn temporary_failure(reason: &str) -> Self {
        Self {
            status_code: "300".into(),
            status_text: reason.into(),
            user_id: None,
        }
    }

    /// Create an authentication failure result.
    pub fn authentication_failure(reason: &str) -> Self {
        Self {
            status_code: "400".into(),
            status_text: reason.into(),
            user_id: None,
        }
    }

    /// Create an internal error result.
    pub fn internal_error(reason: &str) -> Self {
        Self {
            status_code: "500".into(),
            status_text: reason.into(),
            user_id: None,
        }
    }

    /// Whether the validation was successful.
    pub fn is_ok(&self) -> bool {
        self.status_code == "200"
    }

    /// Whether the failure is temporary (300).
    pub fn is_temporary(&self) -> bool {
        self.status_code == "300"
    }
}

/// Trait for credential validation used by PLAIN server.
///
/// Applications provide an implementation (e.g., via ZAP or a static list).
/// This is synchronous — for async validation, the mechanism should be
/// driven externally using the `Mechanism` trait's polling methods.
pub trait CredentialValidator: Send + Sync {
    /// Validate a username/password pair.
    ///
    /// Returns a `ValidationResult` indicating success or failure.
    fn validate(&self, username: &str, password: &str, domain: &str) -> ValidationResult;
}

/// A static username/password validator for testing and simple deployments.
pub struct StaticCredentialValidator {
    credentials: Vec<(String, String)>,
}

impl StaticCredentialValidator {
    /// Create a validator from a list of (username, password) pairs.
    pub fn new(credentials: Vec<(String, String)>) -> Self {
        Self { credentials }
    }
}

impl CredentialValidator for StaticCredentialValidator {
    fn validate(&self, username: &str, password: &str, _domain: &str) -> ValidationResult {
        for (u, p) in &self.credentials {
            if u == username && p == password {
                return ValidationResult::success(username.to_string());
            }
        }
        ValidationResult::authentication_failure("Invalid username or password")
    }
}

// ============================================================================
// PLAIN Client
// ============================================================================

/// PLAIN client mechanism — initiates the PLAIN handshake.
///
/// # Example
///
/// ```rust
/// use zmq_core::security::PlainClient;
/// use zmq_core::codec::mechanism::{Mechanism, MechanismResult};
///
/// let mut client = PlainClient::new("alice".into(), "secret".into());
///
/// // Client sends HELLO
/// let hello = client.next_handshake_output().unwrap();
/// assert!(hello.starts_with(b"\x05HELLO"));
/// assert!(hello.windows(5).any(|w| w == b"alice"));
/// ```
pub struct PlainClient {
    username: String,
    password: String,
    state: PlainClientState,
}

impl PlainClient {
    /// Create a new PLAIN client with the given credentials.
    pub fn new(username: String, password: String) -> Self {
        Self {
            username,
            password,
            state: PlainClientState::SendingHello,
        }
    }

    /// Build the HELLO command: `\x05HELLO + 1-byte-username-len + username + 1-byte-password-len + password`
    fn build_hello(&self) -> Vec<u8> {
        let username_bytes = self.username.as_bytes();
        let password_bytes = self.password.as_bytes();

        // Truncate to 255 bytes per field (u8 length prefix)
        let ulen = std::cmp::min(username_bytes.len(), 255) as u8;
        let plen = std::cmp::min(password_bytes.len(), 255) as u8;

        let capacity = HELLO_PREFIX_LEN + 1 + ulen as usize + 1 + plen as usize;
        let mut cmd = Vec::with_capacity(capacity);

        cmd.extend_from_slice(HELLO_PREFIX);
        cmd.push(ulen);
        cmd.extend_from_slice(&username_bytes[..ulen as usize]);
        cmd.push(plen);
        cmd.extend_from_slice(&password_bytes[..plen as usize]);

        cmd
    }

    /// Build the INITIATE command with minimal properties.
    fn build_initiate() -> Vec<u8> {
        // INITIATE prefix. In a full implementation, this would include
        // ZMTP metadata properties (Socket-Type, Identity, etc.).
        INITIATE_PREFIX.to_vec()
    }
}

impl Mechanism for PlainClient {
    fn mechanism_type(&self) -> SecurityMechanism {
        SecurityMechanism::Plain
    }

    fn is_handshake_complete(&self) -> bool {
        self.state == PlainClientState::Ready
    }

    fn process_handshake(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        match self.state {
            PlainClientState::WaitingForWelcome => self.process_in_waiting_for_welcome(data),
            PlainClientState::WaitingForReady => self.process_in_waiting_for_ready(data),
            other => Err(ZmqError::Protocol(format!(
                "PLAIN client: unexpected handshake data in state {:?}",
                other
            ))),
        }
    }

    fn next_handshake_output(&mut self) -> Option<Vec<u8>> {
        match self.state {
            PlainClientState::SendingHello => {
                let cmd = self.build_hello();
                self.state = PlainClientState::WaitingForWelcome;
                Some(cmd)
            }
            PlainClientState::SendingInitiate => {
                let cmd = Self::build_initiate();
                self.state = PlainClientState::WaitingForReady;
                Some(cmd)
            }
            _ => None,
        }
    }

    fn user_id(&self) -> Option<&str> {
        // PLAIN client doesn't receive a user ID from the server
        None
    }
}

impl PlainClient {
    /// Process incoming data while waiting for WELCOME (or ERROR).
    fn process_in_waiting_for_welcome(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        if self.is_welcome(data) {
            self.state = PlainClientState::SendingInitiate;
            return Ok(MechanismResult::NeedMore(vec![]));
        }
        if self.is_error(data) {
            return self.handle_error(data);
        }
        Err(unexpected_command("WELCOME or ERROR", data))
    }

    /// Process incoming data while waiting for READY (or ERROR).
    fn process_in_waiting_for_ready(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        if data.len() >= READY_PREFIX_LEN && data[..READY_PREFIX_LEN] == *READY_PREFIX {
            self.state = PlainClientState::Ready;
            return Ok(MechanismResult::Success { user_id: None });
        }
        if self.is_error(data) {
            return self.handle_error(data);
        }
        Err(unexpected_command("READY or ERROR", data))
    }

    /// Check if data is a valid WELCOME command.
    fn is_welcome(&self, data: &[u8]) -> bool {
        data.len() == WELCOME_PREFIX_LEN && data == WELCOME_PREFIX
    }

    /// Check if data is an ERROR command.
    fn is_error(&self, data: &[u8]) -> bool {
        data.len() >= ERROR_PREFIX_LEN && data[..ERROR_PREFIX_LEN] == *ERROR_PREFIX
    }

    /// Handle an ERROR command from the server.
    fn handle_error(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        self.state = PlainClientState::ErrorReceived;
        let reason = extract_error_reason(data);
        Ok(MechanismResult::Error(ZmqError::Security(format!(
            "PLAIN handshake: server sent ERROR: {}",
            reason
        ))))
    }
}

// ============================================================================
// PLAIN Server
// ============================================================================

/// PLAIN server mechanism — responds to the PLAIN handshake.
///
/// The server validates client credentials using an optional
/// `CredentialValidator`. If no validator is configured, all
/// connections are rejected.
///
/// # Example
///
/// ```rust
/// use zmq_core::security::{PlainServer, StaticCredentialValidator};
/// use zmq_core::codec::mechanism::{Mechanism, MechanismResult};
///
/// let validator = StaticCredentialValidator::new(vec![
///     ("alice".into(), "secret".into()),
/// ]);
/// let mut server = PlainServer::with_validator(
///     "global".into(),
///     Box::new(validator),
/// );
/// ```
pub struct PlainServer {
    state: PlainServerState,
    domain: String,
    validator: Option<Box<dyn CredentialValidator>>,
    authenticated_user: Option<String>,
    pending_error: Option<String>,
}

impl PlainServer {
    /// Create a new PLAIN server with no credential validator.
    /// All connections will be rejected (ERROR sent).
    pub fn new(domain: String) -> Self {
        Self {
            state: PlainServerState::WaitingForHello,
            domain,
            validator: None,
            authenticated_user: None,
            pending_error: None,
        }
    }

    /// Create a new PLAIN server with a credential validator.
    pub fn with_validator(domain: String, validator: Box<dyn CredentialValidator>) -> Self {
        Self {
            state: PlainServerState::WaitingForHello,
            domain,
            validator: Some(validator),
            authenticated_user: None,
            pending_error: None,
        }
    }

    /// Get the domain this server uses for ZAP validation.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Build a WELCOME command.
    fn build_welcome() -> Vec<u8> {
        WELCOME_PREFIX.to_vec()
    }

    /// Build a READY command.
    fn build_ready() -> Vec<u8> {
        READY_PREFIX.to_vec()
    }

    /// Build an ERROR command with the pending error reason.
    fn build_error(&self) -> Vec<u8> {
        let reason = self.pending_error.as_deref().unwrap_or("");
        let reason_bytes = reason.as_bytes();
        let reason_len = std::cmp::min(reason_bytes.len(), 255);

        let mut cmd = Vec::with_capacity(ERROR_PREFIX_LEN + 1 + reason_len);
        cmd.extend_from_slice(ERROR_PREFIX);
        cmd.push(reason_len as u8);
        cmd.extend_from_slice(&reason_bytes[..reason_len]);
        cmd
    }

    /// Validate client credentials using the configured validator.
    fn validate_credentials(&mut self, username: &str, password: &str) {
        if let Some(ref validator) = self.validator {
            let result = validator.validate(username, password, &self.domain);
            match result.status_code.as_str() {
                "200" => {
                    self.authenticated_user = result.user_id;
                    self.state = PlainServerState::SendingWelcome;
                }
                "300" => {
                    // Temporary failure — silently disconnect (per CURVEZMQ RFC).
                    // Do NOT send an ERROR message.
                    self.state = PlainServerState::ErrorSent;
                }
                _ => {
                    // 400 or 500 — send ERROR to client.
                    self.pending_error = Some(result.status_text);
                    self.state = PlainServerState::SendingError;
                }
            }
        } else {
            // No validator configured — reject all connections.
            self.pending_error = Some("PLAIN mechanism requires ZAP or a credential validator".into());
            self.state = PlainServerState::SendingError;
        }
    }
}

impl Mechanism for PlainServer {
    fn mechanism_type(&self) -> SecurityMechanism {
        SecurityMechanism::Plain
    }

    fn is_handshake_complete(&self) -> bool {
        self.state == PlainServerState::Ready
    }

    fn process_handshake(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        match self.state {
            PlainServerState::WaitingForHello => self.process_hello(data),
            PlainServerState::WaitingForInitiate => self.process_initiate(data),
            other => Err(ZmqError::Protocol(format!(
                "PLAIN server: unexpected handshake data in state {:?}",
                other
            ))),
        }
    }

    fn next_handshake_output(&mut self) -> Option<Vec<u8>> {
        match self.state {
            PlainServerState::SendingWelcome => {
                let cmd = Self::build_welcome();
                self.state = PlainServerState::WaitingForInitiate;
                Some(cmd)
            }
            PlainServerState::SendingReady => {
                let cmd = Self::build_ready();
                self.state = PlainServerState::Ready;
                Some(cmd)
            }
            PlainServerState::SendingError => {
                let cmd = self.build_error();
                self.state = PlainServerState::ErrorSent;
                Some(cmd)
            }
            _ => None,
        }
    }

    fn user_id(&self) -> Option<&str> {
        self.authenticated_user.as_deref()
    }
}

impl PlainServer {
    /// Process a HELLO command from the client.
    fn process_hello(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        // Validate HELLO prefix
        if data.len() < HELLO_PREFIX_LEN || data[..HELLO_PREFIX_LEN] != *HELLO_PREFIX {
            return Err(ZmqError::Protocol(format!(
                "PLAIN server: expected HELLO command, got {:?}",
                if data.len() > 10 { &data[..10] } else { data }
            )));
        }

        let mut pos = HELLO_PREFIX_LEN;

        // Parse username length
        if pos >= data.len() {
            return Err(ZmqError::Protocol(
                "PLAIN server: HELLO missing username length".into(),
            ));
        }
        let username_len = data[pos] as usize;
        pos += 1;

        // Parse username
        if pos + username_len > data.len() {
            return Err(ZmqError::Protocol(
                "PLAIN server: HELLO username truncated".into(),
            ));
        }
        let username = String::from_utf8_lossy(&data[pos..pos + username_len]).to_string();
        pos += username_len;

        // Parse password length
        if pos >= data.len() {
            return Err(ZmqError::Protocol(
                "PLAIN server: HELLO missing password length".into(),
            ));
        }
        let password_len = data[pos] as usize;
        pos += 1;

        // Parse password
        if pos + password_len != data.len() {
            return Err(ZmqError::Protocol(format!(
                "PLAIN server: HELLO password length mismatch (expected {} bytes at position {}, got {} bytes total)",
                password_len, pos, data.len()
            )));
        }
        let password = String::from_utf8_lossy(&data[pos..pos + password_len]).to_string();

        // Validate credentials
        self.validate_credentials(&username, &password);

        Ok(MechanismResult::NeedMore(vec![]))
    }

    /// Process an INITIATE command from the client.
    fn process_initiate(&mut self, data: &[u8]) -> ZmqResult<MechanismResult> {
        if data.len() < INITIATE_PREFIX_LEN || data[..INITIATE_PREFIX_LEN] != *INITIATE_PREFIX {
            return Err(ZmqError::Protocol(format!(
                "PLAIN server: expected INITIATE command, got {:?}",
                if data.len() > 12 { &data[..12] } else { data }
            )));
        }

        // Optional: parse and validate INITIATE properties (e.g., Socket-Type compatibility).
        // For now, we accept any INITIATE command and proceed to READY.
        self.state = PlainServerState::SendingReady;
        Ok(MechanismResult::NeedMore(vec![]))
    }
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Build an error for unexpected handshake commands.
fn unexpected_command(expected: &str, data: &[u8]) -> ZmqError {
    let preview = if data.len() > 16 { &data[..16] } else { data };
    ZmqError::Protocol(format!(
        "PLAIN: expected {}, got {} bytes: {:?}",
        expected,
        data.len(),
        preview
    ))
}

/// Extract error reason from an ERROR command.
/// Format: `\x05ERROR [1-byte reason_len] [reason]`
fn extract_error_reason(data: &[u8]) -> String {
    if data.len() > ERROR_PREFIX_LEN {
        let reason_len = data[ERROR_PREFIX_LEN] as usize;
        let start = ERROR_PREFIX_LEN + 1;
        let end = std::cmp::min(start + reason_len, data.len());
        String::from_utf8_lossy(&data[start..end]).to_string()
    } else {
        String::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== PlainClient tests ====================

    #[test]
    fn test_plain_client_creation() {
        let c = PlainClient::new("alice".into(), "secret".into());
        assert_eq!(c.mechanism_type(), SecurityMechanism::Plain);
        assert!(!c.is_handshake_complete());
        assert!(c.user_id().is_none());
    }

    #[test]
    fn test_plain_client_sends_hello() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        let output = c.next_handshake_output().unwrap();

        // Verify HELLO prefix
        assert_eq!(&output[..HELLO_PREFIX_LEN], HELLO_PREFIX);

        // Verify username
        let ulen = output[HELLO_PREFIX_LEN] as usize;
        assert_eq!(ulen, 5); // "alice"
        assert_eq!(&output[HELLO_PREFIX_LEN + 1..HELLO_PREFIX_LEN + 1 + ulen], b"alice");

        // Verify password
        let plen_pos = HELLO_PREFIX_LEN + 1 + ulen;
        let plen = output[plen_pos] as usize;
        assert_eq!(plen, 6); // "secret"
        assert_eq!(&output[plen_pos + 1..plen_pos + 1 + plen], b"secret");

        // Verify total length
        assert_eq!(output.len(), HELLO_PREFIX_LEN + 1 + ulen + 1 + plen);
    }

    #[test]
    fn test_plain_client_hello_empty_credentials() {
        let mut c = PlainClient::new("".into(), "".into());
        let output = c.next_handshake_output().unwrap();
        // Should have HELLO prefix + 0 len username + 0 len password
        assert_eq!(output.len(), HELLO_PREFIX_LEN + 2);
        assert_eq!(output[HELLO_PREFIX_LEN], 0); // username length
        assert_eq!(output[HELLO_PREFIX_LEN + 1], 0); // password length
    }

    #[test]
    fn test_plain_client_long_credentials_truncated() {
        let long = "a".repeat(300);
        let mut c = PlainClient::new(long.clone(), long);
        let output = c.next_handshake_output().unwrap();
        let ulen = output[HELLO_PREFIX_LEN] as usize;
        assert_eq!(ulen, 255); // truncated
    }

    #[test]
    fn test_plain_client_receives_welcome() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        c.next_handshake_output(); // send HELLO
        let result = c.process_handshake(WELCOME_PREFIX).unwrap();
        assert!(matches!(result, MechanismResult::NeedMore(_)));
        assert!(!c.is_handshake_complete());
    }

    #[test]
    fn test_plain_client_sends_initiate_after_welcome() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        c.next_handshake_output(); // HELLO
        c.process_handshake(WELCOME_PREFIX).unwrap(); // WELCOME
        let output = c.next_handshake_output().unwrap(); // INITIATE
        assert_eq!(&output[..INITIATE_PREFIX_LEN], INITIATE_PREFIX);
    }

    #[test]
    fn test_plain_client_full_handshake() {
        let mut c = PlainClient::new("alice".into(), "secret".into());

        // Step 1: send HELLO
        let hello = c.next_handshake_output().unwrap();
        assert!(hello.starts_with(HELLO_PREFIX));

        // Step 2: receive WELCOME
        let result = c.process_handshake(WELCOME_PREFIX).unwrap();
        assert!(matches!(result, MechanismResult::NeedMore(_)));

        // Step 3: send INITIATE
        let initiate = c.next_handshake_output().unwrap();
        assert!(initiate.starts_with(INITIATE_PREFIX));

        // Step 4: receive READY
        let result = c.process_handshake(READY_PREFIX).unwrap();
        assert!(matches!(result, MechanismResult::Success { user_id: None }));
        assert!(c.is_handshake_complete());
    }

    #[test]
    fn test_plain_client_error_instead_of_welcome() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        c.next_handshake_output(); // HELLO

        let error_cmd = build_error_cmd("Invalid credentials");
        let result = c.process_handshake(&error_cmd).unwrap();
        assert!(matches!(result, MechanismResult::Error(_)));
    }

    #[test]
    fn test_plain_client_error_instead_of_ready() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        c.next_handshake_output(); // HELLO
        c.process_handshake(WELCOME_PREFIX).unwrap(); // WELCOME
        c.next_handshake_output(); // INITIATE

        let error_cmd = build_error_cmd("Connection rejected");
        let result = c.process_handshake(&error_cmd).unwrap();
        assert!(matches!(result, MechanismResult::Error(_)));
    }

    #[test]
    fn test_plain_client_unexpected_data() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        c.next_handshake_output(); // HELLO

        // Send garbage
        let result = c.process_handshake(b"\x99GARBAGE");
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_client_wrong_state_data() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        // Try to receive before sending anything
        let result = c.process_handshake(WELCOME_PREFIX);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_client_no_double_output() {
        let mut c = PlainClient::new("alice".into(), "secret".into());
        assert!(c.next_handshake_output().is_some()); // HELLO
        assert!(c.next_handshake_output().is_none()); // Nothing until WELCOME
        c.process_handshake(WELCOME_PREFIX).unwrap();
        assert!(c.next_handshake_output().is_some()); // INITIATE
        assert!(c.next_handshake_output().is_none()); // Nothing until READY
    }

    // ==================== PlainServer tests ====================

    #[test]
    fn test_plain_server_creation() {
        let s = PlainServer::new("global".into());
        assert_eq!(s.mechanism_type(), SecurityMechanism::Plain);
        assert!(!s.is_handshake_complete());
        assert_eq!(s.domain(), "global");
    }

    #[test]
    fn test_plain_server_with_validator() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let s = PlainServer::with_validator("test".into(), Box::new(validator));
        assert_eq!(s.domain(), "test");
    }

    #[test]
    fn test_plain_server_rejects_without_validator() {
        let mut s = PlainServer::new("global".into());
        let hello = build_hello_cmd("alice", "secret");
        s.process_handshake(&hello).unwrap();
        // Should be in SendingError state
        let error = s.next_handshake_output().unwrap();
        assert!(error.starts_with(ERROR_PREFIX));
    }

    #[test]
    fn test_plain_server_process_valid_hello() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        let hello = build_hello_cmd("alice", "secret");
        let result = s.process_handshake(&hello).unwrap();
        assert!(matches!(result, MechanismResult::NeedMore(_)));

        // Should send WELCOME
        let output = s.next_handshake_output().unwrap();
        assert_eq!(&output, WELCOME_PREFIX);
    }

    #[test]
    fn test_plain_server_process_invalid_hello() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        let hello = build_hello_cmd("alice", "wrong_password");
        s.process_handshake(&hello).unwrap();

        // Should send ERROR
        let output = s.next_handshake_output().unwrap();
        assert!(output.starts_with(ERROR_PREFIX));
    }

    #[test]
    fn test_plain_server_full_handshake() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // Step 1: receive HELLO
        let hello = build_hello_cmd("alice", "secret");
        s.process_handshake(&hello).unwrap();

        // Step 2: send WELCOME
        let welcome = s.next_handshake_output().unwrap();
        assert_eq!(&welcome, WELCOME_PREFIX);

        // Step 3: receive INITIATE
        let result = s.process_handshake(INITIATE_PREFIX).unwrap();
        assert!(matches!(result, MechanismResult::NeedMore(_)));

        // Step 4: send READY
        let ready = s.next_handshake_output().unwrap();
        assert!(ready.starts_with(READY_PREFIX));
        assert!(s.is_handshake_complete());
        assert_eq!(s.user_id(), Some("alice"));
    }

    #[test]
    fn test_plain_server_malformed_hello_missing_username() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // Send just HELLO prefix with no data
        let result = s.process_handshake(HELLO_PREFIX);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_malformed_hello_truncated_username() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // HELLO + username_len=10 but only 3 bytes of username
        let mut cmd = HELLO_PREFIX.to_vec();
        cmd.push(10); // claim 10-byte username
        cmd.extend_from_slice(b"abc"); // only 3 bytes

        let result = s.process_handshake(&cmd);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_malformed_hello_missing_password() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // HELLO + username_len=5 + "alice" + no password
        let mut cmd = HELLO_PREFIX.to_vec();
        cmd.push(5);
        cmd.extend_from_slice(b"alice");
        // Missing password length and password

        let result = s.process_handshake(&cmd);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_malformed_hello_password_length_mismatch() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // HELLO + ulen=5 + "alice" + plen=10 + "short" (only 5 bytes)
        let mut cmd = HELLO_PREFIX.to_vec();
        cmd.push(5);
        cmd.extend_from_slice(b"alice");
        cmd.push(10); // claim 10-byte password
        cmd.extend_from_slice(b"short"); // only 5 bytes

        let result = s.process_handshake(&cmd);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_unexpected_data() {
        let mut s = PlainServer::new("global".into());
        let result = s.process_handshake(b"\x99GARBAGE");
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_bad_initiate() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        // Process HELLO and send WELCOME
        let hello = build_hello_cmd("alice", "secret");
        s.process_handshake(&hello).unwrap();
        s.next_handshake_output(); // WELCOME

        // Send garbage instead of INITIATE
        let result = s.process_handshake(b"\x99BAD_INIT");
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_server_user_id_on_success() {
        let validator = StaticCredentialValidator::new(vec![
            ("bob".into(), "password123".into()),
        ]);
        let mut s = PlainServer::with_validator("global".into(), Box::new(validator));

        let hello = build_hello_cmd("bob", "password123");
        s.process_handshake(&hello).unwrap();
        s.next_handshake_output(); // WELCOME
        s.process_handshake(INITIATE_PREFIX).unwrap();
        s.next_handshake_output(); // READY

        assert_eq!(s.user_id(), Some("bob"));
        assert!(s.is_handshake_complete());
    }

    #[test]
    fn test_plain_server_no_output_after_error() {
        let mut s = PlainServer::new("global".into());
        let hello = build_hello_cmd("alice", "secret");
        s.process_handshake(&hello).unwrap();
        s.next_handshake_output(); // ERROR
        assert!(s.next_handshake_output().is_none());
    }

    // ==================== Integration: client + server ====================

    #[test]
    fn test_plain_full_client_server_handshake() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut server = PlainServer::with_validator("global".into(), Box::new(validator));
        let mut client = PlainClient::new("alice".into(), "secret".into());

        // Client → Server: HELLO
        let hello = client.next_handshake_output().unwrap();
        let s_result = server.process_handshake(&hello).unwrap();
        assert!(matches!(s_result, MechanismResult::NeedMore(_)));

        // Server → Client: WELCOME
        let welcome = server.next_handshake_output().unwrap();
        let c_result = client.process_handshake(&welcome).unwrap();
        assert!(matches!(c_result, MechanismResult::NeedMore(_)));

        // Client → Server: INITIATE
        let initiate = client.next_handshake_output().unwrap();
        let s_result = server.process_handshake(&initiate).unwrap();
        assert!(matches!(s_result, MechanismResult::NeedMore(_)));

        // Server → Client: READY
        let ready = server.next_handshake_output().unwrap();
        let c_result = client.process_handshake(&ready).unwrap();
        assert!(matches!(c_result, MechanismResult::Success { user_id: None }));

        assert!(client.is_handshake_complete());
        assert!(server.is_handshake_complete());
    }

    #[test]
    fn test_plain_client_server_auth_failure() {
        let validator = StaticCredentialValidator::new(vec![
            ("alice".into(), "secret".into()),
        ]);
        let mut server = PlainServer::with_validator("global".into(), Box::new(validator));
        let mut client = PlainClient::new("eve".into(), "hacker".into());

        // Client sends HELLO with bad credentials
        let hello = client.next_handshake_output().unwrap();
        server.process_handshake(&hello).unwrap();

        // Server sends ERROR
        let error = server.next_handshake_output().unwrap();
        assert!(error.starts_with(ERROR_PREFIX));

        // Client receives ERROR
        let c_result = client.process_handshake(&error).unwrap();
        assert!(matches!(c_result, MechanismResult::Error(_)));
        assert!(!client.is_handshake_complete());
        assert!(!server.is_handshake_complete());
    }

    // ==================== Helper functions for tests ====================

    fn build_hello_cmd(username: &str, password: &str) -> Vec<u8> {
        let ulen = username.len().min(255) as u8;
        let plen = password.len().min(255) as u8;
        let mut cmd = Vec::with_capacity(HELLO_PREFIX_LEN + 1 + ulen as usize + 1 + plen as usize);
        cmd.extend_from_slice(HELLO_PREFIX);
        cmd.push(ulen);
        cmd.extend_from_slice(&username.as_bytes()[..ulen as usize]);
        cmd.push(plen);
        cmd.extend_from_slice(&password.as_bytes()[..plen as usize]);
        cmd
    }

    fn build_error_cmd(reason: &str) -> Vec<u8> {
        let reason = &reason[..reason.len().min(255)];
        let mut cmd = Vec::with_capacity(ERROR_PREFIX_LEN + 1 + reason.len());
        cmd.extend_from_slice(ERROR_PREFIX);
        cmd.push(reason.len() as u8);
        cmd.extend_from_slice(reason.as_bytes());
        cmd
    }
}
