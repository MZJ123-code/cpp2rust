//! Socket options — 1:1 translation of C++ `options_t`.
//!
//! All ZMQ_* socket options. Used by `ZSocket::set_option` / `get_option`.

use std::time::Duration;

/// Security mechanism for socket connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityMechanism {
    #[default]
    Null,
    Plain,
    Curve,
}

/// All ZeroMQ socket options.
#[derive(Debug, Clone)]
pub struct SocketOptions {
    // Connection
    pub linger: i32,
    pub sndhwm: i32,
    pub rcvhwm: i32,
    pub sndtimeo: i32,
    pub rcvtimeo: i32,
    pub reconnect_ivl: i32,
    pub reconnect_ivl_max: i32,
    pub backlog: i32,
    pub ipv6: bool,
    pub immediate: bool,
    pub conflate: bool,

    // Transport
    pub tcp_keepalive: i32,
    pub tcp_keepalive_idle: i32,
    pub tcp_keepalive_cnt: i32,
    pub tcp_keepalive_intvl: i32,
    pub tcp_nodelay: bool,

    // Security
    pub mechanism: SecurityMechanism,
    pub curve_server: bool,
    pub curve_publickey: [u8; 32],
    pub curve_secretkey: [u8; 32],
    pub curve_serverkey: [u8; 32],
    pub plain_username: String,
    pub plain_password: String,
    pub zap_domain: String,

    // PUB/SUB
    pub xpub_verbose: bool,
    pub xpub_nodrop: bool,
    pub subscribe: Vec<Vec<u8>>,
    pub unsubscribe: Vec<Vec<u8>>,

    // Routing
    pub routing_id: Vec<u8>,
    pub router_mandatory: bool,
    pub router_handover: bool,
    pub probe_router: bool,

    // Heartbeats
    pub heartbeat_ivl: i32,
    pub heartbeat_timeout: i32,
    pub heartbeat_ttl: i32,

    // Rate limiting
    pub rate: i32,
    pub recovery_ivl: i32,
    pub multicast_hops: i32,

    // Misc
    pub rcvbuf: i32,
    pub sndbuf: i32,
    pub tos: i32,
    pub use_fd: i32,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            linger: 30000,
            sndhwm: 1000,
            rcvhwm: 1000,
            sndtimeo: -1,
            rcvtimeo: -1,
            reconnect_ivl: 100,
            reconnect_ivl_max: 0,
            backlog: 100,
            ipv6: false,
            immediate: false,
            conflate: false,
            tcp_keepalive: -1,
            tcp_keepalive_idle: -1,
            tcp_keepalive_cnt: -1,
            tcp_keepalive_intvl: -1,
            tcp_nodelay: true,
            mechanism: SecurityMechanism::Null,
            curve_server: false,
            curve_publickey: [0u8; 32],
            curve_secretkey: [0u8; 32],
            curve_serverkey: [0u8; 32],
            plain_username: String::new(),
            plain_password: String::new(),
            zap_domain: String::new(),
            xpub_verbose: false,
            xpub_nodrop: false,
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
            routing_id: Vec::new(),
            router_mandatory: false,
            router_handover: false,
            probe_router: false,
            heartbeat_ivl: 0,
            heartbeat_timeout: 0,
            heartbeat_ttl: 0,
            rate: 100,
            recovery_ivl: 10000,
            multicast_hops: 1,
            rcvbuf: 0,
            sndbuf: 0,
            tos: 0,
            use_fd: 0,
        }
    }
}
