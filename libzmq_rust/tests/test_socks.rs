mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// SOCKS proxy option — can be set on a socket.
#[test]
#[ignore = "ZMQ_SOCKS_PROXY option not yet implemented"]
fn test_socks_proxy_options() {
    let _ctx = TestContext::new();
    // Would test:
    // - NULL proxy (equivalent to not-set, returns empty string)
    // - Valid proxy "somehost:1080"
    // - Empty value not allowed for proxy
}

/// SOCKS username/password options.
#[test]
#[ignore = "ZMQ_SOCKS_USERNAME/PASSWORD options not yet implemented"]
fn test_socks_userpass_options() {
    let _ctx = TestContext::new();
    // Would test:
    // - NULL username/password (not-set, return "")
    // - Empty values
    // - Valid values
    // - 255-byte limit
    // - Too-long values rejected
}

/// Push/Pull without SOCKS proxy.
#[test]
#[ignore = "SOCKS test needs Push/Pull socket implementation"]
fn test_socks_no_socks() {
    let ctx = TestContext::new();
    let push = ctx.socket(SocketType::Push);
    let pull = ctx.socket(SocketType::Pull);

    ctx.bind_inproc(&push, "push-pull-no-socks");
    ctx.connect_inproc(&pull, "push-pull-no-socks");

    s_send_seq(&push, &[Some("ABC"), None]);
    s_send_seq(&push, &[Some("DEF"), None]);

    s_recv_seq(&pull, RecvFlags::NONE, &[Some("ABC"), None]);
    s_recv_seq(&pull, RecvFlags::NONE, &[Some("DEF"), None]);
}

/// SOCKS proxy with delay.
#[test]
#[ignore = "SOCKS proxy support not yet implemented"]
fn test_socks_delay() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy domainname resolution.
#[test]
#[ignore = "SOCKS proxy support not yet implemented"]
fn test_socks_domainname() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy IPv6.
#[test]
#[ignore = "SOCKS proxy support not yet implemented"]
fn test_socks_ipv6() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with IPv6 square-bracket notation.
#[test]
#[ignore = "SOCKS proxy support not yet implemented"]
fn test_socks_ipv6_sb() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with bind-before-connect.
#[test]
#[ignore = "SOCKS proxy support not yet implemented"]
fn test_socks_bind_before_connect() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with basic auth.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy basic auth with delay.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth_delay() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with empty username.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth_empty_user() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with null username.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth_null_user() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with empty password.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth_empty_pass() {
    let _ctx = TestContext::new();
}

/// SOCKS proxy with null password.
#[test]
#[ignore = "SOCKS proxy and draft API not yet implemented"]
fn test_socks_basic_auth_null_pass() {
    let _ctx = TestContext::new();
}
