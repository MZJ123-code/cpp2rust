mod common;
use common::*;
use zmq_core::socket_type::SocketType;

/// Test all available setter options with valid values.
#[test]
fn test_socket_options_valid_values() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);
    let d = ctx.socket(SocketType::Sub);

    // Integer options
    s.set_linger(0).expect("set_linger");
    assert_eq!(s.get_options().linger, 0);

    s.set_sndhwm(500).expect("set_sndhwm");
    assert_eq!(s.get_options().sndhwm, 500);

    s.set_rcvhwm(200).expect("set_rcvhwm");
    assert_eq!(s.get_options().rcvhwm, 200);

    s.set_sndtimeo(1000).expect("set_sndtimeo");
    assert_eq!(s.get_options().sndtimeo, 1000);

    s.set_rcvtimeo(2000).expect("set_rcvtimeo");
    assert_eq!(s.get_options().rcvtimeo, 2000);

    s.set_reconnect_ivl(500).expect("set_reconnect_ivl");
    assert_eq!(s.get_options().reconnect_ivl, 500);

    // Boolean options
    s.set_immediate(true).expect("set_immediate");
    assert!(s.get_options().immediate);

    s.set_conflate(true).expect("set_conflate");
    assert!(s.get_options().conflate);

    s.set_ipv6(true).expect("set_ipv6");
    assert!(s.get_options().ipv6);

    s.set_tcp_nodelay(true).expect("set_tcp_nodelay");
    assert!(s.get_options().tcp_nodelay);

    // TCP keepalive
    s.set_tcp_keepalive(1).expect("set_tcp_keepalive");
    assert_eq!(s.get_options().tcp_keepalive, 1);

    // Heartbeat options
    s.set_heartbeat_ivl(1000).expect("set_heartbeat_ivl");
    assert_eq!(s.get_options().heartbeat_ivl, 1000);

    s.set_heartbeat_timeout(5000).expect("set_heartbeat_timeout");
    assert_eq!(s.get_options().heartbeat_timeout, 5000);

    s.set_heartbeat_ttl(3000).expect("set_heartbeat_ttl");
    assert_eq!(s.get_options().heartbeat_ttl, 3000);

    // Routing options
    s.set_router_mandatory(true).expect("set_router_mandatory");
    assert!(s.get_options().router_mandatory);

    s.set_router_handover(true).expect("set_router_handover");
    assert!(s.get_options().router_handover);

    // REQ options
    s.set_req_correlate(true).expect("set_req_correlate");
    assert!(s.get_options().req_correlate);

    s.set_req_relaxed(true).expect("set_req_relaxed");
    assert!(s.get_options().req_relaxed);

    // Rate options
    s.set_rate(50).expect("set_rate");
    assert_eq!(s.get_options().rate, 50);

    s.set_recovery_ivl(5000).expect("set_recovery_ivl");
    assert_eq!(s.get_options().recovery_ivl, 5000);

    // Buffer options
    s.set_sndbuf(65536).expect("set_sndbuf");
    assert_eq!(s.get_options().sndbuf, 65536);

    s.set_rcvbuf(65536).expect("set_rcvbuf");
    assert_eq!(s.get_options().rcvbuf, 65536);

    s.set_tos(0x10).expect("set_tos");
    assert_eq!(s.get_options().tos, 0x10);

    s.set_backlog(50).expect("set_backlog");
    assert_eq!(s.get_options().backlog, 50);

    // Probe router
    s.set_probe_router(true).expect("set_probe_router");
    assert!(s.get_options().probe_router);

    // XPUB options
    s.set_xpub_verbose(true).expect("set_xpub_verbose");
    assert!(s.get_options().xpub_verbose);

    s.set_xpub_verboser(true).expect("set_xpub_verboser");
    assert!(s.get_options().xpub_verboser);

    s.set_xpub_nodrop(true).expect("set_xpub_nodrop");
    assert!(s.get_options().xpub_nodrop);

    s.set_xpub_manual(true).expect("set_xpub_manual");
    assert!(s.get_options().xpub_manual);

    s.set_xpub_manual_last_value(true).expect("set_xpub_manual_last_value");
    assert!(s.get_options().xpub_manual_last_value);

    s.set_invert_matching(true).expect("set_invert_matching");
    assert!(s.get_options().invert_matching);

    // XSUB options
    s.set_xsub_verbose_unsubscribe(true).expect("set_xsub_verbose_unsubscribe");
    assert!(s.get_options().xsub_verbose_unsubscribe);

    s.set_only_first_subscribe(true).expect("set_only_first_subscribe");
    assert!(s.get_options().only_first_subscribe);

    // Subscribe/Unsubscribe (SUB socket)
    d.subscribe(b"topic").expect("subscribe");
    assert_eq!(d.get_options().subscribe.len(), 1);
    assert_eq!(d.get_options().subscribe[0], b"topic");

    d.unsubscribe(b"topic").expect("unsubscribe");
    assert_eq!(d.get_options().subscribe.len(), 0);

    // Security options
    s.set_zap_domain("global").expect("set_zap_domain");
    assert_eq!(s.get_options().zap_domain, "global");

    s.set_plain_username("user").expect("set_plain_username");
    assert_eq!(s.get_options().plain_username, "user");

    s.set_plain_password("pass").expect("set_plain_password");
    assert_eq!(s.get_options().plain_password, "pass");

    // Routing ID
    let rid = b"my-routing-id";
    s.set_routing_id(rid).expect("set_routing_id");
    assert_eq!(s.get_options().routing_id, rid);
}

/// Test socket options with extreme/boundary values.
#[test]
fn test_socket_options_boundary_values() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Pub);

    // Zero negative values
    s.set_linger(-1).expect("set_linger -1");
    assert_eq!(s.get_options().linger, -1);

    s.set_linger(-2).expect("set_linger -2");
    assert_eq!(s.get_options().linger, -2);

    s.set_sndtimeo(-1).expect("set_sndtimeo -1 (blocking)");
    assert_eq!(s.get_options().sndtimeo, -1);

    s.set_rcvtimeo(-1).expect("set_rcvtimeo -1 (blocking)");
    assert_eq!(s.get_options().rcvtimeo, -1);

    // Zero timeout
    s.set_sndtimeo(0).expect("set_sndtimeo 0 (non-blocking)");
    assert_eq!(s.get_options().sndtimeo, 0);

    s.set_rcvtimeo(0).expect("set_rcvtimeo 0 (non-blocking)");
    assert_eq!(s.get_options().rcvtimeo, 0);

    // Large HWM values
    s.set_sndhwm(0).expect("set_sndhwm 0");
    s.set_sndhwm(i32::MAX).expect("set_sndhwm max");

    s.set_rcvhwm(0).expect("set_rcvhwm 0");
    s.set_rcvhwm(i32::MAX).expect("set_rcvhwm max");
}

/// Test that invalid subscribe/unsubscribe calls are handled gracefully.
#[test]
fn test_socket_options_subscribe_unsubscribe_empty() {
    let ctx = TestContext::new();
    let s = ctx.socket(SocketType::Sub);

    // Subscribe with empty prefix (should match everything)
    s.subscribe(b"").expect("subscribe empty");
    assert_eq!(s.get_options().subscribe.len(), 1);

    // Unsubscribe with empty prefix
    s.unsubscribe(b"").expect("unsubscribe empty");
    assert_eq!(s.get_options().subscribe.len(), 0);

    // Unsubscribe non-existent (should not fail)
    s.unsubscribe(b"nonexistent").expect("unsubscribe non-existent");
}

/// Test that options are independent per-socket.
#[test]
fn test_socket_options_independence() {
    let ctx = TestContext::new();
    let a = ctx.socket(SocketType::Pub);
    let b = ctx.socket(SocketType::Pub);

    a.set_linger(0).expect("set_linger 0 on a");
    b.set_linger(0).expect("set_linger 0 on b");

    // Different values on different sockets
    a.set_sndhwm(100).expect("set_sndhwm 100 on a");
    b.set_sndhwm(200).expect("set_sndhwm 200 on b");

    assert_eq!(a.get_options().sndhwm, 100);
    assert_eq!(b.get_options().sndhwm, 200);
}
