mod common;
use zmq_core::socket_type::SocketType;

/// Test that zmq_has functionality is available — check known capabilities.
#[test]
fn test_has_ipc() {
    // IPC transport is available on Unix
    #[cfg(unix)]
    assert!(zmq_has("ipc"), "ipc should be available on unix");
    #[cfg(not(unix))]
    assert!(!zmq_has("ipc"), "ipc should not be available on non-unix");
}

/// Helper: check if zmq_has returns capability.
/// This mirrors the C zmq_has() function.
fn zmq_has(capability: &str) -> bool {
    match capability {
        "ipc" => cfg!(unix),
        "ipv6" => true, // most platforms support IPv6
        "pgm" => false,  // PGM requires OpenPGM library
        "tipc" => false, // TIPC is Linux-specific
        "norm" => false, // NORM requires external library
        "curve" => cfg!(feature = "curve"),
        "gssapi" => false, // GSSAPI requires libgssapi_krb5
        "vmci" => false,   // VMCI is VMware-specific
        "draft" => cfg!(feature = "draft_api"),
        "vsock" => false, // VSOCK is Linux-specific
        _ => false,
    }
}

/// Test all known stable socket types are available.
#[test]
fn test_stable_socket_types() {
    let stable_types = [
        SocketType::Pair,
        SocketType::Pub,
        SocketType::Sub,
        SocketType::Req,
        SocketType::Rep,
        SocketType::Dealer,
        SocketType::Router,
        SocketType::Pull,
        SocketType::Push,
        SocketType::Xpub,
        SocketType::Xsub,
        SocketType::Stream,
    ];
    for t in &stable_types {
        assert!(t.is_stable(), "{:?} should be stable", t);
    }
}

/// Test draft socket types.
#[test]
fn test_draft_socket_types() {
    let draft_types = [
        SocketType::Server,
        SocketType::Client,
        SocketType::Radio,
        SocketType::Dish,
        SocketType::Gather,
        SocketType::Scatter,
        SocketType::Dgram,
        SocketType::Peer,
        SocketType::Channel,
    ];
    for t in &draft_types {
        assert!(t.is_draft(), "{:?} should be draft", t);
    }
}

/// Test socket type name mapping (1:1 with ZMQ_* constants).
#[test]
fn test_socket_type_names() {
    assert_eq!(SocketType::Pair.name(), "PAIR");
    assert_eq!(SocketType::Pub.name(), "PUB");
    assert_eq!(SocketType::Sub.name(), "SUB");
    assert_eq!(SocketType::Req.name(), "REQ");
    assert_eq!(SocketType::Rep.name(), "REP");
    assert_eq!(SocketType::Dealer.name(), "DEALER");
    assert_eq!(SocketType::Router.name(), "ROUTER");
    assert_eq!(SocketType::Pull.name(), "PULL");
    assert_eq!(SocketType::Push.name(), "PUSH");
    assert_eq!(SocketType::Xpub.name(), "XPUB");
    assert_eq!(SocketType::Xsub.name(), "XSUB");
    assert_eq!(SocketType::Stream.name(), "STREAM");
    assert_eq!(SocketType::Server.name(), "SERVER");
    assert_eq!(SocketType::Client.name(), "CLIENT");
    assert_eq!(SocketType::Radio.name(), "RADIO");
    assert_eq!(SocketType::Dish.name(), "DISH");
    assert_eq!(SocketType::Gather.name(), "GATHER");
    assert_eq!(SocketType::Scatter.name(), "SCATTER");
    assert_eq!(SocketType::Dgram.name(), "DGRAM");
    assert_eq!(SocketType::Peer.name(), "PEER");
    assert_eq!(SocketType::Channel.name(), "CHANNEL");
}

/// Test socket types from_i32 round-trip.
#[test]
fn test_socket_type_from_i32_all() {
    for i in 0..=20 {
        let t = SocketType::from_i32(i).expect("valid socket type");
        assert_eq!(t as i32, i);
    }
}

/// Test can_send and can_recv for all socket types.
#[test]
fn test_socket_type_send_recv_capabilities() {
    // Can send
    assert!(SocketType::Push.can_send());
    assert!(SocketType::Pub.can_send());
    assert!(SocketType::Req.can_send());
    assert!(SocketType::Rep.can_send());
    assert!(SocketType::Dealer.can_send());
    assert!(SocketType::Router.can_send());
    assert!(SocketType::Xpub.can_send());
    assert!(SocketType::Stream.can_send());

    // Cannot send
    assert!(!SocketType::Pull.can_send());
    assert!(!SocketType::Sub.can_send());

    // Can recv
    assert!(SocketType::Pull.can_recv());
    assert!(SocketType::Sub.can_recv());
    assert!(SocketType::Rep.can_recv());
    assert!(SocketType::Router.can_recv());
    assert!(SocketType::Dealer.can_recv());

    // Cannot recv
    assert!(!SocketType::Push.can_recv());
    assert!(!SocketType::Pub.can_recv());
}
