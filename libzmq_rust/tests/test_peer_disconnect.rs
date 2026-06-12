//! 1:1 translation of C++ `tests/test_peer_disconnect.cpp`
mod common;

// PEER_DISCONNECT test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "zmq_connect_peer and zmq_disconnect_peer not yet exposed on ZSocket"]
fn test_peer_disconnect() {
    // C++ tests:
    // 1. Create peer1, bind
    // 2. Create peer2, zmq_connect_peer → peer1_routing_id
    // 3. peer2 sends to peer1 with routing_id
    // 4. peer1 receives, captures peer2_routing_id
    // 5. zmq_disconnect_peer(peer1, peer2_routing_id)
    // 6. peer1 sends back → EHOSTUNREACH
}
