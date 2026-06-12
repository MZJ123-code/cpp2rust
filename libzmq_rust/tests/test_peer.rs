//! 1:1 translation of C++ `tests/test_peer.cpp`
mod common;

// PEER test uses only #[ignore] stubs; no active imports needed.

#[test]
#[ignore = "zmq_connect_peer not yet exposed on ZSocket"]
fn test_peer() {
    // C++ tests:
    // 1. Create peer1, bind
    // 2. Create peer2, zmq_connect_peer(peer2, endpoint) → gets peer1_routing_id
    // 3. peer2 sends to peer1 using routing_id
    // 4. peer1 receives, captures peer2_routing_id
    // 5. peer1 sends back to peer2 using that routing_id
    // 6. peer2 receives, verifies routing_id matches peer1
}
