//! 1:1 translation of C++ `tests/test_shutdown_stress.cpp`.
//!
//! Stress test: many parallel connections and shutdown.
//! Uses inproc transport with multiple threads.
mod common;

use common::*;
use std::sync::Arc;
use std::thread;
use zmq_core::socket_type::SocketType;
use zmq_context::ZContext;

const THREAD_COUNT: usize = 100;

#[test]
fn test_shutdown_stress() {
    for _j in 0..10 {
        let ctx = Arc::new(ZContext::new());

        let pub_socket = ctx.socket(SocketType::Pub).unwrap();
        let ep = "inproc://shutdown-stress";
        pub_socket.bind(ep).unwrap();

        let mut handles = Vec::new();
        for _i in 0..THREAD_COUNT {
            let ctx_clone = Arc::clone(&ctx);
            handles.push(thread::spawn(move || {
                let sock = ctx_clone.socket(SocketType::Sub).unwrap();
                let _ = sock.connect(ep);
                // Start closing the socket while connecting is underway
                let _ = sock.close();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let _ = pub_socket.close();
        let _ = ctx.shutdown();
    }
}
