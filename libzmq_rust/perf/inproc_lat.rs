//! 1:1 translation of C++ `code/libzmq/perf/inproc_lat.cpp`
//! Run: cargo run --example inproc_lat --release [msg_size] [roundtrip_count]
//! C++ baseline: 30B x 10k → 8.414 us avg roundtrip
//!
//! Measures REQ→REP roundtrip latency through the full socket path,
//! matching the C++ benchmark scope.

use std::time::Instant;
use zmq_context::ZContext;
use zmq_context::socket::{SendFlags, RecvFlags};
use zmq_core::socket_type::SocketType;
use zmq_core::message::ZmqMessage;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let msg_size: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let roundtrips: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    println!("message size: {} [B]", msg_size);
    println!("roundtrip count: {}", roundtrips);

    let ctx = ZContext::new();
    let req = ctx.socket(SocketType::Req).unwrap();
    let rep = ctx.socket(SocketType::Rep).unwrap();

    let addr = format!("inproc://lat_bench_{}", std::process::id());
    rep.bind(&addr).unwrap();
    req.connect(&addr).unwrap();

    let payload: Vec<u8> = vec![b'x'; msg_size];

    // Spawn replier thread
    let rep_handle = std::thread::spawn(move || {
        for _ in 0..roundtrips {
            let msg = rep.recv(RecvFlags::NONE).unwrap();
            rep.send(msg, SendFlags::NONE).unwrap();
        }
    });

    std::thread::sleep(std::time::Duration::from_millis(10));

    let start = Instant::now();

    for _ in 0..roundtrips {
        let msg = ZmqMessage::from_slice(&payload);
        req.send(msg, SendFlags::NONE).unwrap();
        let _ = req.recv(RecvFlags::NONE).unwrap();
    }

    rep_handle.join().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    let avg_latency = elapsed / (roundtrips as f64) * 1_000_000.0;

    println!("average latency: {:.3} [us]", avg_latency);
    println!("C++ baseline: 8.414 [us] (REQ→REP full socket path)");
    println!("Ratio: {:.1}%", 8.414 / avg_latency * 100.0);
}
