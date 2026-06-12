//! 1:1 translation of C++ `code/libzmq/perf/inproc_thr.cpp`
//! Run: cargo run --example inproc_thr --release [msg_size] [msg_count]
//! C++ baseline: 30B x 100k → 11,940,298 msg/s
//!
//! Measures the full PUSH→PULL socket path (including socket layer,
//! load-balancer, and fair-queue), matching the C++ benchmark scope.

use std::time::Instant;
use zmq_context::ZContext;
use zmq_context::socket::{SendFlags, RecvFlags};
use zmq_core::socket_type::SocketType;
use zmq_core::message::ZmqMessage;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let msg_size: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let msg_count: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    println!("message size: {} [B]", msg_size);
    println!("message count: {}", msg_count);

    let ctx = ZContext::new();
    let push = ctx.socket(SocketType::Push).unwrap();
    let pull = ctx.socket(SocketType::Pull).unwrap();

    // Use a unique inproc address to avoid conflicts
    let addr = format!("inproc://thr_bench_{}", std::process::id());
    push.bind(&addr).unwrap();
    pull.connect(&addr).unwrap();

    let payload: Vec<u8> = vec![b'x'; msg_size];

    // Spawn receiver thread
    let recv_count = msg_count;
    let recv_handle = std::thread::spawn(move || {
        for _ in 0..recv_count {
            let _ = pull.recv(RecvFlags::NONE).unwrap();
        }
    });

    // Give receiver time to start
    std::thread::sleep(std::time::Duration::from_millis(10));

    let start = Instant::now();

    for _ in 0..msg_count {
        let msg = ZmqMessage::from_slice(&payload);
        push.send(msg, SendFlags::NONE).unwrap();
    }

    recv_handle.join().unwrap();
    let elapsed = start.elapsed().as_secs_f64();

    let msg_per_sec = msg_count as f64 / elapsed;
    let mb_per_sec = (msg_count as f64 * msg_size as f64) / (1024.0 * 1024.0) / elapsed;

    println!("mean throughput: {:.0} [msg/s]", msg_per_sec);
    println!("mean throughput: {:.3} [Mb/s]", mb_per_sec);
    println!("C++ baseline: 11,940,298 msg/s, 2865.672 Mb/s (full PUSH→PULL path)");
    println!("Ratio: {:.1}%", msg_per_sec / 11_940_298.0 * 100.0);
}
