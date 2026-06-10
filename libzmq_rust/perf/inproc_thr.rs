//! 1:1 translation of C++ `code/libzmq/perf/inproc_thr.cpp`
//! Run: cargo run --example inproc_thr --release [msg_size] [msg_count]
//! C++ baseline: 30B x 100k → 11,940,298 msg/s

use std::time::Instant;
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::message::ZmqMessage;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let msg_size: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let msg_count: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    println!("message size: {} [B]", msg_size);
    println!("message count: {}", msg_count);

    let payload: Vec<u8> = vec![b'x'; msg_size];
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    let start = Instant::now();

    for _ in 0..msg_count {
        pipe.write(ZmqMessage::from_slice(&payload), false);
        pipe.flush();
        pipe.check_read();
        let _ = pipe.read();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let msg_per_sec = msg_count as f64 / elapsed;
    let mb_per_sec = (msg_count as f64 * msg_size as f64) / (1024.0 * 1024.0) / elapsed;

    println!("mean throughput: {:.0} [msg/s]", msg_per_sec);
    println!("mean throughput: {:.3} [Mb/s]", mb_per_sec);
    println!("C++ baseline: 11,940,298 msg/s, 2865.672 Mb/s");
    println!("Ratio: {:.1}%", msg_per_sec / 11_940_298.0 * 100.0);
    std::mem::forget(pipe);
}
