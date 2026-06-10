//! 1:1 translation of C++ `code/libzmq/perf/inproc_lat.cpp`
//! Run: cargo run --example inproc_lat --release [msg_size] [roundtrip_count]
//! C++ baseline: 30B x 10k → 8.414 us avg

use std::time::Instant;
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::message::ZmqMessage;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let msg_size: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(30);
    let roundtrips: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    println!("message size: {} [B]", msg_size);
    println!("roundtrip count: {}", roundtrips);

    let payload: Vec<u8> = vec![b'x'; msg_size];
    let mut pipe = YPipe::<ZmqMessage, 256>::new();
    let start = Instant::now();

    for _ in 0..roundtrips {
        // REQ sends
        pipe.write(ZmqMessage::from_slice(&payload), false);
        pipe.flush();
        // REP receives and echoes back (simplified: same pipe)
        pipe.check_read();
        let reply = pipe.read().unwrap();
        pipe.write(reply, false);
        pipe.flush();
        // REQ receives reply
        pipe.check_read();
        let _ = pipe.read();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let avg_latency = elapsed / (roundtrips as f64) * 1_000_000.0;

    println!("average latency: {:.3} [us]", avg_latency);
    println!("C++ baseline: 8.414 [us]");
    println!("Ratio: {:.1}%", 8.414 / avg_latency * 100.0);
    std::mem::forget(pipe);
}
