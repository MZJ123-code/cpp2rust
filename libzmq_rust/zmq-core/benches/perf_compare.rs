//! Performance comparison benchmarks — matches C++ `perf/` tests.
//! C++ baselines (from build_mingw/bin):
//!   inproc_lat (30B, 10k): 8.414 us avg
//!   inproc_thr (30B, 100k): 11,940,298 msg/s

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use zmq_core::data_structures::ypipe::YPipe;

/// Latency: write+flush → check_read+read, round-trip
fn bench_inproc_lat(c: &mut Criterion) {
    c.bench_function("inproc_lat_ypipe", |b| {
        let mut pipe = YPipe::<i32, 256>::new();
        let mut value: i32 = 0;
        b.iter(|| {
            pipe.write(value, false);
            pipe.flush();
            if pipe.check_read() {
                black_box(pipe.read());
            }
            value = value.wrapping_add(1);
        });
    });
}

/// Throughput: batch write+flush+read through ypipe
fn bench_inproc_thr(c: &mut Criterion) {
    c.bench_function("inproc_thr_ypipe_batch1000", |b| {
        let mut pipe = YPipe::<i32, 256>::new();
        b.iter(|| {
            for i in 0..1000i32 {
                pipe.write(i, false);
                pipe.flush();
                pipe.check_read();
                black_box(pipe.read());
            }
        });
    });
}

criterion_group!(benches, bench_inproc_lat, bench_inproc_thr);
criterion_main!(benches);
