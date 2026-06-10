//! Criterion benchmarks for core data structures.
//! 1:1 translation of C++ `perf/` benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use zmq_core::data_structures::ypipe::YPipe;
use zmq_core::data_structures::yqueue::YQueue;
use zmq_core::message::ZmqMessage;

fn bench_yqueue_push_pop(c: &mut Criterion) {
    c.bench_function("yqueue_push_pop_1000", |b| {
        let mut queue = YQueue::<i32, 256>::new();
        b.iter(|| {
            for i in 0..1000 {
                queue.push();
                *queue.back_mut() = black_box(i);
                black_box(queue.front());
                queue.pop();
            }
        });
    });
}

fn bench_ypipe_write_read(c: &mut Criterion) {
    c.bench_function("ypipe_write_flush_read_1000", |b| {
        let mut pipe = YPipe::<i32, 256>::new();
        let mut i = 0i32;
        b.iter(|| {
            pipe.write(i, false);
            pipe.flush();
            pipe.check_read();
            black_box(pipe.read());
            i = i.wrapping_add(1);
        });
    });
}

fn bench_msg_create(c: &mut Criterion) {
    c.bench_function("msg_create_small", |b| {
        b.iter(|| {
            ZmqMessage::from_slice(black_box(b"hello"));
        });
    });
}

criterion_group!(benches, bench_yqueue_push_pop, bench_ypipe_write_read, bench_msg_create);
criterion_main!(benches);
