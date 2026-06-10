//! 1:1 translation of C++ `code/libzmq/perf/benchmark_radix_tree.cpp`
//! Run: cargo run --example benchmark_radix_tree --release
//! C++ baseline: radix_tree 103.0 ns/lookup, trie 198.4 ns/lookup

use std::time::Instant;
use zmq_core::data_structures::radix_tree::RadixTree;
use zmq_core::data_structures::trie::SubscriptionTrie;

fn main() {
    let key_count = 10_000usize;
    let query_count = 1_000_000usize;
    let key_size = 20usize;

    println!("keys = {}, queries = {}, key size = {}", key_count, query_count, key_size);

    // Benchmark RadixTree
    let mut rt = RadixTree::new();
    for i in 0..key_count {
        let key: Vec<u8> = (0..key_size).map(|j| ((i + j) % 256) as u8).collect();
        rt.add(&key);
    }
    let start = Instant::now();
    for i in 0..query_count {
        let q: Vec<u8> = (0..key_size).map(|j| ((i + j) % 256) as u8).collect();
        rt.check(&q);
    }
    let rt_ns = start.elapsed().as_nanos() as f64 / query_count as f64;

    // Benchmark Trie
    let mut trie = SubscriptionTrie::new();
    for i in 0..key_count {
        let key: Vec<u8> = (0..key_size).map(|j| ((i + j) % 256) as u8).collect();
        trie.add(&key, i);
    }
    let start = Instant::now();
    for i in 0..query_count {
        let q: Vec<u8> = (0..key_size).map(|j| ((i + j) % 256) as u8).collect();
        trie.has_match(&q);
    }
    let trie_ns = start.elapsed().as_nanos() as f64 / query_count as f64;

    println!("[trie]");
    println!("Average lookup time = {:.1} ns", trie_ns);
    println!("[radix_tree]");
    println!("Average lookup time = {:.1} ns", rt_ns);
    println!();
    println!("C++ baseline: trie 198.4 ns, radix_tree 103.0 ns");
    println!("Trie ratio:     {:.1}%", 198.4 / trie_ns * 100.0);
    println!("RadixTree ratio:{:.1}%", 103.0 / rt_ns * 100.0);
}
