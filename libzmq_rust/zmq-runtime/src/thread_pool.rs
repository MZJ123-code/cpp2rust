//! I/O thread pool for distributing connections across threads.
//!
//! 1:1 translation of C++ `ctx_t` I/O thread management +
//! `io_thread_t` per-thread event loop.
//!
//! The thread pool manages a fixed number of I/O threads, each running
//! its own polling loop. New connections are assigned to the least-loaded
//! thread (or by affinity mask), matching the C++ `choose_io_thread()` algorithm.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::poller::Poller;
use zmq_core::mailbox::Mailbox;

// ─── Thread state constants (atomic) ───────────────────────────

const STATE_CREATED: usize = 0;
const STATE_RUNNING: usize = 1;
const STATE_STOPPING: usize = 2;
const STATE_STOPPED: usize = 3;

// ─── IoThread ──────────────────────────────────────────────────

/// A single I/O thread, equivalent to C++ `io_thread_t`.
///
/// Each I/O thread:
/// - Runs a `Poller` for I/O multiplexing
/// - Has a `Mailbox` for receiving commands from other threads
/// - Has a `Signaler` that fires when a new command arrives in the mailbox
/// - Runs an event loop that polls for I/O and processes commands
pub struct IoThread {
    /// Thread ID (index in the pool, matches C++ tid)
    tid: usize,
    /// The poller — owned by this thread, accessed only from the worker
    poller: Option<Poller>,
    /// Mailbox for receiving commands
    mailbox: Option<Mailbox>,
    /// The poller handle for the mailbox signaler
    _mailbox_handle: Option<crate::poller::Handle>,
    /// Current load (from the poller)
    load: Arc<AtomicUsize>,
    /// Thread state (atomic to avoid mutex deadlocks on Windows)
    state: Arc<AtomicUsize>,
    /// Whether the thread has been asked to stop
    stop_flag: Arc<AtomicBool>,
    /// Handle to the spawned thread
    thread_handle: Option<JoinHandle<()>>,
}

impl IoThread {
    /// Create a new I/O thread (not yet started).
    pub fn new(tid: usize) -> io::Result<Self> {
        let poller = Poller::new()?;
        let mailbox = Mailbox::new();

        Ok(Self {
            tid,
            poller: Some(poller),
            mailbox: Some(mailbox.0),
            _mailbox_handle: None,
            load: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(AtomicUsize::new(STATE_CREATED)),
            stop_flag: Arc::new(AtomicBool::new(false)),
            thread_handle: None,
        })
    }

    /// Get the thread ID.
    pub fn tid(&self) -> usize {
        self.tid
    }

    /// Get the current load (number of registered FDs).
    pub fn get_load(&self) -> usize {
        self.load.load(Ordering::Relaxed)
    }

    /// Get the mailbox sender for sending commands to this thread.
    pub fn mailbox_sender(&self) -> Option<zmq_core::mailbox::MailboxSender> {
        // Will be properly wired when the IO thread is spawned
        None
    }

    /// Check if the thread is running.
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_RUNNING
    }

    /// Check if the thread has been created (not yet started or stopped).
    pub fn is_active(&self) -> bool {
        let s = self.state.load(Ordering::Acquire);
        s == STATE_CREATED || s == STATE_RUNNING
    }

    /// Check if the thread has stopped.
    pub fn is_stopped(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_STOPPED
    }

    /// Mark this thread as running.
    pub fn start(&mut self) {
        self.state.store(STATE_RUNNING, Ordering::Release);
    }

    /// Mark this thread for stopping.
    pub fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
        self.state.store(STATE_STOPPING, Ordering::Release);
    }

    /// Mark this thread as stopped.
    pub fn mark_stopped(&self) {
        self.state.store(STATE_STOPPED, Ordering::Release);
    }

    /// Wait for the thread to terminate.
    pub fn join(&mut self) -> thread::Result<()> {
        if let Some(handle) = self.thread_handle.take() {
            handle.join()
        } else {
            Ok(())
        }
    }
}

impl Drop for IoThread {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Release);
    }
}

// ─── ThreadPool ────────────────────────────────────────────────

/// Manages a pool of I/O threads.
///
/// Equivalent to the I/O thread management in C++ `ctx_t`.
///
/// The pool maintains `num_threads` I/O threads. New connections are
/// distributed across threads using a least-loaded strategy (like the
/// C++ `choose_io_thread()`), with optional affinity mask support.
pub struct ThreadPool {
    /// The I/O threads in this pool
    threads: Vec<IoThread>,
    /// Number of threads
    thread_count: usize,
    /// Round-robin counter for connection distribution
    round_robin_counter: AtomicUsize,
    /// Whether the pool has been started
    started: AtomicBool,
}

impl ThreadPool {
    /// Create a new thread pool with the specified number of I/O threads.
    ///
    /// Threads are created but not started. Call `start_all()` to mark them
    /// as running (actual OS thread spawning is done by the transport layer).
    pub fn new(num_threads: usize) -> io::Result<Self> {
        let mut threads = Vec::with_capacity(num_threads);

        for tid in 0..num_threads {
            let thread = IoThread::new(tid)?;
            threads.push(thread);
        }

        Ok(Self {
            threads,
            thread_count: num_threads,
            round_robin_counter: AtomicUsize::new(0),
            started: AtomicBool::new(false),
        })
    }

    /// Get the number of threads in the pool.
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Choose the least-loaded I/O thread for a new connection.
    ///
    /// `affinity` — bitmask of eligible threads (0 = all threads allowed).
    /// Returns `None` if no eligible thread is available.
    ///
    /// Algorithm matches C++ `ctx_t::choose_io_thread()`:
    /// 1. Iterate over threads
    /// 2. Check affinity mask
    /// 3. Pick the one with the minimum load
    pub fn choose_io_thread(&self, affinity: u64) -> Option<usize> {
        let mut min_load = usize::MAX;
        let mut selected: Option<usize> = None;

        for (i, thread) in self.threads.iter().enumerate() {
            // Check affinity mask: bit i must be set, or mask is 0 (all eligible)
            let eligible = affinity == 0 || (affinity & (1u64 << i)) != 0;

            if eligible && thread.is_active() {
                let load = thread.get_load();
                if selected.is_none() || load < min_load {
                    min_load = load;
                    selected = Some(i);
                }
            }
        }

        selected
    }

    /// Choose an I/O thread using round-robin distribution.
    ///
    /// Each call returns the next thread index in sequence.
    pub fn choose_io_thread_round_robin(&self) -> Option<usize> {
        if self.thread_count == 0 {
            return None;
        }
        let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.thread_count;
        if self.threads[idx].is_active() {
            Some(idx)
        } else {
            // Fall back to least-loaded
            self.choose_io_thread(0)
        }
    }

    /// Get a reference to a specific I/O thread.
    pub fn get_thread(&self, index: usize) -> Option<&IoThread> {
        self.threads.get(index)
    }

    /// Get a mutable reference to a specific I/O thread.
    pub fn get_thread_mut(&mut self, index: usize) -> Option<&mut IoThread> {
        self.threads.get_mut(index)
    }

    /// Get all thread indices sorted by load (least loaded first).
    pub fn threads_by_load(&self) -> Vec<usize> {
        let mut indexed: Vec<(usize, usize)> = self
            .threads
            .iter()
            .enumerate()
            .map(|(i, t)| (t.get_load(), i))
            .collect();
        indexed.sort_by_key(|(load, _)| *load);
        indexed.into_iter().map(|(_, i)| i).collect()
    }

    /// Mark all I/O threads as running.
    ///
    /// This is idempotent — calling it multiple times is safe.
    /// (Actual OS thread spawning is done by the transport layer;
    /// this just marks the threads as ready to accept work.)
    pub fn start_all(&mut self) {
        if self.started.load(Ordering::Acquire) {
            return;
        }

        for thread in &mut self.threads {
            thread.start();
        }

        self.started.store(true, Ordering::Release);
    }

    /// Stop all I/O threads and wait for them to terminate.
    pub fn stop_all(&mut self) {
        if !self.started.load(Ordering::Acquire) {
            return;
        }

        for thread in &mut self.threads {
            thread.stop();
        }

        for thread in &mut self.threads {
            let _ = thread.join();
        }

        self.started.store(false, Ordering::Release);
    }

    /// Stop a specific I/O thread by index.
    pub fn stop_thread(&mut self, index: usize) -> io::Result<()> {
        if let Some(thread) = self.threads.get_mut(index) {
            thread.stop();
            let _ = thread.join();
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid thread index: {}", index),
            ))
        }
    }

    /// Check if the pool has been started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// Get the total load across all threads.
    pub fn total_load(&self) -> usize {
        self.threads.iter().map(|t| t.get_load()).sum()
    }

    /// Get the average load per thread.
    pub fn average_load(&self) -> f64 {
        if self.thread_count == 0 {
            return 0.0;
        }
        self.total_load() as f64 / self.thread_count as f64
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for thread in &mut self.threads {
            thread.stop();
        }
        for thread in &mut self.threads {
            let _ = thread.join();
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_thread_pool() {
        let pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.thread_count(), 4);
    }

    #[test]
    fn test_create_single_thread_pool() {
        let pool = ThreadPool::new(1).unwrap();
        assert_eq!(pool.thread_count(), 1);
    }

    #[test]
    fn test_create_empty_pool() {
        let pool = ThreadPool::new(0).unwrap();
        assert_eq!(pool.thread_count(), 0);
        assert!(pool.choose_io_thread(0).is_none());
    }

    #[test]
    fn test_thread_initial_state() {
        let pool = ThreadPool::new(2).unwrap();
        for i in 0..2 {
            let thread = pool.get_thread(i).unwrap();
            assert!(thread.is_active());
            assert!(!thread.is_running());
            assert!(!thread.is_stopped());
        }
    }

    #[test]
    fn test_start_all() {
        let mut pool = ThreadPool::new(2).unwrap();
        assert!(!pool.is_started());
        pool.start_all();
        assert!(pool.is_started());
        for i in 0..2 {
            assert!(pool.get_thread(i).unwrap().is_running());
        }
    }

    #[test]
    fn test_stop_all() {
        let mut pool = ThreadPool::new(1).unwrap();
        pool.start_all();
        assert!(pool.is_started());
        pool.stop_all();
        assert!(!pool.is_started());
    }

    #[test]
    fn test_choose_io_thread_no_affinity() {
        let mut pool = ThreadPool::new(4).unwrap();
        pool.start_all();
        let chosen = pool.choose_io_thread(0);
        assert!(chosen.is_some());
        assert!(chosen.unwrap() < 4);
    }

    #[test]
    fn test_choose_io_thread_with_affinity() {
        let mut pool = ThreadPool::new(4).unwrap();
        pool.start_all();

        // Affinity mask: only thread 2 is eligible (bit 2 = 0b0100 = 4)
        let chosen = pool.choose_io_thread(4);
        assert_eq!(chosen, Some(2));

        // Affinity mask: threads 0 and 3 are eligible (bits 0,3 = 0b1001 = 9)
        let chosen = pool.choose_io_thread(9);
        assert!(chosen == Some(0) || chosen == Some(3));
    }

    #[test]
    fn test_choose_io_thread_none_eligible() {
        let mut pool = ThreadPool::new(2).unwrap();
        pool.start_all();
        pool.stop_all();

        // No active threads => None
        let chosen = pool.choose_io_thread(0);
        assert!(chosen.is_none());
    }

    #[test]
    fn test_round_robin_distribution() {
        let mut pool = ThreadPool::new(4).unwrap();
        pool.start_all();

        let mut counts = vec![0usize; 4];
        for _ in 0..100 {
            if let Some(idx) = pool.choose_io_thread_round_robin() {
                counts[idx] += 1;
            }
        }

        for count in &counts {
            assert!(*count >= 20, "each thread should get roughly 1/4 of connections");
        }
    }

    #[test]
    fn test_threads_by_load() {
        let mut pool = ThreadPool::new(4).unwrap();
        pool.start_all();

        let sorted = pool.threads_by_load();
        assert_eq!(sorted.len(), 4);
        let mut sorted_clone = sorted.clone();
        sorted_clone.sort();
        assert_eq!(sorted_clone, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_total_load() {
        let mut pool = ThreadPool::new(3).unwrap();
        pool.start_all();
        assert_eq!(pool.total_load(), 0);
        assert!((pool.average_load() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_get_thread_out_of_bounds() {
        let pool = ThreadPool::new(2).unwrap();
        assert!(pool.get_thread(5).is_none());
    }

    #[test]
    fn test_stop_specific_thread() {
        let mut pool = ThreadPool::new(2).unwrap();
        pool.start_all();
        let result = pool.stop_thread(0);
        assert!(result.is_ok());
        // Invalid index
        let result = pool.stop_thread(99);
        assert!(result.is_err());
    }

    #[test]
    fn test_start_idempotent() {
        let mut pool = ThreadPool::new(1).unwrap();
        pool.start_all();
        assert!(pool.is_started());
        pool.start_all(); // second start is no-op
        assert!(pool.is_started());
    }

    #[test]
    fn test_stop_idempotent() {
        let mut pool = ThreadPool::new(1).unwrap();
        pool.start_all();
        pool.stop_all();
        assert!(!pool.is_started());
        pool.stop_all(); // second stop is no-op
        assert!(!pool.is_started());
    }

    #[test]
    fn test_tid_matches_index() {
        let pool = ThreadPool::new(3).unwrap();
        for i in 0..3 {
            let thread = pool.get_thread(i).unwrap();
            assert_eq!(thread.tid(), i);
        }
    }

    #[test]
    fn test_load_initial_zero() {
        let pool = ThreadPool::new(5).unwrap();
        for i in 0..5 {
            let thread = pool.get_thread(i).unwrap();
            assert_eq!(thread.get_load(), 0);
        }
    }
}
