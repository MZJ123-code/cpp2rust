//! Lock-free SPSC pipe — write/flush/read with batching.
//!
//! Replaces C++ `ypipe.hpp`. Built on top of `YQueue`, adding:
//! - `write(value, incomplete)` — stage a value, optionally incomplete (multi-part)
//! - `flush()` — atomically publish all completed writes to the consumer
//! - `check_read()` — check if data is available (prefetch)
//! - `read(&mut T)` — consume a value
//! - `probe(fn)` — inspect the front value without removing it
//!
//! ## Mechanism
//!
//! The pipe uses a terminator element in the yqueue. Three producer-side
//! pointers (`w`, `f`) track write progress. One atomic pointer (`c`) is
//! the single contention point: the producer sets it to the latest flushed
//! position, and the consumer reads it to find available data.
//!
//! When the consumer sees `c == NULL`, it means the producer explicitly
//! signaled that there's no data (reader should sleep).

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::yqueue::{YQueue, YQUEUE_CHUNK_SIZE};

/// Lock-free SPSC pipe.
///
/// Only one thread may write; only one thread may read.
pub struct YPipe<T, const N: usize = YQUEUE_CHUNK_SIZE> {
    /// The underlying chunked queue. Contains a terminator element.
    queue: YQueue<T, N>,

    /// Pointer to the first un-flushed item. Writer only.
    w: *const T,

    /// Pointer to the first un-prefetched item. Reader only.
    r: *const T,

    /// Pointer to the first item to be flushed. Writer only.
    f: *const T,

    /// Atomic pointer — the single point of contention.
    /// Points past the last flushed item. NULL means reader is asleep.
    c: AtomicPtr<T>,
}

impl<T, const N: usize> YPipe<T, N> {
    /// Create a new pipe. The terminator element is pre-inserted.
    pub fn new() -> Self {
        let mut queue = YQueue::<T, N>::new();
        queue.push(); // terminator element (uninitialized, never dropped)
        let terminator_ptr: *const T = queue.back_mut() as *const T;

        let c = AtomicPtr::new(terminator_ptr as *mut T);

        Self {
            queue,
            w: terminator_ptr,
            r: terminator_ptr,
            f: terminator_ptr,
            c,
        }
    }

    /// Write a value to the pipe without flushing.
    ///
    /// If `incomplete` is true, the value is part of a multi-part message
    /// and will NOT be flushed until a subsequent `write` with `incomplete=false`.
    #[inline(always)]
    pub fn write(&mut self, value: T, incomplete: bool) {
        // Place the value at the current back slot (overwrites old terminator)
        unsafe { ptr::write(self.queue.back_mut(), value) };
        self.queue.push();
        // The new terminator at back_mut() is uninitialized.
        // Drop handles this by checking _has_data before dropping.

        // Advance flush pointer if the message is complete
        if !incomplete {
            self.f = self.queue.back() as *const T;
        }
    }

    /// Pop an incomplete item. Returns `Some(value)` if an incomplete item
    /// exists and was removed, `None` if all pending items are complete.
    pub fn unwrite(&mut self) -> Option<T> {
        if self.f as *const T == self.queue.back() as *const T {
            return None;
        }
        self.queue.unpush();
        // SAFETY: the value was written by this producer and is being removed
        let value = unsafe { ptr::read(self.queue.back()) };
        Some(value)
    }

    /// Flush all completed writes to make them visible to the consumer.
    ///
    /// Returns `true` if the reader is active. Returns `false` if the reader
    /// is asleep (c was NULL), meaning the caller should wake it.
    #[inline(always)]
    pub fn flush(&mut self) -> bool {
        // Nothing to flush
        if self.w == self.f {
            return true;
        }

        // Try to CAS c from w to f
        let old = self
            .c
            .compare_exchange(
                self.w as *mut T,
                self.f as *mut T,
                Ordering::AcqRel,
                Ordering::Acquire,
            );

        match old {
            Ok(_) => {
                // CAS succeeded — reader is alive
                self.w = self.f;
                true
            }
            Err(actual) => {
                // CAS failed — reader is asleep (c was not w, likely NULL)
                // Set c directly (non-atomic is OK since reader is asleep)
                self.c.store(self.f as *mut T, Ordering::Release);
                self.w = self.f;
                // Return false to tell caller to wake the reader
                actual.is_null()
            }
        }
    }

    /// Check if data is available for reading. Prefetches from the atomic `c`.
    #[inline(always)]
    pub fn check_read(&mut self) -> bool {
        // Was a value already prefetched?
        let front_ptr = self.queue.front() as *const T;
        if front_ptr != self.r && !self.r.is_null() {
            return true;
        }

        // Try to prefetch: CAS c from front_ptr to NULL
        let old = self.c.compare_exchange(
            front_ptr as *mut T,
            ptr::null_mut(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        match old {
            Ok(actual) => {
                // CAS succeeded — c was pointing to the front, meaning
                // there's nothing new. Set r to front and return false.
                self.r = actual as *const T;
                false
            }
            Err(actual) => {
                // CAS failed — c was different, meaning new data is available
                self.r = actual as *const T;
                // If r equals front or is NULL, nothing to read
                !(self.r == front_ptr || self.r.is_null())
            }
        }
    }

    /// Read a value from the pipe. Returns `Some(value)` if data was available.
    #[inline(always)]
    pub fn read(&mut self) -> Option<T> {
        if !self.check_read() {
            return None;
        }
        // SAFETY: the value is owned by this consumer, the producer won't touch it
        let value = unsafe { ptr::read(std::ptr::from_ref(self.queue.front()) as *mut T) };
        self.queue.pop();
        Some(value)
    }

    /// Inspect the front element with a probe function. Returns `Some(result)`
    /// if data is available, `None` otherwise.
    ///
    /// The probe function receives a reference to the front element.
    /// The element is NOT removed from the queue.
    pub fn probe<F, R>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        if !self.check_read() {
            return None;
        }
        Some(f(self.queue.front()))
    }
}

// ─── Thread safety ─────────────────────────────────────────────
unsafe impl<T: Send, const N: usize> Send for YPipe<T, N> {}

// ─── Drop ─────────────────────────────────────────────────────
impl<T, const N: usize> Drop for YPipe<T, N> {
    fn drop(&mut self) {
        // Drop any remaining values that were flushed but not consumed
        while self.check_read() {
            let _ = self.read(); // ptr::read drops the value
        }
        // The remaining element is the uninitialized terminator.
        // Just pop it without dropping — it's uninitialized memory.
        self.queue.pop();
    }
}

// ─── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let _pipe: YPipe<i32, 4> = YPipe::new();
    }

    #[test]
    fn test_check_read_empty() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        assert!(!pipe.check_read());
    }

    #[test]
    fn test_read_empty() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        assert!(pipe.read().is_none());
    }

    #[test]
    fn test_write_without_flush_not_readable() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(42, false);
        // Write without flush should NOT be visible
        assert!(!pipe.check_read());
        assert!(pipe.read().is_none());
    }

    #[test]
    fn test_write_flush_and_read() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(42, false);
        assert!(pipe.flush());
        assert!(pipe.check_read());
        assert_eq!(pipe.read(), Some(42));
    }

    #[test]
    fn test_multiple_writes() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        for i in 0..10 {
            pipe.write(i, false);
        }
        assert!(pipe.flush());
        for i in 0..10 {
            assert!(pipe.check_read());
            assert_eq!(pipe.read(), Some(i));
        }
        assert!(!pipe.check_read());
    }

    #[test]
    fn test_incomplete_write() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(1, true); // incomplete
        pipe.write(2, false); // completes the message
        // Incomplete write should not be flushed
        assert!(pipe.flush());
        // Both become visible together
        assert!(pipe.check_read());
        assert_eq!(pipe.read(), Some(1));
        assert!(pipe.check_read());
        assert_eq!(pipe.read(), Some(2));
    }

    #[test]
    fn test_unwrite() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(1, false);
        pipe.write(2, true); // incomplete
        // Can't unwrite the completed message
        assert_eq!(pipe.unwrite(), Some(2));
        // Now only 1 is pending
        assert!(pipe.flush());
        assert_eq!(pipe.read(), Some(1));
    }

    #[test]
    fn test_probe() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(99, false);
        pipe.flush();
        let result = pipe.probe(|&x| x * 2);
        assert_eq!(result, Some(198));
        // Probe doesn't consume
        assert_eq!(pipe.read(), Some(99));
    }

    #[test]
    fn test_flush_returns_true_when_reader_active() {
        let mut pipe: YPipe<i32, 4> = YPipe::new();
        pipe.write(1, false);
        // First flush with active reader (c == w initially)
        assert!(pipe.flush());
    }
}
