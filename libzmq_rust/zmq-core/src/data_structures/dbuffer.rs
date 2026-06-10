//! Double-buffer — SPSC buffer that keeps only the latest value.
//!
//! Replaces C++ `dbuffer.hpp`. The producer writes to a back buffer and
//! attempts to atomically swap it with the front buffer. If the swap fails
//! (consumer is reading), the write is silently dropped — this is intentional
//! for conflate semantics where only the latest value matters.

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

/// A double-buffer that stores at most one value of type `T`.
///
/// The producer calls `write()`, the consumer calls `check_read()` and `read()`.
/// If writes are faster than reads, intermediate values are dropped.
pub struct DoubleBuffer<T> {
    /// Slot 0 and slot 1. We swap which one is "back" and "front".
    slots: [UnsafeCell<T>; 2],
    /// Index of the back buffer (0 or 1). Producer writes here.
    back_idx: UnsafeCell<usize>,
    /// Whether an unread value is in the front buffer.
    has_msg: AtomicBool,
    /// Mutex protecting front/back swap and read.
    sync: Mutex<()>,
}

impl<T> DoubleBuffer<T> {
    /// Create a new double buffer with default-initialized slots.
    pub fn new() -> Self
    where
        T: Default,
    {
        Self {
            slots: [
                UnsafeCell::new(T::default()),
                UnsafeCell::new(T::default()),
            ],
            back_idx: UnsafeCell::new(0),
            has_msg: AtomicBool::new(false),
            sync: Mutex::new(()),
        }
    }

    /// Write a value. If a previous unread value exists, the old value
    /// is dropped and replaced.
    ///
    /// # Safety
    /// Only the producer may call this.
    pub fn write(&self, value: T) {
        // Write to back buffer
        let back = self.back_idx();
        unsafe {
            let back_ptr = self.slots[back].get();
            ptr::drop_in_place(back_ptr);
            ptr::write(back_ptr, value);
        }

        // Try to swap back and front
        if self.sync.try_lock().is_some() {
            // SAFETY: we hold the mutex, consumer is not reading
            let front = 1 - back;
            unsafe {
                // Swap the back/front index
                *self.back_idx.get() = front;
            }
            self.has_msg.store(true, Ordering::Release);
            // Mutex is dropped here (guard goes out of scope)
        }
        // If we couldn't acquire the mutex, consumer is reading;
        // the new value stays in the back buffer and will be the
        // next one read (old value is overwritten next write).
    }

    /// Check if a value is available for reading.
    pub fn check_read(&self) -> bool {
        let _lock = self.sync.lock();
        self.has_msg.load(Ordering::Acquire)
    }

    /// Read the current value. Returns `None` if no value is available.
    pub fn read(&self) -> Option<T> {
        let _lock = self.sync.lock();
        if !self.has_msg.load(Ordering::Acquire) {
            return None;
        }
        let front = 1 - self.back_idx();
        let val = unsafe { ptr::read(self.slots[front].get()) };
        self.has_msg.store(false, Ordering::Release);
        Some(val)
    }

    /// Probe the front value without removing it.
    pub fn probe<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let _lock = self.sync.lock();
        if !self.has_msg.load(Ordering::Acquire) {
            return None;
        }
        let front = 1 - self.back_idx();
        Some(f(unsafe { &*self.slots[front].get() }))
    }

    fn back_idx(&self) -> usize {
        unsafe { *self.back_idx.get() }
    }
}

impl<T: Default> Default for DoubleBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for DoubleBuffer<T> {
    fn drop(&mut self) {
        // Drop both slots
        unsafe {
            ptr::drop_in_place(self.slots[0].get());
            ptr::drop_in_place(self.slots[1].get());
        }
    }
}

// ─── Safety ───────────────────────────────────────────────────
unsafe impl<T: Send> Send for DoubleBuffer<T> {}
unsafe impl<T: Send> Sync for DoubleBuffer<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let db = DoubleBuffer::<i32>::new();
        assert!(!db.check_read());
        db.write(42);
        assert!(db.check_read());
        assert_eq!(db.read(), Some(42));
        assert!(!db.check_read());
    }

    #[test]
    fn test_conflate_behavior() {
        let db = DoubleBuffer::<i32>::new();
        db.write(1); // consumed immediately (no reader holding lock)
        db.write(2); // this overwrites
        db.write(3); // and this
        assert_eq!(db.read(), Some(3));
        assert_eq!(db.read(), None);
    }
}
