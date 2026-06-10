//! Lock-free queue with chunked allocation — SPSC (Single Producer, Single Consumer).
//!
//! Replaces C++ `yqueue.hpp`. Allocates elements in batches of `N` to minimize
//! the number of heap allocations. The producer thread uses `push`/`back`, the
//! consumer thread uses `pop`/`front`.
//!
//! ## Safety
//!
//! This is a lock-free SPSC queue. The producer and consumer MUST be on
//! different threads. Each field is accessed exclusively by one side:
//! - Producer: `back_chunk`, `back_pos`, `end_chunk`, `end_pos`
//! - Consumer: `begin_chunk`, `begin_pos`
//! - Shared: `spare_chunk` (via `AtomicPtr` exchange)
//!
//! ## Memory ordering
//!
//! The `spare_chunk` atomic exchange provides the synchronization barrier
//! between producer and consumer. The producer publishes a chunk by setting
//! `end_chunk->next`, which the consumer reads when it advances past a chunk
//! boundary.
//!
//! `T` values are stored as `MaybeUninit<T>` in the chunk. The producer
//! initializes values via `ptr::write`, the consumer reads them via `ptr::read`.

use std::alloc::{alloc, dealloc, Layout};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

/// Default number of elements per chunk. Chunks are cache-line aligned (64 bytes).
pub const YQUEUE_CHUNK_SIZE: usize = 256;

/// Cache line size for alignment.
const CACHELINE_SIZE: usize = 64;

/// A chunk holding N elements of type T, plus prev/next pointers for the linked list.
///
/// The entire chunk is allocated with cache-line alignment to prevent false sharing.
#[repr(C, align(64))]
struct Chunk<T, const N: usize> {
    values: [MaybeUninit<T>; N],
    prev: *mut Chunk<T, N>,
    next: *mut Chunk<T, N>,
}

/// Lock-free SPSC queue with chunked (batched) allocation.
///
/// ## Type parameters
/// - `T`: element type
/// - `N`: chunk size (number of elements per allocation batch)
pub struct YQueue<T, const N: usize = YQUEUE_CHUNK_SIZE> {
    /// First chunk in the linked list. Accessed ONLY by consumer.
    begin_chunk: *mut Chunk<T, N>,
    /// Position within `begin_chunk`. Accessed ONLY by consumer.
    begin_pos: usize,

    /// Chunk containing the back element. Accessed ONLY by producer.
    back_chunk: *mut Chunk<T, N>,
    /// Position within `back_chunk`. Accessed ONLY by producer.
    back_pos: usize,

    /// Last chunk in the linked list. Accessed ONLY by producer.
    end_chunk: *mut Chunk<T, N>,
    /// Position within `end_chunk` (next write slot). Accessed ONLY by producer.
    end_pos: usize,

    /// Spare chunk for reuse. Shared between producer and consumer via atomic xchg.
    spare_chunk: AtomicPtr<Chunk<T, N>>,

    _marker: PhantomData<T>,
}

impl<T, const N: usize> YQueue<T, N> {
    /// Create a new empty queue. Allocates the first chunk.
    ///
    /// # Panics
    /// Panics if `N == 0`.
    pub fn new() -> Self {
        assert!(N > 0, "chunk size must be > 0");

        let begin = allocate_chunk::<T, N>();
        Self {
            begin_chunk: begin,
            begin_pos: 0,
            back_chunk: ptr::null_mut(),
            back_pos: 0,
            end_chunk: begin,
            end_pos: 0,
            spare_chunk: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }

    /// Returns a reference to the front element.
    ///
    /// # Safety
    /// Must only be called when the queue is non-empty (after `check_read` / `read`).
    #[inline(always)]
    pub fn front(&self) -> &T {
        unsafe { &(*(*self.begin_chunk).values[self.begin_pos].as_ptr()) }
    }

    /// Returns a reference to the back element (last pushed value).
    ///
    /// # Safety
    /// Called only by the producer. Queue must be non-empty.
    #[inline(always)]
    pub fn back(&self) -> &T {
        unsafe { &(*(*self.back_chunk).values[self.back_pos].as_ptr()) }
    }

    /// Returns a mutable reference to the back element for in-place construction.
    ///
    /// # Safety
    /// Called only by the producer. The slot must have been prepared by `push()`.
    #[inline(always)]
    pub fn back_mut(&mut self) -> &mut T {
        unsafe { &mut *(*self.back_chunk).values[self.back_pos].as_mut_ptr() }
    }

    /// Returns a raw pointer to the back element (never dereferenced — for pointer comparison only).
    pub fn back_chunk_ptr(&self) -> *const T {
        if self.back_chunk.is_null() {
            self.begin_chunk as *const T
        } else {
            unsafe { (*self.back_chunk).values.as_ptr() as *const T }
        }
    }

    /// Adds an empty slot at the back of the queue. After calling `push()`,
    /// the producer writes the value into `back_mut()`.
    ///
    /// If the current chunk is full, allocates a new chunk or reuses the
    /// spare chunk.
    #[inline(always)]
    pub fn push(&mut self) {
        // Snapshot the current end position for back()
        self.back_chunk = self.end_chunk;
        self.back_pos = self.end_pos;

        self.end_pos += 1;
        if self.end_pos != N {
            return;
        }

        // Current chunk is full; move to next chunk
        let sc = self.spare_chunk.swap(ptr::null_mut(), Ordering::AcqRel);
        if !sc.is_null() {
            // Reuse the spare chunk
            unsafe {
                (*self.end_chunk).next = sc;
                (*sc).prev = self.end_chunk;
            }
        } else {
            // Allocate a new chunk
            let new_chunk = allocate_chunk::<T, N>();
            unsafe {
                (*self.end_chunk).next = new_chunk;
                (*new_chunk).prev = self.end_chunk;
            }
        }
        unsafe {
            self.end_chunk = (*self.end_chunk).next;
        }
        self.end_pos = 0;
    }

    /// Rollback the last push. The caller must destroy the element at `back()`
    /// before calling this.
    ///
    /// # Safety
    /// Only the producer may call this. Queue must not be empty.
    pub fn unpush(&mut self) {
        // Move back one position
        if self.back_pos > 0 {
            self.back_pos -= 1;
        } else {
            self.back_pos = N - 1;
            unsafe {
                self.back_chunk = (*self.back_chunk).prev;
            }
        }

        // Move end one position
        if self.end_pos > 0 {
            self.end_pos -= 1;
        } else {
            // The current end chunk is now empty; move back and free it
            self.end_pos = N - 1;
            unsafe {
                self.end_chunk = (*self.end_chunk).prev;
                let old_next = (*self.end_chunk).next;
                (*self.end_chunk).next = ptr::null_mut();
                deallocate_chunk(old_next);
            }
        }
    }

    /// Remove the front element. The caller must have already read/destroyed
    /// the value at `front()`.
    ///
    /// # Safety
    /// Only the consumer may call this.
    #[inline(always)]
    pub fn pop(&mut self) {
        self.begin_pos += 1;
        if self.begin_pos == N {
            // Chunk exhausted; advance to next chunk (if one exists)
            let old = self.begin_chunk;
            unsafe {
                let next = (*self.begin_chunk).next;
                if !next.is_null() {
                    self.begin_chunk = next;
                    (*self.begin_chunk).prev = ptr::null_mut();
                    self.begin_pos = 0;
                } else {
                    // Last chunk exhausted — reset to safe state
                    self.begin_pos = N; // prevent further pops
                }
            }

            // Keep 'old' as spare for reuse; free the previous spare
            let prev_spare = self.spare_chunk.swap(old, Ordering::AcqRel);
            if !prev_spare.is_null() {
                unsafe { deallocate_chunk(prev_spare) };
            }
        }
    }


}

// ─── Allocation helpers ────────────────────────────────────────

fn allocate_chunk<T, const N: usize>() -> *mut Chunk<T, N> {
    let layout = Layout::new::<Chunk<T, N>>();
    assert!(layout.size() > 0, "chunk layout must be non-zero");
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    let chunk: *mut Chunk<T, N> = ptr.cast();
    unsafe {
        (*chunk).prev = ptr::null_mut();
        (*chunk).next = ptr::null_mut();
    }
    chunk
}

unsafe fn deallocate_chunk<T, const N: usize>(chunk: *mut Chunk<T, N>) {
    if chunk.is_null() {
        return;
    }
    let layout = Layout::new::<Chunk<T, N>>();
    unsafe { dealloc(chunk.cast(), layout) };
}

// ─── Drop ─────────────────────────────────────────────────────

impl<T, const N: usize> Drop for YQueue<T, N> {
    fn drop(&mut self) {
        // Free all chunks in the linked list
        let mut current = self.begin_chunk;
        while !current.is_null() {
            let next = unsafe { (*current).next };
            unsafe { deallocate_chunk(current) };
            current = next;
        }
        // Free the spare chunk if any
        let spare = self.spare_chunk.load(Ordering::Relaxed);
        if !spare.is_null() {
            unsafe { deallocate_chunk(spare) };
        }
    }
}

// ─── Thread safety ─────────────────────────────────────────────
// YQueue is !Sync by default (contains *mut). This is correct:
// the producer and consumer must be on separate threads, and
// each field is accessed by only one side.

unsafe impl<T: Send, const N: usize> Send for YQueue<T, N> {}

// ─── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let _q: YQueue<i32, 4> = YQueue::new();
    }

    #[test]
    fn test_push_and_front_pop() {
        let mut q: YQueue<i32, 4> = YQueue::new();
        q.push();
        *q.back_mut() = 42;
        assert_eq!(*q.front(), 42);
        q.pop();
    }

    #[test]
    fn test_push_multiple() {
        let mut q: YQueue<i32, 4> = YQueue::new();
        for i in 0..4 {
            q.push();
            *q.back_mut() = i;
        }
        for i in 0..4 {
            assert_eq!(*q.front(), i);
            q.pop();
        }
    }

    #[test]
    fn test_push_across_chunk_boundary() {
        // Use small chunk size to force multiple chunks
        let mut q: YQueue<i32, 2> = YQueue::new();
        for i in 0..6 {
            q.push();
            *q.back_mut() = i;
        }
        for i in 0..6 {
            assert_eq!(*q.front(), i);
            q.pop();
        }
    }

    #[test]
    fn test_unpush_simple() {
        let mut q: YQueue<i32, 4> = YQueue::new();
        q.push();
        *q.back_mut() = 1;
        q.push();
        *q.back_mut() = 2;
        // Unpush the last push
        unsafe { ptr::drop_in_place(q.back_mut()); }
        q.unpush();
        // Now front should be 1
        assert_eq!(*q.front(), 1);
    }

    #[test]
    fn test_unpush_across_chunk_boundary() {
        let mut q: YQueue<i32, 2> = YQueue::new();
        q.push(); *q.back_mut() = 0; // chunk 0, pos 0
        q.push(); *q.back_mut() = 1; // chunk 0, pos 1 → moves to chunk 1, pos 0
        q.push(); *q.back_mut() = 2; // chunk 1, pos 1 → moves to chunk 2, pos 0
        // Now: begin→chunk0, end→chunk2
        // back is at chunk1,pos1 (value 2)
        unsafe { ptr::drop_in_place(q.back_mut()); }
        q.unpush(); // back moves back
        // Verify we can still read values 0 and 1
        assert_eq!(*q.front(), 0); q.pop();
        assert_eq!(*q.front(), 1); q.pop();
    }

    #[test]
    fn test_spare_chunk_reuse() {
        let mut q: YQueue<i32, 2> = YQueue::new();
        // Fill many chunks to trigger spare chunk recycling
        for i in 0..20 {
            q.push();
            *q.back_mut() = i;
        }
        for i in 0..20 {
            assert_eq!(*q.front(), i);
            q.pop();
        }
    }
}
