//! Fair queue — round-robin fan-in from multiple pipes.
//!
//! Replaces C++ `fq.hpp`. Receives messages from multiple attached pipes,
//! servicing them in round-robin order so no pipe starves.
//!
//! Unlike the C++ version which stores the pipes in an array with an
//! active/passive split for O(1) scheduling, this version uses a simple
//! round-robin iterator. For the initial implementation, correctness is
//! prioritized over the O(1) micro-optimization.

/// A fair queue that round-robins between attached pipes.
///
/// Pipes are identified by `usize` IDs. The queue tracks which pipes
/// have data available and services them fairly.
#[derive(Debug, Default)]
pub struct FairQueue {
    /// Pipe IDs in insertion order
    pipes: Vec<usize>,
    /// Which pipes are currently active (have data)
    active: Vec<bool>,
    /// Current round-robin position
    current: usize,
    /// Number of active pipes (for quick emptiness check)
    active_count: usize,
}

impl FairQueue {
    pub fn new() -> Self {
        Self {
            pipes: Vec::new(),
            active: Vec::new(),
            current: 0,
            active_count: 0,
        }
    }

    /// Attach a pipe to this queue. Returns its index in the pipe list.
    pub fn attach(&mut self, pipe_id: usize) {
        self.pipes.push(pipe_id);
        self.active.push(false);
    }

    /// Mark a pipe as terminated (remove it from the queue).
    /// Uses swap-remove for O(1) deletion.
    pub fn terminated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            let last = self.pipes.len() - 1;
            if pos != last {
                self.pipes.swap(pos, last);
                self.active.swap(pos, last);
                if self.active[pos] {
                    self.active_count -= 1;
                }
            }
            self.pipes.pop();
            self.active.pop();
            if self.current >= self.pipes.len() {
                self.current = 0;
            }
        }
    }

    /// Mark a pipe as having data available.
    pub fn activated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            if !self.active[pos] {
                self.active[pos] = true;
                self.active_count += 1;
            }
        }
    }

    /// Mark a pipe as having no data available.
    pub fn deactivated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            if self.active[pos] {
                self.active[pos] = false;
                self.active_count -= 1;
            }
        }
    }

    /// Whether any pipe has data available.
    pub fn has_in(&self) -> bool {
        self.active_count > 0
    }

    /// Get the next pipe ID that has data available (round-robin).
    /// Returns `None` if no pipe has data.
    pub fn next_active(&mut self) -> Option<usize> {
        if self.active_count == 0 {
            return None;
        }
        let len = self.pipes.len();
        for _ in 0..len {
            if self.current >= len {
                self.current = 0;
            }
            if self.active[self.current] {
                let id = self.pipes[self.current];
                self.current = (self.current + 1) % len;
                return Some(id);
            }
            self.current = (self.current + 1) % len;
        }
        None
    }

    /// Number of attached pipes.
    pub fn pipe_count(&self) -> usize {
        self.pipes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_queue() {
        let mut fq = FairQueue::new();
        assert!(!fq.has_in());
        assert_eq!(fq.next_active(), None);
    }

    #[test]
    fn test_round_robin() {
        let mut fq = FairQueue::new();
        fq.attach(1);
        fq.attach(2);
        fq.attach(3);
        fq.activated(1);
        fq.activated(2);
        fq.activated(3);
        assert_eq!(fq.next_active(), Some(1));
        assert_eq!(fq.next_active(), Some(2));
        assert_eq!(fq.next_active(), Some(3));
        assert_eq!(fq.next_active(), Some(1)); // wraps around
    }

    #[test]
    fn test_deactivated_skipped() {
        let mut fq = FairQueue::new();
        fq.attach(1);
        fq.attach(2);
        fq.activated(1);
        fq.activated(2);
        fq.deactivated(2);
        // Only pipe 1 should be returned
        assert_eq!(fq.next_active(), Some(1));
        assert_eq!(fq.next_active(), Some(1));
    }

    #[test]
    fn test_terminated() {
        let mut fq = FairQueue::new();
        fq.attach(1);
        fq.attach(2);
        fq.attach(3);
        fq.terminated(2);
        assert_eq!(fq.pipe_count(), 2);
    }
}
