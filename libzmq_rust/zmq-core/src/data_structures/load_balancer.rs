//! Load balancer — round-robin fan-out to multiple pipes.
//!
//! Replaces C++ `lb.hpp`. Sends messages to attached pipes in round-robin
//! order, ensuring load-balanced distribution across all available pipes.

/// A load balancer that round-robins between attached pipes.
///
/// Like `FairQueue` but for the send direction: messages are distributed
/// evenly across all pipes that have capacity.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    /// Pipe IDs in insertion order
    pipes: Vec<usize>,
    /// Which pipes are currently active (have send capacity)
    active: Vec<bool>,
    /// Current round-robin position
    current: usize,
    /// Number of active pipes
    active_count: usize,
}

impl LoadBalancer {
    pub fn new() -> Self {
        Self {
            pipes: Vec::new(),
            active: Vec::new(),
            current: 0,
            active_count: 0,
        }
    }

    /// Attach a pipe. Returns its index.
    pub fn attach(&mut self, pipe_id: usize) {
        self.pipes.push(pipe_id);
        self.active.push(false);
    }

    /// Mark a pipe as terminated (O(1) swap-remove).
    pub fn terminated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            let last = self.pipes.len() - 1;
            if pos != last {
                self.pipes.swap(pos, last);
                self.active.swap(pos, last);
            }
            self.pipes.pop();
            self.active.pop();
            if self.current >= self.pipes.len() {
                self.current = 0;
            }
        }
    }

    /// Mark a pipe as having send capacity.
    pub fn activated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            if !self.active[pos] {
                self.active[pos] = true;
                self.active_count += 1;
            }
        }
    }

    /// Mark a pipe as full.
    pub fn deactivated(&mut self, pipe_id: usize) {
        if let Some(pos) = self.pipes.iter().position(|&id| id == pipe_id) {
            if self.active[pos] {
                self.active[pos] = false;
                self.active_count -= 1;
            }
        }
    }

    /// Whether any pipe can accept messages.
    pub fn has_out(&self) -> bool {
        self.active_count > 0
    }

    /// Get the next pipe ID to send to (round-robin).
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
    fn test_round_robin_send() {
        let mut lb = LoadBalancer::new();
        lb.attach(10);
        lb.attach(20);
        lb.attach(30);
        lb.activated(10);
        lb.activated(20);
        lb.activated(30);
        assert!(lb.has_out());
        assert_eq!(lb.next_active(), Some(10));
        assert_eq!(lb.next_active(), Some(20));
        assert_eq!(lb.next_active(), Some(30));
        assert_eq!(lb.next_active(), Some(10));
    }
}
