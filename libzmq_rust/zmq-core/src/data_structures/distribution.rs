//! Distribution — fan-out (send to all) or filtered send to pipes.
//!
//! Replaces C++ `dist.hpp`. Sends a message to all attached pipes
//! or a subset matching a subscription filter.

/// A distributor that sends to all attached pipes.
#[derive(Debug, Default)]
pub struct Distribution {
    /// Pipe IDs for fan-out
    pipes: Vec<usize>,
}

impl Distribution {
    pub fn new() -> Self {
        Self { pipes: Vec::new() }
    }

    /// Attach a pipe.
    pub fn attach(&mut self, pipe_id: usize) {
        self.pipes.push(pipe_id);
    }

    /// Remove a pipe (O(n) for simplicity).
    pub fn terminated(&mut self, pipe_id: usize) {
        self.pipes.retain(|&id| id != pipe_id);
    }

    /// Get all pipe IDs that match the given filter.
    /// If `matching` is empty, all pipes match.
    pub fn matching_pipes(&self, matching: &[usize]) -> Vec<usize> {
        if matching.is_empty() {
            self.pipes.clone()
        } else {
            self.pipes
                .iter()
                .filter(|id| matching.contains(id))
                .copied()
                .collect()
        }
    }

    /// Get all pipe IDs.
    pub fn all_pipes(&self) -> &[usize] {
        &self.pipes
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
    fn test_fanout() {
        let mut dist = Distribution::new();
        dist.attach(1);
        dist.attach(2);
        dist.attach(3);
        assert_eq!(dist.all_pipes(), &[1, 2, 3]);
        assert_eq!(dist.matching_pipes(&[]), vec![1, 2, 3]);
    }

    #[test]
    fn test_filtered_send() {
        let mut dist = Distribution::new();
        dist.attach(1);
        dist.attach(2);
        dist.attach(3);
        assert_eq!(dist.matching_pipes(&[2]), vec![2]);
    }

    #[test]
    fn test_terminated() {
        let mut dist = Distribution::new();
        dist.attach(1);
        dist.attach(2);
        dist.terminated(1);
        assert_eq!(dist.pipe_count(), 1);
    }
}
