//! Routing ID helper — manages routing ID to pipe mappings.
//!
//! Replaces C++ `routing_socket_base_t`. Used by ROUTER, STREAM, and SERVER
//! sockets. Provides shared routing table logic without class inheritance.

use std::collections::HashMap;
use std::sync::Arc;

use crate::pipe::Pipe;

/// Routing store that maps routing IDs to output pipes and tracks
/// which outbound pipes are active (have write capacity).
pub struct RoutingStore {
    /// Map routing_id → pipe
    id_map: HashMap<u32, Arc<Pipe>>,
    /// Map pipe_id → routing_id (reverse lookup)
    pipe_to_id: HashMap<usize, u32>,
    /// Map routing_id → whether the outbound pipe is currently active
    out_active: HashMap<u32, bool>,
    /// Next integral routing ID (monotonically increasing)
    next_id: u32,
}

impl RoutingStore {
    /// Create a new empty routing store. The first allocated routing
    /// ID will never be zero (zero is reserved).
    pub fn new() -> Self {
        // Use a random starting point, but never zero
        Self {
            id_map: HashMap::new(),
            pipe_to_id: HashMap::new(),
            out_active: HashMap::new(),
            next_id: 1,
        }
    }

    /// Look up a pipe by routing ID. Returns `None` if the routing
    /// ID is unknown.
    pub fn lookup(&self, routing_id: u32) -> Option<&Arc<Pipe>> {
        self.id_map.get(&routing_id)
    }

    /// Add a pipe with the given routing ID. If the routing ID is
    /// already mapped, the old entry is replaced.
    pub fn add(&mut self, routing_id: u32, pipe: Arc<Pipe>) {
        let pipe_id = pipe.id();
        self.id_map.insert(routing_id, Arc::clone(&pipe));
        self.pipe_to_id.insert(pipe_id, routing_id);
        self.out_active.insert(routing_id, true);
    }

    /// Remove all entries associated with a pipe.
    pub fn erase(&mut self, pipe: &Pipe) {
        let pipe_id = pipe.id();
        if let Some(rid) = self.pipe_to_id.remove(&pipe_id) {
            self.id_map.remove(&rid);
            self.out_active.remove(&rid);
        }
    }

    /// Mark an outbound pipe as active (has write capacity).
    pub fn activate(&mut self, pipe: &Pipe) {
        if let Some(rid) = self.pipe_to_id.get(&pipe.id()) {
            self.out_active.insert(*rid, true);
        }
    }

    /// Check if the outbound pipe for a routing ID is active.
    pub fn is_active(&self, routing_id: u32) -> bool {
        self.out_active.get(&routing_id).copied().unwrap_or(false)
    }

    /// Set the active state of an outbound pipe.
    pub fn set_active(&mut self, routing_id: u32, active: bool) {
        self.out_active.insert(routing_id, active);
    }

    /// Generate the next routing ID (monotonically increasing, wraps
    /// around gracefully, and skips zero).
    pub fn generate_routing_id(&mut self) -> u32 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1;
            }
            if !self.id_map.contains_key(&id) {
                return id;
            }
        }
    }

    /// Whether any outbound pipes exist.
    pub fn has_out(&self) -> bool {
        !self.id_map.is_empty()
    }

    /// Whether the routing table is empty.
    pub fn is_empty(&self) -> bool {
        self.id_map.is_empty()
    }

    /// Number of entries in the routing table.
    pub fn len(&self) -> usize {
        self.id_map.len()
    }

    /// Iterate over all (routing_id, pipe) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &Arc<Pipe>)> {
        self.id_map.iter()
    }
}

impl Default for RoutingStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe::Pipe;

    #[test]
    fn test_add_and_lookup() {
        let mut store = RoutingStore::new();
        let (p1, _p2) = Pipe::new_pair(100);

        store.add(42, p1.clone());
        assert_eq!(store.lookup(42).map(|p| p.id()), Some(p1.id()));
        assert!(store.lookup(99).is_none());
        assert!(store.has_out());
        assert!(!store.is_empty());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_erase() {
        let mut store = RoutingStore::new();
        let (p1, _p2) = Pipe::new_pair(200);
        store.add(10, p1.clone());
        store.erase(&p1);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(!store.has_out());
    }

    #[test]
    fn test_generate_routing_id_skips_existing() {
        let mut store = RoutingStore::new();
        let id1 = store.generate_routing_id();
        let (p1, _p2) = Pipe::new_pair(300);
        store.add(id1, p1);
        let id2 = store.generate_routing_id();
        assert_ne!(id1, id2);
        // Never zero
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
    }

    #[test]
    fn test_activate_deactivate() {
        let mut store = RoutingStore::new();
        let (p1, _p2) = Pipe::new_pair(400);
        store.add(7, p1.clone());
        assert!(store.is_active(7));
        store.set_active(7, false);
        assert!(!store.is_active(7));
    }
}
