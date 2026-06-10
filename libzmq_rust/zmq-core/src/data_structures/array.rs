//! Indexed array with O(1) element removal.
//!
//! Replaces C++ `array.hpp`. Uses `Vec` + swap-remove for O(1) deletion.
//! The C++ version uses multiple integer ID tags to allow the same element
//! to participate in multiple arrays simultaneously. We simplify by using
//! a single index tracking map.

use std::collections::HashMap;

/// An indexed container that supports O(1) removal by ID.
///
/// Elements are stored in a `Vec`. When an element is removed, it is
/// swapped with the last element and popped (swap-remove).
/// A `HashMap` tracks each element's current position.
pub struct IndexArray<T> {
    items: Vec<T>,
    /// Maps element ID → position in `items`
    positions: HashMap<usize, usize>,
    next_id: usize,
}

impl<T> IndexArray<T> {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            positions: HashMap::new(),
            next_id: 0,
        }
    }

    /// Add an element and return its ID.
    pub fn push(&mut self, item: T) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.positions.insert(id, self.items.len());
        self.items.push(item);
        id
    }

    /// Remove an element by ID. Returns `Some(element)` if found.
    pub fn remove(&mut self, id: usize) -> Option<T> {
        let pos = self.positions.remove(&id)?;
        // Swap with last element and pop
        let last_idx = self.items.len() - 1;
        if pos != last_idx {
            self.items.swap(pos, last_idx);
            // Update the position of the swapped element
            // (we need to find which ID maps to `pos` now — the one that was at `last_idx`)
            let swapped_id = self
                .positions
                .iter()
                .find(|(_, &v)| v == last_idx)
                .map(|(&k, _)| k);
            if let Some(sid) = swapped_id {
                self.positions.insert(sid, pos);
            }
        }
        Some(self.items.pop().unwrap())
    }

    /// Get a reference to an element by ID.
    pub fn get(&self, id: usize) -> Option<&T> {
        self.positions.get(&id).map(|&pos| &self.items[pos])
    }

    /// Get a mutable reference.
    pub fn get_mut(&mut self, id: usize) -> Option<&mut T> {
        self.positions.get(&id).map(|&pos| &mut self.items[pos])
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Iterate over all elements.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}

impl<T> Default for IndexArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_get() {
        let mut arr = IndexArray::new();
        let id = arr.push(42);
        assert_eq!(arr.get(id), Some(&42));
    }

    #[test]
    fn test_remove_swaps() {
        let mut arr = IndexArray::new();
        let a = arr.push(1);
        let b = arr.push(2);
        let c = arr.push(3);
        assert_eq!(arr.remove(b), Some(2));
        // c should still be accessible
        assert_eq!(arr.get(a), Some(&1));
        assert_eq!(arr.get(c), Some(&3));
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_remove_last() {
        let mut arr = IndexArray::new();
        let a = arr.push(10);
        assert_eq!(arr.remove(a), Some(10));
        assert!(arr.is_empty());
    }
}
