//! Radix tree (compressed trie) for prefix-based key storage.
//!
//! 1:1 translation of C++ `radix_tree.hpp` / `radix_tree.cpp`.
//!
//! Stores keys with reference counting. Each key can be added multiple times;
//! `rm` decrements the refcount and only returns true when it hits zero.
//!
//! ## C++ API mapping
//! - `add(data, size)` → `add(key)` returns bool (true = new unique key)
//! - `rm(data, size)` → `remove(key)` returns bool (true = last ref removed)
//! - `check(data, size)` → `has_match(query)` — true if any stored key is prefix of query
//! - `size()` → `len()` — total reference count
//! - `apply(callback, arg)` → `apply(callback)`

use std::collections::HashMap;

/// Compute the length of the common prefix between two byte slices.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[derive(Debug)]
struct RadixNode {
    /// The prefix stored at this node (multi-byte path segment)
    prefix: Vec<u8>,
    /// Reference count for the key ending at this node. 0 if no key ends here.
    refcount: usize,
    /// Child nodes keyed by the first byte of their prefix
    children: HashMap<u8, Box<RadixNode>>,
}

impl RadixNode {
    fn new(prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            refcount: 0,
            children: HashMap::new(),
        }
    }
}

/// A radix tree with reference-counted keys.
///
/// Supports add/remove/check operations. Each key can be added multiple
/// times; the internal refcount tracks the number of active references.
#[derive(Debug)]
pub struct RadixTree {
    root: RadixNode,
}

impl RadixTree {
    pub fn new() -> Self {
        Self {
            root: RadixNode::new(Vec::new()),
        }
    }

    // ─── Add ───────────────────────────────────────────────

    /// Add a key. Returns `true` if this is the first time the key was added
    /// (i.e., refcount went from 0 to 1).
    pub fn add(&mut self, key: &[u8]) -> bool {
        Self::add_recursive(&mut self.root, key)
    }

    fn add_recursive(node: &mut RadixNode, key: &[u8]) -> bool {
        let common = common_prefix(&node.prefix, key);

        if common == node.prefix.len() {
            // Node's prefix is fully consumed
            let key_rest = &key[common..];
            if key_rest.is_empty() {
                // Exact match — increment refcount
                let is_first = node.refcount == 0;
                node.refcount += 1;
                return is_first;
            }
            // Follow or create child
            let first_byte = key_rest[0];
            if let Some(child) = node.children.get_mut(&first_byte) {
                return Self::add_recursive(child, key_rest);
            }
            let mut new_child = Box::new(RadixNode::new(key_rest.to_vec()));
            new_child.refcount = 1;
            node.children.insert(first_byte, new_child);
            return true;
        }

        // Need to split this node
        let node_rest = node.prefix[common..].to_vec();
        let split_first_byte = node_rest[0];

        // Create child for the old node's remainder
        let old_children = std::mem::take(&mut node.children);
        let split_node = Box::new(RadixNode {
            prefix: node_rest,
            refcount: node.refcount,
            children: old_children,
        });

        node.prefix.truncate(common);
        node.refcount = 0;
        node.children.clear();
        node.children.insert(split_first_byte, split_node);

        let key_rest = &key[common..];

        if key_rest.is_empty() {
            // Key ends at the split point — mark this node
            node.refcount = 1;
            return true;
        }

        // Create child for the new key's remainder
        let key_first_byte = key_rest[0];
        let mut key_child = Box::new(RadixNode::new(key_rest.to_vec()));
        key_child.refcount = 1;
        node.children.insert(key_first_byte, key_child);
        true
    }

    // ─── Remove ─────────────────────────────────────────────

    /// Remove one reference to a key. Returns `true` if the last reference
    /// was removed (refcount reached 0).
    pub fn remove(&mut self, key: &[u8]) -> bool {
        Self::rm_recursive(&mut self.root, key)
    }

    fn rm_recursive(node: &mut RadixNode, key: &[u8]) -> bool {
        let common = common_prefix(&node.prefix, key);
        if common != node.prefix.len() {
            return false; // key doesn't reach this node
        }

        let key_rest = &key[common..];
        if key_rest.is_empty() {
            if node.refcount == 0 {
                return false;
            }
            node.refcount -= 1;
            return node.refcount == 0;
        }

        let first_byte = key_rest[0];
        if let Some(child) = node.children.get_mut(&first_byte) {
            Self::rm_recursive(child, key_rest)
        } else {
            false
        }
    }

    // ─── Check (prefix match) ──────────────────────────────

    /// Check if any stored key is a prefix of `query`.
    /// Returns `true` if a match is found.
    pub fn check(&self, query: &[u8]) -> bool {
        Self::check_recursive(&self.root, query)
    }

    fn check_recursive(node: &RadixNode, query: &[u8]) -> bool {
        let common = common_prefix(&node.prefix, query);
        if common != node.prefix.len() {
            return false;
        }

        // If this node has a key, it's a prefix match
        if node.refcount > 0 {
            return true;
        }

        let query_rest = &query[common..];
        if query_rest.is_empty() {
            return false;
        }
        if let Some(child) = node.children.get(&query_rest[0]) {
            Self::check_recursive(child, query_rest)
        } else {
            false
        }
    }

    /// Alias for check() — has a prefix match.
    pub fn has_match(&self, query: &[u8]) -> bool {
        self.check(query)
    }

    // ─── Apply (iterate) ───────────────────────────────────

    /// Apply a callback to all keys stored in the tree.
    /// Each key is passed exactly once (per refcount).
    pub fn apply<F>(&self, callback: &mut F)
    where
        F: FnMut(&[u8]),
    {
        let mut path = Vec::new();
        Self::apply_recursive(&self.root, &mut path, callback);
    }

    fn apply_recursive<F>(node: &RadixNode, path: &mut Vec<u8>, callback: &mut F)
    where
        F: FnMut(&[u8]),
    {
        path.extend_from_slice(&node.prefix);
        for _ in 0..node.refcount {
            callback(path);
        }
        for child in node.children.values() {
            Self::apply_recursive(child, path, callback);
        }
        path.truncate(path.len() - node.prefix.len());
    }

    // ─── Size ───────────────────────────────────────────────

    /// Total number of key references (sum of all refcounts).
    pub fn len(&self) -> usize {
        let mut total = 0usize;
        Self::count_refs(&self.root, &mut total);
        total
    }

    fn count_refs(node: &RadixNode, total: &mut usize) {
        *total += node.refcount;
        for child in node.children.values() {
            Self::count_refs(child, total);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for RadixTree {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests (1:1 from C++ unittest_radix_tree.cpp) ──────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let tree = RadixTree::new();
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_add_single_entry() {
        let mut tree = RadixTree::new();
        assert!(tree.add(b"foo"));
    }

    #[test]
    fn test_add_same_entry_twice() {
        let mut tree = RadixTree::new();
        assert!(tree.add(b"test"));
        assert!(!tree.add(b"test"));
    }

    #[test]
    fn test_rm_when_empty() {
        let mut tree = RadixTree::new();
        assert!(!tree.remove(b"test"));
    }

    #[test]
    fn test_rm_single_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"temporary");
        assert!(tree.remove(b"temporary"));
    }

    #[test]
    fn test_rm_unique_entry_twice() {
        let mut tree = RadixTree::new();
        tree.add(b"test");
        assert!(tree.remove(b"test"));
        assert!(!tree.remove(b"test"));
    }

    #[test]
    fn test_rm_duplicate_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"test");
        tree.add(b"test");
        // First rm removes one ref, but another remains
        assert!(!tree.remove(b"test"));
        // Second rm removes the last ref
        assert!(tree.remove(b"test"));
    }

    #[test]
    fn test_rm_common_prefix() {
        let mut tree = RadixTree::new();
        tree.add(b"checkpoint");
        tree.add(b"checklist");
        // "check" is a prefix but not stored as a key
        assert!(!tree.remove(b"check"));
    }

    #[test]
    fn test_rm_common_prefix_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"checkpoint");
        tree.add(b"checklist");
        tree.add(b"check");
        assert!(tree.remove(b"check"));
    }

    #[test]
    fn test_rm_null_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"");
        assert!(tree.remove(b""));
    }

    #[test]
    fn test_check_empty() {
        let tree = RadixTree::new();
        assert!(!tree.check(b"foo"));
    }

    #[test]
    fn test_check_added_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"entry");
        assert!(tree.check(b"entry"));
    }

    #[test]
    fn test_check_common_prefix() {
        let mut tree = RadixTree::new();
        tree.add(b"introduce");
        tree.add(b"introspect");
        assert!(!tree.check(b"intro")); // not a stored key
    }

    #[test]
    fn test_check_prefix() {
        let mut tree = RadixTree::new();
        tree.add(b"toasted");
        assert!(!tree.check(b"toast"));
        assert!(!tree.check(b"toaste"));
        assert!(!tree.check(b"toaster"));
    }

    #[test]
    fn test_check_nonexistent_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"red");
        assert!(!tree.check(b"blue"));
    }

    #[test]
    fn test_check_query_longer_than_entry() {
        let mut tree = RadixTree::new();
        tree.add(b"foo");
        // "foo" is a prefix of "foobar", so check returns true
        assert!(tree.check(b"foobar"));
    }

    #[test]
    fn test_check_null_entry_added() {
        let mut tree = RadixTree::new();
        tree.add(b"");
        // Empty key is a prefix of everything
        assert!(tree.check(b"all queries return true"));
    }

    #[test]
    fn test_size() {
        let mut tree = RadixTree::new();
        let keys: Vec<&[u8]> = vec![
            b"tester" as &[u8],
            b"water",
            b"slow",
            b"slower",
            b"test",
            b"team",
            b"toast",
        ];

        // Add all keys once
        for key in &keys {
            assert!(tree.add(key));
        }
        assert_eq!(tree.len(), keys.len());

        // Add again — none are new
        for key in &keys {
            assert!(!tree.add(key));
        }
        assert_eq!(tree.len(), 2 * keys.len());

        // Remove one ref each — none reach zero
        for key in &keys {
            assert!(!tree.remove(key));
        }
        assert_eq!(tree.len(), keys.len());

        // Remove last ref — each reaches zero
        for key in &keys {
            assert!(tree.remove(key));
        }
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_apply() {
        let mut tree = RadixTree::new();
        let keys: Vec<&[u8]> = vec![
            b"tester", b"water", b"slow", b"slower",
            b"test", b"team", b"toast",
        ];

        for key in &keys {
            tree.add(key);
        }

        let mut collected: Vec<Vec<u8>> = Vec::new();
        tree.apply(&mut |key: &[u8]| {
            collected.push(key.to_vec());
        });

        // Each key should appear exactly once
        assert_eq!(collected.len(), keys.len());
        for key in &keys {
            assert!(collected.iter().any(|k| k.as_slice() == *key));
        }
    }
}
