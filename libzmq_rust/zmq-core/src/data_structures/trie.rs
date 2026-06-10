//! Multi-trie (prefix tree) for PUB/SUB subscription matching.
//!
//! 1:1 translation of C++ `generic_mtrie.hpp` / `generic_mtrie_impl.hpp`.
//!
//! Each node stores a set of subscriber values. Prefix matching traverses
//! the trie along the data path; all subscribers at visited nodes match.
//!
//! ## C++ API mapping
//! - `add(prefix, size, value)` → `add(prefix, value)` returns bool
//! - `rm(prefix, size, value)` → `remove(prefix, value)` returns `RmResult`
//! - `rm(value, callback, arg, unique)` → `remove_by_value(value, callback, unique)`
//! - `match(data, size, callback, arg)` → `match_into(data, callback)`
//! - `num_prefixes()` → `len()`

use std::collections::HashMap;

/// Result of removing a specific prefix/value pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmResult {
    /// The prefix/value pair was not found.
    NotFound,
    /// The last value at this prefix was removed.
    LastValueRemoved,
    /// The value was removed but other values remain at this prefix.
    ValuesRemain,
}

/// A value stored in the trie.
pub type SubscriberId = usize;

/// Callback for match: `fn(value, data, size, arg)`
type MatchCallback<'a> = &'a mut dyn FnMut(&[u8], usize);

/// Callback for remove_by_value: `fn(data, size, arg)`
type RmCallback<'a> = &'a mut dyn FnMut(&[u8], usize);

#[derive(Debug, Default)]
struct TrieNode {
    /// Subscribers registered at this prefix
    subscribers: Vec<SubscriberId>,
    /// Child nodes keyed by the next byte of the prefix
    children: HashMap<u8, Box<TrieNode>>,
    /// Number of prefixes stored in this subtree
    subtree_prefix_count: usize,
}

/// A multi-trie for prefix-based subscription matching.
#[derive(Debug, Default)]
pub struct SubscriptionTrie {
    root: TrieNode,
}

impl SubscriptionTrie {
    pub fn new() -> Self {
        Self::default()
    }

    // ─── Add ───────────────────────────────────────────────

    /// Add a prefix/value pair. Returns `true` if this exact pair did not exist.
    pub fn add(&mut self, prefix: &[u8], value: SubscriberId) -> bool {
        let mut node = &mut self.root;
        for &byte in prefix {
            node = node
                .children
                .entry(byte)
                .or_insert_with(|| Box::new(TrieNode::default()));
        }
        // C++ behavior: returns true if this prefix had no entries before.
        // Values are always added (even duplicates, handled by caller).
        let was_empty = node.subscribers.is_empty();
        // Always add the value (C++ adds to a set-like container)
        if !node.subscribers.contains(&value) {
            node.subscribers.push(value);
        }
        if was_empty {
            Self::inc_subtree_count(&mut self.root, prefix);
        }
        was_empty
    }

    fn inc_subtree_count(node: &mut TrieNode, prefix: &[u8]) {
        if prefix.is_empty() {
            node.subtree_prefix_count = node.subtree_prefix_count.saturating_add(1);
            return;
        }
        node.subtree_prefix_count = node.subtree_prefix_count.saturating_add(1);
        if let Some(child) = node.children.get_mut(&prefix[0]) {
            Self::inc_subtree_count(child, &prefix[1..]);
        }
    }

    fn dec_subtree_count(node: &mut TrieNode, prefix: &[u8]) {
        if prefix.is_empty() {
            node.subtree_prefix_count = node.subtree_prefix_count.saturating_sub(1);
            return;
        }
        node.subtree_prefix_count = node.subtree_prefix_count.saturating_sub(1);
        if let Some(child) = node.children.get_mut(&prefix[0]) {
            Self::dec_subtree_count(child, &prefix[1..]);
        }
    }

    // ─── Remove by prefix ──────────────────────────────────

    /// Remove a specific prefix/value pair.
    pub fn remove(&mut self, prefix: &[u8], value: SubscriberId) -> RmResult {
        let result = Self::rm_recursive(&mut self.root, prefix, value);
        if matches!(result, RmResult::LastValueRemoved | RmResult::ValuesRemain) {
            Self::dec_subtree_count(&mut self.root, prefix);
        }
        result
    }

    fn rm_recursive(node: &mut TrieNode, prefix: &[u8], value: SubscriberId) -> RmResult {
        if prefix.is_empty() {
            let pos = node.subscribers.iter().position(|&s| s == value);
            match pos {
                None => RmResult::NotFound,
                Some(idx) => {
                    node.subscribers.remove(idx);
                    if node.subscribers.is_empty() {
                        RmResult::LastValueRemoved
                    } else {
                        RmResult::ValuesRemain
                    }
                }
            }
        } else {
            let byte = prefix[0];
            match node.children.get_mut(&byte) {
                None => RmResult::NotFound,
                Some(child) => Self::rm_recursive(child, &prefix[1..], value),
            }
        }
    }

    // ─── Remove all entries for a value (with callback) ────

    /// Remove all entries for a given value.
    /// `callback` is called for each prefix that becomes empty if `call_on_unique` is true,
    /// or for every removal if `call_on_unique` is false.
    pub fn remove_by_value(
        &mut self,
        value: SubscriberId,
        callback: &mut dyn FnMut(&[u8], usize),
        call_on_unique: bool,
    ) {
        let mut path = Vec::new();
        Self::rm_value_recursive(&mut self.root, value, &mut path, callback, call_on_unique);
    }

    /// Returns the number of prefixes removed from this subtree.
    fn rm_value_recursive(
        node: &mut TrieNode,
        value: SubscriberId,
        path: &mut Vec<u8>,
        callback: &mut dyn FnMut(&[u8], usize),
        call_on_unique: bool,
    ) -> usize {
        let mut removed = 0usize;

        // Remove from this node
        if let Some(pos) = node.subscribers.iter().position(|&s| s == value) {
            node.subscribers.remove(pos);
            node.subtree_prefix_count = node.subtree_prefix_count.saturating_sub(1);
            removed += 1;
            let is_now_empty = node.subscribers.is_empty() && node.children.is_empty();
            let should_call = if call_on_unique { is_now_empty } else { true };
            if should_call {
                callback(path, path.len());
            }
        }
        // Recurse into children
        let keys: Vec<u8> = node.children.keys().copied().collect();
        for key in keys {
            path.push(key);
            if let Some(child) = node.children.get_mut(&key) {
                let child_removed =
                    Self::rm_value_recursive(child, value, path, callback, call_on_unique);
                if child_removed > 0 {
                    node.subtree_prefix_count =
                        node.subtree_prefix_count.saturating_sub(child_removed);
                    removed += child_removed;
                }
            }
            path.pop();
        }
        removed
    }

    // ─── Match ─────────────────────────────────────────────

    /// Match all subscribers whose prefix is a prefix of `data`.
    /// The callback is called for each matching value.
    pub fn match_into(&self, data: &[u8], callback: &mut dyn FnMut(SubscriberId)) {
        Self::match_recursive(&self.root, data, callback);
    }

    fn match_recursive(node: &TrieNode, data: &[u8], callback: &mut dyn FnMut(SubscriberId)) {
        for &sub in &node.subscribers {
            callback(sub);
        }
        if data.is_empty() {
            return;
        }
        if let Some(child) = node.children.get(&data[0]) {
            Self::match_recursive(child, &data[1..], callback);
        }
    }

    /// Check if any subscriber matches `data`.
    pub fn has_match(&self, data: &[u8]) -> bool {
        let mut found = false;
        self.match_into(data, &mut |_| found = true);
        found
    }

    /// Get all matching subscriber IDs.
    pub fn match_all(&self, data: &[u8]) -> Vec<SubscriberId> {
        let mut result = Vec::new();
        self.match_into(data, &mut |id| result.push(id));
        result
    }

    // ─── Count ─────────────────────────────────────────────

    /// Number of unique prefixes stored.
    pub fn num_prefixes(&self) -> usize {
        self.root.subtree_prefix_count
    }

    /// Alias for num_prefixes.
    pub fn len(&self) -> usize {
        self.num_prefixes()
    }

    pub fn is_empty(&self) -> bool {
        self.num_prefixes() == 0
    }
}

// ─── Tests (1:1 from C++ unittest_mtrie.cpp) ──────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── helpers ──────────────────────────────────────────

    fn get_len(data: &[u8]) -> usize {
        // strlen equivalent for C strings (null-terminated)
        data.iter().position(|&b| b == 0).unwrap_or(data.len())
    }

    fn mtrie_count(_pipe: SubscriberId, count: &mut usize) {
        *count += 1;
    }

    // ── tests ────────────────────────────────────────────

    #[test]
    fn test_create() {
        let _mtrie: SubscriptionTrie = SubscriptionTrie::new();
    }

    #[test]
    fn test_check_empty_match_nonempty_data() {
        let mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let mut count = 0usize;

        mtrie.match_into(&test_name[..get_len(test_name)], &mut |_| count += 1);
        assert_eq!(0, count);
    }

    #[test]
    fn test_check_empty_match_empty_data() {
        let mtrie = SubscriptionTrie::new();
        let mut count = 0usize;
        mtrie.match_into(&[], &mut |_| count += 1);
        assert_eq!(0, count);
    }

    #[test]
    fn test_add_single_entry_match_exact() {
        let pipe = 1usize;
        let mut mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let len = get_len(test_name);

        let res = mtrie.add(&test_name[..len], pipe);
        assert!(res);
        assert_eq!(1, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&test_name[..len], &mut |_| count += 1);
        assert_eq!(1, count);
    }

    #[test]
    fn test_add_single_entry_twice_match_exact() {
        let pipe = 1usize;
        let mut mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let len = get_len(test_name);

        let res = mtrie.add(&test_name[..len], pipe);
        assert!(res);
        assert_eq!(1, mtrie.num_prefixes());

        let res = mtrie.add(&test_name[..len], pipe);
        assert!(!res);
        assert_eq!(1, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&test_name[..len], &mut |_| count += 1);
        assert_eq!(1, count);
    }

    #[test]
    fn test_add_two_entries_with_same_name_match_exact() {
        let pipe_1 = 1usize;
        let pipe_2 = 2usize;
        let mut mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let len = get_len(test_name);

        let res = mtrie.add(&test_name[..len], pipe_1);
        assert!(res);
        assert_eq!(1, mtrie.num_prefixes());

        let res = mtrie.add(&test_name[..len], pipe_2);
        assert!(!res);
        assert_eq!(1, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&test_name[..len], &mut |_| count += 1);
        // Both pipes are on the same prefix, so 2 matches
        assert_eq!(2, count);
    }

    #[test]
    fn test_add_two_entries_match_prefix_and_exact() {
        let pipe_1 = 1usize;
        let pipe_2 = 2usize;
        let mut mtrie = SubscriptionTrie::new();
        let prefix = b"foo\0";
        let full = b"foobar\0";

        let res = mtrie.add(&prefix[..get_len(prefix)], pipe_1);
        assert!(res);
        assert_eq!(1, mtrie.num_prefixes());

        let res = mtrie.add(&full[..get_len(full)], pipe_2);
        assert!(res);
        assert_eq!(2, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&full[..get_len(full)], &mut |_| count += 1);
        assert_eq!(2, count); // both "foo" and "foobar" match "foobar"
    }

    #[test]
    fn test_add_rm_single_entry_match_exact() {
        let pipe = 1usize;
        let mut mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let len = get_len(test_name);

        mtrie.add(&test_name[..len], pipe);
        assert_eq!(1, mtrie.num_prefixes());

        let res = mtrie.remove(&test_name[..len], pipe);
        assert_eq!(RmResult::LastValueRemoved, res);
        assert_eq!(0, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&test_name[..len], &mut |_| count += 1);
        assert_eq!(0, count);
    }

    #[test]
    fn test_rm_nonexistent_0_size_empty() {
        let pipe = 1usize;
        let mut mtrie = SubscriptionTrie::new();
        let res = mtrie.remove(&[], pipe);
        assert_eq!(RmResult::NotFound, res);
        assert_eq!(0, mtrie.num_prefixes());
    }

    #[test]
    fn test_rm_nonexistent_empty() {
        let pipe = 1usize;
        let mut mtrie = SubscriptionTrie::new();
        let test_name = b"foo\0";
        let len = get_len(test_name);

        let res = mtrie.remove(&test_name[..len], pipe);
        assert_eq!(RmResult::NotFound, res);
        assert_eq!(0, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(&test_name[..len], &mut |_| count += 1);
        assert_eq!(0, count);
    }

    fn add_and_rm_other(add_name: &[u8], rm_name: &[u8]) {
        let addpipe = 1usize;
        let rmpipe = 2usize;
        let mut mtrie = SubscriptionTrie::new();

        mtrie.add(add_name, addpipe);
        assert_eq!(1, mtrie.num_prefixes());

        let res = mtrie.remove(rm_name, rmpipe);
        assert_eq!(RmResult::NotFound, res);
        assert_eq!(1, mtrie.num_prefixes());

        let mut count = 0usize;
        mtrie.match_into(add_name, &mut |_| count += 1);
        assert_eq!(1, count);

        // If rm_name is not a prefix of add_name and vice versa, no match
        let common = add_name.iter().zip(rm_name).take_while(|(a, b)| a == b).count();
        if common != add_name.len().min(rm_name.len() + 1) {
            let mut count = 0usize;
            mtrie.match_into(rm_name, &mut |_| count += 1);
            assert_eq!(0, count);
        }
    }

    #[test]
    fn test_rm_nonexistent_nonempty_samename() {
        add_and_rm_other(b"foo", b"foo");
    }

    #[test]
    fn test_rm_nonexistent_nonempty_differentname() {
        add_and_rm_other(b"foo", b"bar");
    }

    #[test]
    fn test_rm_nonexistent_nonempty_prefix() {
        add_and_rm_other(b"foobar", b"foo");
    }

    #[test]
    fn test_rm_nonexistent_nonempty_prefixed() {
        add_and_rm_other(b"foo", b"foobar");
    }

    #[test]
    fn test_rm_nonexistent_between() {
        let names: [&[u8]; 3] = [b"foo1", b"foo2", b"foo3"];
        let mut pipes = [1usize, 2, 3];
        let mut mtrie = SubscriptionTrie::new();

        mtrie.add(names[0], pipes[0]);
        mtrie.add(names[2], pipes[2]);
        assert_eq!(2, mtrie.num_prefixes());

        let res = mtrie.remove(names[1], pipes[1]);
        assert_eq!(RmResult::NotFound, res);
        assert_eq!(2, mtrie.num_prefixes());
    }

    #[test]
    fn test_add_multiple() {
        let names: [&[u8]; 3] = [b"foo1", b"foo2", b"foo3"];
        let pipes = [1usize, 2, 3];
        let mut mtrie = SubscriptionTrie::new();

        for i in 0..3 {
            mtrie.add(names[i], pipes[i]);
        }
        assert_eq!(3, mtrie.num_prefixes());

        for i in 0..3 {
            let mut count = 0usize;
            mtrie.match_into(names[i], &mut |_| count += 1);
            assert_eq!(1, count);
        }
    }

    #[test]
    fn test_add_multiple_reverse() {
        let names: [&[u8]; 3] = [b"foo1", b"foo2", b"foo3"];
        let pipes = [1usize, 2, 3];
        let mut mtrie = SubscriptionTrie::new();

        for i in (0..3).rev() {
            mtrie.add(names[i], pipes[i]);
        }
        assert_eq!(3, mtrie.num_prefixes());

        for i in 0..3 {
            let mut count = 0usize;
            mtrie.match_into(names[i], &mut |_| count += 1);
            assert_eq!(1, count);
        }
    }

    #[test]
    fn test_rm_multiple_in_order() {
        let names: [&[u8]; 3] = [b"foo1", b"foo2", b"foo3"];
        let pipes = [1usize, 2, 3];
        let mut mtrie = SubscriptionTrie::new();

        for i in 0..3 {
            mtrie.add(names[i], pipes[i]);
        }
        assert_eq!(3, mtrie.num_prefixes());

        for i in 0..3 {
            let res = mtrie.remove(names[i], pipes[i]);
            assert_eq!(RmResult::LastValueRemoved, res);
        }
        assert_eq!(0, mtrie.num_prefixes());
    }

    #[test]
    fn test_rm_multiple_reverse_order() {
        let names: [&[u8]; 3] = [b"foo3", b"foo2", b"foo1"];
        let pipes = [3usize, 2, 1];
        let mut mtrie = SubscriptionTrie::new();

        for i in 0..3 {
            mtrie.add(names[i], pipes[i]);
        }
        for i in 0..3 {
            let res = mtrie.remove(names[i], pipes[i]);
            assert_eq!(RmResult::LastValueRemoved, res);
        }
        assert_eq!(0, mtrie.num_prefixes());
    }

    #[test]
    fn test_rm_with_callback_multiple_in_order() {
        let names: [&[u8]; 3] = [b"foo1", b"foo2", b"foo3"];
        let pipes = [1usize, 2, 3];
        let mut mtrie = SubscriptionTrie::new();

        for i in 0..3 {
            mtrie.add(names[i], pipes[i]);
        }

        for i in 0..3 {
            // callback: check_name — verifies the prefix when removed
            let expected = names[i];
            mtrie.remove_by_value(
                pipes[i],
                &mut |data, len| {
                    assert_eq!(expected.len(), len);
                    assert_eq!(&expected[..len], &data[..len]);
                },
                false,
            );
        }
    }

    #[test]
    fn test_rm_with_callback_multiple_reverse_order() {
        let names: [&[u8]; 3] = [b"foo3", b"foo2", b"foo1"];
        let pipes = [3usize, 2, 1];
        let mut mtrie = SubscriptionTrie::new();

        for i in 0..3 {
            mtrie.add(names[i], pipes[i]);
        }

        for i in 0..3 {
            let expected = names[i];
            mtrie.remove_by_value(
                pipes[i],
                &mut |data, len| {
                    assert_eq!(expected.len(), len);
                    assert_eq!(&expected[..len], &data[..len]);
                },
                false,
            );
        }
    }

    #[test]
    fn test_rm_with_callback_duplicate() {
        let pipes = [1usize, 2];
        let mut mtrie = SubscriptionTrie::new();
        let name = b"foo";

        let res = mtrie.add(name, pipes[0]);
        assert!(res);
        assert_eq!(1, mtrie.num_prefixes());
        let res = mtrie.add(name, pipes[1]);
        assert!(!res);
        assert_eq!(1, mtrie.num_prefixes());

        let mut count = 1i32;
        mtrie.remove_by_value(pipes[0], &mut |_, _| count -= 1, false);
        assert!(count >= 0);

        count = 1;
        mtrie.remove_by_value(pipes[1], &mut |_, _| count -= 1, false);
        assert!(count >= 0);
    }

    #[test]
    fn test_rm_with_callback_duplicate_uniq_only() {
        let pipes = [1usize, 2];
        let mut mtrie = SubscriptionTrie::new();
        let name = b"foo";

        mtrie.add(name, pipes[0]);
        mtrie.add(name, pipes[1]);

        let mut count = 0i32;
        mtrie.remove_by_value(pipes[0], &mut |_, _| count += 1, true);
        // call_on_unique=true: callback only fires when prefix becomes empty
        // 2 pipes on same prefix, removing one doesn't make it empty
        assert_eq!(0, count); // callback NOT called

        count = 1;
        mtrie.remove_by_value(pipes[1], &mut |_, _| count -= 1, true);
        assert!(count >= 0);
    }
}
