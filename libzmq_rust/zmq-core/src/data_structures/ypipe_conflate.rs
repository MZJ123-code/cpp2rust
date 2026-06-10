//! Conflating pipe — SPSC pipe that keeps only the latest value.
//!
//! Replaces C++ `ypipe_conflate.hpp`. Built on `DoubleBuffer`.
//! Unlike `YPipe`, this discards all but the most recent write.
//! Implements the same write/flush/check_read/read interface as `YPipe`.

use super::dbuffer::DoubleBuffer;

/// A pipe that conflates: only the latest written value is kept.
pub struct YPipeConflate<T> {
    dbuffer: DoubleBuffer<T>,
    /// Tracks whether the reader is known to be awake.
    reader_awake: bool,
}

impl<T: Default> YPipeConflate<T> {
    /// Create a new conflating pipe.
    pub fn new() -> Self {
        Self {
            dbuffer: DoubleBuffer::new(),
            reader_awake: false,
        }
    }

    /// Write a value. `incomplete` is ignored (conflate has no multi-part).
    pub fn write(&mut self, value: T, _incomplete: bool) {
        self.dbuffer.write(value);
    }

    /// Conflate pipe has no incomplete items.
    pub fn unwrite(&self) -> Option<T> {
        None
    }

    /// Flush is a no-op for conflate.
    /// Returns `reader_awake` to mimic ypipe semantics.
    pub fn flush(&mut self) -> bool {
        self.reader_awake
    }

    /// Check if data is available.
    pub fn check_read(&mut self) -> bool {
        let res = self.dbuffer.check_read();
        if !res {
            self.reader_awake = false;
        }
        res
    }

    /// Read the latest value.
    pub fn read(&mut self) -> Option<T> {
        if !self.check_read() {
            return None;
        }
        self.dbuffer.read()
    }

    /// Probe the front value.
    pub fn probe<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        self.dbuffer.probe(f)
    }
}

unsafe impl<T: Send> Send for YPipeConflate<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflate_drops_intermediate() {
        let mut pipe: YPipeConflate<i32> = YPipeConflate::new();
        pipe.write(1, false);
        pipe.write(2, false);
        pipe.write(3, false);
        pipe.flush();
        assert_eq!(pipe.read(), Some(3)); // only latest kept
        assert_eq!(pipe.read(), None);
    }
}
