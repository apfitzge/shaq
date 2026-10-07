//! Synchronization primitives selected for production or Loom model checking.

#[cfg(not(feature = "loom"))]
pub(crate) use core::sync::atomic;

#[cfg(feature = "loom")]
pub(crate) use loom::sync::atomic;
