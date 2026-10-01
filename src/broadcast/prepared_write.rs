//! Explicit publication with cancellation by default. Preparation retains an
//! exclusive producer borrow and changes no shared frontier; Drop does no work.

use super::Producer;
use std::{mem::MaybeUninit, num::NonZeroUsize};

/// One cell whose capacity is proven, but whose sequence is not yet reserved.
///
/// Use [`Self::commit`] for a value or [`Self::commit_initialized`] after in-place
/// initialization. Dropping or forgetting this guard publishes nothing, even
/// if its cell has already been written. See [`Producer::try_prepare_write`].
///
/// Capacity is checked before parsing, and a parsing error cancels publication:
/// ```
/// use shaq::broadcast::Producer;
/// # fn publish(producer: &mut Producer<u64>, input: &str) -> Result<(), Box<dyn std::error::Error>> {
/// let prepared = producer.try_prepare_write().ok_or("queue full")?;
/// let value = input.parse::<u64>()?;
/// prepared.commit(value);
/// # Ok(())
/// # }
/// ```
///
/// The producer remains exclusively borrowed until commit or cancellation:
/// ```compile_fail
/// use shaq::broadcast::Producer;
/// # fn publish(producer: &mut Producer<u64>) {
/// let prepared = producer.try_prepare_write().unwrap();
/// producer.try_write(1).unwrap();
/// prepared.commit(2);
/// # }
/// ```
#[must_use = "prepared writes publish only when explicitly committed"]
pub struct PreparedWrite<'a, T: Copy> {
    pub(super) producer: &'a mut Producer<T>,
    pub(super) start: usize,
}

impl<T: Copy> AsMut<MaybeUninit<T>> for PreparedWrite<'_, T> {
    fn as_mut(&mut self) -> &mut MaybeUninit<T> {
        let mut ptr = self.producer.lane.payload_ptr(self.start).cast();
        // SAFETY: preparation proved prior readers finished with this cell;
        // the exclusive producer borrow keeps it unpublished and writable.
        unsafe { ptr.as_mut() }
    }
}

impl<T: Copy> PreparedWrite<'_, T> {
    /// Writes `value` and publishes it. No capacity check remains at commit.
    pub fn commit(mut self, value: T) {
        self.as_mut().write(value);
        // SAFETY: the value was just initialized above.
        unsafe { self.commit_initialized() };
    }

    /// Publishes the prepared cell without writing it.
    ///
    /// # Safety
    /// The cell must contain an initialized, valid `T` satisfying the queue's
    /// cross-process payload contract. If slice consumers participate, every
    /// exposed payload byte must also be initialized (including any padding).
    pub unsafe fn commit_initialized(self) {
        self.producer.commit_prepared(self.start, NonZeroUsize::MIN);
    }
}

impl<T: Copy> Drop for PreparedWrite<'_, T> {
    fn drop(&mut self) {
        // Cancellation needs no shared-state update, even after partial writes.
    }
}

/// A batch with capacity proven, published only by an explicit commit.
///
/// Cells can be initialized in place in any order, but only an initialized
/// prefix may be committed. Uncommitted suffix cells remain available for
/// future writes. Dropping or forgetting the guard publishes nothing.
///
/// ```
/// use shaq::broadcast::Producer;
/// use std::num::NonZeroUsize;
/// # fn publish(producer: &mut Producer<u64>) -> Option<()> {
/// let mut batch = producer.try_prepare_write_batch(NonZeroUsize::new(4).unwrap())?;
/// batch.as_mut(0).write(10);
/// batch.as_mut(1).write(20);
/// // SAFETY: exactly the committed prefix is initialized; the suffix is cancelled.
/// unsafe { batch.commit_prefix(2) };
/// # Some(())
/// # }
/// ```
#[must_use = "prepared batches publish only when explicitly committed"]
pub struct PreparedWriteBatch<'a, T: Copy> {
    pub(super) producer: &'a mut Producer<T>,
    pub(super) start: usize,
    pub(super) count: NonZeroUsize,
}

impl<T: Copy> PreparedWriteBatch<'_, T> {
    /// Number of cells available for initialization; always nonzero.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Borrows one unpublished cell, handling physical ring wrap.
    ///
    /// # Panics
    /// Panics if `index >= self.len()`. No frontier advances on panic.
    pub fn as_mut(&mut self, index: usize) -> &mut MaybeUninit<T> {
        assert!(index < self.len(), "prepared batch index out of bounds");
        let mut ptr = self
            .producer
            .lane
            .payload_ptr(self.start.wrapping_add(index))
            .cast();
        // SAFETY: index is in the prepared range; capacity remains valid for
        // the entire exclusive producer borrow. No reader may access this cell.
        unsafe { ptr.as_mut() }
    }

    /// Publishes every prepared cell without writing them.
    ///
    /// # Safety
    /// All cells must contain initialized, valid `T` values, including the
    /// cross-process and slice-consumer requirements of
    /// [`PreparedWrite::commit_initialized`].
    pub unsafe fn commit(self) {
        let count = self.len();
        // SAFETY: caller guarantees all `count` cells are initialized.
        unsafe { self.commit_prefix(count) };
    }

    /// Publishes only `count` initialized cells, cancelling the remaining suffix.
    /// Zero cancels the entire batch. Publication and wake happen once per batch.
    ///
    /// # Panics
    /// Panics before publication if `count > self.len()`.
    ///
    /// # Safety
    /// If `count <= self.len()`, every cell in `0..count` must contain an
    /// initialized, valid `T`, including the cross-process and slice-consumer requirements of
    /// [`PreparedWrite::commit_initialized`]. Suffix cells need not be initialized.
    pub unsafe fn commit_prefix(self, count: usize) {
        assert!(
            count <= self.len(),
            "committed prefix exceeds prepared batch"
        );
        if let Some(count) = NonZeroUsize::new(count) {
            self.producer.commit_prepared(self.start, count);
        }
    }

    /// Copies `items` into the prepared prefix and publishes that prefix.
    /// An empty slice cancels the entire batch.
    ///
    /// # Panics
    /// Panics before writing or publishing if `items.len() > self.len()`.
    pub fn commit_from_slice(mut self, items: &[T]) {
        assert!(items.len() <= self.len(), "slice exceeds prepared batch");
        for (index, value) in items.iter().copied().enumerate() {
            self.as_mut(index).write(value);
        }
        // SAFETY: exactly this prefix was initialized above.
        unsafe { self.commit_prefix(items.len()) };
    }
}

impl<T: Copy> Drop for PreparedWriteBatch<'_, T> {
    fn drop(&mut self) {
        // No reservation to roll back, and no implicit publication on unwind.
    }
}
