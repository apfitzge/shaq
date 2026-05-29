//! Bounded lossy broadcast queue with separated payload storage.
//!
//! The broadcast ring stores only single-atomic payload handles. Payload bytes
//! live in a separate fixed-size pool and are reclaimed by producers after
//! checking consumer hazard ranges. Consumers that obtain a direct read guard
//! publish the guarded sequence range, so producers can overwrite ring entries
//! without racing the guarded payload bytes.
//!
//! Producer reservation first claims payload storage only. Ring positions are
//! reserved during explicit publication, after payload bytes have been written.
//! A consumer that has not published a hazard before its ring position is
//! reserved for overwrite may observe an overrun and skip forward. A consumer
//! that already holds a direct read guard keeps the guarded payloads retired
//! until the guard is dropped and a producer reclaims them.
//!
//! The high-level by-value APIs still copy payload bytes out of shared memory.
//! As with the other shared-memory queues, callers must use the same `T` for all
//! producers and consumers attached to the same region. The consumer-slot count
//! is a const generic on [`Producer`] and [`Consumer`]; it defaults to
//! [`DEFAULT_CONSUMER_SLOTS`] and is validated when joining. Broadcast payloads
//! may be overwritten, cancelled, recovered, or read by value without running
//! `T`'s destructor on the shared-memory copy, so duplicating and forgetting
//! payload values must be valid for the chosen `T`.
//!
//! # Safety and failure model
//!
//! The `try_` APIs are fallible, but they are not guaranteed to be non-blocking:
//! publication may spin behind an earlier producer that is publishing to the
//! ring. If a producer process exits after reserving ring positions but before
//! advancing the publication cursor, later producers may remain blocked until
//! [`Producer::recover_as_exclusive`] is used.
//!
//! Write batches reserve payload storage before reserving ring positions.
//! Unpublished batches can therefore exhaust the fixed payload pool even though
//! the broadcast ring is lossy. Old overwritten payloads are reclaimed only
//! after the new publication is visible, keeping the serialized publication
//! window as small as possible.
//!
//! Zero-sized payload types and payload types whose alignment exceeds the shared
//! memory region alignment are not supported.

mod payload_pool;

use crate::{error::Error, normalized_capacity, shmem::Region, CacheAlignedAtomicSize, VERSION};
use core::{
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
use payload_pool::{
    PayloadHandle, PayloadHeader, PayloadPool, PayloadReleaseBatch, ProtectedPayload,
    ReservedPayloads,
};
use std::{fs::File, sync::Arc};

/// Unique identifier for broadcast queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");
const MAX_RING_CAPACITY: usize = 1usize << 30;
pub const DEFAULT_CONSUMER_SLOTS: usize = 8;

pub struct Producer<T, const CONSUMER_SLOTS: usize = DEFAULT_CONSUMER_SLOTS> {
    queue: SharedQueue<T, CONSUMER_SLOTS>,
}

impl<T, const CONSUMER_SLOTS: usize> Producer<T, CONSUMER_SLOTS> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - After initialization, `file` must not be truncated or resized while any
    ///   handle remains joined to the queue.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T` and
    ///   `CONSUMER_SLOTS`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer.
        let (region, header) =
            unsafe { SharedQueueHeader::create::<T, CONSUMER_SLOTS>(file, file_size) }?;
        // SAFETY: `header` belongs to `region` and was initialized above.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T` and
    ///   `CONSUMER_SLOTS`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T, CONSUMER_SLOTS>(file)?;
        // SAFETY: `header` belongs to `region` and was validated by join.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    ///
    /// The consumer starts at the current producer publication cursor and will
    /// only observe values published after it joins.
    pub fn join_as_consumer(&self) -> Result<Consumer<T, CONSUMER_SLOTS>, Error> {
        Consumer::from_queue(self.queue.clone())
    }

    /// Calculates the minimum file size for this producer's consumer-slot count.
    pub const fn minimum_file_size(capacity: usize) -> usize {
        minimum_file_size_for_consumer_slots::<T, CONSUMER_SLOTS>(capacity)
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY: forwarded from this function's safety contract.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Returns the normalized ring capacity in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Writes `item` into the queue, or returns it if no payload can be
    /// reserved.
    ///
    /// This may wait behind earlier producers that are publishing to the ring.
    /// The `try_` prefix means payload exhaustion returns `Err(item)`, not that
    /// the operation is guaranteed to be non-blocking.
    ///
    /// Queue publication uses the queue's internal release ordering.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        let mut batch = match self.reserve_write_batch(1) {
            Some(batch) => batch,
            None => return Err(item),
        };
        batch.write_next(item);
        batch.publish();
        Ok(())
    }

    /// Reserves exactly `count` payloads for writing.
    ///
    /// The payloads are returned to the free list when the batch is dropped. Call
    /// [`WriteBatch::publish`] after initializing them. Batch payload
    /// reservation uses one pool reservation operation for the whole batch.
    #[must_use]
    pub fn reserve_write_batch(&self, count: usize) -> Option<WriteBatch<'_, T, CONSUMER_SLOTS>> {
        let chain = self.queue.reserve_write_batch(count)?;
        Some(WriteBatch {
            queue: &self.queue,
            chain,
            next_write_payload_index: chain.first_payload_index(),
            written: 0,
            _marker: PhantomData,
        })
    }

    /// Abandons all buffered and reserved values left behind by previous users.
    ///
    /// # Safety
    /// - The caller must have exclusive access through this producer handle. No
    ///   other [`Producer`] or [`Consumer`] handle may be live or accessing the
    ///   shared queue.
    /// - Racing with any live producer or consumer process/thread may corrupt the
    ///   queue.
    pub unsafe fn recover_as_exclusive(&self) {
        self.queue.recover_as_exclusive();
    }
}

impl<T, const CONSUMER_SLOTS: usize> Clone for Producer<T, CONSUMER_SLOTS> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T: Send, const CONSUMER_SLOTS: usize> Send for Producer<T, CONSUMER_SLOTS> {}
unsafe impl<T: Send + Sync, const CONSUMER_SLOTS: usize> Sync for Producer<T, CONSUMER_SLOTS> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryReadError {
    Empty,
    Skipped(usize),
}

pub struct Consumer<T, const CONSUMER_SLOTS: usize = DEFAULT_CONSUMER_SLOTS> {
    queue: SharedQueue<T, CONSUMER_SLOTS>,
    hazard: ConsumerHazard,
    next: usize,
    read_batch_payloads: Vec<ProtectedPayload>,
}

impl<T, const CONSUMER_SLOTS: usize> Consumer<T, CONSUMER_SLOTS> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// The consumer starts at the current producer publication cursor and will
    /// only observe values published after it joins.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - After initialization, `file` must not be truncated or resized while any
    ///   handle remains joined to the queue.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T` and
    ///   `CONSUMER_SLOTS`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer.
        let (region, header) =
            unsafe { SharedQueueHeader::create::<T, CONSUMER_SLOTS>(file, file_size) }?;
        // SAFETY: `header` belongs to `region` and was initialized above.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Self::from_queue(queue)
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// The consumer starts at the current producer publication cursor and will
    /// only observe values published after it joins.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T` and
    ///   `CONSUMER_SLOTS`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T, CONSUMER_SLOTS>(file)?;
        // SAFETY: `header` belongs to `region` and was validated by join.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Self::from_queue(queue)
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Producer<T, CONSUMER_SLOTS> {
        Producer {
            queue: self.queue.clone(),
        }
    }

    /// Calculates the minimum file size for this consumer's consumer-slot count.
    pub const fn minimum_file_size(capacity: usize) -> usize {
        minimum_file_size_for_consumer_slots::<T, CONSUMER_SLOTS>(capacity)
    }

    /// Returns the normalized ring capacity in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    fn from_queue(queue: SharedQueue<T, CONSUMER_SLOTS>) -> Result<Self, Error> {
        let next = queue.published();
        let hazard = queue.acquire_consumer_hazard()?;
        Ok(Self {
            queue,
            hazard,
            next,
            read_batch_payloads: Vec::new(),
        })
    }

    /// Creates another consumer with the same local cursor.
    pub fn try_clone(&self) -> Result<Self, Error> {
        let hazard = self.queue.acquire_consumer_hazard()?;
        Ok(Self {
            queue: self.queue.clone(),
            hazard,
            next: self.next,
            read_batch_payloads: Vec::new(),
        })
    }

    fn readable_range(&mut self, max: usize) -> Result<(usize, usize), TryReadError> {
        if max == 0 {
            return Err(TryReadError::Empty);
        }

        let start = self.next;
        let available = self.queue.published().wrapping_sub(start);
        if available == 0 {
            return Err(TryReadError::Empty);
        }
        if available > self.queue.capacity() {
            return Err(Self::record_current_overrun(
                &self.queue,
                &mut self.next,
                start,
                0,
            ));
        }

        let count = available.min(max);
        if let Err(overrun) = self.queue.validate_window(start, count) {
            return Err(Self::record_overrun(&mut self.next, overrun, 0));
        }

        Ok((start, count))
    }

    fn record_current_overrun(
        queue: &SharedQueue<T, CONSUMER_SLOTS>,
        next: &mut usize,
        start: usize,
        min_skipped: usize,
    ) -> TryReadError {
        Self::record_overrun(
            next,
            queue.overrun_at_reserved(start, queue.reserved()),
            min_skipped,
        )
    }

    fn record_overrun(
        next: &mut usize,
        overrun: WindowOverrun,
        min_skipped: usize,
    ) -> TryReadError {
        *next = overrun.next;
        TryReadError::Skipped(overrun.skipped.max(min_skipped))
    }

    /// Repositions the consumer to the oldest item still retained in the ring.
    pub fn sync_to_oldest(&mut self) {
        self.next = self.queue.oldest_available();
    }

    /// Repositions the consumer to the producer publication cursor.
    ///
    /// Returns the number of published items skipped by this repositioning.
    pub fn sync_to_latest(&mut self) -> usize {
        let published = self.queue.published();
        let skipped = published.wrapping_sub(self.next);
        self.next = published;
        skipped
    }

    /// Attempts to read and commit one value from the queue.
    ///
    /// The payload is duplicated from hazard-protected payload storage with a
    /// typed read.
    pub fn try_read(&mut self) -> Result<T, TryReadError> {
        // SAFETY: construction establishes the broadcast payload contract.
        let direct = unsafe { self.try_read_direct()? };
        Ok(direct.read())
    }

    /// Attempts to directly access one value from the queue.
    ///
    /// The returned guard publishes this consumer's hazard range. References
    /// and pointers obtained from the guard remain stable until the guard is
    /// dropped, even if producers overwrite the ring entry.
    ///
    /// # Safety
    /// Callers must only access the returned payload in ways valid for
    /// shared-memory bytes of `T` in this process.
    pub unsafe fn try_read_direct(
        &mut self,
    ) -> Result<DirectRead<'_, T, CONSUMER_SLOTS>, TryReadError> {
        let (start, _) = self.readable_range(1)?;

        self.hazard.protect(start, 1);
        let handle = self.queue.handle_at(start);
        let Some(payload) = self.queue.payload_pool.payload_for_handle(handle) else {
            self.hazard.clear();
            return Err(Self::record_current_overrun(
                &self.queue,
                &mut self.next,
                start,
                1,
            ));
        };

        if let Err(overrun) = self.queue.validate_window(start, 1) {
            self.hazard.clear();
            return Err(Self::record_overrun(&mut self.next, overrun, 0));
        }

        Ok(DirectRead {
            next: &mut self.next,
            queue: &self.queue,
            hazard: self.hazard,
            start,
            payload,
        })
    }

    /// Attempts to directly access up to `max` values from the queue.
    ///
    /// The returned guard publishes one hazard range for the batch.
    ///
    /// # Safety
    /// Callers must only access returned payloads in ways valid for
    /// shared-memory bytes of `T` in this process.
    pub unsafe fn try_read_direct_batch(
        &mut self,
        max: usize,
    ) -> Result<DirectReadBatch<'_, T, CONSUMER_SLOTS>, TryReadError> {
        let (start, count) = self.readable_range(max)?;

        let queue = &self.queue;
        let payloads = &mut self.read_batch_payloads;
        payloads.clear();
        if payloads.capacity() < count {
            payloads.reserve_exact(count - payloads.capacity());
        }

        self.hazard.protect(start, count);
        for index in 0..count {
            let position = start.wrapping_add(index);
            let handle = queue.handle_at(position);
            let Some(payload) = queue.payload_pool.payload_for_handle(handle) else {
                self.hazard.clear();
                payloads.clear();
                return Err(Self::record_current_overrun(
                    queue,
                    &mut self.next,
                    start,
                    1,
                ));
            };
            payloads.push(payload);
        }

        if let Err(overrun) = queue.validate_window(start, count) {
            self.hazard.clear();
            payloads.clear();
            return Err(Self::record_overrun(&mut self.next, overrun, 0));
        }

        Ok(DirectReadBatch {
            next: &mut self.next,
            queue,
            hazard: self.hazard,
            start,
            payloads,
        })
    }
}

impl<T, const CONSUMER_SLOTS: usize> Drop for Consumer<T, CONSUMER_SLOTS> {
    fn drop(&mut self) {
        self.hazard.release_owner();
    }
}

unsafe impl<T: Send, const CONSUMER_SLOTS: usize> Send for Consumer<T, CONSUMER_SLOTS> {}
unsafe impl<T: Send + Sync, const CONSUMER_SLOTS: usize> Sync for Consumer<T, CONSUMER_SLOTS> {}

/// Calculates the minimum file size required for a queue with the requested
/// capacity.
///
/// The returned size is for the normalized ring capacity, rounded up to a power
/// of two.
///
/// # Panics
/// Panics for zero capacity, capacities above the broadcast maximum,
/// unsupported `T`, or layout arithmetic overflow. In const contexts these
/// panics are compile-time errors.
pub const fn minimum_file_size<T>(capacity: usize) -> usize {
    minimum_file_size_for_consumer_slots::<T, DEFAULT_CONSUMER_SLOTS>(capacity)
}

/// Calculates the minimum file size required for a queue with the requested
/// capacity and consumer-slot count.
///
/// The returned size is for the normalized ring capacity, rounded up to a power
/// of two.
///
/// # Panics
/// Panics for zero capacity, zero consumer slots, capacities above the broadcast
/// maximum, unsupported `T`, or layout arithmetic overflow. In const contexts
/// these panics are compile-time errors.
pub const fn minimum_file_size_for_consumer_slots<T, const CONSUMER_SLOTS: usize>(
    capacity: usize,
) -> usize {
    let capacity = SharedQueueHeader::ring_capacity_for_requested_capacity(capacity);
    SharedQueueHeader::total_size_for_ring_capacity::<T, CONSUMER_SLOTS>(capacity)
}

/// Calculates the minimum region size required for a queue with given capacity.
pub const fn minimum_region_size<T>(capacity: usize) -> usize {
    minimum_file_size::<T>(capacity)
}

/// Calculates the minimum region size required for a queue with given capacity
/// and consumer-slot count.
pub const fn minimum_region_size_for_consumer_slots<T, const CONSUMER_SLOTS: usize>(
    capacity: usize,
) -> usize {
    minimum_file_size_for_consumer_slots::<T, CONSUMER_SLOTS>(capacity)
}

/// Shared-memory broadcast queue view.
///
/// Binary file/region layout:
///
/// ```text
/// offset 0
/// +---------------------------+
/// | SharedQueueHeader         |
/// |   cacheline 0             | magic, version, buffer_mask, consumer_slots
/// |   cacheline 1             | payload_pool_head
/// |   cacheline 2             | payload_retired_head
/// |   cacheline 3             | producer_reservation
/// |   cacheline 4             | producer_publication
/// +---------------------------+
/// | consumer hazard slots     | [ConsumerHazardSlot; CONSUMER_SLOTS]
/// +---------------------------+
/// | ring                      | [PayloadHandle; ring_capacity]
/// +---------------------------+
/// | payload headers           | [PayloadHeader; payload_capacity]
/// +---------------------------+
/// | payload storage           | [T; payload_capacity]
/// +---------------------------+
/// ```
///
/// `ring_capacity` is `buffer_mask + 1`; `payload_capacity` is
/// `ring_capacity * payload_pool::PAYLOADS_PER_RING_ENTRY`.
struct SharedQueue<T, const CONSUMER_SLOTS: usize> {
    /// Shared metadata and producer cursors.
    header: NonNull<SharedQueueHeader>,
    /// Published values. Each entry contains a `PayloadHandle`.
    ring: NonNull<AtomicU64>,
    /// Fixed table of consumer-owned hazard ranges.
    hazard_slots: NonNull<ConsumerHazardSlot>,
    /// Payload lifecycle management: reservation, retirement, and reclamation.
    payload_pool: PayloadPool<T>,
    /// Ring index mask. The ring capacity is `buffer_mask + 1`.
    buffer_mask: usize,

    // NB: Region must be declared last so header/ring/payload_pool stay valid.
    /// Memory mapping that owns the header, ring, payload headers, and payload data.
    region: Arc<Region>,
}

#[derive(Clone, Copy)]
struct WindowOverrun {
    skipped: usize,
    next: usize,
}

#[repr(C, align(64))]
struct CacheAlignedAtomicU64 {
    inner: AtomicU64,
}

impl core::ops::Deref for CacheAlignedAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

const HAZARD_FREE: u64 = 0;
const HAZARD_INACTIVE: u64 = 1;
const HAZARD_ACTIVE: u64 = 2;

#[repr(C, align(64))]
struct ConsumerHazardSlot {
    state: AtomicU64,
    start: AtomicUsize,
    end: AtomicUsize,
}

impl ConsumerHazardSlot {
    const fn new() -> Self {
        Self {
            state: AtomicU64::new(HAZARD_FREE),
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        }
    }

    fn acquire(&self) -> bool {
        self.state
            .compare_exchange(
                HAZARD_FREE,
                HAZARD_INACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn protect(&self, start: usize, end: usize) {
        self.start.store(start, Ordering::Relaxed);
        self.end.store(end, Ordering::Relaxed);
        self.state.store(HAZARD_ACTIVE, Ordering::SeqCst);
    }

    fn clear(&self) {
        self.state.store(HAZARD_INACTIVE, Ordering::SeqCst);
    }

    fn release_owner(&self) {
        let _ = self.state.compare_exchange(
            HAZARD_INACTIVE,
            HAZARD_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn reset(&self) {
        self.start.store(0, Ordering::Relaxed);
        self.end.store(0, Ordering::Relaxed);
        self.state.store(HAZARD_FREE, Ordering::Release);
    }

    fn active_range(&self) -> Option<HazardRange> {
        if self.state.load(Ordering::SeqCst) != HAZARD_ACTIVE {
            return None;
        }

        let start = self.start.load(Ordering::Relaxed);
        let end = self.end.load(Ordering::Relaxed);
        if start == end {
            None
        } else {
            Some(HazardRange { start, end })
        }
    }
}

#[derive(Clone, Copy)]
struct ConsumerHazard {
    slot: NonNull<ConsumerHazardSlot>,
}

impl ConsumerHazard {
    fn protect(self, start: usize, count: usize) {
        // SAFETY: `slot` points into the live shared queue hazard table.
        unsafe { self.slot.as_ref() }.protect(start, start.wrapping_add(count));
    }

    fn clear(self) {
        // SAFETY: `slot` points into the live shared queue hazard table.
        unsafe { self.slot.as_ref() }.clear();
    }

    fn release_owner(self) {
        // SAFETY: `slot` points into the live shared queue hazard table.
        unsafe { self.slot.as_ref() }.release_owner();
    }
}

#[derive(Clone, Copy)]
struct HazardRange {
    start: usize,
    end: usize,
}

impl HazardRange {
    fn protects(self, sequence: usize) -> bool {
        sequence.wrapping_sub(self.start) < self.end.wrapping_sub(self.start)
    }
}

#[derive(Clone, Copy)]
struct HazardSnapshot<const CONSUMER_SLOTS: usize> {
    ranges: [HazardRange; CONSUMER_SLOTS],
    len: usize,
}

impl<const CONSUMER_SLOTS: usize> HazardSnapshot<CONSUMER_SLOTS> {
    fn new() -> Self {
        Self {
            ranges: [HazardRange { start: 0, end: 0 }; CONSUMER_SLOTS],
            len: 0,
        }
    }

    fn push(&mut self, range: HazardRange) {
        debug_assert!(self.len < CONSUMER_SLOTS);
        self.ranges[self.len] = range;
        self.len += 1;
    }

    fn protects(&self, sequence: usize) -> bool {
        self.ranges[..self.len]
            .iter()
            .any(|range| range.protects(sequence))
    }
}

impl<T, const CONSUMER_SLOTS: usize> Clone for SharedQueue<T, CONSUMER_SLOTS> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            ring: self.ring,
            hazard_slots: self.hazard_slots,
            payload_pool: self.payload_pool.clone(),
            buffer_mask: self.buffer_mask,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T, const CONSUMER_SLOTS: usize> SharedQueue<T, CONSUMER_SLOTS> {
    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    #[inline]
    fn published(&self) -> usize {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        unsafe { self.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire)
    }

    #[inline]
    fn reserved(&self) -> usize {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        unsafe { self.header.as_ref() }
            .producer_reservation
            .load(Ordering::Acquire)
    }

    #[inline]
    fn oldest_available(&self) -> usize {
        self.reserved().saturating_sub(self.capacity())
    }

    #[inline]
    fn overrun_at_reserved(&self, start: usize, reserved: usize) -> WindowOverrun {
        let capacity = self.capacity();
        WindowOverrun {
            skipped: reserved.wrapping_sub(start).wrapping_sub(capacity),
            next: reserved.saturating_sub(capacity),
        }
    }

    fn reserve_write_batch(&self, count: usize) -> Option<ReservedPayloads> {
        if count == 0 || count > self.capacity() {
            return None;
        }

        self.payload_pool.reserve_payloads_exact(count).or_else(|| {
            self.reclaim_retired_payloads();
            self.payload_pool.reserve_payloads_exact(count)
        })
    }

    fn reserve_ring_positions(&self, count: usize) -> usize {
        let capacity = self.capacity();
        debug_assert!(count != 0 && count <= capacity);
        let max_pending = capacity.wrapping_sub(count);

        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        let mut producer_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let pending = producer_reservation.wrapping_sub(producer_publication);
            if pending > max_pending {
                core::hint::spin_loop();
                producer_reservation = header.producer_reservation.load(Ordering::Relaxed);
                continue;
            }

            let new_reservation = producer_reservation.wrapping_add(count);
            match header.producer_reservation.compare_exchange_weak(
                producer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return producer_reservation,
                Err(current) => producer_reservation = current,
            }
        }
    }

    fn publish_reserved_payload_chain(&self, chain: ReservedPayloads) {
        let count = chain.len();
        debug_assert!(count != 0 && count <= self.capacity());

        let start = self.reserve_ring_positions(count);
        let mut retire_batch = PayloadReleaseBatch::new();
        let mut cursor = self
            .payload_pool
            .payload_cursor(chain.first_payload_index());
        let last_index = count.wrapping_sub(1);
        for index in 0..count {
            let payload_index = cursor.current();
            let handle = self.payload_pool.handle_for_payload(payload_index);
            let position = start.wrapping_add(index);
            let old = self.install_reserved_handle(position, handle);
            self.payload_pool.retire_to_batch(
                old,
                position.wrapping_sub(self.capacity()),
                &mut retire_batch,
            );
            if index != last_index {
                cursor.advance();
            }
        }

        self.publish_reserved(start, count);
        let hazards = self.hazard_snapshot();
        self.payload_pool
            .reclaim_retired_batch(retire_batch, |sequence| hazards.protects(sequence));
        self.payload_pool
            .reclaim_retired_payloads(|sequence| hazards.protects(sequence));
    }

    fn publish_reserved(&self, start: usize, count: usize) {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        while header.producer_publication.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }

        header
            .producer_publication
            .store(start.wrapping_add(count), Ordering::Release);
    }

    fn install_reserved_handle(&self, position: usize, handle: PayloadHandle) -> PayloadHandle {
        let ring_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.ring.add(ring_index).as_ref() };
        PayloadHandle(cell.swap(handle.0, Ordering::AcqRel))
    }

    #[inline]
    fn handle_at(&self, position: usize) -> PayloadHandle {
        let ring_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        PayloadHandle(unsafe { self.ring.add(ring_index).as_ref() }.load(Ordering::Acquire))
    }

    #[inline]
    fn validate_window(&self, start: usize, count: usize) -> Result<(), WindowOverrun> {
        debug_assert!(count <= self.capacity());
        let reserved = self.reserved();
        if reserved.wrapping_sub(start) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        if reserved.wrapping_sub(start.wrapping_add(count)) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        Ok(())
    }

    fn acquire_consumer_hazard(&self) -> Result<ConsumerHazard, Error> {
        for index in 0..CONSUMER_SLOTS {
            // SAFETY: index is bounded by CONSUMER_SLOTS.
            let slot = unsafe { self.hazard_slots.add(index) };
            // SAFETY: slot points into the live hazard table.
            if unsafe { slot.as_ref() }.acquire() {
                return Ok(ConsumerHazard { slot });
            }
        }

        Err(Error::ConsumerSlotsExhausted)
    }

    fn hazard_snapshot(&self) -> HazardSnapshot<CONSUMER_SLOTS> {
        let mut snapshot = HazardSnapshot::new();
        for index in 0..CONSUMER_SLOTS {
            // SAFETY: index is bounded by CONSUMER_SLOTS.
            let slot = unsafe { self.hazard_slots.add(index).as_ref() };
            if let Some(range) = slot.active_range() {
                snapshot.push(range);
            }
        }
        snapshot
    }

    fn reclaim_retired_payloads(&self) {
        let hazards = self.hazard_snapshot();
        self.payload_pool
            .reclaim_retired_payloads(|sequence| hazards.protects(sequence));
    }

    fn recover_as_exclusive(&self) {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);

        for index in 0..self.capacity() {
            // SAFETY: index is in bounds.
            unsafe { self.ring.add(index).as_ref() }
                .store(PayloadHandle::EMPTY.0, Ordering::Release);
        }
        for index in 0..CONSUMER_SLOTS {
            // SAFETY: index is bounded by CONSUMER_SLOTS.
            unsafe { self.hazard_slots.add(index).as_ref() }.reset();
        }
        self.payload_pool.recover_as_exclusive();
    }

    /// Creates a new shared queue from a header pointer and region.
    ///
    /// # Safety
    /// - `region` must back the allocation at `header`.
    /// - `header` must be non-null and properly aligned.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        let header_ref = unsafe { header.as_ref() };
        let buffer_mask = header_ref.buffer_mask as usize;
        let buffer_size_in_items = buffer_mask.wrapping_add(1);
        SharedQueueHeader::validate_ring_capacity_for_file::<T, CONSUMER_SLOTS>(
            buffer_size_in_items,
            region.size(),
        )?;

        let payload_capacity =
            SharedQueueHeader::payload_capacity_for_ring_capacity(buffer_size_in_items)?;
        // SAFETY: layout validation above proves these regions exist.
        let hazard_slots = unsafe { SharedQueueHeader::hazard_slots_from_header(header) };
        // SAFETY: layout validation above proves these regions exist.
        let ring = unsafe { SharedQueueHeader::ring_from_header::<CONSUMER_SLOTS>(header) };
        // SAFETY: layout validation above proves these regions exist.
        let payload_headers = unsafe {
            SharedQueueHeader::payload_headers_from_header::<CONSUMER_SLOTS>(
                header,
                buffer_size_in_items,
            )
        };
        // SAFETY: layout validation above proves these regions exist.
        let payloads = unsafe {
            SharedQueueHeader::payloads_from_header::<T, CONSUMER_SLOTS>(
                header,
                buffer_size_in_items,
            )
        };

        Ok(Self {
            header,
            ring,
            payload_pool: PayloadPool::new(
                NonNull::from(&header_ref.payload_pool_head.inner),
                NonNull::from(&header_ref.payload_retired_head.inner),
                payload_headers,
                payloads,
                payload_capacity,
            ),
            region,
            buffer_mask,
            hazard_slots,
        })
    }
}

#[repr(C)]
struct SharedQueueHeader {
    // Cold metadata cacheline.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,
    consumer_slots: u32,

    /// Payload-pool free-list head.
    payload_pool_head: CacheAlignedAtomicU64,
    /// Payload-pool retired-list head.
    payload_retired_head: CacheAlignedAtomicU64,

    /// Producer reservation cursor.
    ///
    /// Producers atomically advance this with CAS to claim ring positions. A
    /// claimed-but-unpublished overwrite may make older unprotected items
    /// unavailable to consumers.
    producer_reservation: CacheAlignedAtomicSize,
    /// Producer publication cursor.
    ///
    /// Producers advance this in-order after publishing payload handles into
    /// the ring. Consumers use it to determine which sequence numbers exist.
    producer_publication: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    unsafe fn create<T, const CONSUMER_SLOTS: usize>(
        file: &File,
        size: usize,
    ) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        file.set_len(size as u64)?;

        let ring_capacity = Self::calculate_ring_capacity::<T, CONSUMER_SLOTS>(size)?;
        let region = Region::map_file(file, size)?;
        let header = region.addr().cast::<Self>();
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        unsafe { Self::initialize::<CONSUMER_SLOTS>(header, ring_capacity) };
        Ok((region, header))
    }

    const fn hazard_slots_offset() -> usize {
        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<ConsumerHazardSlot>())
    }

    const fn ring_offset<const CONSUMER_SLOTS: usize>() -> usize {
        (Self::hazard_slots_offset() + CONSUMER_SLOTS * core::mem::size_of::<ConsumerHazardSlot>())
            .next_multiple_of(core::mem::align_of::<AtomicU64>())
    }

    const fn payload_headers_offset<const CONSUMER_SLOTS: usize>(ring_capacity: usize) -> usize {
        (Self::ring_offset::<CONSUMER_SLOTS>() + ring_capacity * core::mem::size_of::<AtomicU64>())
            .next_multiple_of(core::mem::align_of::<PayloadHeader>())
    }

    const fn payloads_offset<T, const CONSUMER_SLOTS: usize>(ring_capacity: usize) -> usize {
        let payload_capacity = ring_capacity * payload_pool::PAYLOADS_PER_RING_ENTRY;
        (Self::payload_headers_offset::<CONSUMER_SLOTS>(ring_capacity)
            + payload_capacity * core::mem::size_of::<PayloadHeader>())
        .next_multiple_of(core::mem::align_of::<T>())
    }

    const fn ring_capacity_for_requested_capacity(capacity: usize) -> usize {
        assert!(capacity != 0, "broadcast capacity must be non-zero");
        assert!(
            capacity <= MAX_RING_CAPACITY,
            "broadcast capacity exceeds maximum"
        );

        normalized_capacity(capacity)
    }

    fn payload_capacity_for_ring_capacity(ring_capacity: usize) -> Result<usize, Error> {
        let Some(payload_capacity) = payload_pool::capacity_for_ring_capacity(ring_capacity) else {
            return Err(Error::InvalidBufferSize);
        };
        Ok(payload_capacity)
    }

    const fn total_size_checked<T, const CONSUMER_SLOTS: usize>(
        ring_capacity: usize,
    ) -> Option<usize> {
        if core::mem::size_of::<T>() == 0
            || core::mem::align_of::<T>() > crate::shmem::MINIMUM_REGION_ALIGNMENT
            || CONSUMER_SLOTS == 0
            || CONSUMER_SLOTS > u32::MAX as usize
        {
            return None;
        }

        let Some(payload_capacity) =
            ring_capacity.checked_mul(payload_pool::PAYLOADS_PER_RING_ENTRY)
        else {
            return None;
        };
        let Some(ring_bytes) = ring_capacity.checked_mul(core::mem::size_of::<AtomicU64>()) else {
            return None;
        };
        let Some(hazard_slots_bytes) =
            CONSUMER_SLOTS.checked_mul(core::mem::size_of::<ConsumerHazardSlot>())
        else {
            return None;
        };
        let Some(hazard_slots_end) = Self::hazard_slots_offset().checked_add(hazard_slots_bytes)
        else {
            return None;
        };
        let Some(ring_offset) =
            checked_next_multiple_of(hazard_slots_end, core::mem::align_of::<AtomicU64>())
        else {
            return None;
        };
        let Some(ring_end) = ring_offset.checked_add(ring_bytes) else {
            return None;
        };
        let Some(payload_headers_offset) =
            checked_next_multiple_of(ring_end, core::mem::align_of::<PayloadHeader>())
        else {
            return None;
        };
        let Some(payload_header_bytes) =
            payload_capacity.checked_mul(core::mem::size_of::<PayloadHeader>())
        else {
            return None;
        };
        let Some(payload_headers_end) = payload_headers_offset.checked_add(payload_header_bytes)
        else {
            return None;
        };
        let Some(payloads_offset) =
            checked_next_multiple_of(payload_headers_end, core::mem::align_of::<T>())
        else {
            return None;
        };
        let Some(payload_bytes) = payload_capacity.checked_mul(core::mem::size_of::<T>()) else {
            return None;
        };
        payloads_offset.checked_add(payload_bytes)
    }

    const fn total_size_for_ring_capacity<T, const CONSUMER_SLOTS: usize>(
        ring_capacity: usize,
    ) -> usize {
        assert!(
            CONSUMER_SLOTS != 0,
            "broadcast consumer slot count must be non-zero"
        );
        assert!(
            CONSUMER_SLOTS <= u32::MAX as usize,
            "broadcast consumer slot count exceeds maximum"
        );
        assert!(
            core::mem::size_of::<T>() > 0,
            "zero-sized types are not supported"
        );
        assert!(
            core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
            "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
        );

        match Self::total_size_checked::<T, CONSUMER_SLOTS>(ring_capacity) {
            Some(size) => size,
            None => panic!("broadcast queue size overflow"),
        }
    }

    fn validate_ring_capacity_for_file<T, const CONSUMER_SLOTS: usize>(
        ring_capacity: usize,
        file_size: usize,
    ) -> Result<(), Error> {
        if ring_capacity == 0
            || !ring_capacity.is_power_of_two()
            || ring_capacity > MAX_RING_CAPACITY
        {
            return Err(Error::InvalidBufferSize);
        }

        let Some(minimum_size) = Self::total_size_checked::<T, CONSUMER_SLOTS>(ring_capacity)
        else {
            return Err(Error::InvalidBufferSize);
        };
        if minimum_size > file_size {
            return Err(Error::InvalidBufferSize);
        }

        Ok(())
    }

    fn calculate_ring_capacity<T, const CONSUMER_SLOTS: usize>(
        file_size: usize,
    ) -> Result<usize, Error> {
        let mut capacity = 1usize;
        let Some(minimum) = Self::total_size_checked::<T, CONSUMER_SLOTS>(capacity) else {
            return Err(Error::InvalidBufferSize);
        };
        if file_size < minimum {
            return Err(Error::InvalidBufferSize);
        }

        while capacity < MAX_RING_CAPACITY {
            let next = capacity << 1;
            let Some(size) = Self::total_size_checked::<T, CONSUMER_SLOTS>(next) else {
                break;
            };
            if size > file_size {
                break;
            }
            capacity = next;
        }

        Ok(capacity)
    }

    unsafe fn initialize<const CONSUMER_SLOTS: usize>(
        mut header_ptr: NonNull<Self>,
        ring_capacity: usize,
    ) {
        let payload_capacity = Self::payload_capacity_for_ring_capacity(ring_capacity)
            .expect("validated payload capacity");

        // SAFETY: caller guarantees unique access during initialization.
        let header = unsafe { header_ptr.as_mut() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);
        header.payload_pool_head.store(
            payload_pool::initial_free_head(payload_capacity),
            Ordering::Release,
        );
        header
            .payload_retired_head
            .store(payload_pool::initial_retired_head(), Ordering::Release);
        header.buffer_mask = u32::try_from(ring_capacity - 1).unwrap();
        header.consumer_slots = u32::try_from(CONSUMER_SLOTS).expect("validated consumer slots");
        header.version = VERSION;

        // SAFETY: The calculated layout reserves CONSUMER_SLOTS hazard slots.
        let hazard_slots = unsafe { Self::hazard_slots_from_header(header_ptr) };
        for index in 0..CONSUMER_SLOTS {
            // SAFETY: index is in bounds for the hazard slot array.
            unsafe {
                hazard_slots
                    .add(index)
                    .as_ptr()
                    .write(ConsumerHazardSlot::new())
            };
        }

        // SAFETY: The calculated layout reserves `ring_capacity` entries.
        let ring = unsafe { Self::ring_from_header::<CONSUMER_SLOTS>(header_ptr) };
        for index in 0..ring_capacity {
            // SAFETY: index is in bounds for the ring array.
            unsafe {
                ring.add(index)
                    .as_ptr()
                    .write(AtomicU64::new(PayloadHandle::EMPTY.0))
            };
        }

        let payload_headers = unsafe {
            Self::payload_headers_from_header::<CONSUMER_SLOTS>(header_ptr, ring_capacity)
        };
        // SAFETY: The calculated layout reserves `payload_capacity` payload headers.
        unsafe { payload_pool::initialize_payload_headers(payload_headers, payload_capacity) };

        header.magic.store(MAGIC, Ordering::Release);
    }

    fn join<T, const CONSUMER_SLOTS: usize>(
        file: &File,
    ) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        let file_size = file.metadata()?.len() as usize;
        if file_size < core::mem::size_of::<Self>() {
            return Err(Error::InvalidBufferSize);
        }
        let region = Region::map_file(file, file_size)?;
        let header = region.addr().cast::<Self>();
        {
            // SAFETY: mmap alignment is sufficient for the header.
            let header_ref = unsafe { header.as_ref() };
            if header_ref.magic.load(Ordering::Acquire) != MAGIC {
                return Err(Error::InvalidMagic);
            }
            if header_ref.version != VERSION {
                return Err(Error::InvalidVersion {
                    expected: VERSION,
                    actual: header_ref.version,
                });
            }
            if header_ref.consumer_slots as usize != CONSUMER_SLOTS {
                return Err(Error::InvalidConsumerSlots {
                    expected: CONSUMER_SLOTS,
                    actual: header_ref.consumer_slots as usize,
                });
            }
            let ring_capacity = (header_ref.buffer_mask as usize).wrapping_add(1);
            Self::validate_ring_capacity_for_file::<T, CONSUMER_SLOTS>(ring_capacity, file_size)?;
        }

        Ok((region, header))
    }

    unsafe fn ring_from_header<const CONSUMER_SLOTS: usize>(
        header: NonNull<Self>,
    ) -> NonNull<AtomicU64> {
        let offset = Self::ring_offset::<CONSUMER_SLOTS>();
        // SAFETY: caller guarantees the allocation includes the ring layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn hazard_slots_from_header(header: NonNull<Self>) -> NonNull<ConsumerHazardSlot> {
        let offset = Self::hazard_slots_offset();
        // SAFETY: caller guarantees the allocation includes the hazard-slot layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payload_headers_from_header<const CONSUMER_SLOTS: usize>(
        header: NonNull<Self>,
        ring_capacity: usize,
    ) -> NonNull<PayloadHeader> {
        let offset = Self::payload_headers_offset::<CONSUMER_SLOTS>(ring_capacity);
        // SAFETY: caller guarantees the allocation includes the payload-header layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payloads_from_header<T, const CONSUMER_SLOTS: usize>(
        header: NonNull<Self>,
        ring_capacity: usize,
    ) -> NonNull<T> {
        let offset = Self::payloads_offset::<T, CONSUMER_SLOTS>(ring_capacity);
        // SAFETY: caller guarantees the allocation includes the payload layout.
        unsafe { header.byte_add(offset).cast() }
    }
}

const fn checked_next_multiple_of(value: usize, multiple: usize) -> Option<usize> {
    let remainder = value % multiple;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(multiple - remainder)
    }
}

/// Result of writing an iterator into a [`WriteBatch`].
///
/// This reports how many values were consumed and returns the advanced iterator
/// state so callers do not lose values when the batch fills before the iterator
/// is exhausted.
#[must_use = "the returned iterator may contain values that were not written"]
pub struct WriteIterResult<I> {
    written: usize,
    batch_filled: bool,
    remaining: I,
}

impl<I> WriteIterResult<I> {
    /// Number of values consumed from the iterator and written into the batch.
    pub fn written(&self) -> usize {
        self.written
    }

    /// Returns `true` if every reserved payload in the batch has been written.
    ///
    /// A batch must be filled before it can be published.
    pub fn batch_filled(&self) -> bool {
        self.batch_filled
    }

    /// Returns the advanced iterator state.
    ///
    /// If `batch_filled` is true, this iterator may still contain values that
    /// did not fit in the batch.
    pub fn into_remaining(self) -> I {
        self.remaining
    }
}

#[must_use]
pub struct WriteBatch<'a, T, const CONSUMER_SLOTS: usize = DEFAULT_CONSUMER_SLOTS> {
    queue: &'a SharedQueue<T, CONSUMER_SLOTS>,
    chain: ReservedPayloads,
    next_write_payload_index: u32,
    written: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T, const CONSUMER_SLOTS: usize> WriteBatch<'a, T, CONSUMER_SLOTS> {
    pub fn len(&self) -> usize {
        self.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn advance_payload(&mut self) -> u32 {
        assert!(self.written < self.len());
        let payload_index = self.next_write_payload_index;
        self.written += 1;
        if self.written != self.len() {
            self.next_write_payload_index =
                self.queue.payload_pool.next_payload_index(payload_index);
        }
        payload_index
    }

    /// Writes a value into the next reserved payload.
    ///
    /// This is the fast sequential batch path; it walks the private payload
    /// chain once and performs ordinary stores.
    ///
    /// # Panics
    /// Panics if every reserved payload has already been consumed with this
    /// method.
    pub fn write_next(&mut self, value: T) {
        let payload_index = self.advance_payload();
        // SAFETY: This batch owns the reserved payload.
        unsafe {
            self.queue
                .payload_pool
                .payload_at(payload_index)
                .as_ptr()
                .write(value)
        };
    }

    /// Writes values from `values` into reserved payloads in reservation order.
    ///
    /// Stops when either the iterator is exhausted or every reserved payload
    /// has been written. The returned iterator is the advanced iterator state;
    /// no extra value is consumed to determine whether values remain.
    pub fn write_iter<I>(&mut self, values: I) -> WriteIterResult<I::IntoIter>
    where
        I: IntoIterator<Item = T>,
    {
        let mut values = values.into_iter();
        let mut written = 0usize;
        while self.written < self.len() {
            let Some(value) = values.next() else {
                break;
            };
            self.write_next(value);
            written += 1;
        }

        WriteIterResult {
            written,
            batch_filled: self.written == self.len(),
            remaining: values,
        }
    }

    /// Publishes the reserved payloads to the broadcast ring.
    ///
    /// # Panics
    /// Panics unless every reserved payload has been written.
    pub fn publish(self) {
        assert_eq!(self.written, self.len());
        let this = ManuallyDrop::new(self);
        this.queue.publish_reserved_payload_chain(this.chain);
    }
}

impl<'a, T, const CONSUMER_SLOTS: usize> Drop for WriteBatch<'a, T, CONSUMER_SLOTS> {
    fn drop(&mut self) {
        self.queue.payload_pool.cancel_reserved_payloads(self.chain);
    }
}

#[must_use]
pub struct DirectRead<'a, T, const CONSUMER_SLOTS: usize = DEFAULT_CONSUMER_SLOTS> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T, CONSUMER_SLOTS>,
    hazard: ConsumerHazard,
    start: usize,
    payload: ProtectedPayload,
}

impl<'a, T, const CONSUMER_SLOTS: usize> DirectRead<'a, T, CONSUMER_SLOTS> {
    /// Returns a raw pointer to the hazard-protected payload.
    pub fn as_ptr(&self) -> *const T {
        self.queue
            .payload_pool
            .protected_payload_ptr(self.payload)
            .as_ptr()
    }

    /// Duplicates the hazard-protected payload with a typed read and advances the
    /// consumer cursor.
    ///
    /// The payload remains in shared memory; this returns a by-value duplicate.
    pub fn read(self) -> T {
        // SAFETY: This guard protects an initialized payload.
        let value = unsafe { self.as_ptr().read() };
        self.commit();
        value
    }

    /// Advances the consumer cursor and clears the hazard when the guard is
    /// dropped.
    pub fn commit(self) {
        *self.next = self.start.wrapping_add(1);
    }
}

impl<'a, T, const CONSUMER_SLOTS: usize> AsRef<T> for DirectRead<'a, T, CONSUMER_SLOTS> {
    /// Returns a shared reference to the hazard-protected payload.
    fn as_ref(&self) -> &T {
        // SAFETY: This guard publishes a hazard for an initialized payload.
        unsafe {
            self.queue
                .payload_pool
                .protected_payload_ptr(self.payload)
                .as_ref()
        }
    }
}

impl<'a, T, const CONSUMER_SLOTS: usize> Drop for DirectRead<'a, T, CONSUMER_SLOTS> {
    fn drop(&mut self) {
        self.hazard.clear();
    }
}

#[must_use]
pub struct DirectReadBatch<'a, T, const CONSUMER_SLOTS: usize = DEFAULT_CONSUMER_SLOTS> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T, CONSUMER_SLOTS>,
    hazard: ConsumerHazard,
    start: usize,
    payloads: &'a mut Vec<ProtectedPayload>,
}

impl<'a, T, const CONSUMER_SLOTS: usize> DirectReadBatch<'a, T, CONSUMER_SLOTS> {
    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    /// Returns a pointer to the hazard-protected payload at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len`.
    pub fn as_ptr(&self, index: usize) -> *const T {
        assert!(index < self.payloads.len());
        self.queue
            .payload_pool
            .protected_payload_ptr(self.payloads[index])
            .as_ptr()
    }

    /// Returns a shared reference to the hazard-protected payload at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len`.
    pub fn as_ref(&self, index: usize) -> &T {
        assert!(index < self.payloads.len());
        // SAFETY: The index was checked above and this guard publishes a hazard.
        unsafe {
            self.queue
                .payload_pool
                .protected_payload_ptr(self.payloads[index])
                .as_ref()
        }
    }

    /// Duplicates the hazard-protected payload at `index` with a typed read.
    ///
    /// The payload remains in shared memory; this returns a by-value duplicate.
    ///
    /// # Panics
    /// Panics if `index >= len`.
    pub fn read(&self, index: usize) -> T {
        assert!(index < self.payloads.len());
        // SAFETY: The index was checked above and this guard publishes a hazard.
        unsafe { self.as_ptr(index).read() }
    }

    /// Advances the consumer cursor and clears the hazard when the guard is
    /// dropped.
    pub fn commit(self) {
        *self.next = self.start.wrapping_add(self.payloads.len());
    }
}

impl<'a, T, const CONSUMER_SLOTS: usize> Drop for DirectReadBatch<'a, T, CONSUMER_SLOTS> {
    fn drop(&mut self) {
        self.hazard.clear();
        self.payloads.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 8;
    const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);

    fn create_test_queue<T>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        create_test_queue_with_slots::<T, DEFAULT_CONSUMER_SLOTS>(file_size)
    }

    fn create_test_queue_with_slots<T, const CONSUMER_SLOTS: usize>(
        file_size: usize,
    ) -> (
        File,
        Producer<T, CONSUMER_SLOTS>,
        Consumer<T, CONSUMER_SLOTS>,
    ) {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe { Producer::<T, CONSUMER_SLOTS>::create(&file, file_size) }
            .expect("Failed to create producer");
        let consumer =
            unsafe { Consumer::<T, CONSUMER_SLOTS>::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    #[test]
    fn test_producer_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..BUFFER_CAPACITY {
            producer.try_write(i as Item).unwrap();
        }

        for i in 0..BUFFER_CAPACITY {
            assert_eq!(consumer.try_read().unwrap(), i as Item);
        }
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_reserve_batch_and_read_items() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = producer
            .reserve_write_batch(4)
            .expect("reserve_batch failed");
        let result = batch.write_iter((0..batch.len()).map(|value| value as u64));
        assert_eq!(result.written(), 4);
        assert!(result.batch_filled());
        batch.publish();

        for expected in 0..4 {
            assert_eq!(consumer.try_read().unwrap(), expected);
        }
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_underfilled_write_iter_drop_cancels_publication() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        {
            let mut batch = producer
                .reserve_write_batch(4)
                .expect("reserve_batch failed");
            let result = batch.write_iter(0..2);
            assert_eq!(result.written(), 2);
            assert!(!result.batch_filled());
        }

        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_overfilled_write_iter_returns_remaining_values() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = producer
            .reserve_write_batch(2)
            .expect("reserve_batch failed");
        let result = batch.write_iter(0..4);
        assert_eq!(result.written(), 2);
        assert!(result.batch_filled());
        assert_eq!(result.into_remaining().collect::<Vec<_>>(), vec![2, 3]);
        batch.publish();

        assert_eq!(consumer.try_read().unwrap(), 0);
        assert_eq!(consumer.try_read().unwrap(), 1);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_unpublished_batch_drop_cancels_publication() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        {
            let _batch = producer.reserve_write_batch(1).expect("reserve write");
        }

        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_unpublished_batch_drop_recycles_payloads() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let batch1 = producer
            .reserve_write_batch(BUFFER_CAPACITY)
            .expect("reserve first batch");
        let _batch2 = producer
            .reserve_write_batch(BUFFER_CAPACITY)
            .expect("reserve second batch");
        assert!(producer.reserve_write_batch(1).is_none());

        drop(batch1);

        assert!(producer.reserve_write_batch(BUFFER_CAPACITY).is_some());
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_publication_order_follows_publish_not_reserve() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        let mut first = producer.reserve_write_batch(1).expect("reserve first");
        let mut second = producer2.reserve_write_batch(1).expect("reserve second");

        second.write_next(2);
        second.publish();
        first.write_next(1);
        first.publish();

        assert_eq!(consumer.try_read().unwrap(), 2);
        assert_eq!(consumer.try_read().unwrap(), 1);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_multiple_consumers_receive_all_values() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = consumer.try_clone().expect("clone consumer");

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();
        while let Ok(v) = consumer.try_read() {
            values1.push(v);
        }
        while let Ok(v) = consumer2.try_read() {
            values2.push(v);
        }

        assert_eq!(values1, vec![0, 1, 2, 3]);
        assert_eq!(values2, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_overrun_repositions_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..(BUFFER_CAPACITY as u64 + 2) {
            producer.try_write(i).unwrap();
        }

        assert_eq!(consumer.try_read(), Err(TryReadError::Skipped(2)));
        for expected in 2..(BUFFER_CAPACITY as u64 + 2) {
            assert_eq!(consumer.try_read().unwrap(), expected);
        }
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_publish_repositions_unprotected_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut lagging_consumer = consumer.try_clone().expect("clone consumer");

        producer.try_write(1).unwrap();
        let mut batch = producer
            .reserve_write_batch(BUFFER_CAPACITY)
            .expect("reserve wraparound batch");
        let len = batch.len();
        let result = batch.write_iter((0..len).map(|index| 10 + index as u64));
        assert_eq!(result.written(), len);
        assert!(result.batch_filled());

        assert_eq!(consumer.try_read().unwrap(), 1);

        batch.publish();

        assert_eq!(lagging_consumer.try_read(), Err(TryReadError::Skipped(1)));
    }

    #[test]
    fn test_sync_modes() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        assert_eq!(consumer.sync_to_latest(), 4);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));

        for i in 4..8 {
            producer.try_write(i).unwrap();
        }

        consumer.sync_to_oldest();
        for expected in 0..8 {
            assert_eq!(consumer.try_read().unwrap(), expected);
        }
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_try_read_direct_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(42).unwrap();

        let direct = unsafe { consumer.try_read_direct() }.unwrap();
        assert_eq!(*direct.as_ref(), 42);
        direct.commit();
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_direct_read_by_value_commits_payload() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(42).unwrap();

        let direct = unsafe { consumer.try_read_direct() }.unwrap();
        let value = direct.read();
        assert_eq!(value, 42);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_try_read_direct_batch_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let direct = unsafe { consumer.try_read_direct_batch(8) }.unwrap();
        assert_eq!(direct.len(), 4);
        for index in 0..direct.len() {
            assert_eq!(*direct.as_ref(index), index as u64);
        }
        direct.commit();
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_direct_read_batch_copies_payloads() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let direct = unsafe { consumer.try_read_direct_batch(8) }.unwrap();
        let mut values = Vec::with_capacity(direct.len());
        for index in 0..direct.len() {
            values.push(direct.read(index));
        }
        direct.commit();
        for (index, value) in values.into_iter().enumerate() {
            assert_eq!(value, index as u64);
        }
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_hazard_direct_read_survives_ring_overwrite() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(1).unwrap();
        let direct = unsafe { consumer.try_read_direct() }.unwrap();

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(10 + i).unwrap();
        }

        assert_eq!(*direct.as_ref(), 1);
        direct.commit();
    }

    #[test]
    fn test_hazard_direct_batch_survives_ring_overwrite() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for item in 0..4 {
            producer.try_write(item).unwrap();
        }

        let direct = unsafe { consumer.try_read_direct_batch(4) }.unwrap();
        for item in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(20 + item).unwrap();
        }

        for index in 0..direct.len() {
            assert_eq!(*direct.as_ref(index), index as u64);
        }
        direct.commit();
    }

    #[test]
    fn test_payload_not_reused_while_direct_guard_is_alive() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(1).unwrap();
        let direct = unsafe { consumer.try_read_direct() }.unwrap();
        let ptr = direct.as_ptr();

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(100 + i).unwrap();
        }

        unsafe {
            assert_eq!(*ptr, 1);
        }
        drop(direct);
    }

    #[test]
    fn test_reservation_fails_when_all_payloads_are_hazard_protected() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(i).unwrap();
        }
        let guarded = unsafe { consumer.try_read_direct_batch(BUFFER_CAPACITY) }.unwrap();
        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(100 + i).unwrap();
        }

        assert!(producer.reserve_write_batch(1).is_none());
        drop(guarded);
        assert!(producer.reserve_write_batch(1).is_some());
    }

    #[test]
    fn test_batch_release_recycles_payloads() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(i).unwrap();
        }
        let guarded = unsafe { consumer.try_read_direct_batch(BUFFER_CAPACITY) }.unwrap();
        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(100 + i).unwrap();
        }

        assert!(producer.reserve_write_batch(BUFFER_CAPACITY).is_none());
        drop(guarded);
        assert!(producer.reserve_write_batch(BUFFER_CAPACITY).is_some());
    }

    #[test]
    fn test_recover_as_exclusive_clears_stale_hazard() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");
        let mut consumer = unsafe { Consumer::<Item>::join(&file) }.expect("join failed");

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(i).unwrap();
        }
        let guarded = unsafe { consumer.try_read_direct_batch(BUFFER_CAPACITY) }.unwrap();
        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(100 + i).unwrap();
        }

        assert!(producer.reserve_write_batch(1).is_none());
        core::mem::forget(guarded);
        drop(consumer);
        assert!(producer.reserve_write_batch(1).is_none());

        unsafe {
            producer.recover_as_exclusive();
        }

        assert!(producer.reserve_write_batch(1).is_some());
    }

    #[test]
    fn test_producer_recover_as_exclusive() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");

        for item in 0..BUFFER_CAPACITY as Item {
            producer.try_write(item).unwrap();
        }

        let batch = producer.reserve_write_batch(1).expect("reserve write");
        core::mem::forget(batch);

        unsafe {
            producer.recover_as_exclusive();
        }
        let mut joined = unsafe { Consumer::<Item>::join(&file) }.expect("join after recovery");

        assert_eq!(joined.try_read(), Err(TryReadError::Empty));
        producer.try_write(2).unwrap();

        assert_eq!(joined.try_read().unwrap(), 2);
    }

    #[test]
    fn test_join_consumer_starts_at_latest_publication() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");

        for i in 0..(BUFFER_CAPACITY as u64 + 3) {
            producer.try_write(i).unwrap();
        }

        let mut consumer = unsafe { Consumer::<Item>::join(&file) }.expect("join failed");
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));

        producer.try_write(99).unwrap();
        assert_eq!(consumer.try_read().unwrap(), 99);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_join_rejects_too_small_file() {
        let file = create_temp_shmem_file().unwrap();
        file.set_len(1).expect("truncate file");

        match unsafe { Producer::<Item>::join(&file) } {
            Err(Error::InvalidBufferSize) => {}
            Err(err) => panic!("unexpected producer join error: {err}"),
            Ok(_) => panic!("producer join unexpectedly succeeded"),
        }

        match unsafe { Consumer::<Item>::join(&file) } {
            Err(Error::InvalidBufferSize) => {}
            Err(err) => panic!("unexpected consumer join error: {err}"),
            Ok(_) => panic!("consumer join unexpectedly succeeded"),
        }
    }

    #[test]
    fn test_join_allows_trailing_file_bytes() {
        let file = create_temp_shmem_file().unwrap();
        let exact_size = minimum_file_size::<u64>(4);
        let producer =
            unsafe { Producer::<u64>::create(&file, exact_size) }.expect("create failed");

        file.set_len((exact_size + 4096) as u64)
            .expect("grow queue file");

        let joined_producer = unsafe { Producer::<u64>::join(&file) }.expect("producer join");
        let joined_consumer = unsafe { Consumer::<u64>::join(&file) }.expect("consumer join");

        assert_eq!(producer.capacity(), 4);
        assert_eq!(joined_producer.capacity(), 4);
        assert_eq!(joined_consumer.capacity(), 4);
    }

    #[test]
    fn test_join_rejects_consumer_slot_mismatch() {
        const CREATED_SLOTS: usize = 2;
        const JOINED_SLOTS: usize = 3;
        const BUFFER_SIZE: usize =
            minimum_file_size_for_consumer_slots::<Item, CREATED_SLOTS>(BUFFER_CAPACITY);

        let file = create_temp_shmem_file().unwrap();
        let _producer = unsafe { Producer::<Item, CREATED_SLOTS>::create(&file, BUFFER_SIZE) }
            .expect("create failed");

        match unsafe { Producer::<Item, JOINED_SLOTS>::join(&file) } {
            Err(Error::InvalidConsumerSlots { expected, actual }) => {
                assert_eq!(expected, JOINED_SLOTS);
                assert_eq!(actual, CREATED_SLOTS);
            }
            Err(err) => panic!("unexpected producer join error: {err}"),
            Ok(_) => panic!("producer join unexpectedly succeeded"),
        }

        match unsafe { Consumer::<Item, JOINED_SLOTS>::join(&file) } {
            Err(Error::InvalidConsumerSlots { expected, actual }) => {
                assert_eq!(expected, JOINED_SLOTS);
                assert_eq!(actual, CREATED_SLOTS);
            }
            Err(err) => panic!("unexpected consumer join error: {err}"),
            Ok(_) => panic!("consumer join unexpectedly succeeded"),
        }
    }

    #[test]
    fn test_consumer_slots_exhaustion_and_drop_releases_slot() {
        const CONSUMER_SLOTS: usize = 2;
        const BUFFER_SIZE: usize =
            minimum_file_size_for_consumer_slots::<Item, CONSUMER_SLOTS>(BUFFER_CAPACITY);

        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe { Producer::<Item, CONSUMER_SLOTS>::create(&file, BUFFER_SIZE) }
            .expect("create failed");
        let mut consumers = Vec::new();

        for _ in 0..CONSUMER_SLOTS {
            consumers.push(producer.join_as_consumer().expect("join consumer"));
        }

        match producer.join_as_consumer() {
            Err(Error::ConsumerSlotsExhausted) => {}
            Err(err) => panic!("unexpected consumer slot error: {err}"),
            Ok(_) => panic!("consumer join unexpectedly succeeded"),
        }

        consumers.pop();
        assert!(producer.join_as_consumer().is_ok());
    }

    #[test]
    fn test_consumer_try_clone_reports_slot_exhaustion() {
        const CONSUMER_SLOTS: usize = 2;
        const BUFFER_SIZE: usize =
            minimum_file_size_for_consumer_slots::<Item, CONSUMER_SLOTS>(BUFFER_CAPACITY);

        let (_file, _producer, consumer) =
            create_test_queue_with_slots::<Item, CONSUMER_SLOTS>(BUFFER_SIZE);
        let mut consumers = Vec::new();

        for _ in 1..CONSUMER_SLOTS {
            consumers.push(consumer.try_clone().expect("clone consumer"));
        }

        match consumer.try_clone() {
            Err(Error::ConsumerSlotsExhausted) => {}
            Err(err) => panic!("unexpected clone error: {err}"),
            Ok(_) => panic!("consumer clone unexpectedly succeeded"),
        }
    }

    #[test]
    fn test_clone_producer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        producer.try_write(10).unwrap();
        producer2.try_write(20).unwrap();

        let mut values = Vec::new();
        while let Ok(v) = consumer.try_read() {
            values.push(v);
        }
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_cross_role_joins() {
        let (_file, producer1, mut consumer1) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = producer1.join_as_consumer().expect("join as consumer");
        let producer2 = consumer2.join_as_producer();

        producer1.try_write(100).unwrap();
        producer2.try_write(200).unwrap();

        assert_eq!(consumer1.try_read().unwrap(), 100);
        assert_eq!(consumer1.try_read().unwrap(), 200);
        assert_eq!(consumer2.try_read().unwrap(), 100);
        assert_eq!(consumer2.try_read().unwrap(), 200);
    }

    #[test]
    fn test_minimum_file_size_rounds_up_capacity() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe { Producer::<u64>::create(&file, minimum_file_size::<u64>(3)) }
            .expect("create failed");
        let consumer = unsafe { Consumer::<u64>::join(&file) }.expect("join failed");

        assert_eq!(producer.capacity(), 4);
        assert_eq!(consumer.capacity(), 4);
        assert_eq!(producer.queue.payload_pool.capacity(), 8);
    }

    #[test]
    fn test_minimum_file_size_rejects_invalid_inputs() {
        assert!(std::panic::catch_unwind(|| minimum_file_size::<u64>(0)).is_err());
        assert!(
            std::panic::catch_unwind(|| minimum_file_size::<u64>(MAX_RING_CAPACITY + 1)).is_err()
        );
        assert!(std::panic::catch_unwind(|| minimum_region_size::<u64>(usize::MAX)).is_err());
        assert!(std::panic::catch_unwind(|| minimum_file_size::<()>(1)).is_err());
        assert!(
            std::panic::catch_unwind(|| minimum_file_size_for_consumer_slots::<u64, 0>(1)).is_err()
        );
    }
}
