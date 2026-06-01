//! Bounded lossy broadcast queue with separated payload storage.
//!
//! The broadcast ring stores only single-atomic payload handles. Payload bytes
//! live in a separate fixed-size pool and are reclaimed by producers after
//! checking consumer hazard ranges. Consumers that obtain a direct read guard
//! publish the guarded sequence range, so producers can overwrite ring entries
//! without racing the guarded payload bytes.
//!
//! Each producer owns one publication lane. Producers write payload bytes into a
//! separated fixed payload pool, then publish generation-checked payload handles
//! directly into that lane's ring. Consumers merge producer lanes
//! opportunistically; publication order is preserved within one lane, not
//! across different lanes.
//! A consumer that has not published a hazard before its ring position is
//! reserved for overwrite may observe an overrun and skip forward. A consumer
//! that already holds a direct read guard keeps the guarded payloads retired
//! until the guard is dropped and a producer reclaims them.
//!
//! The high-level by-value APIs still copy payload bytes out of shared memory.
//! As with the other shared-memory queues, callers must use the same `T` for all
//! producers and consumers attached to the same region. Producer- and
//! consumer-slot counts are runtime layout parameters configured with
//! [`BroadcastConfig`] when the queue is created. Broadcast payloads may be
//! overwritten, cancelled, recovered, or read by value without running `T`'s
//! destructor on the shared-memory copy, so duplicating and forgetting payload
//! values must be valid for the chosen `T`.
//!
//! # Safety and failure model
//!
//! The `try_` APIs are fallible, but they are not guaranteed to be non-blocking:
//! payload exhaustion returns failure to producers instead of making them wait.
//! If a producer process exits while holding a producer lane, that lane and any
//! cached payloads remain unavailable until [`Producer::recover_as_exclusive`]
//! is used.
//!
//! Write batches reserve payload storage but do not reserve ring positions until
//! publish. Publishing reserves ring positions for overwrite before handles are
//! installed; hazard-protected payloads remain retired until a producer can
//! reclaim them safely.
//! Producer handles keep a process-local payload cache to reduce shared free-list
//! traffic. Normal producer drop returns cached payloads and releases the
//! producer lane; an abandoned producer may strand a lane and cached payloads until
//! [`Producer::recover_as_exclusive`] is used.
//!
//! Zero-sized payload types and payload types whose alignment exceeds the shared
//! memory region alignment are not supported.

mod payload_pool;

use crate::{error::Error, normalized_capacity, shmem::Region, CacheAlignedAtomicSize, VERSION};
use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};
use payload_pool::{
    PayloadHandle, PayloadHeader, PayloadPool, PayloadReleaseBatch, ProtectedPayload,
    ReservedPayloads, RetiredPayload,
};
use std::{fs::File, sync::Arc, time::Duration};

/// Unique identifier for broadcast queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");
const CACHELINE_SIZE: usize = 64;
const MAX_RING_CAPACITY: usize = 1usize << 30;
const PRODUCER_PAYLOAD_CACHE_REFILL: usize = 64;
const PRODUCER_RETIRED_RECLAIM_THRESHOLD: usize = 64;
const CONSUMER_WAIT_SPINS: usize = 256;
/// Maximum number of payload handles a direct batch read can guard at once.
pub const MAX_DIRECT_READ_BATCH: usize = 1024;
/// Default number of concurrently registered producer lanes.
pub const DEFAULT_PRODUCER_SLOTS: usize = 8;
/// Default number of concurrently registered consumers.
pub const DEFAULT_CONSUMER_SLOTS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BroadcastConfig {
    capacity: usize,
    producer_slots: usize,
    consumer_slots: usize,
}

impl BroadcastConfig {
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            producer_slots: DEFAULT_PRODUCER_SLOTS,
            consumer_slots: DEFAULT_CONSUMER_SLOTS,
        }
    }

    pub const fn with_slots(capacity: usize, producer_slots: usize, consumer_slots: usize) -> Self {
        Self {
            capacity,
            producer_slots,
            consumer_slots,
        }
    }

    pub const fn with_producer_slots(mut self, producer_slots: usize) -> Self {
        self.producer_slots = producer_slots;
        self
    }

    pub const fn with_consumer_slots(mut self, consumer_slots: usize) -> Self {
        self.consumer_slots = consumer_slots;
        self
    }

    pub const fn capacity(self) -> usize {
        self.capacity
    }

    pub const fn producer_slots(self) -> usize {
        self.producer_slots
    }

    pub const fn consumer_slots(self) -> usize {
        self.consumer_slots
    }

    pub fn minimum_file_size<T>(self) -> usize {
        let capacity = SharedQueueHeader::ring_capacity_for_requested_capacity(self.capacity);
        SharedQueueHeader::total_size_for_ring_capacity::<T>(
            capacity,
            self.producer_slots,
            self.consumer_slots,
        )
    }
}

pub struct Producer<T> {
    queue: SharedQueue<T>,
    lane: ProducerLane,
    local: UnsafeCell<ProducerLocal>,
}

impl<T> Producer<T> {
    /// Creates a new producer for the shared queue in the provided file with
    /// the given runtime layout.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - After initialization, `file` must not be truncated or resized while any
    ///   handle remains joined to the queue.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    /// - If a producer exits without dropping its handle, its lane and payloads
    ///   in that producer's local cache remain unavailable until exclusive
    ///   recovery.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, config) }?;
        // SAFETY: `header` belongs to `region` and was initialized above.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    /// - If a producer exits without dropping its handle, its lane and payloads
    ///   in that producer's local cache remain unavailable until exclusive
    ///   recovery.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` belongs to `region` and was validated by join.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    ///
    /// The consumer starts at each producer lane's current publication cursor
    /// and will only observe values published after it joins.
    pub fn join_as_consumer(&self) -> Result<Consumer<T>, Error> {
        Consumer::from_queue(self.queue.clone())
    }

    /// Forcibly creates a Consumer at `index` using this producer's mapping.
    ///
    /// The claimed consumer starts at each producer lane's current publication
    /// cursor and will only observe values published after it joins.
    ///
    /// # Safety
    /// - The caller must guarantee no live [`Consumer`] handle owns `index`.
    /// - The caller must guarantee no live direct-read guard from `index` can
    ///   still access payloads.
    /// - Racing with any live consumer that owns `index` may let producers
    ///   reclaim payload storage still being read.
    pub unsafe fn claim_consumer_slot(&self, index: usize) -> Result<Consumer<T>, Error> {
        // SAFETY: forwarded from this function's safety contract.
        unsafe { Consumer::from_queue_at_index(self.queue.clone(), index) }
    }

    /// Calculates the minimum file size for a broadcast queue config.
    pub fn minimum_file_size(config: BroadcastConfig) -> usize {
        config.minimum_file_size::<T>()
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        // SAFETY: forwarded from this function's safety contract.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        let lane = queue.acquire_producer_lane()?;
        Ok(Self {
            queue,
            lane,
            local: UnsafeCell::new(ProducerLocal::new()),
        })
    }

    /// Returns the normalized ring capacity in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Creates another producer with its own lane.
    pub fn try_clone(&self) -> Result<Self, Error> {
        let lane = self.queue.acquire_producer_lane()?;
        Ok(Self {
            queue: self.queue.clone(),
            lane,
            local: UnsafeCell::new(ProducerLocal::new()),
        })
    }

    /// Writes `item` into the queue, or returns it if no payload can be
    /// reserved.
    ///
    /// This can fail if payload storage is exhausted or this producer already
    /// has an outstanding unpublished batch.
    /// The `try_` prefix means payload exhaustion returns `Err(item)`, not that
    /// the operation is guaranteed to be non-blocking.
    ///
    /// Queue publication uses the queue's internal release ordering.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        let lane = self.lane.index();
        match self.with_local(|local| self.queue.try_write_direct(lane, item, local)) {
            DirectWriteResult::Published => return Ok(()),
            DirectWriteResult::NoPayload(item) => return Err(item),
        }
    }

    /// Reserves exactly `count` payloads for writing.
    ///
    /// The payloads are released when the batch is dropped. Call
    /// [`WriteBatch::publish`] after initializing them. Each producer supports
    /// one outstanding unpublished batch at a time.
    #[must_use]
    pub fn reserve_write_batch(&self, count: usize) -> Option<WriteBatch<'_, T>> {
        let lane = self.lane.index();
        let reservation =
            self.with_local(|local| self.queue.reserve_write_batch(lane, count, local))?;
        Some(WriteBatch {
            producer: self,
            reservation,
            next_write_payload_index: reservation.chain.first_payload_index(),
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
        self.with_local(|local| *local = ProducerLocal::new());
        self.queue.recover_as_exclusive();
        self.lane.mark_active();
    }

    #[inline]
    fn with_local<R>(&self, f: impl FnOnce(&mut ProducerLocal) -> R) -> R {
        // SAFETY: `Producer` is not `Sync`, so this handle cannot be used
        // concurrently from multiple threads without external synchronization.
        f(unsafe { &mut *self.local.get() })
    }

    fn publish_reserved_payload_chain(&self, reservation: ProducerReservation) {
        self.with_local(|local| {
            self.queue
                .publish_reserved_payload_chain(reservation, local)
        });
    }

    fn cancel_reserved_payloads(&self, reservation: ProducerReservation) {
        self.with_local(|local| self.queue.cancel_reserved_payloads(reservation, local));
    }

    fn flush_local(&self) {
        let lane = self.lane.index();
        self.with_local(|local| self.queue.flush_producer_local(lane, local));
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.flush_local();
        self.lane.release_owner();
    }
}

unsafe impl<T: Send> Send for Producer<T> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryReadError {
    Empty,
    Skipped(usize),
}

pub struct Consumer<T> {
    queue: SharedQueue<T>,
    hazard: ConsumerHazard,
    scan_start_lane: usize,
    read_batch_payloads: [ProtectedPayload; MAX_DIRECT_READ_BATCH],
}

impl<T> Consumer<T> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given runtime layout.
    ///
    /// The consumer starts at each producer lane's current publication cursor
    /// and will only observe values published after it joins.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - After initialization, `file` must not be truncated or resized while any
    ///   handle remains joined to the queue.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, config) }?;
        // SAFETY: `header` belongs to `region` and was initialized above.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Self::from_queue(queue)
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// The consumer starts at each producer lane's current publication cursor
    /// and will only observe values published after it joins.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T`.
    /// - Any process that may read, dereference, inspect, duplicate, forget, or
    ///   drop a queued value must be able to do so validly for that value in
    ///   that process.
    /// - Safe by-value reads duplicate payload bytes with typed reads. Broadcast
    ///   payloads may also be overwritten, cancelled, or recovered without
    ///   running `T`'s destructor on the shared-memory copy. The chosen `T` must
    ///   make those operations valid.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` belongs to `region` and was validated by join.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Self::from_queue(queue)
    }

    /// Forcibly joins an existing queue by taking ownership of consumer slot
    /// `index`.
    ///
    /// This clears all hazard ranges previously published by that consumer slot,
    /// marks the slot owned by the returned consumer, and starts at each
    /// producer lane's current publication cursor.
    ///
    /// # Safety
    /// - `file` must satisfy the same requirements as [`Self::join`].
    /// - The caller must guarantee no live [`Consumer`] handle owns `index`.
    /// - The caller must guarantee no live direct-read guard from `index` can
    ///   still access payloads.
    /// - Racing with any live consumer that owns `index` may let producers
    ///   reclaim payload storage still being read.
    pub unsafe fn claim_slot(file: &File, index: usize) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` belongs to `region` and was validated by join.
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::from_queue_at_index(queue, index) }
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Result<Producer<T>, Error> {
        let queue = self.queue.clone();
        let lane = queue.acquire_producer_lane()?;
        Ok(Producer {
            queue,
            lane,
            local: UnsafeCell::new(ProducerLocal::new()),
        })
    }

    /// Calculates the minimum file size for a broadcast queue config.
    pub fn minimum_file_size(config: BroadcastConfig) -> usize {
        config.minimum_file_size::<T>()
    }

    /// Returns the normalized ring capacity in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Returns the runtime consumer slot owned by this handle.
    pub fn slot_index(&self) -> usize {
        self.hazard.index
    }

    fn from_queue(queue: SharedQueue<T>) -> Result<Self, Error> {
        let hazard = queue.acquire_consumer_hazard()?;
        Self::from_queue_with_hazard(queue, hazard)
    }

    unsafe fn from_queue_at_index(queue: SharedQueue<T>, index: usize) -> Result<Self, Error> {
        // SAFETY: forwarded from this function's safety contract.
        let hazard = unsafe { queue.claim_consumer_hazard(index) }?;
        Self::from_queue_with_hazard(queue, hazard)
    }

    fn from_queue_with_hazard(
        queue: SharedQueue<T>,
        hazard: ConsumerHazard,
    ) -> Result<Self, Error> {
        for lane in 0..queue.producer_slots() {
            queue
                .consumer_cursor(hazard.index, lane)
                .store(queue.published(lane), Ordering::Relaxed);
        }
        Ok(Self {
            queue,
            hazard,
            scan_start_lane: 0,
            read_batch_payloads: [ProtectedPayload::EMPTY; MAX_DIRECT_READ_BATCH],
        })
    }

    /// Creates another consumer with the same local cursor.
    pub fn try_clone(&self) -> Result<Self, Error> {
        let hazard = self.queue.acquire_consumer_hazard()?;
        for lane in 0..self.queue.producer_slots() {
            self.queue
                .consumer_cursor(hazard.index, lane)
                .store(self.next_for_lane(lane), Ordering::Relaxed);
        }
        Ok(Self {
            queue: self.queue.clone(),
            hazard,
            scan_start_lane: self.scan_start_lane,
            read_batch_payloads: [ProtectedPayload::EMPTY; MAX_DIRECT_READ_BATCH],
        })
    }

    #[inline]
    fn next_for_lane(&self, lane: usize) -> usize {
        self.queue
            .consumer_cursor(self.hazard.index, lane)
            .load(Ordering::Relaxed)
    }

    #[inline]
    fn set_next_for_lane(&self, lane: usize, next: usize) {
        self.queue
            .consumer_cursor(self.hazard.index, lane)
            .store(next, Ordering::Relaxed);
    }

    fn readable_range_for_lane(
        &mut self,
        lane: usize,
        max: usize,
    ) -> Result<Option<(usize, usize)>, TryReadError> {
        if max == 0 {
            return Err(TryReadError::Empty);
        }

        let start = self.next_for_lane(lane);
        let available = self.queue.published(lane).wrapping_sub(start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self
                .queue
                .overrun_at_reserved(start, self.queue.reserved(lane));
            return Err(self.record_overrun(lane, overrun, 0));
        }

        let count = available.min(max).min(MAX_DIRECT_READ_BATCH);
        if let Err(overrun) = self.queue.validate_window(lane, start, count) {
            return Err(self.record_overrun(lane, overrun, 0));
        }

        Ok(Some((start, count)))
    }

    fn readable_range(&mut self, max: usize) -> Result<(usize, usize, usize), TryReadError> {
        if self.queue.producer_slots() == 0 {
            return Err(TryReadError::Empty);
        }

        let producer_slots = self.queue.producer_slots();
        for offset in 0..producer_slots {
            let lane = (self.scan_start_lane + offset) % producer_slots;
            if let Some((start, count)) = self.readable_range_for_lane(lane, max)? {
                return Ok((lane, start, count));
            }
        }

        Err(TryReadError::Empty)
    }

    fn record_current_overrun(
        &self,
        lane: usize,
        start: usize,
        min_skipped: usize,
    ) -> TryReadError {
        self.record_overrun(
            lane,
            self.queue
                .overrun_at_reserved(start, self.queue.reserved(lane)),
            min_skipped,
        )
    }

    fn record_overrun(
        &self,
        lane: usize,
        overrun: WindowOverrun,
        min_skipped: usize,
    ) -> TryReadError {
        self.set_next_for_lane(lane, overrun.next);
        TryReadError::Skipped(overrun.skipped.max(min_skipped))
    }

    /// Repositions the consumer to the oldest item still retained in the ring.
    pub fn sync_to_oldest(&mut self) {
        for lane in 0..self.queue.producer_slots() {
            self.set_next_for_lane(lane, self.queue.oldest_available(lane));
        }
    }

    /// Repositions the consumer to the producer publication cursor.
    ///
    /// Returns the number of published items skipped by this repositioning.
    pub fn sync_to_latest(&mut self) -> usize {
        let mut skipped = 0usize;
        for lane in 0..self.queue.producer_slots() {
            let published = self.queue.published(lane);
            skipped = skipped.saturating_add(published.wrapping_sub(self.next_for_lane(lane)));
            self.set_next_for_lane(lane, published);
        }
        skipped
    }

    fn has_read_event(&self) -> bool {
        for lane in 0..self.queue.producer_slots() {
            if self
                .queue
                .published(lane)
                .wrapping_sub(self.next_for_lane(lane))
                != 0
            {
                return true;
            }
        }
        false
    }

    #[cfg(target_os = "linux")]
    pub fn wait_for_work_timeout(&mut self, timeout: Duration) -> bool {
        if self.has_read_event() {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }

        for _ in 0..CONSUMER_WAIT_SPINS {
            core::hint::spin_loop();
            if self.has_read_event() {
                return true;
            }
        }

        let wait_guard = self.queue.register_waiting_consumer();
        if self.has_read_event() {
            drop(wait_guard);
            return true;
        }
        let _ = self
            .queue
            .wait_for_consumer_wake(wait_guard.generation(), timeout);
        self.has_read_event()
    }

    #[cfg(target_os = "linux")]
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<T, TryReadError> {
        if !self.has_read_event() && !self.wait_for_work_timeout(timeout) {
            return Err(TryReadError::Empty);
        }
        self.try_read()
    }

    /// Attempts to directly access up to `max` values, blocking only while no
    /// producer lane has readable work or until `timeout` elapses.
    ///
    /// This follows the same empty-queue protocol as [`Self::wait_for_work_timeout`]:
    /// try the batch read first, register as waiting only if the queue is
    /// empty, then retry after a producer wake or timeout.
    ///
    /// # Safety
    /// Callers must only access returned payloads in ways valid for
    /// shared-memory bytes of `T` in this process.
    #[cfg(target_os = "linux")]
    pub unsafe fn read_direct_batch_timeout(
        &mut self,
        max: usize,
        timeout: Duration,
    ) -> Result<DirectReadBatch<'_, T>, TryReadError> {
        if max == 0 {
            return Err(TryReadError::Empty);
        }
        if !self.has_read_event() && !self.wait_for_work_timeout(timeout) {
            return Err(TryReadError::Empty);
        }
        // SAFETY: forwarded from this function's safety contract.
        unsafe { self.try_read_direct_batch(max) }
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
    pub unsafe fn try_read_direct(&mut self) -> Result<DirectRead<'_, T>, TryReadError> {
        let (lane, start, _) = self.readable_range(1)?;

        self.hazard.protect(lane, start, 1);
        let handle = self.queue.handle_at(lane, start);
        let Some(payload) = self.queue.payload_pool.payload_for_protected_handle(handle) else {
            self.hazard.clear(lane);
            return Err(self.record_current_overrun(lane, start, 1));
        };

        if let Err(overrun) = self.queue.validate_window(lane, start, 1) {
            self.hazard.clear(lane);
            return Err(self.record_overrun(lane, overrun, 0));
        }

        Ok(DirectRead {
            cursor: NonNull::from(self.queue.consumer_cursor(self.hazard.index, lane)),
            producer_slots: self.queue.producer_slots(),
            scan_start_lane: &mut self.scan_start_lane,
            queue: &self.queue,
            hazard: self.hazard,
            lane,
            start,
            payload,
        })
    }

    /// Attempts to directly access up to `max` values from the queue.
    ///
    /// The returned guard publishes one hazard range for the batch. At most
    /// [`MAX_DIRECT_READ_BATCH`] values are returned because the consumer uses
    /// fixed scratch storage for protected payload handles.
    ///
    /// # Safety
    /// Callers must only access returned payloads in ways valid for
    /// shared-memory bytes of `T` in this process.
    pub unsafe fn try_read_direct_batch(
        &mut self,
        max: usize,
    ) -> Result<DirectReadBatch<'_, T>, TryReadError> {
        let (lane, start, count) = self.readable_range(max)?;

        debug_assert!(count <= MAX_DIRECT_READ_BATCH);

        self.hazard.protect(lane, start, count);
        for index in 0..count {
            let position = start.wrapping_add(index);
            let handle = self.queue.handle_at(lane, position);
            let Some(payload) = self.queue.payload_pool.payload_for_protected_handle(handle) else {
                self.hazard.clear(lane);
                return Err(self.record_current_overrun(lane, start, 1));
            };
            self.read_batch_payloads[index] = payload;
        }

        if let Err(overrun) = self.queue.validate_window(lane, start, count) {
            self.hazard.clear(lane);
            return Err(self.record_overrun(lane, overrun, 0));
        }

        Ok(DirectReadBatch {
            cursor: NonNull::from(self.queue.consumer_cursor(self.hazard.index, lane)),
            producer_slots: self.queue.producer_slots(),
            scan_start_lane: &mut self.scan_start_lane,
            queue: &self.queue,
            hazard: self.hazard,
            lane,
            start,
            payloads: NonNull::new(self.read_batch_payloads.as_mut_ptr())
                .expect("fixed payload scratch is non-null"),
            len: count,
            _marker: PhantomData,
        })
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.hazard.release_owner();
    }
}

unsafe impl<T: Send> Send for Consumer<T> {}
unsafe impl<T: Send + Sync> Sync for Consumer<T> {}

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
pub fn minimum_file_size<T>(capacity: usize) -> usize {
    BroadcastConfig::new(capacity).minimum_file_size::<T>()
}

/// Calculates the minimum file size required for a queue with the requested
/// capacity and runtime slot counts.
///
/// The returned size is for the normalized per-lane ring capacity, rounded up
/// to a power of two.
///
/// # Panics
/// Panics for zero capacity, zero slot counts, capacities above the broadcast
/// maximum, unsupported `T`, or layout arithmetic overflow.
pub fn minimum_file_size_for_config<T>(config: BroadcastConfig) -> usize {
    config.minimum_file_size::<T>()
}

/// Calculates the minimum region size required for a queue with given capacity.
pub fn minimum_region_size<T>(capacity: usize) -> usize {
    minimum_file_size::<T>(capacity)
}

/// Calculates the minimum region size required for a queue with given capacity
/// and runtime slot counts.
pub fn minimum_region_size_for_config<T>(config: BroadcastConfig) -> usize {
    minimum_file_size_for_config::<T>(config)
}

/// Shared-memory broadcast queue view.
///
/// Binary file/region layout:
///
/// ```text
/// offset 0
/// +---------------------------+
/// | SharedQueueHeader         |
/// |   cacheline 0             | magic, version, buffer_mask, slot counts
/// |   cacheline 1             | consumer_wait_generation
/// |   cacheline 2             | consumer_waiters
/// +---------------------------+
/// | payload free heads        | [AtomicU64; producer_slots], 64-byte stride
/// +---------------------------+
/// | payload retired heads     | [AtomicU64; producer_slots], 64-byte stride
/// +---------------------------+
/// | producer lanes            | [ProducerSlot; producer_slots]
/// +---------------------------+
/// | consumer hazard slots     | [ConsumerHazardSlot; consumer_slots * producer_slots]
/// +---------------------------+
/// | consumer cursors          | [AtomicUsize; consumer_slots * producer_slots]
/// +---------------------------+
/// | ring                      | producer_slots lanes, 64-byte rounded stride
/// +---------------------------+
/// | payload headers           | producer_slots lanes, 64-byte rounded stride
/// +---------------------------+
/// | payload storage           | producer_slots lanes, 64-byte rounded stride
/// +---------------------------+
/// ```
///
/// `ring_capacity` is `buffer_mask + 1`; each producer owns an independent
/// ring lane and payload-pool lane.
struct SharedQueue<T> {
    /// Shared metadata and producer cursors.
    header: NonNull<SharedQueueHeader>,
    /// Runtime producer lanes.
    producer_lanes: NonNull<ProducerSlot>,
    /// Published values. Each entry contains a `PayloadHandle`.
    ring: NonNull<AtomicU64>,
    /// Fixed table of consumer-owned, per-lane hazard ranges.
    hazard_slots: NonNull<ConsumerHazardSlot>,
    /// Per-consumer, per-lane local cursors.
    consumer_cursors: NonNull<AtomicUsize>,
    /// Payload lifecycle management: reservation, retirement, and reclamation.
    payload_pool: PayloadPool<T>,
    /// Ring index mask. The ring capacity is `buffer_mask + 1`.
    buffer_mask: usize,
    /// Number of ring cells allocated per producer lane, including padding.
    ring_lane_stride: usize,
    producer_slots: usize,
    consumer_slots: usize,

    // NB: Region must be declared last so header/ring/payload_pool stay valid.
    /// Memory mapping that owns the header, ring, payload headers, and payload data.
    region: Arc<Region>,
}

#[derive(Clone, Copy)]
struct WindowOverrun {
    skipped: usize,
    next: usize,
}

struct ProducerLocal {
    free: PayloadReleaseBatch,
    retired: PayloadReleaseBatch,
    batch_reserved: bool,
}

#[derive(Clone, Copy)]
struct ProducerReservation {
    chain: ReservedPayloads,
    lane: usize,
}

enum DirectWriteResult<T> {
    Published,
    NoPayload(T),
}

impl ProducerLocal {
    const fn new() -> Self {
        Self {
            free: PayloadReleaseBatch::new(),
            retired: PayloadReleaseBatch::new(),
            batch_reserved: false,
        }
    }
}

const HAZARD_FREE: u64 = 0;
const HAZARD_INACTIVE: u64 = 1;
const HAZARD_ACTIVE: u64 = 2;

const PRODUCER_FREE: u64 = 0;
const PRODUCER_ACTIVE: u64 = 1;

#[repr(C, align(64))]
struct CacheAlignedAtomicU32 {
    inner: AtomicU32,
}

impl core::ops::Deref for CacheAlignedAtomicU32 {
    type Target = AtomicU32;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[repr(C, align(64))]
struct ProducerSlot {
    state: AtomicU64,
    producer_reservation: CacheAlignedAtomicSize,
    producer_publication: CacheAlignedAtomicSize,
}

impl ProducerSlot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(PRODUCER_FREE),
            producer_reservation: CacheAlignedAtomicSize::default(),
            producer_publication: CacheAlignedAtomicSize::default(),
        }
    }

    fn acquire(&self) -> bool {
        self.state
            .compare_exchange(
                PRODUCER_FREE,
                PRODUCER_ACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn release_owner(&self) {
        let _ = self.state.compare_exchange(
            PRODUCER_ACTIVE,
            PRODUCER_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn reset(&self) {
        self.producer_reservation.store(0, Ordering::Release);
        self.producer_publication.store(0, Ordering::Release);
        self.state.store(PRODUCER_FREE, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct ProducerLane {
    slot: NonNull<ProducerSlot>,
    index: usize,
}

impl ProducerLane {
    fn index(self) -> usize {
        self.index
    }

    fn release_owner(self) {
        // SAFETY: `slot` points into the live producer-lane table.
        unsafe { self.slot.as_ref() }.release_owner();
    }

    fn mark_active(self) {
        // SAFETY: `slot` points into the live producer-lane table.
        unsafe { self.slot.as_ref() }
            .state
            .store(PRODUCER_ACTIVE, Ordering::Release);
    }
}

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

    fn mark_owned(&self) {
        self.state.store(HAZARD_INACTIVE, Ordering::Release);
    }

    fn force_claim(&self) {
        self.start.store(0, Ordering::Relaxed);
        self.end.store(0, Ordering::Relaxed);
        self.state.store(HAZARD_INACTIVE, Ordering::Release);
    }

    fn protect(&self, start: usize, end: usize) {
        self.start.store(start, Ordering::Relaxed);
        self.end.store(end, Ordering::Relaxed);
        self.state.store(HAZARD_ACTIVE, Ordering::Release);
    }

    fn clear(&self) {
        self.state.store(HAZARD_INACTIVE, Ordering::Release);
    }

    fn release_primary_owner(&self) {
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

    fn active_range(&self, lane: usize) -> Option<HazardRange> {
        if self.state.load(Ordering::Acquire) != HAZARD_ACTIVE {
            return None;
        }

        let start = self.start.load(Ordering::Relaxed);
        let end = self.end.load(Ordering::Relaxed);
        if start == end {
            None
        } else {
            Some(HazardRange { lane, start, end })
        }
    }
}

#[derive(Clone, Copy)]
struct ConsumerHazard {
    slots: NonNull<ConsumerHazardSlot>,
    index: usize,
    producer_slots: usize,
}

impl ConsumerHazard {
    fn protect(self, lane: usize, start: usize, count: usize) {
        // SAFETY: `lane_slot` points into the live shared queue hazard table.
        unsafe { self.lane_slot(lane).as_ref() }.protect(start, start.wrapping_add(count));
    }

    fn clear(self, lane: usize) {
        // SAFETY: `lane_slot` points into the live shared queue hazard table.
        unsafe { self.lane_slot(lane).as_ref() }.clear();
    }

    fn release_owner(self) {
        for lane in 1..self.producer_slots {
            // SAFETY: `lane_slot` points into the live shared queue hazard table.
            unsafe { self.lane_slot(lane).as_ref() }.reset();
        }
        // SAFETY: `lane_slot` points into the live shared queue hazard table.
        unsafe { self.lane_slot(0).as_ref() }.release_primary_owner();
    }

    fn lane_slot(self, lane: usize) -> NonNull<ConsumerHazardSlot> {
        debug_assert!(lane < self.producer_slots);
        let offset = self
            .index
            .checked_mul(self.producer_slots)
            .and_then(|base| base.checked_add(lane))
            .expect("consumer hazard index overflow");
        // SAFETY: index and lane are bounded by the hazard table dimensions.
        unsafe { self.slots.add(offset) }
    }
}

#[derive(Clone, Copy)]
struct HazardRange {
    lane: usize,
    start: usize,
    end: usize,
}

impl HazardRange {
    fn protects(self, retired: RetiredPayload) -> bool {
        retired.lane() == self.lane
            && retired.sequence().wrapping_sub(self.start) < self.end.wrapping_sub(self.start)
    }
}

#[cfg(target_os = "linux")]
struct WaitingConsumer<'a> {
    header: &'a SharedQueueHeader,
    generation: u32,
}

#[cfg(target_os = "linux")]
impl<'a> WaitingConsumer<'a> {
    fn generation(&self) -> u32 {
        self.generation
    }
}

#[cfg(target_os = "linux")]
impl Drop for WaitingConsumer<'_> {
    fn drop(&mut self) {
        self.header.consumer_waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            producer_lanes: self.producer_lanes,
            ring: self.ring,
            hazard_slots: self.hazard_slots,
            consumer_cursors: self.consumer_cursors,
            payload_pool: self.payload_pool.clone(),
            buffer_mask: self.buffer_mask,
            ring_lane_stride: self.ring_lane_stride,
            producer_slots: self.producer_slots,
            consumer_slots: self.consumer_slots,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T> SharedQueue<T> {
    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    #[inline]
    fn producer_slots(&self) -> usize {
        self.producer_slots
    }

    #[inline]
    fn producer_lane(&self, lane: usize) -> &ProducerSlot {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: lane is bounded by producer_slots.
        unsafe { self.producer_lanes.add(lane).as_ref() }
    }

    #[inline]
    fn consumer_cursor(&self, consumer: usize, lane: usize) -> &AtomicUsize {
        debug_assert!(consumer < self.consumer_slots);
        debug_assert!(lane < self.producer_slots);
        let index = consumer
            .checked_mul(self.producer_slots)
            .and_then(|base| base.checked_add(lane))
            .expect("consumer cursor index overflow");
        // SAFETY: consumer and lane are bounded by the cursor table dimensions.
        unsafe { self.consumer_cursors.add(index).as_ref() }
    }

    #[inline]
    fn consumer_hazard_slot(&self, consumer: usize, lane: usize) -> &ConsumerHazardSlot {
        debug_assert!(consumer < self.consumer_slots);
        debug_assert!(lane < self.producer_slots);
        let index = consumer
            .checked_mul(self.producer_slots)
            .and_then(|base| base.checked_add(lane))
            .expect("consumer hazard index overflow");
        // SAFETY: consumer and lane are bounded by the hazard table dimensions.
        unsafe { self.hazard_slots.add(index).as_ref() }
    }

    #[inline]
    fn published(&self, lane: usize) -> usize {
        self.producer_lane(lane)
            .producer_publication
            .load(Ordering::Acquire)
    }

    #[inline]
    fn reserved(&self, lane: usize) -> usize {
        self.producer_lane(lane)
            .producer_reservation
            .load(Ordering::Acquire)
    }

    #[inline]
    fn oldest_available(&self, lane: usize) -> usize {
        self.reserved(lane).saturating_sub(self.capacity())
    }

    #[inline]
    fn overrun_at_reserved(&self, start: usize, reserved: usize) -> WindowOverrun {
        let capacity = self.capacity();
        WindowOverrun {
            skipped: reserved.wrapping_sub(start).wrapping_sub(capacity),
            next: reserved.saturating_sub(capacity),
        }
    }

    fn reserve_write_batch(
        &self,
        lane: usize,
        count: usize,
        local: &mut ProducerLocal,
    ) -> Option<ProducerReservation> {
        if count == 0 || count > self.capacity() || local.batch_reserved {
            return None;
        }

        let chain = match self.reserve_payloads_for_write(lane, count, local) {
            Some(chain) => chain,
            None => {
                self.reclaim_local_retired(lane, local);
                self.reserve_payloads_for_write(lane, count, local)?
            }
        };

        local.batch_reserved = true;
        Some(ProducerReservation { chain, lane })
    }

    fn try_write_direct(
        &self,
        lane: usize,
        item: T,
        local: &mut ProducerLocal,
    ) -> DirectWriteResult<T> {
        if local.batch_reserved {
            return DirectWriteResult::NoPayload(item);
        }

        let Some(chain) = self.reserve_payloads_for_write(lane, 1, local) else {
            self.reclaim_local_retired(lane, local);
            let Some(chain) = self.reserve_payloads_for_write(lane, 1, local) else {
                return DirectWriteResult::NoPayload(item);
            };
            self.write_and_publish_one(lane, chain, item, local);
            return DirectWriteResult::Published;
        };

        self.write_and_publish_one(lane, chain, item, local);
        DirectWriteResult::Published
    }

    fn write_and_publish_one(
        &self,
        lane: usize,
        chain: ReservedPayloads,
        item: T,
        local: &mut ProducerLocal,
    ) {
        debug_assert_eq!(chain.len(), 1);
        // SAFETY: this producer owns the reserved payload.
        unsafe {
            self.payload_pool
                .payload_at_in_lane(lane, chain.first_payload_index())
                .as_ptr()
                .write(item)
        };
        self.publish_one_payload(lane, chain.first_payload_index(), local);
    }

    fn publish_one_payload(&self, lane: usize, payload_index: u32, local: &mut ProducerLocal) {
        let producer_lane = self.producer_lane(lane);
        let start = producer_lane.producer_publication.load(Ordering::Acquire);
        let end = start.wrapping_add(1);
        producer_lane
            .producer_reservation
            .store(end, Ordering::Release);
        let handle = self
            .payload_pool
            .handle_for_payload_in_lane(lane, payload_index);
        let old = self.install_reserved_handle(lane, start, handle);
        self.payload_pool.retire_to_batch_in_lane(
            lane,
            old,
            RetiredPayload::new(lane, start.wrapping_sub(self.capacity())),
            &mut local.retired,
        );
        producer_lane
            .producer_publication
            .store(end, Ordering::Release);
        self.wake_waiting_consumers();
        self.reclaim_local_retired_if_needed(lane, local);
    }

    fn publish_payload_chain(
        &self,
        lane: usize,
        chain: ReservedPayloads,
        local: &mut ProducerLocal,
    ) {
        let producer_lane = self.producer_lane(lane);
        let start = producer_lane.producer_publication.load(Ordering::Acquire);
        let end = start.wrapping_add(chain.len());
        producer_lane
            .producer_reservation
            .store(end, Ordering::Release);
        self.install_reserved_payload_chain(
            lane,
            start,
            chain.first_payload_index(),
            chain.len(),
            local,
        );
        producer_lane
            .producer_publication
            .store(end, Ordering::Release);
        self.wake_waiting_consumers();
        self.reclaim_local_retired_if_needed(lane, local);
    }

    fn reserve_payloads_for_write(
        &self,
        lane: usize,
        count: usize,
        local: &mut ProducerLocal,
    ) -> Option<ReservedPayloads> {
        if let Some(chain) = self.reserve_from_local_free(lane, count, local) {
            return Some(chain);
        }

        self.reclaim_local_retired(lane, local);
        if let Some(chain) = self.reserve_from_local_free(lane, count, local) {
            return Some(chain);
        }

        self.reclaim_shared_retired(lane, local);
        if let Some(chain) = self.reserve_from_local_free(lane, count, local) {
            return Some(chain);
        }

        let refill_count = count
            .max(PRODUCER_PAYLOAD_CACHE_REFILL)
            .min(self.capacity());
        if let Some(refill) = self
            .payload_pool
            .take_free_payloads_exact(lane, refill_count)
        {
            self.payload_pool
                .prepend_reserved_payloads_to_batch_in_lane(lane, refill, &mut local.free);
        } else if refill_count != count {
            let refill = self.payload_pool.take_free_payloads_exact(lane, count)?;
            self.payload_pool
                .prepend_reserved_payloads_to_batch_in_lane(lane, refill, &mut local.free);
        } else {
            return None;
        }

        self.reserve_from_local_free(lane, count, local)
    }

    fn publish_reserved_payload_chain(
        &self,
        reservation: ProducerReservation,
        local: &mut ProducerLocal,
    ) {
        self.publish_payload_chain(reservation.lane, reservation.chain, local);
        local.batch_reserved = false;
        self.reclaim_local_retired_if_needed(reservation.lane, local);
    }

    fn cancel_reserved_payloads(
        &self,
        reservation: ProducerReservation,
        local: &mut ProducerLocal,
    ) {
        self.payload_pool
            .prepend_reserved_payloads_to_batch_in_lane(
                reservation.lane,
                reservation.chain,
                &mut local.free,
            );
        local.batch_reserved = false;
        self.reclaim_local_retired_if_needed(reservation.lane, local);
    }

    fn flush_producer_local(&self, lane: usize, local: &mut ProducerLocal) {
        self.reclaim_local_retired(lane, local);

        let free = local.free.take();
        let retired = local.retired.take();
        self.payload_pool.flush_release_batch(lane, free);
        self.payload_pool.flush_retired_batch(lane, retired);
    }

    fn install_reserved_payload_chain(
        &self,
        lane: usize,
        start: usize,
        first_payload_index: u32,
        count: usize,
        local: &mut ProducerLocal,
    ) {
        let mut cursor = self.payload_pool.payload_cursor(lane, first_payload_index);
        let last_index = count.wrapping_sub(1);
        for index in 0..count {
            let payload_index = cursor.current();
            let handle = self
                .payload_pool
                .handle_for_payload_in_lane(lane, payload_index);
            let position = start.wrapping_add(index);
            let old = self.install_reserved_handle(lane, position, handle);
            self.payload_pool.retire_to_batch_in_lane(
                lane,
                old,
                RetiredPayload::new(lane, position.wrapping_sub(self.capacity())),
                &mut local.retired,
            );
            if index != last_index {
                cursor.advance();
            }
        }
    }

    fn reclaim_local_retired_if_needed(&self, lane: usize, local: &mut ProducerLocal) {
        if local.retired.len() >= PRODUCER_RETIRED_RECLAIM_THRESHOLD {
            self.reclaim_local_retired(lane, local);
        }
    }

    fn install_reserved_handle(
        &self,
        lane: usize,
        position: usize,
        handle: PayloadHandle,
    ) -> PayloadHandle {
        let ring_index = position & self.buffer_mask;
        debug_assert!(lane < self.producer_slots);
        debug_assert!(ring_index < self.capacity());
        let cell_index = lane * self.ring_lane_stride + ring_index;
        // SAFETY: lane and mask ensure index is in bounds.
        let cell = unsafe { self.ring.add(cell_index).as_ref() };
        let old = PayloadHandle(cell.load(Ordering::Acquire));
        cell.store(handle.0, Ordering::Release);
        old
    }

    #[inline]
    fn handle_at(&self, lane: usize, position: usize) -> PayloadHandle {
        let ring_index = position & self.buffer_mask;
        debug_assert!(lane < self.producer_slots);
        debug_assert!(ring_index < self.capacity());
        let cell_index = lane * self.ring_lane_stride + ring_index;
        // SAFETY: lane and mask ensure index is in bounds.
        PayloadHandle(unsafe { self.ring.add(cell_index).as_ref() }.load(Ordering::Acquire))
    }

    #[inline]
    fn validate_window(
        &self,
        lane: usize,
        start: usize,
        count: usize,
    ) -> Result<(), WindowOverrun> {
        debug_assert!(count <= self.capacity());
        let reserved = self.reserved(lane);
        if reserved.wrapping_sub(start) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        if reserved.wrapping_sub(start.wrapping_add(count)) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        Ok(())
    }

    fn acquire_consumer_hazard(&self) -> Result<ConsumerHazard, Error> {
        for index in 0..self.consumer_slots {
            let primary = self.consumer_hazard_slot(index, 0);
            if primary.acquire() {
                for lane in 1..self.producer_slots {
                    self.consumer_hazard_slot(index, lane).mark_owned();
                }
                return Ok(ConsumerHazard {
                    slots: self.hazard_slots,
                    index,
                    producer_slots: self.producer_slots,
                });
            }
        }

        Err(Error::ConsumerSlotsExhausted)
    }

    unsafe fn claim_consumer_hazard(&self, index: usize) -> Result<ConsumerHazard, Error> {
        if index >= self.consumer_slots {
            return Err(Error::InvalidConsumerIndex {
                index,
                slots: self.consumer_slots,
            });
        }

        for lane in 0..self.producer_slots {
            self.consumer_hazard_slot(index, lane).force_claim();
        }

        Ok(ConsumerHazard {
            slots: self.hazard_slots,
            index,
            producer_slots: self.producer_slots,
        })
    }

    fn acquire_producer_lane(&self) -> Result<ProducerLane, Error> {
        for index in 0..self.producer_slots {
            // SAFETY: index is bounded by producer_slots.
            let slot = unsafe { self.producer_lanes.add(index) };
            // SAFETY: slot points into the live producer-lane table.
            if unsafe { slot.as_ref() }.acquire() {
                return Ok(ProducerLane { slot, index });
            }
        }

        Err(Error::ProducerSlotsExhausted)
    }

    fn retired_payload_is_protected(&self, retired: RetiredPayload) -> bool {
        for index in 0..self.consumer_slots {
            let slot = self.consumer_hazard_slot(index, retired.lane());
            if let Some(range) = slot.active_range(retired.lane()) {
                if range.protects(retired) {
                    return true;
                }
            }
        }
        false
    }

    fn reserve_from_local_free(
        &self,
        lane: usize,
        count: usize,
        local: &mut ProducerLocal,
    ) -> Option<ReservedPayloads> {
        let chain =
            self.payload_pool
                .pop_batch_payloads_exact_in_lane(lane, &mut local.free, count)?;
        self.payload_pool
            .prepare_reserved_payloads_in_lane(lane, chain);
        Some(chain)
    }

    fn reclaim_local_retired(&self, lane: usize, local: &mut ProducerLocal) {
        if local.retired.is_empty() {
            return;
        }

        let retired = local.retired.take();
        self.payload_pool.reclaim_retired_batch_to_batches_in_lane(
            lane,
            retired,
            |retired| self.retired_payload_is_protected(retired),
            &mut local.free,
            &mut local.retired,
        );
    }

    fn reclaim_shared_retired(&self, lane: usize, local: &mut ProducerLocal) {
        self.payload_pool.reclaim_retired_payloads_to_batches(
            lane,
            |retired| self.retired_payload_is_protected(retired),
            &mut local.free,
            &mut local.retired,
        );
    }

    fn wake_waiting_consumers(&self) {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        if header.consumer_waiters.load(Ordering::Acquire) == 0 {
            return;
        }

        header
            .consumer_wait_generation
            .fetch_add(1, Ordering::Release);
        #[cfg(target_os = "linux")]
        futex_wake_all(&header.consumer_wait_generation.inner);
    }

    #[cfg(target_os = "linux")]
    fn register_waiting_consumer(&self) -> WaitingConsumer<'_> {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        header.consumer_waiters.fetch_add(1, Ordering::AcqRel);
        let generation = header.consumer_wait_generation.load(Ordering::Acquire);
        WaitingConsumer { header, generation }
    }

    #[cfg(target_os = "linux")]
    fn wait_for_consumer_wake(&self, generation: u32, timeout: Duration) -> bool {
        // SAFETY: Header is non-null and valid for the queue lifetime.
        let header = unsafe { self.header.as_ref() };
        futex_wait_timeout(&header.consumer_wait_generation.inner, generation, timeout)
    }

    fn recover_as_exclusive(&self) {
        for lane in 0..self.producer_slots {
            // SAFETY: lane is in bounds.
            unsafe { self.producer_lanes.add(lane).as_ref() }.reset();
            for index in 0..self.capacity() {
                let offset = lane * self.ring_lane_stride + index;
                // SAFETY: offset is in bounds.
                unsafe { self.ring.add(offset).as_ref() }
                    .store(PayloadHandle::EMPTY.0, Ordering::Release);
            }
        }
        for consumer in 0..self.consumer_slots {
            for lane in 0..self.producer_slots {
                self.consumer_hazard_slot(consumer, lane).reset();
            }
        }
        for consumer in 0..self.consumer_slots {
            for lane in 0..self.producer_slots {
                self.consumer_cursor(consumer, lane)
                    .store(0, Ordering::Relaxed);
            }
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
        let producer_slots = header_ref.producer_slots as usize;
        let consumer_slots = header_ref.consumer_slots as usize;
        SharedQueueHeader::validate_ring_capacity_for_file::<T>(
            buffer_size_in_items,
            producer_slots,
            consumer_slots,
            region.size(),
        )?;

        let payload_lane_capacity = payload_pool::capacity_for_ring_capacity(buffer_size_in_items)
            .ok_or(Error::InvalidBufferSize)?;
        let ring_lane_stride = SharedQueueHeader::ring_lane_stride(buffer_size_in_items);
        let header_lane_stride_bytes =
            SharedQueueHeader::payload_header_lane_stride_bytes(payload_lane_capacity);
        let payload_lane_stride_bytes =
            SharedQueueHeader::payload_lane_stride_bytes::<T>(payload_lane_capacity);
        // SAFETY: layout validation above proves these regions exist.
        let free_heads = unsafe { SharedQueueHeader::payload_free_heads_from_header(header) };
        // SAFETY: layout validation above proves these regions exist.
        let retired_heads = unsafe { SharedQueueHeader::payload_retired_heads_from_header(header) };
        // SAFETY: layout validation above proves these regions exist.
        let producer_lanes = unsafe { SharedQueueHeader::producer_slots_from_header(header) };
        // SAFETY: layout validation above proves these regions exist.
        let hazard_slots =
            unsafe { SharedQueueHeader::hazard_slots_from_header(header, producer_slots) };
        // SAFETY: layout validation above proves these regions exist.
        let consumer_cursors = unsafe {
            SharedQueueHeader::consumer_cursors_from_header(header, producer_slots, consumer_slots)
        };
        // SAFETY: layout validation above proves these regions exist.
        let ring = unsafe {
            SharedQueueHeader::ring_from_header(
                header,
                buffer_size_in_items,
                producer_slots,
                consumer_slots,
            )
        };
        // SAFETY: layout validation above proves these regions exist.
        let payload_headers = unsafe {
            SharedQueueHeader::payload_headers_from_header(
                header,
                buffer_size_in_items,
                producer_slots,
                consumer_slots,
            )
        };
        // SAFETY: layout validation above proves these regions exist.
        let payloads = unsafe {
            SharedQueueHeader::payloads_from_header::<T>(
                header,
                buffer_size_in_items,
                producer_slots,
                consumer_slots,
            )
        };

        Ok(Self {
            header,
            producer_lanes,
            ring,
            payload_pool: PayloadPool::new(
                free_heads,
                retired_heads,
                payload_headers,
                payloads,
                payload_lane_capacity,
                header_lane_stride_bytes,
                payload_lane_stride_bytes,
                producer_slots,
            ),
            region,
            buffer_mask,
            ring_lane_stride,
            producer_slots,
            consumer_slots,
            hazard_slots,
            consumer_cursors,
        })
    }
}

#[repr(C)]
struct SharedQueueHeader {
    // Cold metadata cacheline.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,
    producer_slots: u32,
    consumer_slots: u32,

    /// Shared futex generation for consumers sleeping on empty queues.
    consumer_wait_generation: CacheAlignedAtomicU32,
    /// Advisory count of consumers currently registered for futex wakeups.
    consumer_waiters: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    unsafe fn create<T>(
        file: &File,
        config: BroadcastConfig,
    ) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        let ring_capacity = Self::ring_capacity_for_requested_capacity(config.capacity);
        let Some(size) = Self::total_size_checked::<T>(
            ring_capacity,
            config.producer_slots,
            config.consumer_slots,
        ) else {
            return Err(Error::InvalidBufferSize);
        };
        file.set_len(size as u64)?;

        let region = Region::map_file(file, size)?;
        let header = region.addr().cast::<Self>();
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        unsafe { Self::initialize(header, ring_capacity, config) };
        Ok((region, header))
    }

    const fn ring_lane_stride(ring_capacity: usize) -> usize {
        ring_capacity.next_multiple_of(CACHELINE_SIZE / core::mem::size_of::<AtomicU64>())
    }

    const fn payload_lane_capacity_for_ring_capacity(ring_capacity: usize) -> usize {
        ring_capacity * payload_pool::PAYLOADS_PER_RING_ENTRY
    }

    const fn payload_header_lane_stride_bytes(lane_capacity: usize) -> usize {
        (lane_capacity * core::mem::size_of::<PayloadHeader>()).next_multiple_of(CACHELINE_SIZE)
    }

    const fn payload_lane_stride_bytes<T>(lane_capacity: usize) -> usize {
        (lane_capacity * core::mem::size_of::<T>()).next_multiple_of(CACHELINE_SIZE)
    }

    const fn payload_free_heads_offset() -> usize {
        core::mem::size_of::<Self>().next_multiple_of(CACHELINE_SIZE)
    }

    const fn payload_retired_heads_offset(producer_slots: usize) -> usize {
        (Self::payload_free_heads_offset() + producer_slots * CACHELINE_SIZE)
            .next_multiple_of(CACHELINE_SIZE)
    }

    const fn producer_slots_offset(producer_slots: usize) -> usize {
        (Self::payload_retired_heads_offset(producer_slots) + producer_slots * CACHELINE_SIZE)
            .next_multiple_of(core::mem::align_of::<ProducerSlot>())
    }

    const fn hazard_slots_offset(producer_slots: usize) -> usize {
        (Self::producer_slots_offset(producer_slots)
            + producer_slots * core::mem::size_of::<ProducerSlot>())
        .next_multiple_of(core::mem::align_of::<ConsumerHazardSlot>())
    }

    const fn consumer_cursors_offset(producer_slots: usize, consumer_slots: usize) -> usize {
        let hazard_count = producer_slots * consumer_slots;
        (Self::hazard_slots_offset(producer_slots)
            + hazard_count * core::mem::size_of::<ConsumerHazardSlot>())
        .next_multiple_of(core::mem::align_of::<AtomicUsize>())
    }

    const fn ring_offset(
        _ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> usize {
        let cursor_count = producer_slots * consumer_slots;
        (Self::consumer_cursors_offset(producer_slots, consumer_slots)
            + cursor_count * core::mem::size_of::<AtomicUsize>())
        .next_multiple_of(CACHELINE_SIZE)
    }

    const fn payload_headers_offset(
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> usize {
        (Self::ring_offset(ring_capacity, producer_slots, consumer_slots)
            + Self::ring_lane_stride(ring_capacity)
                * producer_slots
                * core::mem::size_of::<AtomicU64>())
        .next_multiple_of(CACHELINE_SIZE)
    }

    const fn payloads_offset<T>(
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> usize {
        let payload_lane_capacity = Self::payload_lane_capacity_for_ring_capacity(ring_capacity);
        (Self::payload_headers_offset(ring_capacity, producer_slots, consumer_slots)
            + Self::payload_header_lane_stride_bytes(payload_lane_capacity) * producer_slots)
            .next_multiple_of(max_usize(CACHELINE_SIZE, core::mem::align_of::<T>()))
    }

    const fn ring_capacity_for_requested_capacity(capacity: usize) -> usize {
        assert!(capacity != 0, "broadcast capacity must be non-zero");
        assert!(
            capacity <= MAX_RING_CAPACITY,
            "broadcast capacity exceeds maximum"
        );

        normalized_capacity(capacity)
    }

    const fn total_size_checked<T>(
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> Option<usize> {
        if core::mem::size_of::<T>() == 0
            || core::mem::align_of::<T>() > crate::shmem::MINIMUM_REGION_ALIGNMENT
            || producer_slots == 0
            || producer_slots > u32::MAX as usize
            || consumer_slots == 0
            || consumer_slots > u32::MAX as usize
        {
            return None;
        }

        let Some(ring_lane_stride) = checked_next_multiple_of(
            ring_capacity,
            CACHELINE_SIZE / core::mem::size_of::<AtomicU64>(),
        ) else {
            return None;
        };
        let Some(total_ring_entries) = ring_lane_stride.checked_mul(producer_slots) else {
            return None;
        };
        let Some(payload_lane_capacity) =
            ring_capacity.checked_mul(payload_pool::PAYLOADS_PER_RING_ENTRY)
        else {
            return None;
        };
        let Some(payload_capacity) = payload_lane_capacity.checked_mul(producer_slots) else {
            return None;
        };
        if payload_capacity == 0 || payload_capacity > u32::MAX as usize {
            return None;
        }
        let Some(ring_bytes) = total_ring_entries.checked_mul(core::mem::size_of::<AtomicU64>())
        else {
            return None;
        };
        let Some(free_heads_bytes) = producer_slots.checked_mul(CACHELINE_SIZE) else {
            return None;
        };
        let Some(free_heads_end) = Self::payload_free_heads_offset().checked_add(free_heads_bytes)
        else {
            return None;
        };
        let Some(retired_heads_offset) = checked_next_multiple_of(free_heads_end, CACHELINE_SIZE)
        else {
            return None;
        };
        let Some(retired_heads_bytes) = producer_slots.checked_mul(CACHELINE_SIZE) else {
            return None;
        };
        let Some(retired_heads_end) = retired_heads_offset.checked_add(retired_heads_bytes) else {
            return None;
        };
        let Some(producer_slots_offset) =
            checked_next_multiple_of(retired_heads_end, core::mem::align_of::<ProducerSlot>())
        else {
            return None;
        };
        let Some(producer_slots_bytes) =
            producer_slots.checked_mul(core::mem::size_of::<ProducerSlot>())
        else {
            return None;
        };
        let Some(producer_slots_end) = producer_slots_offset.checked_add(producer_slots_bytes)
        else {
            return None;
        };
        let Some(hazard_slot_count) = producer_slots.checked_mul(consumer_slots) else {
            return None;
        };
        let Some(hazard_slots_bytes) =
            hazard_slot_count.checked_mul(core::mem::size_of::<ConsumerHazardSlot>())
        else {
            return None;
        };
        let Some(hazard_slots_offset) = checked_next_multiple_of(
            producer_slots_end,
            core::mem::align_of::<ConsumerHazardSlot>(),
        ) else {
            return None;
        };
        let Some(hazard_slots_end) = hazard_slots_offset.checked_add(hazard_slots_bytes) else {
            return None;
        };
        let Some(cursor_bytes) = hazard_slot_count.checked_mul(core::mem::size_of::<AtomicUsize>())
        else {
            return None;
        };
        let Some(cursor_offset) =
            checked_next_multiple_of(hazard_slots_end, core::mem::align_of::<AtomicUsize>())
        else {
            return None;
        };
        let Some(cursor_end) = cursor_offset.checked_add(cursor_bytes) else {
            return None;
        };
        let Some(ring_offset) = checked_next_multiple_of(cursor_end, CACHELINE_SIZE) else {
            return None;
        };
        let Some(ring_end) = ring_offset.checked_add(ring_bytes) else {
            return None;
        };
        let Some(payload_headers_offset) = checked_next_multiple_of(ring_end, CACHELINE_SIZE)
        else {
            return None;
        };
        let Some(payload_header_lane_bytes) =
            payload_lane_capacity.checked_mul(core::mem::size_of::<PayloadHeader>())
        else {
            return None;
        };
        let Some(payload_header_lane_stride_bytes) =
            checked_next_multiple_of(payload_header_lane_bytes, CACHELINE_SIZE)
        else {
            return None;
        };
        let Some(payload_header_bytes) =
            payload_header_lane_stride_bytes.checked_mul(producer_slots)
        else {
            return None;
        };
        let Some(payload_headers_end) = payload_headers_offset.checked_add(payload_header_bytes)
        else {
            return None;
        };
        let Some(payloads_offset) = checked_next_multiple_of(
            payload_headers_end,
            max_usize(CACHELINE_SIZE, core::mem::align_of::<T>()),
        ) else {
            return None;
        };
        let Some(payload_lane_bytes) = payload_lane_capacity.checked_mul(core::mem::size_of::<T>())
        else {
            return None;
        };
        let Some(payload_lane_stride_bytes) =
            checked_next_multiple_of(payload_lane_bytes, CACHELINE_SIZE)
        else {
            return None;
        };
        let Some(payload_bytes) = payload_lane_stride_bytes.checked_mul(producer_slots) else {
            return None;
        };
        payloads_offset.checked_add(payload_bytes)
    }

    const fn total_size_for_ring_capacity<T>(
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> usize {
        assert!(
            producer_slots != 0,
            "broadcast producer slot count must be non-zero"
        );
        assert!(
            producer_slots <= u32::MAX as usize,
            "broadcast producer slot count exceeds maximum"
        );
        assert!(
            consumer_slots != 0,
            "broadcast consumer slot count must be non-zero"
        );
        assert!(
            consumer_slots <= u32::MAX as usize,
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

        match Self::total_size_checked::<T>(ring_capacity, producer_slots, consumer_slots) {
            Some(size) => size,
            None => panic!("broadcast queue size overflow"),
        }
    }

    fn validate_ring_capacity_for_file<T>(
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
        file_size: usize,
    ) -> Result<(), Error> {
        if ring_capacity == 0
            || !ring_capacity.is_power_of_two()
            || ring_capacity > MAX_RING_CAPACITY
        {
            return Err(Error::InvalidBufferSize);
        }

        let Some(minimum_size) =
            Self::total_size_checked::<T>(ring_capacity, producer_slots, consumer_slots)
        else {
            return Err(Error::InvalidBufferSize);
        };
        if minimum_size > file_size {
            return Err(Error::InvalidBufferSize);
        }
        Ok(())
    }

    unsafe fn initialize(
        mut header_ptr: NonNull<Self>,
        ring_capacity: usize,
        config: BroadcastConfig,
    ) {
        let payload_lane_capacity = payload_pool::capacity_for_ring_capacity(ring_capacity)
            .expect("validated payload lane capacity");
        let header_lane_stride_bytes =
            Self::payload_header_lane_stride_bytes(payload_lane_capacity);

        // SAFETY: caller guarantees unique access during initialization.
        let header = unsafe { header_ptr.as_mut() };
        header.consumer_wait_generation.store(0, Ordering::Release);
        header.consumer_waiters.store(0, Ordering::Release);
        header.buffer_mask = u32::try_from(ring_capacity - 1).unwrap();
        header.producer_slots =
            u32::try_from(config.producer_slots).expect("validated producer slots");
        header.consumer_slots =
            u32::try_from(config.consumer_slots).expect("validated consumer slots");
        header.version = VERSION;

        let free_heads = unsafe { Self::payload_free_heads_from_header(header_ptr) };
        let retired_heads = unsafe { Self::payload_retired_heads_from_header(header_ptr) };
        for lane in 0..config.producer_slots {
            // SAFETY: lane is in bounds for the per-lane head tables.
            unsafe {
                free_heads
                    .cast::<u8>()
                    .byte_add(lane * CACHELINE_SIZE)
                    .cast::<AtomicU64>()
                    .as_ptr()
                    .write(AtomicU64::new(payload_pool::initial_free_head(
                        lane,
                        payload_lane_capacity,
                    )));
                retired_heads
                    .cast::<u8>()
                    .byte_add(lane * CACHELINE_SIZE)
                    .cast::<AtomicU64>()
                    .as_ptr()
                    .write(AtomicU64::new(payload_pool::initial_retired_head()));
            }
        }

        let producer_slots = unsafe { Self::producer_slots_from_header(header_ptr) };
        for index in 0..config.producer_slots {
            // SAFETY: index is in bounds for the producer slot array.
            unsafe {
                producer_slots
                    .add(index)
                    .as_ptr()
                    .write(ProducerSlot::new())
            };
        }

        // SAFETY: The calculated layout reserves consumer hazard slots.
        let hazard_slots =
            unsafe { Self::hazard_slots_from_header(header_ptr, config.producer_slots) };
        for index in 0..config.consumer_slots * config.producer_slots {
            // SAFETY: index is in bounds for the hazard slot array.
            unsafe {
                hazard_slots
                    .add(index)
                    .as_ptr()
                    .write(ConsumerHazardSlot::new())
            };
        }

        let consumer_cursors = unsafe {
            Self::consumer_cursors_from_header(
                header_ptr,
                config.producer_slots,
                config.consumer_slots,
            )
        };
        for index in 0..config.producer_slots * config.consumer_slots {
            // SAFETY: index is in bounds for the consumer cursor table.
            unsafe {
                consumer_cursors
                    .add(index)
                    .as_ptr()
                    .write(AtomicUsize::new(0))
            };
        }

        let total_ring_entries = Self::ring_lane_stride(ring_capacity) * config.producer_slots;

        // SAFETY: The calculated layout reserves all lane ring entries.
        let ring = unsafe {
            Self::ring_from_header(
                header_ptr,
                ring_capacity,
                config.producer_slots,
                config.consumer_slots,
            )
        };
        for index in 0..total_ring_entries {
            // SAFETY: index is in bounds for the ring array.
            unsafe {
                ring.add(index)
                    .as_ptr()
                    .write(AtomicU64::new(PayloadHandle::EMPTY.0))
            };
        }

        let payload_headers = unsafe {
            Self::payload_headers_from_header(
                header_ptr,
                ring_capacity,
                config.producer_slots,
                config.consumer_slots,
            )
        };
        // SAFETY: The calculated layout reserves the padded per-lane payload headers.
        unsafe {
            payload_pool::initialize_payload_headers(
                payload_headers,
                payload_lane_capacity,
                header_lane_stride_bytes,
                config.producer_slots,
            )
        };

        header.magic.store(MAGIC, Ordering::Release);
    }

    fn join<T>(file: &File) -> Result<(Arc<Region>, NonNull<Self>), Error> {
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
            let ring_capacity = (header_ref.buffer_mask as usize).wrapping_add(1);
            Self::validate_ring_capacity_for_file::<T>(
                ring_capacity,
                header_ref.producer_slots as usize,
                header_ref.consumer_slots as usize,
                file_size,
            )?;
        }

        Ok((region, header))
    }

    unsafe fn producer_slots_from_header(header: NonNull<Self>) -> NonNull<ProducerSlot> {
        let producer_slots = unsafe { header.as_ref() }.producer_slots as usize;
        let offset = Self::producer_slots_offset(producer_slots);
        // SAFETY: caller guarantees the allocation includes the producer-slot layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payload_free_heads_from_header(header: NonNull<Self>) -> NonNull<AtomicU64> {
        let offset = Self::payload_free_heads_offset();
        // SAFETY: caller guarantees the allocation includes the free-head layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payload_retired_heads_from_header(header: NonNull<Self>) -> NonNull<AtomicU64> {
        let producer_slots = unsafe { header.as_ref() }.producer_slots as usize;
        let offset = Self::payload_retired_heads_offset(producer_slots);
        // SAFETY: caller guarantees the allocation includes the retired-head layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn ring_from_header(
        header: NonNull<Self>,
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> NonNull<AtomicU64> {
        let offset = Self::ring_offset(ring_capacity, producer_slots, consumer_slots);
        // SAFETY: caller guarantees the allocation includes the ring layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn hazard_slots_from_header(
        header: NonNull<Self>,
        producer_slots: usize,
    ) -> NonNull<ConsumerHazardSlot> {
        let offset = Self::hazard_slots_offset(producer_slots);
        // SAFETY: caller guarantees the allocation includes the hazard-slot layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn consumer_cursors_from_header(
        header: NonNull<Self>,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> NonNull<AtomicUsize> {
        let offset = Self::consumer_cursors_offset(producer_slots, consumer_slots);
        // SAFETY: caller guarantees the allocation includes the consumer-cursor layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payload_headers_from_header(
        header: NonNull<Self>,
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> NonNull<PayloadHeader> {
        let offset = Self::payload_headers_offset(ring_capacity, producer_slots, consumer_slots);
        // SAFETY: caller guarantees the allocation includes the payload-header layout.
        unsafe { header.byte_add(offset).cast() }
    }

    unsafe fn payloads_from_header<T>(
        header: NonNull<Self>,
        ring_capacity: usize,
        producer_slots: usize,
        consumer_slots: usize,
    ) -> NonNull<T> {
        let offset = Self::payloads_offset::<T>(ring_capacity, producer_slots, consumer_slots);
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

const fn max_usize(left: usize, right: usize) -> usize {
    if left > right {
        left
    } else {
        right
    }
}

#[cfg(target_os = "linux")]
fn futex_wait_timeout(addr: &AtomicU32, expected: u32, timeout: Duration) -> bool {
    if timeout.is_zero() {
        return false;
    }

    let addr = addr as *const AtomicU32 as *const libc::c_int;
    let timeout = libc::timespec {
        tv_sec: timeout.as_secs().min(<libc::time_t>::MAX as u64) as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // SAFETY: `addr` points to a shared, 4-byte aligned AtomicU32 that lives
    // for the queue lifetime. `timeout` points to a valid relative timeout for
    // the duration of the syscall.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr,
            libc::FUTEX_WAIT,
            expected as libc::c_int,
            &timeout as *const libc::timespec,
        )
    };
    if result == 0 {
        return true;
    }

    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::EAGAIN) => true,
        Some(libc::ETIMEDOUT | libc::EINTR) => false,
        _ => false,
    }
}

#[cfg(target_os = "linux")]
fn futex_wake_all(addr: &AtomicU32) {
    let addr = addr as *const AtomicU32 as *const libc::c_int;
    // SAFETY: `addr` points to the queue's futex word. Waking all waiters does
    // not require any additional memory passed to the kernel.
    let _ = unsafe { libc::syscall(libc::SYS_futex, addr, libc::FUTEX_WAKE, i32::MAX) };
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
pub struct WriteBatch<'a, T> {
    producer: &'a Producer<T>,
    reservation: ProducerReservation,
    next_write_payload_index: u32,
    written: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.reservation.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn advance_payload(&mut self) -> u32 {
        assert!(self.written < self.len());
        let payload_index = self.next_write_payload_index;
        self.written += 1;
        if self.written != self.len() {
            self.next_write_payload_index = self
                .producer
                .queue
                .payload_pool
                .next_payload_index_in_lane(self.reservation.lane, payload_index);
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
            self.producer
                .queue
                .payload_pool
                .payload_at_in_lane(self.reservation.lane, payload_index)
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
        this.producer
            .publish_reserved_payload_chain(this.reservation);
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        self.producer.cancel_reserved_payloads(self.reservation);
    }
}

#[must_use]
pub struct DirectRead<'a, T> {
    cursor: NonNull<AtomicUsize>,
    producer_slots: usize,
    scan_start_lane: &'a mut usize,
    queue: &'a SharedQueue<T>,
    hazard: ConsumerHazard,
    lane: usize,
    start: usize,
    payload: ProtectedPayload,
}

impl<'a, T> DirectRead<'a, T> {
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
        // SAFETY: `cursor` points into this consumer's cursor row for `lane`.
        unsafe { self.cursor.as_ref() }.store(self.start.wrapping_add(1), Ordering::Relaxed);
        *self.scan_start_lane = (self.lane + 1) % self.producer_slots;
    }
}

impl<'a, T> AsRef<T> for DirectRead<'a, T> {
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

impl<'a, T> Drop for DirectRead<'a, T> {
    fn drop(&mut self) {
        self.hazard.clear(self.lane);
    }
}

#[must_use]
pub struct DirectReadBatch<'a, T> {
    cursor: NonNull<AtomicUsize>,
    producer_slots: usize,
    scan_start_lane: &'a mut usize,
    queue: &'a SharedQueue<T>,
    hazard: ConsumerHazard,
    lane: usize,
    start: usize,
    payloads: NonNull<ProtectedPayload>,
    len: usize,
    _marker: PhantomData<&'a mut ProtectedPayload>,
}

impl<'a, T> DirectReadBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn payload(&self, index: usize) -> ProtectedPayload {
        assert!(index < self.len);
        // SAFETY: `payloads` points at this guard's fixed scratch storage and
        // `index` is bounded by `len`.
        unsafe { *self.payloads.add(index).as_ref() }
    }

    /// Returns a pointer to the hazard-protected payload at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len`.
    pub fn as_ptr(&self, index: usize) -> *const T {
        self.queue
            .payload_pool
            .protected_payload_ptr(self.payload(index))
            .as_ptr()
    }

    /// Returns a shared reference to the hazard-protected payload at `index`.
    ///
    /// # Panics
    /// Panics if `index >= len`.
    pub fn as_ref(&self, index: usize) -> &T {
        // SAFETY: The index was checked above and this guard publishes a hazard.
        unsafe {
            self.queue
                .payload_pool
                .protected_payload_ptr(self.payload(index))
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
        // SAFETY: The index was checked above and this guard publishes a hazard.
        unsafe { self.as_ptr(index).read() }
    }

    /// Advances the consumer cursor and clears the hazard when the guard is
    /// dropped.
    pub fn commit(self) {
        // SAFETY: `cursor` points into this consumer's cursor row for `lane`.
        unsafe { self.cursor.as_ref() }.store(self.start.wrapping_add(self.len), Ordering::Relaxed);
        *self.scan_start_lane = (self.lane + 1) % self.producer_slots;
    }
}

impl<'a, T> Drop for DirectReadBatch<'a, T> {
    fn drop(&mut self) {
        self.hazard.clear(self.lane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 8;
    const BUFFER_SIZE: usize = BUFFER_CAPACITY;

    fn create_test_queue<T>(capacity: usize) -> (File, Producer<T>, Consumer<T>) {
        create_test_queue_with_config::<T>(BroadcastConfig::new(capacity))
    }

    fn create_test_queue_with_slots<T, const CONSUMER_SLOTS: usize>(
        capacity: usize,
    ) -> (File, Producer<T>, Consumer<T>) {
        create_test_queue_with_config::<T>(
            BroadcastConfig::new(capacity).with_consumer_slots(CONSUMER_SLOTS),
        )
    }

    fn create_test_queue_with_config<T>(
        config: BroadcastConfig,
    ) -> (File, Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<T>::create(&file, config) }.expect("Failed to create producer");
        let consumer = unsafe { Consumer::<T>::join(&file) }.expect("Failed to join consumer");

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
        let (_file, producer, mut consumer) = create_test_queue_with_config::<Item>(
            BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(1),
        );

        let batch1 = producer
            .reserve_write_batch(BUFFER_CAPACITY)
            .expect("reserve first batch");
        assert!(producer.reserve_write_batch(1).is_none());

        drop(batch1);

        assert!(producer.reserve_write_batch(BUFFER_CAPACITY).is_some());
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_producer_drop_flushes_local_payload_cache() {
        let file = create_temp_shmem_file().unwrap();
        let producer1 = unsafe {
            Producer::<Item>::create(
                &file,
                BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(2),
            )
        }
        .expect("create failed");
        let producer2 = unsafe { Producer::<Item>::join(&file) }.expect("join failed");

        let batch = producer1.reserve_write_batch(1).expect("reserve cached");
        drop(batch);

        drop(producer1);

        assert!(producer2.reserve_write_batch(1).is_some());
    }

    #[test]
    fn test_producer_lanes_publish_independently() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.try_clone().expect("clone producer");

        let mut first = producer.reserve_write_batch(1).expect("reserve first");
        let mut second = producer2.reserve_write_batch(1).expect("reserve second");

        second.write_next(2);
        second.publish();
        assert_eq!(consumer.try_read().unwrap(), 2);

        first.write_next(1);
        first.publish();

        assert_eq!(consumer.try_read().unwrap(), 1);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_single_producer_allows_one_unpublished_batch() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut first = producer.reserve_write_batch(1).expect("reserve first");
        assert!(producer.reserve_write_batch(1).is_none());
        first.write_next(1);

        first.publish();
        assert!(producer.reserve_write_batch(1).is_some());

        assert_eq!(consumer.try_read().unwrap(), 1);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_batch_publish_is_atomic_for_consumers() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = producer.reserve_write_batch(2).expect("reserve batch");
        batch.write_next(1);
        batch.write_next(2);

        assert_eq!(producer.queue.published(producer.lane.index()), 0);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));

        batch.publish();
        assert_eq!(producer.queue.published(producer.lane.index()), 2);

        assert_eq!(consumer.try_read().unwrap(), 1);
        assert_eq!(consumer.try_read().unwrap(), 2);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));
    }

    #[test]
    fn test_try_write_fails_while_batch_is_unpublished() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut first = producer.reserve_write_batch(1).expect("reserve first");
        first.write_next(1);

        assert_eq!(producer.try_write(2), Err(2));
        assert_eq!(producer.queue.published(producer.lane.index()), 0);
        assert_eq!(consumer.try_read(), Err(TryReadError::Empty));

        first.publish();
        assert_eq!(producer.queue.published(producer.lane.index()), 1);
        assert_eq!(consumer.try_read().unwrap(), 1);
        producer.try_write(2).unwrap();
        assert_eq!(consumer.try_read().unwrap(), 2);
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
    fn test_try_read_direct_batch_caps_at_fixed_scratch_size() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(MAX_DIRECT_READ_BATCH + 8);

        for value in 0..MAX_DIRECT_READ_BATCH + 8 {
            producer.try_write(value as Item).unwrap();
        }

        let direct = unsafe { consumer.try_read_direct_batch(MAX_DIRECT_READ_BATCH + 8) }.unwrap();
        assert_eq!(direct.len(), MAX_DIRECT_READ_BATCH);
        direct.commit();

        let direct = unsafe { consumer.try_read_direct_batch(MAX_DIRECT_READ_BATCH + 8) }.unwrap();
        assert_eq!(direct.len(), 8);
        direct.commit();
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
        let (_file, producer, mut consumer) = create_test_queue_with_config::<Item>(
            BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(1),
        );

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
        let (_file, producer, mut consumer) = create_test_queue_with_config::<Item>(
            BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(1),
        );

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
        let producer = unsafe {
            Producer::<Item>::create(
                &file,
                BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(1),
            )
        }
        .expect("create failed");
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
    fn test_claim_consumer_slot_clears_stale_hazard() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe {
            Producer::<Item>::create(
                &file,
                BroadcastConfig::new(BUFFER_SIZE)
                    .with_producer_slots(1)
                    .with_consumer_slots(1),
            )
        }
        .expect("create failed");
        let mut consumer = unsafe { Consumer::<Item>::join(&file) }.expect("join failed");
        let slot = consumer.slot_index();

        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(i).unwrap();
        }
        let guarded = unsafe { consumer.try_read_direct_batch(BUFFER_CAPACITY) }.unwrap();
        for i in 0..BUFFER_CAPACITY as u64 {
            producer.try_write(100 + i).unwrap();
        }

        assert!(producer.reserve_write_batch(1).is_none());
        core::mem::forget(guarded);
        core::mem::forget(consumer);

        let mut claimed =
            unsafe { Consumer::<Item>::claim_slot(&file, slot) }.expect("claim consumer slot");
        assert_eq!(claimed.slot_index(), slot);
        assert_eq!(claimed.try_read(), Err(TryReadError::Empty));
        assert!(producer.reserve_write_batch(1).is_some());
    }

    #[test]
    fn test_claim_consumer_slot_rejects_out_of_range_index() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe {
            Producer::<Item>::create(
                &file,
                BroadcastConfig::new(BUFFER_SIZE).with_consumer_slots(1),
            )
        }
        .expect("create failed");

        match unsafe { Consumer::<Item>::claim_slot(&file, 1) } {
            Err(Error::InvalidConsumerIndex { index: 1, slots: 1 }) => {}
            Err(err) => panic!("unexpected consumer claim error: {err}"),
            Ok(_) => panic!("consumer claim unexpectedly succeeded"),
        }
        match unsafe { producer.claim_consumer_slot(1) } {
            Err(Error::InvalidConsumerIndex { index: 1, slots: 1 }) => {}
            Err(err) => panic!("unexpected producer-side claim error: {err}"),
            Ok(_) => panic!("producer-side consumer claim unexpectedly succeeded"),
        }
    }

    #[test]
    fn test_producer_recover_as_exclusive() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe {
            Producer::<Item>::create(
                &file,
                BroadcastConfig::new(BUFFER_SIZE).with_producer_slots(1),
            )
        }
        .expect("create failed");

        for item in 0..BUFFER_CAPACITY as Item {
            producer.try_write(item).unwrap();
        }

        let batch = producer.reserve_write_batch(1).expect("reserve write");
        core::mem::forget(batch);

        unsafe {
            producer.recover_as_exclusive();
        }
        match unsafe { Producer::<Item>::join(&file) } {
            Err(Error::ProducerSlotsExhausted) => {}
            Err(err) => panic!("unexpected producer join error: {err}"),
            Ok(_) => panic!("producer join unexpectedly succeeded"),
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
            unsafe { Producer::<Item>::create(&file, BroadcastConfig::new(BUFFER_SIZE)) }
                .expect("create failed");

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
        let producer = unsafe { Producer::<u64>::create(&file, BroadcastConfig::new(4)) }
            .expect("create failed");

        file.set_len((exact_size + 4096) as u64)
            .expect("grow queue file");

        let joined_producer = unsafe { Producer::<u64>::join(&file) }.expect("producer join");
        let joined_consumer = unsafe { Consumer::<u64>::join(&file) }.expect("consumer join");

        assert_eq!(producer.capacity(), 4);
        assert_eq!(joined_producer.capacity(), 4);
        assert_eq!(joined_consumer.capacity(), 4);
    }

    #[test]
    fn test_consumer_slots_exhaustion_and_drop_releases_slot() {
        const CONSUMER_SLOTS: usize = 2;

        let file = create_temp_shmem_file().unwrap();
        let config = BroadcastConfig::new(BUFFER_CAPACITY).with_consumer_slots(CONSUMER_SLOTS);
        let producer = unsafe { Producer::<Item>::create(&file, config) }.expect("create failed");
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
    fn test_producer_slots_exhaustion_and_drop_releases_slot() {
        let file = create_temp_shmem_file().unwrap();
        let config = BroadcastConfig::new(BUFFER_CAPACITY).with_producer_slots(2);
        let producer = unsafe { Producer::<Item>::create(&file, config) }.expect("create failed");
        let producer2 = producer.try_clone().expect("clone producer");

        match producer.try_clone() {
            Err(Error::ProducerSlotsExhausted) => {}
            Err(err) => panic!("unexpected producer slot error: {err}"),
            Ok(_) => panic!("producer clone unexpectedly succeeded"),
        }

        drop(producer2);
        assert!(producer.try_clone().is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_wait_for_work_returns_immediately_when_work_exists() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(42).unwrap();
        assert!(consumer.wait_for_work_timeout(Duration::from_secs(1)));

        assert_eq!(consumer.try_read().unwrap(), 42);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_wait_for_work_timeout_returns_false_without_work() {
        let (_file, _producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        assert!(!consumer.wait_for_work_timeout(Duration::from_millis(1)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_timeout_wakes_after_publish() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let handle = std::thread::spawn(move || {
            let mut consumer = consumer;
            consumer
                .read_timeout(Duration::from_secs(1))
                .expect("timed read")
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        producer.try_write(99).unwrap();

        assert_eq!(handle.join().unwrap(), 99);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_direct_batch_timeout_wakes_after_publish() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let handle = std::thread::spawn(move || {
            let mut consumer = consumer;
            // SAFETY: the test reads initialized u64 payloads from the returned guard.
            let batch = unsafe {
                consumer
                    .read_direct_batch_timeout(4, Duration::from_secs(1))
                    .expect("timed batch read")
            };
            let values = (0..batch.len())
                .map(|index| batch.read(index))
                .collect::<Vec<_>>();
            batch.commit();
            values
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        for value in 0..3 {
            producer.try_write(value).unwrap();
        }

        assert_eq!(handle.join().unwrap(), vec![0, 1, 2]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_direct_batch_timeout_zero_max_does_not_wait() {
        let (_file, _producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        // SAFETY: max=0 returns before exposing any payload references.
        assert_eq!(
            unsafe { consumer.read_direct_batch_timeout(0, Duration::from_secs(1)) }.map(|_| ()),
            Err(TryReadError::Empty)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_read_direct_batch_timeout_returns_empty_without_work() {
        let (_file, _producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        // SAFETY: no payload references are returned on timeout.
        assert_eq!(
            unsafe { consumer.read_direct_batch_timeout(4, Duration::from_millis(1)) }.map(|_| ()),
            Err(TryReadError::Empty)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_wait_registration_does_not_lose_publish_race() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        assert!(!consumer.has_read_event());
        let wait_guard = consumer.queue.register_waiting_consumer();
        let generation = wait_guard.generation();
        producer.try_write(7).unwrap();
        assert!(consumer
            .queue
            .wait_for_consumer_wake(generation, Duration::from_secs(1)));
        drop(wait_guard);

        assert_eq!(consumer.try_read().unwrap(), 7);
    }

    #[test]
    fn test_clone_producer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.try_clone().expect("clone producer");

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
        let producer2 = consumer2.join_as_producer().expect("join producer");

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
        let producer = unsafe { Producer::<u64>::create(&file, BroadcastConfig::new(3)) }
            .expect("create failed");
        let consumer = unsafe { Consumer::<u64>::join(&file) }.expect("join failed");

        assert_eq!(producer.capacity(), 4);
        assert_eq!(consumer.capacity(), 4);
        assert_eq!(producer.queue.payload_pool.capacity(), 64);
    }

    #[test]
    fn test_producer_lane_storage_is_cacheline_separated() {
        let (_file, producer, _consumer) =
            create_test_queue_with_config::<Item>(BroadcastConfig::with_slots(3, 3, 1));
        let ring_capacity = producer.capacity();
        let payload_lane_capacity =
            payload_pool::capacity_for_ring_capacity(ring_capacity).expect("payload capacity");
        let header_lane_stride =
            SharedQueueHeader::payload_header_lane_stride_bytes(payload_lane_capacity);
        let payload_lane_stride =
            SharedQueueHeader::payload_lane_stride_bytes::<Item>(payload_lane_capacity);

        assert_eq!(ring_capacity, 4);
        assert_eq!(producer.queue.ring_lane_stride, 8);
        assert_eq!(
            producer.queue.ring_lane_stride * core::mem::size_of::<AtomicU64>() % CACHELINE_SIZE,
            0
        );
        assert_eq!(header_lane_stride % CACHELINE_SIZE, 0);
        assert_eq!(payload_lane_stride % CACHELINE_SIZE, 0);

        let header = producer.queue.header;
        let free_heads = unsafe { SharedQueueHeader::payload_free_heads_from_header(header) };
        let retired_heads = unsafe { SharedQueueHeader::payload_retired_heads_from_header(header) };
        let ring_base = producer.queue.ring.as_ptr() as usize;
        let payload_headers = unsafe {
            SharedQueueHeader::payload_headers_from_header(
                header,
                ring_capacity,
                producer.queue.producer_slots(),
                producer.queue.consumer_slots,
            )
        };
        let payloads = unsafe {
            SharedQueueHeader::payloads_from_header::<Item>(
                header,
                ring_capacity,
                producer.queue.producer_slots(),
                producer.queue.consumer_slots,
            )
        };
        let payload_header_base = payload_headers.as_ptr() as usize;
        let payload_base = payloads.as_ptr() as usize;

        for lane in 0..producer.queue.producer_slots() {
            let free_head = unsafe {
                free_heads
                    .cast::<u8>()
                    .byte_add(lane * CACHELINE_SIZE)
                    .as_ptr()
            } as usize;
            let retired_head = unsafe {
                retired_heads
                    .cast::<u8>()
                    .byte_add(lane * CACHELINE_SIZE)
                    .as_ptr()
            } as usize;
            let ring_lane = ring_base
                + lane * producer.queue.ring_lane_stride * core::mem::size_of::<AtomicU64>();
            let payload_header_lane = payload_header_base + lane * header_lane_stride;
            let payload_lane = payload_base + lane * payload_lane_stride;

            assert_eq!(free_head % CACHELINE_SIZE, 0);
            assert_eq!(retired_head % CACHELINE_SIZE, 0);
            assert_eq!(ring_lane % CACHELINE_SIZE, 0);
            assert_eq!(payload_header_lane % CACHELINE_SIZE, 0);
            assert_eq!(payload_lane % CACHELINE_SIZE, 0);
        }
    }

    #[test]
    fn test_minimum_file_size_rejects_invalid_inputs() {
        assert!(std::panic::catch_unwind(|| minimum_file_size::<u64>(0)).is_err());
        assert!(
            std::panic::catch_unwind(|| minimum_file_size::<u64>(MAX_RING_CAPACITY + 1)).is_err()
        );
        assert!(std::panic::catch_unwind(|| minimum_region_size::<u64>(usize::MAX)).is_err());
        assert!(std::panic::catch_unwind(|| minimum_file_size::<()>(1)).is_err());
        assert!(std::panic::catch_unwind(|| {
            minimum_file_size_for_config::<u64>(BroadcastConfig::with_slots(1, 0, 1))
        })
        .is_err());
        assert!(std::panic::catch_unwind(|| {
            minimum_file_size_for_config::<u64>(BroadcastConfig::with_slots(1, 1, 0))
        })
        .is_err());
    }
}
