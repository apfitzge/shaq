//! DPDK-style bounded broadcast ring queue.
//!
//! This queue mirrors the producer-side reservation/publication flow of the
//! MPMC queue, but consumers do not reserve shared slots. Instead each
//! consumer tracks its own local cursor. Consumers use direct read guards that
//! expose raw pointers into the ring.
//! Reads fail with a skipped-item count if the consumer falls behind or if
//! producers reserve the same ring slots for overwrite before the read can be
//! validated.
//!
//! Reads use a seqlock-like validation scheme. Instead of storing a sequence
//! value per slot, the queue uses the global `producer_reservation` cursor as
//! the sequence. A consumer checks that its target window is still retained,
//! copies or inspects the payload, and then calls `validate` or `commit` to
//! check the reservation cursor again. Producer reservation advances before a
//! producer may overwrite a slot, so this second check conservatively rejects a
//! snapshot if its slots may have been reserved for overwrite, even if the
//! overwrite has not yet been published.
//!
//! Broadcast producers may overwrite retained slots without waiting for
//! consumers. Payload access that may race with an overwrite must use the
//! high-level atomic read/write methods, the atomic chunk helpers, or external
//! synchronization. A racing atomic read may observe old data, new data, or a
//! mixed old/new snapshot. Consumers must validate or commit after reading and
//! discard the result if validation reports a possible overwrite.
//! The atomic payload API is part of this contract: ordinary Rust loads and
//! stores cannot be used for seqlock-style racing payload access because a
//! plain read racing with a write is a data race in Rust's memory model.
//! The intended direct-read sequence is: obtain a guard, copy or inspect the
//! payload bytes, then call `validate` or `commit`. A successful validation only
//! validates the snapshot already taken; it does not pin the slot, and a
//! producer may overwrite the slot immediately after validation returns.
//!
//! Broadcast payloads are treated as shared-memory bytes. The unsafe
//! constructors require callers to choose a `T` and access pattern for which
//! bytewise copying, sharing, forgetting, and pre-validation inspection are
//! valid.

use crate::{error::Error, normalized_capacity, shmem::Region, CacheAlignedAtomicSize, VERSION};
use core::{
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ptr::{self, NonNull},
    sync::atomic::{fence, AtomicU64, AtomicU8, AtomicUsize, Ordering},
};
use std::{fs::File, num::NonZeroUsize, sync::Arc};

/// Unique identifier for broadcast queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");
const DEFAULT_COOPERATIVE_DESCRIPTOR_CAPACITY: usize = 4096;
const COOPERATIVE_MAX_CAPACITY: usize = 1usize << 31;
pub const MODE_CLASSIC: u32 = 0;
pub const MODE_COOPERATIVE: u32 = 1;

/// Producer publication strategy for a broadcast queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PublicationMode {
    /// Current behavior: each producer publishes its own reservation in FIFO
    /// order, spinning until earlier producers have published.
    Classic = MODE_CLASSIC,
    /// Producers mark reservations ready and cooperatively advance the ready
    /// FIFO prefix, so later guard drops do not spin behind earlier guards.
    Cooperative = MODE_COOPERATIVE,
}

impl PublicationMode {
    const fn as_u32(self) -> u32 {
        self as u32
    }

    fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Classic),
            1 => Some(Self::Cooperative),
            _ => None,
        }
    }
}

impl Default for PublicationMode {
    fn default() -> Self {
        Self::Classic
    }
}

const fn publication_mode_for_mode<const MODE: u32>() -> PublicationMode {
    if MODE == MODE_COOPERATIVE {
        PublicationMode::Cooperative
    } else {
        PublicationMode::Classic
    }
}

fn validate_mode<const MODE: u32>() -> Result<PublicationMode, Error> {
    match MODE {
        MODE_CLASSIC => Ok(PublicationMode::Classic),
        MODE_COOPERATIVE => Ok(PublicationMode::Cooperative),
        _ => Err(Error::InvalidBufferSize),
    }
}

/// Creation options for a broadcast queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Options {
    publication_mode: PublicationMode,
    cooperative_descriptor_capacity: usize,
}

impl Options {
    pub const fn new() -> Self {
        Self {
            publication_mode: PublicationMode::Classic,
            cooperative_descriptor_capacity: 0,
        }
    }

    /// Selects a publication mode for mode-agnostic sizing helpers.
    ///
    /// Typed producers and consumers override this with their const generic
    /// mode during creation.
    pub const fn with_publication_mode(mut self, publication_mode: PublicationMode) -> Self {
        self.publication_mode = publication_mode;
        self
    }

    /// Sets the cooperative descriptor ring capacity.
    ///
    /// The value is rounded up to a power of two and capped at the item
    /// capacity. Passing zero uses the default cooperative descriptor capacity.
    pub const fn with_cooperative_descriptor_capacity(mut self, capacity: usize) -> Self {
        self.cooperative_descriptor_capacity = capacity;
        self
    }

    pub const fn publication_mode(&self) -> PublicationMode {
        self.publication_mode
    }

    pub const fn cooperative_descriptor_capacity(&self) -> usize {
        self.cooperative_descriptor_capacity
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}

/// Classic broadcast producer, kept as the default `Producer` type.
pub type Producer<T> = ModeProducer<T, { MODE_CLASSIC }>;
/// Classic broadcast consumer, kept as the default `Consumer` type.
pub type Consumer<T> = ModeConsumer<T, { MODE_CLASSIC }>;
/// Producer using classic in-order publication.
pub type ClassicProducer<T> = ModeProducer<T, { MODE_CLASSIC }>;
/// Producer using cooperative descriptor publication.
pub type CooperativeProducer<T> = ModeProducer<T, { MODE_COOPERATIVE }>;
/// Consumer joined to a classic broadcast queue.
pub type ClassicConsumer<T> = ModeConsumer<T, { MODE_CLASSIC }>;
/// Consumer joined to a cooperative broadcast queue.
pub type CooperativeConsumer<T> = ModeConsumer<T, { MODE_COOPERATIVE }>;

pub struct ModeProducer<T, const MODE: u32> {
    queue: SharedQueue<T, MODE>,
}

impl<T, const MODE: u32> ModeProducer<T, MODE> {
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
    ///   consumers for the same file must use the same `T`.
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::create_with_options(file, file_size, Options::default()) }
    }

    /// Creates a new producer for the shared queue using explicit options.
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
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn create_with_options(
        file: &File,
        file_size: usize,
        options: Options,
    ) -> Result<Self, Error> {
        let mode = validate_mode::<MODE>()?;
        let options = options.with_publication_mode(mode);
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, file_size, options) }?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - The queue does not validate `T` across processes. All producers and
    ///   consumers for the same file must use the same `T`.
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    ///
    /// The consumer starts at the current producer publication cursor and will
    /// only observe values published after it joins.
    pub fn join_as_consumer(&self) -> ModeConsumer<T, MODE> {
        ModeConsumer::from_queue(self.queue.clone())
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Writes `item` into the queue with atomic chunk stores, or returns it if
    /// a slot cannot be reserved.
    ///
    /// In classic mode, publishing may wait behind earlier producers that have
    /// reserved slots but not yet published them. Cooperative mode avoids that
    /// drop-time spin, though the visibility frontier still waits for earlier
    /// guards.
    ///
    /// The value is copied into the reserved slot with [`write_atomic_chunks`]
    /// before the slot is published to consumers. `ordering` applies to the
    /// payload stores only; queue publication still uses the queue's internal
    /// release ordering.
    ///
    /// # Panics
    /// Panics if `ordering` is not valid for atomic stores.
    pub fn try_write(&self, item: T, ordering: Ordering) -> Result<(), T> {
        // SAFETY: On successful reservation the item is written below.
        let guard = match unsafe { self.reserve_write() } {
            Some(guard) => guard,
            None => return Err(item),
        };
        guard.write_atomic(item, ordering);
        Ok(())
    }

    /// Reserves a slot for writing.
    /// The slot is committed when the guard is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes become visible in
    /// reservation order. In classic mode, holding a [`WriteGuard`] can make
    /// later producers spin while publishing; cooperative mode avoids that
    /// spin, though the visibility frontier still waits for earlier guards.
    ///
    /// # Safety
    /// - Before the guard is dropped, the reserved slot must contain bytes that
    ///   are valid to publish as `T`.
    /// - Payload access that may race with consumers must use atomics or
    ///   external synchronization.
    #[must_use]
    pub unsafe fn reserve_write(&self) -> Option<WriteGuard<'_, T, MODE>> {
        self.queue
            .reserve_write()
            .map(|(cell, reservation)| WriteGuard {
                header: self.queue.header,
                cell,
                start: reservation.start,
                descriptor: reservation.descriptor,
                _marker: PhantomData,
            })
    }

    /// Reserves exactly `count` slots for writing.
    /// The slots are committed when the batch is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes become visible in
    /// reservation order. In classic mode, holding a [`WriteBatch`] can make
    /// later producers spin while publishing; cooperative mode avoids that
    /// spin, though the visibility frontier still waits for earlier batches.
    ///
    /// # Safety
    /// - Before the batch is dropped, each reserved slot must contain bytes
    ///   that are valid to publish as `T`.
    /// - Payload access that may race with consumers must use atomics or
    ///   external synchronization.
    #[must_use]
    pub unsafe fn reserve_write_batch(&self, count: usize) -> Option<WriteBatch<'_, T, MODE>> {
        let reservation = self.queue.reserve_write_batch(count)?;
        Some(WriteBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start: reservation.start,
            descriptor: reservation.descriptor,
            count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }

    /// Abandons all reserved-but-unpublished writes left behind by a previous
    /// producer process.
    ///
    /// This rolls `producer_reservation` back to `producer_publication`,
    /// making capacity consumed by reservations whose guards were lost without
    /// running `Drop` reusable without publishing their slots to consumers.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   process or thread is accessing the shared queue in any role.
    /// - This must only be used when joining as the sole process or thread
    ///   accessing the shared queue.
    /// - Existing [`Consumer`] handles have local cursors from before the
    ///   recovery; recreate them or call [`Consumer::sync_to_latest`] before reuse.
    /// - Retained items that overlap abandoned reservations must not be read
    ///   after recovery; they may have been partially overwritten before the
    ///   previous producer died.
    /// - Racing with any live producer or consumer process/thread may corrupt
    ///   the queue.
    pub unsafe fn recover_as_exclusive(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let publication = header.producer_publication.load(Ordering::Acquire);
        if MODE == MODE_CLASSIC {
            header
                .producer_reservation
                .store(publication, Ordering::Release);
        } else {
            header
                .producer_reservation
                .store(publication, Ordering::Release);
            self.queue.clear_descriptors();
        }
    }
}

impl<T, const MODE: u32> Clone for ModeProducer<T, MODE> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T: Send, const MODE: u32> Send for ModeProducer<T, MODE> {}
unsafe impl<T: Send + Sync, const MODE: u32> Sync for ModeProducer<T, MODE> {}

pub struct ModeConsumer<T, const MODE: u32> {
    queue: SharedQueue<T, MODE>,
    next: usize,
}

impl<T, const MODE: u32> ModeConsumer<T, MODE> {
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
    ///   consumers for the same file must use the same `T`.
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::create_with_options(file, file_size, Options::default()) }
    }

    /// Creates a new consumer for the shared queue using explicit options.
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
    ///   consumers for the same file must use the same `T`.
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn create_with_options(
        file: &File,
        file_size: usize,
        options: Options,
    ) -> Result<Self, Error> {
        let mode = validate_mode::<MODE>()?;
        let options = options.with_publication_mode(mode);
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, file_size, options) }?;
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Ok(Self::from_queue(queue))
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
    ///   consumers for the same file must use the same `T`.
    /// - If a process may read, dereference, inspect, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - By-value and atomic chunk APIs require initialized bytes, including
    ///   padding, and bytewise copying or forgetting the payload must be valid
    ///   for `T`.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization. Pre-validation inspection
    ///   must be valid for any old, new, or mixed snapshot it may observe.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Ok(Self::from_queue(queue))
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> ModeProducer<T, MODE> {
        ModeProducer {
            queue: self.queue.clone(),
        }
    }

    fn from_queue(queue: SharedQueue<T, MODE>) -> Self {
        let next = queue.published();
        Self { queue, next }
    }

    /// Repositions the consumer to the oldest item still retained in the ring.
    ///
    /// This is useful after an overrun when the consumer wants to resume
    /// ordered reads from the earliest value that has not yet been overwritten.
    pub fn sync_to_oldest(&mut self) {
        self.next = self.queue.oldest_available();
    }

    /// Repositions the consumer to the producer publication cursor.
    ///
    /// After this call the consumer will ignore any currently buffered items
    /// and only observe values published afterwards. Returns the number of
    /// published items skipped by this repositioning.
    pub fn sync_to_latest(&mut self) -> usize {
        let published = self.queue.published();
        let skipped = cursor_sub_for_mode::<MODE>(published, self.next);
        self.next = published;
        skipped
    }

    /// Attempts to read and commit one value from the queue.
    ///
    /// The payload is copied with [`read_atomic_chunks`] before validation.
    /// If validation fails, the copied snapshot is discarded and the skipped
    /// item count is returned.
    ///
    /// `ordering` applies to the payload loads only; queue cursor checks still
    /// use the queue's internal acquire ordering.
    ///
    /// # Panics
    /// Panics if `ordering` is not valid for atomic loads.
    pub fn try_read(&mut self, ordering: Ordering) -> Result<Option<T>, usize> {
        // SAFETY: `Consumer` construction establishes the broadcast payload
        // contract for high-level reads.
        let Some(direct) = (unsafe { self.try_read_direct()? }) else {
            return Ok(None);
        };

        let mut item = MaybeUninit::<T>::uninit();
        // SAFETY: `direct.as_ptr()` points at a retained slot for `T`; the
        // broadcast payload contract guarantees the bytes are valid for the
        // chunked atomic read.
        unsafe { read_atomic_chunks(direct.as_ptr(), &mut item, ordering) };
        direct.commit()?;

        // SAFETY: `read_atomic_chunks` initialized all bytes of `item`, and a
        // successful commit validated that the already-read snapshot was still
        // retained rather than stale or mixed.
        Ok(Some(unsafe { item.assume_init() }))
    }

    /// Attempts to read a value from the queue as a raw pointer.
    ///
    /// Prefer [`Self::try_read`] for high-level reads.
    ///
    /// # Safety
    /// - Payload access through the returned guard that may race with producers
    ///   must use atomic accesses or external synchronization.
    /// - Copy or inspect the payload before calling [`DirectRead::validate`] or
    ///   [`DirectRead::commit`]. If validation reports a possible overwrite,
    ///   discard that already-taken snapshot.
    /// - Validation is not a pin. Do not read through the returned pointer
    ///   after validation or commit and assume the same slot contents remain.
    pub unsafe fn try_read_direct(&mut self) -> Result<Option<DirectRead<'_, T, MODE>>, usize> {
        let start = self.next;
        let published = self.queue.published();
        let available = cursor_sub_for_mode::<MODE>(published, start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun_at_reserved(start, self.queue.reserved());
            self.next = overrun.next;
            return Err(overrun.skipped);
        }
        if let Err(overrun) = self.queue.validate_window(start, 1) {
            self.next = overrun.next;
            return Err(overrun.skipped);
        }

        Ok(Some(DirectRead {
            next: &mut self.next,
            queue: &self.queue,
            start,
        }))
    }

    /// Attempts to read up to `max` values from the queue as raw pointers.
    ///
    /// Prefer repeated [`Self::try_read`] calls for high-level reads.
    ///
    /// # Safety
    /// - Payload access through the returned guard that may race with producers
    ///   must use atomic accesses or external synchronization.
    /// - Copy or inspect the payloads before calling
    ///   [`DirectReadBatch::validate`] or [`DirectReadBatch::commit`]. If
    ///   validation reports a possible overwrite, discard those already-taken
    ///   snapshots.
    /// - Validation is not a pin. Do not read through the returned pointers
    ///   after validation or commit and assume the same slot contents remain.
    pub unsafe fn try_read_direct_batch(
        &mut self,
        max: usize,
    ) -> Result<Option<DirectReadBatch<'_, T, MODE>>, usize> {
        if max == 0 {
            return Ok(None);
        }

        let start = self.next;
        let published = self.queue.published();
        let available = cursor_sub_for_mode::<MODE>(published, start);
        if available == 0 {
            return Ok(None);
        }
        if available > self.queue.capacity() {
            let overrun = self.queue.overrun_at_reserved(start, self.queue.reserved());
            self.next = overrun.next;
            return Err(overrun.skipped);
        }

        let count = available.min(max);
        if let Err(overrun) = self.queue.validate_window(start, count) {
            self.next = overrun.next;
            return Err(overrun.skipped);
        }
        Ok(Some(DirectReadBatch {
            next: &mut self.next,
            queue: &self.queue,
            start,
            count,
        }))
    }
}

impl<T, const MODE: u32> Clone for ModeConsumer<T, MODE> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            next: self.next,
        }
    }
}

unsafe impl<T: Send, const MODE: u32> Send for ModeConsumer<T, MODE> {}
unsafe impl<T: Send + Sync, const MODE: u32> Sync for ModeConsumer<T, MODE> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T>(capacity: usize) -> usize {
    minimum_file_size_with_options::<T>(capacity, Options::new())
}

/// Calculates the minimum file size required for a queue with given capacity
/// and options.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size_with_options<T>(capacity: usize, options: Options) -> usize {
    let mut capacity = normalized_capacity(capacity);
    if matches!(options.publication_mode(), PublicationMode::Cooperative)
        && capacity > COOPERATIVE_MAX_CAPACITY
    {
        capacity = COOPERATIVE_MAX_CAPACITY;
    }
    let descriptor_capacity = SharedQueueHeader::descriptor_capacity_for_options(capacity, options);
    let buffer_offset = SharedQueueHeader::buffer_offset_for_capacities::<T>(
        descriptor_capacity,
        options.publication_mode(),
    );
    buffer_offset + capacity * core::mem::size_of::<T>()
}

pub const fn minimum_file_size_for_mode<T, const MODE: u32>(
    capacity: usize,
    options: Options,
) -> usize {
    minimum_file_size_with_options::<T>(
        capacity,
        options.with_publication_mode(publication_mode_for_mode::<MODE>()),
    )
}

/// Calculates the minimum region size required for a queue with given capacity.
pub const fn minimum_region_size<T>(capacity: usize) -> usize {
    minimum_file_size::<T>(capacity)
}

/// Calculates the minimum region size required for a queue with given capacity
/// and options.
pub const fn minimum_region_size_with_options<T>(capacity: usize, options: Options) -> usize {
    minimum_file_size_with_options::<T>(capacity, options)
}

pub const fn minimum_region_size_for_mode<T, const MODE: u32>(
    capacity: usize,
    options: Options,
) -> usize {
    minimum_file_size_for_mode::<T, MODE>(capacity, options)
}

/// Creates a new in-process broadcast queue pair backed by a heap allocation.
///
/// Values left buffered when the queue is dropped may be leaked instead of
/// having their destructors run.
pub fn pair<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), Error> {
    pair_with_options::<T, { MODE_CLASSIC }>(capacity, Options::default())
}

/// Creates a new in-process broadcast queue pair with explicit options.
///
/// Values left buffered when the queue is dropped may be leaked instead of
/// having their destructors run.
pub fn pair_with_options<T: Send, const MODE: u32>(
    capacity: usize,
    options: Options,
) -> Result<(ModeProducer<T, MODE>, ModeConsumer<T, MODE>), Error> {
    let mode = validate_mode::<MODE>()?;
    let options = options.with_publication_mode(mode);
    let region_size = minimum_region_size_with_options::<T>(capacity, options);
    let region = Region::alloc(NonZeroUsize::new(region_size).ok_or(Error::InvalidBufferSize)?)?;
    // SAFETY: `region` is freshly allocated and used only for this queue.
    let header = unsafe { SharedQueueHeader::create_in_region::<T>(&region, options) }?;
    let producer = unsafe { ModeProducer::<T, MODE>::from_header(Arc::clone(&region), header) }?;
    // SAFETY: `region` is freshly allocated and the header was initialized
    // above for the same queue layout.
    let consumer_queue = unsafe { SharedQueue::from_header(region, header) }?;
    let consumer = ModeConsumer::<T, MODE>::from_queue(consumer_queue);
    Ok((producer, consumer))
}

pub fn cooperative_pair<T: Send>(
    capacity: usize,
    options: Options,
) -> Result<(CooperativeProducer<T>, CooperativeConsumer<T>), Error> {
    pair_with_options::<T, { MODE_COOPERATIVE }>(capacity, options)
}

const COOPERATIVE_DESCRIPTOR_PENDING: usize = 0;
const COOPERATIVE_DESCRIPTOR_READY: usize = 1;

#[derive(Clone, Copy)]
struct ProducerReservation {
    start: usize,
    descriptor: usize,
}

#[inline]
fn cursor_add_for_mode<const MODE: u32>(cursor: usize, increment: usize) -> usize {
    if MODE == MODE_CLASSIC {
        cursor.wrapping_add(increment)
    } else {
        cooperative_cursor_add(cursor, increment)
    }
}

#[inline]
fn cursor_sub_for_mode<const MODE: u32>(lhs: usize, rhs: usize) -> usize {
    if MODE == MODE_CLASSIC {
        lhs.wrapping_sub(rhs)
    } else {
        cooperative_cursor_sub(lhs, rhs)
    }
}

#[inline]
fn cooperative_cursor_add(cursor: usize, increment: usize) -> usize {
    (cursor as u32).wrapping_add(increment as u32) as usize
}

#[inline]
fn cooperative_cursor_sub(lhs: usize, rhs: usize) -> usize {
    (lhs as u32).wrapping_sub(rhs as u32) as usize
}

#[inline]
fn cooperative_pack_cursor_pair(item: usize, descriptor: usize) -> usize {
    ((descriptor as u32 as usize) << 32) | item as u32 as usize
}

#[inline]
fn cooperative_packed_item(packed: usize) -> usize {
    packed as u32 as usize
}

#[inline]
fn cooperative_packed_descriptor(packed: usize) -> usize {
    (packed >> 32) as u32 as usize
}

#[repr(C)]
struct CooperativeDescriptor {
    start: AtomicUsize,
    count: AtomicUsize,
    state: AtomicUsize,
}

impl CooperativeDescriptor {
    const fn new() -> Self {
        Self {
            start: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            state: AtomicUsize::new(COOPERATIVE_DESCRIPTOR_PENDING),
        }
    }

    fn initialize_reserved(&self, start: usize, count: usize) {
        // The reservation cursor gives this caller exclusive ownership of the
        // descriptor position. The publication cursor is the reuse frontier,
        // so no descriptor-local free state is needed.
        self.state
            .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
        self.start.store(start, Ordering::Relaxed);
        self.count.store(count, Ordering::Relaxed);
    }
}

/// Reads a value from `src` into `dst` with atomic byte and `u64` loads.
///
/// The source is read as a prefix of atomic `u8` loads up to the first
/// `AtomicU64`-aligned address, then as many atomic `u64` loads as possible,
/// then as trailing atomic `u8` loads. This function does not provide a single
/// atomic snapshot of `T`; a racing read may observe old bytes, new bytes, or a
/// mixed snapshot at the byte/chunk boundaries used by this copy.
/// An acquire-release fence is issued after the payload loads so the full copy
/// remains ordered before later validation or commit cursor checks.
///
/// This is intended for copying broadcast payloads before calling
/// [`DirectRead::validate`] or [`DirectRead::commit`]. Those freshness checks
/// are still required when using direct reads. See the module-level docs for
/// why payload access that may race with producers must be atomic.
///
/// # Safety
/// - Every byte in the source value must be initialized.
///   This excludes `T` values with padding unless those padding bytes are
///   guaranteed to be initialized.
/// - `src` must be valid for reads of `size_of::<T>()` bytes.
/// - `dst` must be valid for writes of `size_of::<T>()` bytes.
/// - `dst` must not overlap with `src`.
/// - `src` must be aligned for `T`; this is checked with `debug_assert!`.
/// - The source memory must be safe to access with atomic `u8` loads and
///   aligned atomic `u64` loads.
/// - Any concurrent writes to the source bytes must also use compatible atomic
///   accesses, or be protected by external synchronization.
/// - If the caller converts `dst` to `T`, it must first validate that the
///   copied bytes are not a stale or mixed snapshot, or otherwise prove that
///   the copied bytes form a valid `T`.
///
/// # Panics
/// Panics if `ordering` is not valid for atomic loads.
pub unsafe fn read_atomic_chunks<T>(src: *const T, dst: &mut MaybeUninit<T>, ordering: Ordering) {
    debug_assert_eq!((src as usize) % core::mem::align_of::<T>(), 0);
    debug_assert_eq!((dst.as_ptr() as usize) % core::mem::align_of::<T>(), 0);

    let byte_len = core::mem::size_of::<T>();
    if byte_len == 0 {
        return;
    }

    let mut src = src.cast::<u8>();
    let mut dst = dst.as_mut_ptr().cast::<u8>();

    let prefix_len = atomic_u64_prefix_len(src as usize, byte_len);
    for _ in 0..prefix_len {
        // SAFETY: Guaranteed by this function's safety contract.
        let byte = unsafe { atomic_load_u8(src, ordering) };
        // SAFETY: `dst` is valid for `byte_len` bytes and this loop remains
        // within the prefix range.
        unsafe {
            dst.write(byte);
            src = src.add(1);
            dst = dst.add(1);
        }
    }

    let mut remaining = byte_len - prefix_len;
    for _ in 0..(remaining / core::mem::size_of::<u64>()) {
        // SAFETY: Prefix handling aligned `src` for `AtomicU64`, and there is
        // a full `u64` chunk remaining.
        let chunk = unsafe { atomic_load_u64(src, ordering) };
        // SAFETY: `dst` is valid for `byte_len` bytes. It is only aligned for
        // `T`, so the chunk is written with unaligned ordinary access.
        unsafe {
            ptr::write_unaligned(dst.cast::<u64>(), chunk);
            src = src.add(core::mem::size_of::<u64>());
            dst = dst.add(core::mem::size_of::<u64>());
        }
    }

    remaining %= core::mem::size_of::<u64>();
    for _ in 0..remaining {
        // SAFETY: Guaranteed by this function's safety contract.
        let byte = unsafe { atomic_load_u8(src, ordering) };
        // SAFETY: `dst` is valid for `byte_len` bytes and this loop remains
        // within the trailing range.
        unsafe {
            dst.write(byte);
            src = src.add(1);
            dst = dst.add(1);
        }
    }

    fence(Ordering::AcqRel);
}

/// Writes a value from `src` into `dst` with atomic byte and `u64` stores.
///
/// The destination is written as a prefix of atomic `u8` stores up to the first
/// `AtomicU64`-aligned address, then as many atomic `u64` stores as possible,
/// then as trailing atomic `u8` stores. This function does not publish `T` as a
/// single atomic value; racing readers may observe old bytes, new bytes, or a
/// mixed snapshot at the byte/chunk boundaries used by this copy.
/// A release fence is issued after the payload stores so later publication
/// cannot be reordered before this copy.
///
/// This is intended for initializing broadcast payloads before the write guard
/// publishes the reserved slot. See the module-level docs for why payload
/// access that may race with consumers must be atomic.
///
/// # Safety
/// - Every byte in the source value must be initialized.
///   This excludes `T` values with padding unless those padding bytes are
///   guaranteed to be initialized.
/// - `src` must be valid for reads of `size_of::<T>()` bytes.
/// - `dst` must be valid for writes of `size_of::<T>()` bytes.
/// - `src` and `dst` must not overlap.
/// - `src` and `dst` must be aligned for `T`; this is checked with
///   `debug_assert!`.
/// - The destination memory must be safe to access with atomic `u8` stores and
///   aligned atomic `u64` stores.
/// - Overwriting `dst` must uphold any ownership and validity invariants for
///   the bytes currently stored there.
/// - Any concurrent reads from the destination bytes must also use compatible
///   atomic accesses, or be protected by external synchronization.
///
/// # Panics
/// Panics if `ordering` is not valid for atomic stores.
pub unsafe fn write_atomic_chunks<T>(dst: *mut T, src: *const T, ordering: Ordering) {
    debug_assert_eq!((dst as usize) % core::mem::align_of::<T>(), 0);
    debug_assert_eq!((src as usize) % core::mem::align_of::<T>(), 0);

    let byte_len = core::mem::size_of::<T>();
    if byte_len == 0 {
        return;
    }

    let mut dst = dst.cast::<u8>();
    let mut src = src.cast::<u8>();

    let prefix_len = atomic_u64_prefix_len(dst as usize, byte_len);
    for _ in 0..prefix_len {
        // SAFETY: `src` is valid for `byte_len` bytes and this loop remains
        // within the prefix range.
        let byte = unsafe { src.read() };
        // SAFETY: Guaranteed by this function's safety contract.
        unsafe {
            atomic_store_u8(dst, byte, ordering);
            src = src.add(1);
            dst = dst.add(1);
        }
    }

    let mut remaining = byte_len - prefix_len;
    for _ in 0..(remaining / core::mem::size_of::<u64>()) {
        // SAFETY: `src` is valid for `byte_len` bytes. It is only aligned for
        // `T`, so the chunk is read with unaligned ordinary access.
        let chunk = unsafe { ptr::read_unaligned(src.cast::<u64>()) };
        // SAFETY: Prefix handling aligned `dst` for `AtomicU64`, and there is
        // a full `u64` chunk remaining.
        unsafe {
            atomic_store_u64(dst, chunk, ordering);
            src = src.add(core::mem::size_of::<u64>());
            dst = dst.add(core::mem::size_of::<u64>());
        }
    }

    remaining %= core::mem::size_of::<u64>();
    for _ in 0..remaining {
        // SAFETY: `src` is valid for `byte_len` bytes and this loop remains
        // within the trailing range.
        let byte = unsafe { src.read() };
        // SAFETY: Guaranteed by this function's safety contract.
        unsafe {
            atomic_store_u8(dst, byte, ordering);
            src = src.add(1);
            dst = dst.add(1);
        }
    }

    fence(Ordering::Release);
}

#[inline]
fn atomic_u64_prefix_len(addr: usize, byte_len: usize) -> usize {
    let alignment = core::mem::align_of::<AtomicU64>();
    let misalignment = addr % alignment;
    if misalignment == 0 {
        0
    } else {
        (alignment - misalignment).min(byte_len)
    }
}

#[inline]
unsafe fn atomic_load_u8(src: *const u8, ordering: Ordering) -> u8 {
    // SAFETY: Caller guarantees `src` is valid for atomic byte loads.
    unsafe { AtomicU8::from_ptr(src.cast_mut()).load(ordering) }
}

#[inline]
unsafe fn atomic_load_u64(src: *const u8, ordering: Ordering) -> u64 {
    debug_assert_eq!((src as usize) % core::mem::align_of::<AtomicU64>(), 0);
    // SAFETY: Caller guarantees `src` is valid and aligned for atomic u64
    // loads.
    unsafe { AtomicU64::from_ptr(src.cast::<u64>().cast_mut()).load(ordering) }
}

#[inline]
unsafe fn atomic_store_u8(dst: *mut u8, value: u8, ordering: Ordering) {
    // SAFETY: Caller guarantees `dst` is valid for atomic byte stores.
    unsafe { AtomicU8::from_ptr(dst).store(value, ordering) };
}

#[inline]
unsafe fn atomic_store_u64(dst: *mut u8, value: u64, ordering: Ordering) {
    debug_assert_eq!((dst as usize) % core::mem::align_of::<AtomicU64>(), 0);
    // SAFETY: Caller guarantees `dst` is valid and aligned for atomic u64
    // stores.
    unsafe { AtomicU64::from_ptr(dst.cast::<u64>()).store(value, ordering) };
}

struct SharedQueue<T, const MODE: u32> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    cooperative_descriptors: Option<NonNull<CooperativeDescriptor>>,
    buffer_mask: usize,
    cooperative_descriptor_mask: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<Region>,
}

#[derive(Clone, Copy)]
struct WindowOverrun {
    skipped: usize,
    next: usize,
}

impl<T, const MODE: u32> Clone for SharedQueue<T, MODE> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            buffer: self.buffer,
            cooperative_descriptors: self.cooperative_descriptors,
            buffer_mask: self.buffer_mask,
            cooperative_descriptor_mask: self.cooperative_descriptor_mask,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T, const MODE: u32> SharedQueue<T, MODE> {
    #[inline]
    fn overrun(&self, start: usize, cursor: usize) -> usize {
        cursor_sub_for_mode::<MODE>(cursor, start).wrapping_sub(self.capacity())
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    #[inline]
    fn published(&self) -> usize {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let publication = unsafe { self.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        if MODE == MODE_CLASSIC {
            publication
        } else {
            cooperative_packed_item(publication)
        }
    }

    #[inline]
    fn reserved(&self) -> usize {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let reservation = unsafe { self.header.as_ref() }
            .producer_reservation
            .load(Ordering::Acquire);
        if MODE == MODE_CLASSIC {
            reservation
        } else {
            cooperative_packed_item(reservation)
        }
    }

    #[inline]
    fn oldest_available_at_reserved(&self, reserved: usize) -> usize {
        if MODE == MODE_CLASSIC || reserved < self.capacity() {
            reserved.saturating_sub(self.capacity())
        } else {
            cooperative_cursor_sub(reserved, self.capacity())
        }
    }

    #[inline]
    fn oldest_available(&self) -> usize {
        self.oldest_available_at_reserved(self.reserved())
    }

    #[inline]
    fn overrun_at_reserved(&self, start: usize, reserved: usize) -> WindowOverrun {
        WindowOverrun {
            skipped: self.overrun(start, reserved),
            next: self.oldest_available_at_reserved(reserved),
        }
    }

    fn reserve_write(&self) -> Option<(NonNull<T>, ProducerReservation)> {
        let reservation = self.reserve_write_batch(1)?;
        let cell_index = reservation.start & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, reservation))
    }

    fn reserve_write_batch(&self, count: usize) -> Option<ProducerReservation> {
        if count == 0 {
            return None;
        }

        let capacity = self.capacity();
        if count > capacity {
            return None;
        }

        if MODE == MODE_CLASSIC {
            self.reserve_write_batch_classic(count, capacity)
        } else {
            self.reserve_write_batch_cooperative(count, capacity)
        }
    }

    fn reserve_write_batch_classic(
        &self,
        count: usize,
        capacity: usize,
    ) -> Option<ProducerReservation> {
        debug_assert!(count != 0);
        debug_assert!(count <= capacity);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut producer_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let pending = producer_reservation.wrapping_sub(producer_publication);
            if pending > capacity - count {
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
                Ok(_) => {
                    return Some(ProducerReservation {
                        start: producer_reservation,
                        descriptor: 0,
                    });
                }
                Err(current) => {
                    producer_reservation = current;
                }
            }
        }
    }

    fn reserve_write_batch_cooperative(
        &self,
        count: usize,
        capacity: usize,
    ) -> Option<ProducerReservation> {
        debug_assert!(count != 0);
        debug_assert!(count <= capacity);

        let descriptors = self.cooperative_descriptors?;
        let descriptor_capacity = self.cooperative_descriptor_mask.wrapping_add(1);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut packed_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_reservation = cooperative_packed_item(packed_reservation);
            let descriptor_reservation = cooperative_packed_descriptor(packed_reservation);

            let packed_publication = header.producer_publication.load(Ordering::Acquire);
            let producer_publication = cooperative_packed_item(packed_publication);
            let pending = cooperative_cursor_sub(producer_reservation, producer_publication);
            if pending > capacity - count {
                core::hint::spin_loop();
                packed_reservation = header.producer_reservation.load(Ordering::Relaxed);
                continue;
            }

            let descriptor_frontier = cooperative_packed_descriptor(packed_publication);
            let used_descriptors =
                cooperative_cursor_sub(descriptor_reservation, descriptor_frontier);
            if used_descriptors >= descriptor_capacity {
                return None;
            }

            let new_reservation = cooperative_pack_cursor_pair(
                cooperative_cursor_add(producer_reservation, count),
                cooperative_cursor_add(descriptor_reservation, 1),
            );
            match header.producer_reservation.compare_exchange_weak(
                packed_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let descriptor_index =
                        descriptor_reservation & self.cooperative_descriptor_mask;
                    // SAFETY: Mask ensures the descriptor index is in bounds.
                    let descriptor = unsafe { descriptors.add(descriptor_index).as_ref() };
                    descriptor.initialize_reserved(producer_reservation, count);
                    return Some(ProducerReservation {
                        start: producer_reservation,
                        descriptor: descriptor_reservation,
                    });
                }
                Err(current) => {
                    packed_reservation = current;
                }
            }
        }
    }

    #[inline]
    fn ptr_at(&self, position: usize) -> *const T {
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        unsafe { self.buffer.add(cell_index).as_ptr() }
    }

    #[inline]
    fn validate_window(&self, start: usize, count: usize) -> Result<(), WindowOverrun> {
        debug_assert!(count <= self.capacity());
        let reserved = self.reserved();
        if cursor_sub_for_mode::<MODE>(reserved, start) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        if cursor_sub_for_mode::<MODE>(reserved, cursor_add_for_mode::<MODE>(start, count))
            > self.capacity()
        {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        Ok(())
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
        let publication_mode = validate_mode::<MODE>()?;
        if header_ref.publication_mode()? != publication_mode {
            return Err(Error::InvalidBufferSize);
        }
        let cooperative_descriptor_mask = if MODE == MODE_COOPERATIVE {
            header_ref.descriptor_mask as usize
        } else {
            0
        };
        let cooperative_descriptor_capacity = if MODE == MODE_COOPERATIVE {
            cooperative_descriptor_mask.wrapping_add(1)
        } else {
            0
        };
        if !buffer_size_in_items.is_power_of_two()
            || buffer_size_in_items == 0
            || (MODE == MODE_COOPERATIVE
                && (!cooperative_descriptor_capacity.is_power_of_two()
                    || cooperative_descriptor_capacity == 0
                    || cooperative_descriptor_capacity > buffer_size_in_items))
            || SharedQueueHeader::calculate_buffer_size_in_items_for_layout::<T>(
                region.size(),
                publication_mode,
                cooperative_descriptor_capacity,
            )? != buffer_size_in_items
        {
            return Err(Error::InvalidBufferSize);
        }

        let cooperative_descriptors = if MODE == MODE_COOPERATIVE {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - cooperative layout validation above proves the descriptor ring exists.
            Some(unsafe { SharedQueueHeader::cooperative_descriptors_from_header(header) })
        } else {
            None
        };

        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - allocation at `header` is large enough to hold the header and the buffer.
        let buffer = unsafe {
            Self::buffer_from_header(header, cooperative_descriptor_capacity, publication_mode)
        };
        Ok(Self {
            header,
            buffer,
            cooperative_descriptors,
            region,
            buffer_mask,
            cooperative_descriptor_mask,
        })
    }

    fn clear_descriptors(&self) {
        if let Some(descriptors) = self.cooperative_descriptors {
            let descriptor_capacity = self.cooperative_descriptor_mask.wrapping_add(1);
            for index in 0..descriptor_capacity {
                // SAFETY: `index < descriptor_capacity`, so the descriptor is in bounds.
                let descriptor = unsafe { descriptors.add(index).as_ref() };
                descriptor
                    .state
                    .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
            }
        }
    }

    /// Gets a pointer to the buffer following the header.
    ///
    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must be of sufficient size to hold the
    ///   header and padding bytes to align the trailing buffer of `T`.
    unsafe fn buffer_from_header(
        header: NonNull<SharedQueueHeader>,
        descriptor_capacity: usize,
        publication_mode: PublicationMode,
    ) -> NonNull<T> {
        let buffer_offset = SharedQueueHeader::buffer_offset_for_capacities::<T>(
            descriptor_capacity,
            publication_mode,
        );

        // SAFETY:
        // - buffer_offset will not overflow isize.
        // - header allocation is large enough to accommodate the alignment.
        let aligned_ptr = unsafe { header.byte_add(buffer_offset) };
        aligned_ptr.cast()
    }
}

#[repr(C)]
struct SharedQueueHeader {
    // Cold metadata cacheline.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,
    publication_mode: u32,
    descriptor_mask: u32,

    /// Producer reservation cursor.
    ///
    /// Producers atomically advance this with CAS to claim slots, but claimed
    /// writes are not visible to consumers until `producer_publication` is
    /// advanced.
    producer_reservation: CacheAlignedAtomicSize,
    /// Producer publication cursor.
    ///
    /// In classic mode this stores the item cursor. In cooperative mode this
    /// stores the packed item and descriptor publication cursors. Producers
    /// advance this in-order after filling reserved slots. Consumers use the
    /// item cursor to determine which values are currently retained in the
    /// ring.
    producer_publication: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    /// Creates and initializes a new shared queue header in `file`.
    ///
    /// # Safety
    /// - The mapping created for `file` must be used to initialize at most one
    ///   queue header.
    /// - The returned `region` must not be passed to any other queue-header
    ///   initialization routine.
    unsafe fn create<T>(
        file: &File,
        size: usize,
        options: Options,
    ) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        file.set_len(size as u64)?;

        let region = Region::map_file(file, size)?;
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let header = unsafe { Self::create_in_region::<T>(&region, options) }?;
        Ok((region, header))
    }

    /// Initializes a shared queue header in `region`.
    ///
    /// # Safety
    /// - This function must be called at most once for a given `region`.
    unsafe fn create_in_region<T>(
        region: &Arc<Region>,
        options: Options,
    ) -> Result<NonNull<Self>, Error> {
        let (buffer_size_in_items, descriptor_capacity) =
            Self::calculate_capacities_with_options::<T>(region.size(), options)?;
        let header = region.addr().cast::<Self>();
        // SAFETY: The header is non-null and aligned properly.
        //         Alignment is guaranteed because mmap ensures that the
        //         memory is aligned to the page size, which is sufficient for the
        //         alignment of `SharedQueueHeader`.
        //         Access is exclusive because the caller guarantees this region
        //         is initialized at most once.
        unsafe {
            Self::initialize(
                header,
                buffer_size_in_items,
                descriptor_capacity,
                options.publication_mode(),
            )
        };
        Ok(header)
    }

    const fn classic_buffer_offset<T>() -> usize {
        const {
            assert!(
                core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
                "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
            )
        }

        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<T>())
    }

    const fn cooperative_descriptors_offset() -> usize {
        core::mem::size_of::<Self>()
            .next_multiple_of(core::mem::align_of::<CooperativeDescriptor>())
    }

    const fn descriptor_capacity_for_options(item_capacity: usize, options: Options) -> usize {
        match options.publication_mode() {
            PublicationMode::Classic => 0,
            PublicationMode::Cooperative => {
                if item_capacity == 0 {
                    0
                } else {
                    let requested = if options.cooperative_descriptor_capacity() == 0 {
                        DEFAULT_COOPERATIVE_DESCRIPTOR_CAPACITY
                    } else {
                        normalized_capacity(options.cooperative_descriptor_capacity())
                    };
                    if requested > item_capacity {
                        item_capacity
                    } else {
                        requested
                    }
                }
            }
        }
    }

    const fn buffer_offset_for_capacities<T>(
        descriptor_capacity: usize,
        publication_mode: PublicationMode,
    ) -> usize {
        const {
            assert!(
                core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
                "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
            )
        }

        match publication_mode {
            PublicationMode::Classic => Self::classic_buffer_offset::<T>(),
            PublicationMode::Cooperative => {
                let after_descriptors = Self::cooperative_descriptors_offset()
                    + descriptor_capacity * core::mem::size_of::<CooperativeDescriptor>();
                after_descriptors.next_multiple_of(core::mem::align_of::<T>())
            }
        }
    }

    fn calculate_capacities_with_options<T>(
        file_size: usize,
        options: Options,
    ) -> Result<(usize, usize), Error> {
        match options.publication_mode() {
            PublicationMode::Classic => Ok((
                Self::calculate_buffer_size_in_items_for_layout::<T>(
                    file_size,
                    PublicationMode::Classic,
                    0,
                )?,
                0,
            )),
            PublicationMode::Cooperative => {
                let descriptors_offset = Self::cooperative_descriptors_offset();
                if file_size < descriptors_offset {
                    return Err(Error::InvalidBufferSize);
                }

                let available_for_items = file_size - descriptors_offset;
                let mut item_capacity = available_for_items / core::mem::size_of::<T>();
                item_capacity = item_capacity.min(COOPERATIVE_MAX_CAPACITY);
                if item_capacity == 0 {
                    return Err(Error::InvalidBufferSize);
                }
                if !item_capacity.is_power_of_two() {
                    item_capacity = item_capacity.next_power_of_two() >> 1;
                    if item_capacity == 0 {
                        return Err(Error::InvalidBufferSize);
                    }
                }

                while item_capacity > 0 {
                    let descriptor_capacity =
                        Self::descriptor_capacity_for_options(item_capacity, options);
                    if descriptor_capacity != 0
                        && Self::total_size_for_capacities::<T>(
                            item_capacity,
                            descriptor_capacity,
                            PublicationMode::Cooperative,
                        ) <= file_size
                    {
                        return Ok((item_capacity, descriptor_capacity));
                    }
                    item_capacity >>= 1;
                }

                return Err(Error::InvalidBufferSize);
            }
        }
    }

    fn calculate_buffer_size_in_items_for_layout<T>(
        file_size: usize,
        publication_mode: PublicationMode,
        descriptor_capacity: usize,
    ) -> Result<usize, Error> {
        const {
            assert!(
                core::mem::size_of::<T>() > 0,
                "zero-sized types are not supported"
            )
        }

        match publication_mode {
            PublicationMode::Classic => {
                let buffer_offset = Self::classic_buffer_offset::<T>();
                if file_size < buffer_offset {
                    return Err(Error::InvalidBufferSize);
                }

                // The buffer size (in units of T) must be a power of two.
                let buffer_size_in_bytes = file_size - buffer_offset;
                let mut buffer_size_in_items = buffer_size_in_bytes / core::mem::size_of::<T>();
                if !buffer_size_in_items.is_power_of_two() {
                    // If not a power of two, round down to the previous power of two.
                    buffer_size_in_items = buffer_size_in_items.next_power_of_two() >> 1;
                    if buffer_size_in_items == 0 {
                        return Err(Error::InvalidBufferSize);
                    }
                }

                Self::validate_capacity(buffer_size_in_items)?;
                Ok(buffer_size_in_items)
            }
            PublicationMode::Cooperative => {
                if descriptor_capacity == 0
                    || !descriptor_capacity.is_power_of_two()
                    || descriptor_capacity > COOPERATIVE_MAX_CAPACITY
                {
                    return Err(Error::InvalidBufferSize);
                }

                let buffer_offset = Self::buffer_offset_for_capacities::<T>(
                    descriptor_capacity,
                    PublicationMode::Cooperative,
                );
                if file_size < buffer_offset {
                    return Err(Error::InvalidBufferSize);
                }
                let mut buffer_size_in_items =
                    (file_size - buffer_offset) / core::mem::size_of::<T>();
                buffer_size_in_items = buffer_size_in_items.min(COOPERATIVE_MAX_CAPACITY);
                if buffer_size_in_items == 0 {
                    return Err(Error::InvalidBufferSize);
                }
                if !buffer_size_in_items.is_power_of_two() {
                    buffer_size_in_items = buffer_size_in_items.next_power_of_two() >> 1;
                    if buffer_size_in_items == 0 {
                        return Err(Error::InvalidBufferSize);
                    }
                }

                if buffer_size_in_items < descriptor_capacity {
                    return Err(Error::InvalidBufferSize);
                }

                Self::validate_capacity(buffer_size_in_items)?;
                Ok(buffer_size_in_items)
            }
        }
    }

    const fn total_size_for_capacities<T>(
        item_capacity: usize,
        descriptor_capacity: usize,
        publication_mode: PublicationMode,
    ) -> usize {
        Self::buffer_offset_for_capacities::<T>(descriptor_capacity, publication_mode)
            + item_capacity * core::mem::size_of::<T>()
    }

    fn validate_capacity(buffer_size_in_items: usize) -> Result<(), Error> {
        // The buffer mask is stored as u32, so the capacity must fit.
        if buffer_size_in_items > u32::MAX as usize + 1 {
            return Err(Error::InvalidBufferSize);
        }
        Ok(())
    }

    /// Initializes the shared queue header.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `header` allocation must be large enough to hold the header and the buffer.
    /// - `access` to `header` must be unique when this is called.
    unsafe fn initialize(
        mut header_ptr: NonNull<Self>,
        buffer_size_in_items: usize,
        descriptor_capacity: usize,
        publication_mode: PublicationMode,
    ) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header_ptr.as_mut() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);
        header.buffer_mask = u32::try_from(buffer_size_in_items - 1).unwrap();
        header.publication_mode = publication_mode.as_u32();
        header.descriptor_mask = if descriptor_capacity == 0 {
            0
        } else {
            u32::try_from(descriptor_capacity - 1).unwrap()
        };
        header.version = VERSION;

        if publication_mode == PublicationMode::Cooperative {
            // SAFETY: The cooperative layout reserves exactly
            // `descriptor_capacity` descriptor entries after the header.
            let descriptors = unsafe { Self::cooperative_descriptors_from_header(header_ptr) };
            for index in 0..descriptor_capacity {
                // SAFETY: `index < descriptor_capacity`, so the descriptor is in bounds.
                unsafe {
                    descriptors
                        .add(index)
                        .as_ptr()
                        .write(CooperativeDescriptor::new())
                };
            }
        }

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
            // SAFETY: The header is non-null and aligned properly.
            //         Alignment is guaranteed because mmap ensures that the
            //         memory is aligned to the page size, which is sufficient for the
            //         alignment of `SharedQueueHeader`.
            let header = unsafe { header.as_ref() };
            if header.magic.load(Ordering::Acquire) != MAGIC {
                return Err(Error::InvalidMagic);
            }
            if header.version != VERSION {
                return Err(Error::InvalidVersion {
                    expected: VERSION,
                    actual: header.version,
                });
            }
            let publication_mode = header.publication_mode()?;
            let buffer_size_in_items = (header.buffer_mask as usize).wrapping_add(1);
            let descriptor_capacity = if publication_mode == PublicationMode::Cooperative {
                (header.descriptor_mask as usize).wrapping_add(1)
            } else {
                0
            };
            if buffer_size_in_items
                != Self::calculate_buffer_size_in_items_for_layout::<T>(
                    file_size,
                    publication_mode,
                    descriptor_capacity,
                )?
            {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((region, header))
    }

    fn publication_mode(&self) -> Result<PublicationMode, Error> {
        PublicationMode::from_u32(self.publication_mode).ok_or(Error::InvalidBufferSize)
    }

    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must include the cooperative descriptor ring.
    unsafe fn cooperative_descriptors_from_header(
        header: NonNull<SharedQueueHeader>,
    ) -> NonNull<CooperativeDescriptor> {
        let descriptors_offset = Self::cooperative_descriptors_offset();

        // SAFETY:
        // - descriptors_offset will not overflow isize.
        // - header allocation is large enough to hold the descriptor ring.
        let descriptors = unsafe { header.byte_add(descriptors_offset) };
        descriptors.cast()
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this producer.
    unsafe fn publish_producer_publication(header_ptr: NonNull<Self>, start: usize, count: usize) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.producer_publication.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .producer_publication
            .store(start.wrapping_add(count), Ordering::Release);
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this producer.
    unsafe fn publish_producer_publication_cooperative(
        header_ptr: NonNull<Self>,
        start: usize,
        count: usize,
        descriptor_position: usize,
    ) {
        // SAFETY: `header_ptr` points to a cooperative queue header.
        let descriptor =
            unsafe { Self::cooperative_descriptor_at(header_ptr, descriptor_position).as_ref() };
        debug_assert_eq!(descriptor.start.load(Ordering::Acquire), start);
        debug_assert_eq!(descriptor.count.load(Ordering::Acquire), count);
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);

        // SAFETY: `header_ptr` points to a cooperative queue header.
        unsafe { Self::try_publish_cooperative(header_ptr) };
    }

    /// # Safety
    /// - `header_ptr` must point to a cooperative queue header.
    unsafe fn try_publish_cooperative(header_ptr: NonNull<Self>) -> usize {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        let publication = header.producer_publication.load(Ordering::Acquire);

        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::try_publish_cooperative_from(header_ptr, publication) }
    }

    /// # Safety
    /// - `header_ptr` must point to a cooperative queue header.
    unsafe fn try_publish_cooperative_from(
        header_ptr: NonNull<Self>,
        mut publication: usize,
    ) -> usize {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        let mut descriptors_published = 0usize;

        loop {
            let descriptor_position = cooperative_packed_descriptor(publication);
            let frontier = cooperative_packed_item(publication);

            // SAFETY: `header_ptr` points to a cooperative queue header.
            let descriptor = unsafe {
                Self::cooperative_descriptor_at(header_ptr, descriptor_position).as_ref()
            };
            if descriptor
                .state
                .compare_exchange(
                    COOPERATIVE_DESCRIPTOR_READY,
                    COOPERATIVE_DESCRIPTOR_PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                let refreshed_publication = header.producer_publication.load(Ordering::Acquire);
                if refreshed_publication != publication {
                    publication = refreshed_publication;
                    continue;
                }
                break;
            }

            let start = descriptor.start.load(Ordering::Acquire);
            let count = descriptor.count.load(Ordering::Acquire);
            if start != frontier || count == 0 {
                descriptor
                    .state
                    .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
                let refreshed_publication = header.producer_publication.load(Ordering::Acquire);
                if refreshed_publication != publication {
                    publication = refreshed_publication;
                    continue;
                }
                break;
            }

            let next = cooperative_cursor_add(frontier, count);
            let next_descriptor_position = cooperative_cursor_add(descriptor_position, 1);
            let next_publication = cooperative_pack_cursor_pair(next, next_descriptor_position);
            header
                .producer_publication
                .store(next_publication, Ordering::Release);
            descriptors_published = descriptors_published.wrapping_add(1);
            publication = next_publication;
        }
        descriptors_published
    }

    /// # Safety
    /// - `header` must point to a cooperative queue header.
    unsafe fn cooperative_descriptor_at(
        header: NonNull<SharedQueueHeader>,
        descriptor_position: usize,
    ) -> NonNull<CooperativeDescriptor> {
        // SAFETY: `header` points to a cooperative queue header.
        let descriptors = unsafe { Self::cooperative_descriptors_from_header(header) };
        // SAFETY: `header` is a valid shared-memory header.
        let header_ref = unsafe { header.as_ref() };
        let index = descriptor_position & header_ref.descriptor_mask as usize;
        // SAFETY: Mask ensures the descriptor index is in bounds.
        unsafe { descriptors.add(index) }
    }
}

#[must_use]
pub struct WriteGuard<'a, T, const MODE: u32 = MODE_CLASSIC> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    descriptor: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T, const MODE: u32> WriteGuard<'a, T, MODE> {
    /// Returns a raw write pointer to the reserved slot.
    ///
    /// Prefer [`Self::write_atomic`] unless the caller needs custom in-place
    /// initialization.
    ///
    /// # Safety
    /// Before the guard is dropped, the reserved slot must contain bytes that
    /// are valid to publish as `T`. Payload access that may race with consumers
    /// must use atomics or external synchronization.
    pub unsafe fn as_mut_ptr(&mut self) -> *mut T {
        self.cell.as_ptr()
    }

    /// Writes a value into the reserved slot with atomic chunk stores.
    ///
    /// `ordering` applies to the payload stores only; queue publication still
    /// uses the queue's internal release ordering.
    ///
    /// # Panics
    /// Panics if `ordering` is not valid for atomic stores. If this happens,
    /// the reserved slot is not published, matching a forgotten guard.
    pub fn write_atomic(self, value: T, ordering: Ordering) {
        let mut this = ManuallyDrop::new(self);

        // SAFETY: This guard owns the reserved slot, and `Producer`
        // construction establishes the broadcast payload contract for
        // high-level writes.
        unsafe { write_atomic_chunks(this.cell.as_ptr(), &value, ordering) };
        core::mem::forget(value);

        // SAFETY: The slot was initialized above, so it can be published.
        unsafe { ManuallyDrop::drop(&mut this) };
    }

    /// Writes a value into the reserved slot with relaxed atomic chunk stores.
    ///
    /// Uses relaxed payload stores; prefer [`Self::write_atomic`] when choosing
    /// an explicit ordering.
    ///
    /// # Safety
    /// No additional requirements.
    pub unsafe fn write(self, value: T) {
        self.write_atomic(value, Ordering::Relaxed);
    }
}

impl<'a, T, const MODE: u32> Drop for WriteGuard<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_producer_publication(self.header, self.start, 1);
            } else {
                SharedQueueHeader::publish_producer_publication_cooperative(
                    self.header,
                    self.start,
                    1,
                    self.descriptor,
                );
            }
        }
    }
}

#[must_use]
pub struct WriteBatch<'a, T, const MODE: u32 = MODE_CLASSIC> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    descriptor: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T, const MODE: u32> WriteBatch<'a, T, MODE> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a raw write pointer to the reserved slot.
    ///
    /// Prefer [`Self::write_atomic`] unless the caller needs custom in-place
    /// initialization.
    ///
    /// # Safety
    /// - `index < count`
    /// - Before the batch is dropped, the reserved slot must contain bytes that
    ///   are valid to publish as `T`.
    /// - Payload access that may race with consumers must use atomics or
    ///   external synchronization.
    pub unsafe fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = cursor_add_for_mode::<MODE>(self.start, index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Writes a value into the slot at `index` with atomic chunk stores.
    ///
    /// `ordering` applies to the payload stores only; queue publication still
    /// uses the queue's internal release ordering when the batch is dropped.
    ///
    /// # Safety
    /// - `index < count`
    ///
    /// # Panics
    /// Panics if `ordering` is not valid for atomic stores.
    pub unsafe fn write_atomic(&mut self, index: usize, value: T, ordering: Ordering) {
        debug_assert!(index < self.count);
        let position = cursor_add_for_mode::<MODE>(self.start, index);
        // SAFETY: The position was reserved for writing, and `Producer`
        // construction establishes the broadcast payload contract for
        // high-level writes.
        unsafe {
            write_atomic_chunks(
                self.buffer.add(position & self.buffer_mask).as_ptr(),
                &value,
                ordering,
            )
        };
        core::mem::forget(value);
    }

    /// Writes a value into the slot at index with relaxed atomic chunk stores.
    ///
    /// Uses relaxed payload stores; prefer [`Self::write_atomic`] when choosing
    /// an explicit ordering.
    ///
    /// # Safety
    /// - `index < count`
    pub unsafe fn write(&mut self, index: usize, value: T) {
        // SAFETY: Caller upholds the index requirement.
        unsafe { self.write_atomic(index, value, Ordering::Relaxed) }
    }
}

impl<'a, T, const MODE: u32> Drop for WriteBatch<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_producer_publication(
                    self.header,
                    self.start,
                    self.count,
                );
            } else {
                SharedQueueHeader::publish_producer_publication_cooperative(
                    self.header,
                    self.start,
                    self.count,
                    self.descriptor,
                );
            }
        }
    }
}

#[must_use]
pub struct DirectRead<'a, T, const MODE: u32 = MODE_CLASSIC> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T, MODE>,
    start: usize,
}

impl<'a, T, const MODE: u32> DirectRead<'a, T, MODE> {
    /// Returns a raw pointer into the broadcast ring.
    ///
    /// Producers may overwrite this slot concurrently. Payload access that may
    /// race with producers must use atomics or external synchronization. Copy
    /// or inspect the payload before calling [`Self::validate`] or
    /// [`Self::commit`]. Validation is not a pin.
    pub fn as_ptr(&self) -> *const T {
        self.queue.ptr_at(self.start)
    }

    /// Copies the pointed-to slot with atomic chunk loads.
    ///
    /// This reads the snapshot before validation. Call [`Self::validate`] or
    /// [`Self::commit`] after reading, and discard the returned bytes if
    /// validation reports a possible overwrite.
    ///
    /// The returned bytes are intentionally [`MaybeUninit<T>`]. Only call
    /// [`MaybeUninit::assume_init`] after validation succeeds, unless another
    /// synchronization scheme proves the bytes form a valid `T`.
    ///
    /// # Panics
    /// Panics if `ordering` is not valid for atomic loads.
    pub fn read_atomic(&self, ordering: Ordering) -> MaybeUninit<T> {
        let mut item = MaybeUninit::<T>::uninit();
        // SAFETY: This guard points at a retained slot for `T`; `Consumer`
        // construction establishes the broadcast payload contract for
        // atomic payload reads.
        unsafe { read_atomic_chunks(self.as_ptr(), &mut item, ordering) };
        item
    }

    /// Checks whether the pointed-to slot has remained retained since the
    /// guard was created.
    ///
    /// This is only a freshness check for a snapshot already read from
    /// [`Self::as_ptr`]. It does not pin the slot. A failed validation means
    /// the caller must discard that snapshot.
    pub fn validate(&self) -> Result<(), usize> {
        self.queue
            .validate_window(self.start, 1)
            .map_err(|overrun| overrun.skipped)
    }

    /// Validates the slot and advances the consumer cursor.
    ///
    /// This is only a freshness check for a snapshot already read from
    /// [`Self::as_ptr`]. It does not pin the slot after advancing the cursor.
    /// A failed validation means the caller must discard that snapshot; on
    /// failure this still repositions the consumer cursor to the oldest item
    /// currently available.
    ///
    /// Dropping a direct read guard without calling `commit` leaves the consumer
    /// cursor unchanged.
    pub fn commit(self) -> Result<(), usize> {
        match self.queue.validate_window(self.start, 1) {
            Ok(()) => {
                *self.next = cursor_add_for_mode::<MODE>(self.start, 1);
                Ok(())
            }
            Err(overrun) => {
                *self.next = overrun.next;
                Err(overrun.skipped)
            }
        }
    }
}

#[must_use]
pub struct DirectReadBatch<'a, T, const MODE: u32 = MODE_CLASSIC> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T, MODE>,
    start: usize,
    count: usize,
}

impl<'a, T, const MODE: u32> DirectReadBatch<'a, T, MODE> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a pointer into the broadcast ring.
    ///
    /// Producers may overwrite this slot concurrently. Payload access that may
    /// race with producers must use atomics or external synchronization. Copy
    /// or inspect the payload before calling [`Self::validate`] or
    /// [`Self::commit`]. Validation is not a pin.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        self.queue
            .ptr_at(cursor_add_for_mode::<MODE>(self.start, index))
    }

    /// Copies the slot at `index` with atomic chunk loads.
    ///
    /// This reads the snapshot before validation. Call [`Self::validate`] or
    /// [`Self::commit`] after reading, and discard the returned bytes if
    /// validation reports a possible overwrite.
    ///
    /// The returned bytes are intentionally [`MaybeUninit<T>`]. Only call
    /// [`MaybeUninit::assume_init`] after validation succeeds, unless another
    /// synchronization scheme proves the bytes form a valid `T`.
    ///
    /// # Panics
    /// Panics if `index >= len` or if `ordering` is not valid for atomic loads.
    pub fn read_atomic(&self, index: usize, ordering: Ordering) -> MaybeUninit<T> {
        assert!(index < self.count);
        let mut item = MaybeUninit::<T>::uninit();
        // SAFETY: The index was checked above, and this guard points at a
        // retained slot for `T`; `Consumer` construction establishes the
        // broadcast payload contract for atomic payload reads.
        unsafe { read_atomic_chunks(self.as_ptr(index), &mut item, ordering) };
        item
    }

    /// Checks whether the pointed-to slots have remained retained since the
    /// guard was created.
    ///
    /// This is only a freshness check for snapshots already read from
    /// [`Self::as_ptr`]. It does not pin the slots. A failed validation means
    /// the caller must discard those snapshots.
    pub fn validate(&self) -> Result<(), usize> {
        self.queue
            .validate_window(self.start, self.count)
            .map_err(|overrun| overrun.skipped)
    }

    /// Validates the slots and advances the consumer cursor.
    ///
    /// This is only a freshness check for snapshots already read from
    /// [`Self::as_ptr`]. It does not pin the slots after advancing the cursor.
    /// A failed validation means the caller must discard those snapshots; on
    /// failure this still repositions the consumer cursor to the oldest item
    /// currently available.
    ///
    /// Dropping a direct read batch without calling `commit` leaves the consumer
    /// cursor unchanged.
    pub fn commit(self) -> Result<(), usize> {
        match self.queue.validate_window(self.start, self.count) {
            Ok(()) => {
                *self.next = cursor_add_for_mode::<MODE>(self.start, self.count);
                Ok(())
            }
            Err(overrun) => {
                *self.next = overrun.next;
                Err(overrun.skipped)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;

    type Item = u64;
    const BUFFER_CAPACITY: usize = 8;
    const BUFFER_SIZE: usize = minimum_file_size::<Item>(BUFFER_CAPACITY);
    const COOPERATIVE_OPTIONS: Options = Options::new();
    const COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS: Options =
        Options::new().with_cooperative_descriptor_capacity(2);
    const COOPERATIVE_BUFFER_SIZE: usize = minimum_file_size_for_mode::<Item, { MODE_COOPERATIVE }>(
        BUFFER_CAPACITY,
        COOPERATIVE_OPTIONS,
    );
    const COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE: usize =
        minimum_file_size_for_mode::<Item, { MODE_COOPERATIVE }>(
            BUFFER_CAPACITY,
            COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
        );

    fn create_test_queue<T>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        create_test_queue_with_options::<T, { MODE_CLASSIC }>(file_size, Options::default())
    }

    fn create_test_queue_with_options<T, const MODE: u32>(
        file_size: usize,
        options: Options,
    ) -> (File, ModeProducer<T, MODE>, ModeConsumer<T, MODE>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { ModeProducer::<T, MODE>::create_with_options(&file, file_size, options) }
                .expect("Failed to create producer");
        let consumer =
            unsafe { ModeConsumer::<T, MODE>::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    unsafe fn write_item<T, const MODE: u32>(producer: &ModeProducer<T, MODE>, item: T) {
        assert!(producer.try_write(item, Ordering::Relaxed).is_ok());
    }

    unsafe fn read_item<T, const MODE: u32>(
        consumer: &mut ModeConsumer<T, MODE>,
    ) -> Result<Option<T>, usize> {
        consumer.try_read(Ordering::Relaxed)
    }

    #[derive(Debug, PartialEq)]
    struct NonCopy(u64);

    fn patterned_bytes<const N: usize>() -> [u8; N] {
        core::array::from_fn(|index| (index as u8).wrapping_mul(37).wrapping_add(11))
    }

    #[test]
    fn test_read_atomic_chunks_copies_aligned_source() {
        const LEN: usize = 17;
        let expected = patterned_bytes::<LEN>();
        let src = expected;
        let mut dst = MaybeUninit::<[u8; LEN]>::uninit();

        // SAFETY: The test payload is POD without padding, all bytes are
        // initialized, `src` is aligned for `[u8; LEN]`, and there are no
        // concurrent accesses.
        unsafe { read_atomic_chunks(&src, &mut dst, Ordering::Relaxed) };

        // SAFETY: `read_atomic_chunks` initialized all bytes of `[u8; LEN]`.
        assert_eq!(unsafe { dst.assume_init() }, expected);
    }

    #[test]
    fn test_write_atomic_chunks_copies_aligned_destination() {
        const LEN: usize = 19;
        let expected = patterned_bytes::<LEN>();
        let mut dst = [0u8; LEN];

        // SAFETY: The test payload is POD without padding, all bytes are
        // initialized, `dst` is aligned for `[u8; LEN]`, and there are no
        // concurrent accesses.
        unsafe { write_atomic_chunks(&mut dst, &expected, Ordering::Relaxed) };

        assert_eq!(dst, expected);
    }

    #[test]
    fn test_atomic_chunks_copy_u64_aligned_payloads() {
        let expected = [
            0x1020_3040_5060_7080,
            0x90a0_b0c0_d0e0_f001,
            0x2345_6789_abcd_ef01,
        ];
        let mut read_dst = MaybeUninit::<[u64; 3]>::uninit();

        // SAFETY: `[u64; 3]` is POD without padding, all bytes are
        // initialized, and there are no concurrent accesses.
        unsafe { read_atomic_chunks(&expected, &mut read_dst, Ordering::Relaxed) };

        // SAFETY: `read_atomic_chunks` initialized all bytes of `[u64; 3]`.
        assert_eq!(unsafe { read_dst.assume_init() }, expected);

        let mut written = [0u64; 3];
        // SAFETY: `[u64; 3]` is POD without padding, all bytes are
        // initialized, and there are no concurrent accesses.
        unsafe { write_atomic_chunks(&mut written, &expected, Ordering::Relaxed) };
        assert_eq!(written, expected);
    }

    #[test]
    fn test_atomic_chunks_copy_small_payloads() {
        let expected = [9u8, 8, 7];
        let mut read_dst = MaybeUninit::<[u8; 3]>::uninit();

        // SAFETY: `[u8; 3]` is POD without padding, all bytes are initialized,
        // and there are no concurrent accesses.
        unsafe { read_atomic_chunks(&expected, &mut read_dst, Ordering::Relaxed) };

        // SAFETY: `read_atomic_chunks` initialized all bytes of `[u8; 3]`.
        assert_eq!(unsafe { read_dst.assume_init() }, expected);

        let mut written = [0u8; 3];
        // SAFETY: `[u8; 3]` is POD without padding, all bytes are initialized,
        // and there are no concurrent accesses.
        unsafe { write_atomic_chunks(&mut written, &expected, Ordering::Relaxed) };
        assert_eq!(written, expected);
    }

    #[test]
    #[should_panic]
    fn test_read_atomic_chunks_panics_on_invalid_load_ordering() {
        let src = [1u64];
        let mut dst = MaybeUninit::<[u64; 1]>::uninit();

        // SAFETY: The pointers and payload satisfy this function's safety
        // contract; the invalid load ordering is expected to panic.
        unsafe { read_atomic_chunks(&src, &mut dst, Ordering::Release) };
    }

    #[test]
    #[should_panic]
    fn test_write_atomic_chunks_panics_on_invalid_store_ordering() {
        let src = [1u64];
        let mut dst = [0u64];

        // SAFETY: The pointers and payload satisfy this function's safety
        // contract; the invalid store ordering is expected to panic.
        unsafe { write_atomic_chunks(&mut dst, &src, Ordering::Acquire) };
    }

    #[test]
    fn test_producer_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..BUFFER_CAPACITY {
            unsafe { write_item(&producer, i as Item) };
        }

        for i in 0..BUFFER_CAPACITY {
            assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(i as Item)));
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_reserve_batch_and_read_items() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = unsafe { producer.reserve_write_batch(4) }.expect("reserve_batch failed");
        for index in 0..batch.len() {
            unsafe { batch.write(index, index as u64) };
        }
        drop(batch);

        for expected in 0..4 {
            assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(expected)));
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_multiple_consumers_receive_all_values() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = consumer.clone();

        for i in 0..4 {
            unsafe { write_item(&producer, i) };
        }

        let mut values1 = Vec::new();
        let mut values2 = Vec::new();
        while let Some(v) = unsafe { read_item(&mut consumer) }.unwrap() {
            values1.push(v);
        }
        while let Some(v) = unsafe { read_item(&mut consumer2) }.unwrap() {
            values2.push(v);
        }

        assert_eq!(values1, vec![0, 1, 2, 3]);
        assert_eq!(values2, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_overrun_repositions_consumer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..(BUFFER_CAPACITY as u64 + 2) {
            unsafe { write_item(&producer, i) };
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Err(2));
        for expected in 2..(BUFFER_CAPACITY as u64 + 2) {
            assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(expected)));
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_overrun_repositions_past_pending_overwrites() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..(BUFFER_CAPACITY as u64 + 1) {
            unsafe { write_item(&producer, i) };
        }

        let batch = unsafe { producer.reserve_write_batch(1) }.expect("reserve overwrite");

        assert_eq!(unsafe { read_item(&mut consumer) }, Err(2));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(2)));

        core::mem::forget(batch);
    }

    #[test]
    fn test_sync_modes() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            unsafe { write_item(&producer, i) };
        }

        assert_eq!(consumer.sync_to_latest(), 4);
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));

        for i in 4..8 {
            unsafe { write_item(&producer, i) };
        }

        consumer.sync_to_oldest();
        for expected in 0..8 {
            assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(expected)));
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_try_read_direct_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe { write_item(&producer, 42) };

        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        unsafe {
            assert_eq!(*direct.as_ptr(), 42);
        }
        assert_eq!(direct.validate(), Ok(()));
        assert_eq!(direct.commit(), Ok(()));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_direct_read_read_atomic_copies_payload() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe { write_item(&producer, 42) };

        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        let value = direct.read_atomic(Ordering::Relaxed);
        assert_eq!(direct.commit(), Ok(()));
        // SAFETY: `commit` validated that the copied snapshot was still
        // retained.
        assert_eq!(unsafe { value.assume_init() }, 42);
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_non_copy_items_can_be_read_directly() {
        let buffer_size = minimum_file_size::<NonCopy>(BUFFER_CAPACITY);
        let (_file, producer, mut consumer) = create_test_queue::<NonCopy>(buffer_size);

        unsafe { write_item(&producer, NonCopy(42)) };

        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        let value = unsafe { direct.as_ptr().read() };
        assert_eq!(value, NonCopy(42));
        assert_eq!(direct.commit(), Ok(()));
    }

    #[test]
    fn test_try_read_direct_batch_reads_without_copy() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            unsafe { write_item(&producer, i) };
        }

        let direct = unsafe { consumer.try_read_direct_batch(8) }
            .unwrap()
            .unwrap();
        assert_eq!(direct.len(), 4);
        for index in 0..direct.len() {
            unsafe {
                assert_eq!(*direct.as_ptr(index), index as u64);
            }
        }
        assert_eq!(direct.validate(), Ok(()));
        assert_eq!(direct.commit(), Ok(()));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_direct_read_batch_read_atomic_copies_payloads() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            unsafe { write_item(&producer, i) };
        }

        let direct = unsafe { consumer.try_read_direct_batch(8) }
            .unwrap()
            .unwrap();
        assert_eq!(direct.len(), 4);
        let mut values = Vec::with_capacity(direct.len());
        for index in 0..direct.len() {
            values.push(direct.read_atomic(index, Ordering::Relaxed));
        }
        assert_eq!(direct.commit(), Ok(()));
        for (index, value) in values.into_iter().enumerate() {
            // SAFETY: `commit` validated that the copied snapshots were still
            // retained.
            assert_eq!(unsafe { value.assume_init() }, index as u64);
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_try_read_direct_detects_overrun_after_access() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe { write_item(&producer, 1) };
        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        unsafe {
            let _ = *direct.as_ptr();
        }

        for i in 0..BUFFER_CAPACITY as u64 {
            unsafe { write_item(&producer, 10 + i) };
        }

        assert_eq!(direct.validate(), Err(1));
        assert_eq!(direct.commit(), Err(1));
    }

    #[test]
    fn test_read_detects_slot_reserved_for_overwrite() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe { write_item(&producer, 1) };
        let batch = unsafe { producer.reserve_write_batch(BUFFER_CAPACITY) }
            .expect("reserve wraparound batch");

        assert_eq!(unsafe { read_item(&mut consumer) }, Err(1));
        core::mem::forget(batch);
    }

    #[test]
    fn test_direct_read_detects_slot_reserved_for_overwrite() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe { write_item(&producer, 1) };
        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        let batch = unsafe { producer.reserve_write_batch(BUFFER_CAPACITY) }
            .expect("reserve wraparound batch");

        assert_eq!(direct.validate(), Err(1));
        assert_eq!(direct.commit(), Err(1));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        core::mem::forget(batch);
    }

    #[test]
    fn test_direct_read_batch_commit_repositions_after_overrun() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for item in 0..4 {
            unsafe { write_item(&producer, item) };
        }

        let direct = unsafe { consumer.try_read_direct_batch(4) }
            .unwrap()
            .unwrap();
        let batch = unsafe { producer.reserve_write_batch(BUFFER_CAPACITY) }
            .expect("reserve wraparound batch");

        assert_eq!(direct.commit(), Err(4));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        core::mem::forget(batch);
    }

    #[test]
    fn test_producer_recover_as_exclusive() {
        let (file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for item in 0..BUFFER_CAPACITY as Item {
            unsafe { write_item(&producer, item) };
        }

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve write");
        unsafe {
            *guard.as_mut_ptr() = 99;
        }
        core::mem::forget(guard);

        unsafe {
            producer.recover_as_exclusive();
        }
        consumer.sync_to_latest();
        let mut joined = unsafe { Consumer::<Item>::join(&file) }.expect("join after recovery");

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        assert_eq!(unsafe { read_item(&mut joined) }, Ok(None));
        unsafe { write_item(&producer, 2) };

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(2)));
        assert_eq!(unsafe { read_item(&mut joined) }, Ok(Some(2)));
    }

    #[test]
    fn test_join_consumer_starts_at_latest_publication() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");

        for i in 0..(BUFFER_CAPACITY as u64 + 3) {
            unsafe { write_item(&producer, i) };
        }

        let mut consumer = unsafe { Consumer::<Item>::join(&file) }.expect("join failed");
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));

        unsafe { write_item(&producer, 99) };
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(99)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
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
    fn test_clone_producer() {
        let (_file, producer, mut consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        unsafe { write_item(&producer, 10) };
        unsafe { write_item(&producer2, 20) };

        let mut values = Vec::new();
        while let Some(v) = unsafe { read_item(&mut consumer) }.unwrap() {
            values.push(v);
        }
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_cross_role_joins() {
        let (_file, producer1, mut consumer1) = create_test_queue::<Item>(BUFFER_SIZE);
        let mut consumer2 = producer1.join_as_consumer();
        let producer2 = consumer2.join_as_producer();

        unsafe { write_item(&producer1, 100) };
        unsafe { write_item(&producer2, 200) };

        assert_eq!(unsafe { read_item(&mut consumer1) }.unwrap(), Some(100));
        assert_eq!(unsafe { read_item(&mut consumer1) }.unwrap(), Some(200));
        assert_eq!(unsafe { read_item(&mut consumer2) }.unwrap(), Some(100));
        assert_eq!(unsafe { read_item(&mut consumer2) }.unwrap(), Some(200));
    }

    #[test]
    fn test_minimum_file_size_rounds_up_capacity() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe { Producer::<u64>::create(&file, minimum_file_size::<u64>(3)) }
            .expect("create failed");
        let consumer = unsafe { Consumer::<u64>::join(&file) }.expect("join failed");

        assert_eq!(producer.queue.capacity(), 4);
        assert_eq!(consumer.queue.capacity(), 4);
    }

    #[test]
    fn test_cooperative_minimum_file_size_rounds_up_capacity() {
        let file = create_temp_shmem_file().unwrap();
        let options = COOPERATIVE_OPTIONS;
        let producer = unsafe {
            CooperativeProducer::<u64>::create_with_options(
                &file,
                minimum_file_size_for_mode::<u64, { MODE_COOPERATIVE }>(3, options),
                options,
            )
        }
        .expect("create failed");
        let consumer = unsafe { CooperativeConsumer::<u64>::join(&file) }.expect("join failed");

        assert_eq!(producer.queue.capacity(), 4);
        assert_eq!(consumer.queue.capacity(), 4);
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .publication_mode()
                .unwrap(),
            PublicationMode::Cooperative
        );
    }

    #[test]
    fn test_cooperative_descriptor_capacity_is_configurable() {
        let file = create_temp_shmem_file().unwrap();
        let producer = unsafe {
            CooperativeProducer::<u64>::create_with_options(
                &file,
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            )
        }
        .expect("create failed");
        let header = unsafe { producer.queue.header.as_ref() };

        assert_eq!(producer.queue.capacity(), BUFFER_CAPACITY);
        assert_eq!(producer.queue.cooperative_descriptor_mask + 1, 2);
        assert_eq!(header.descriptor_mask + 1, 2);
    }

    #[test]
    fn test_cooperative_later_producer_drop_does_not_wait() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        let first = unsafe { producer.reserve_write() }.expect("reserve first");
        let second = unsafe { producer.reserve_write() }.expect("reserve second");

        unsafe {
            second.write(20);
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .producer_publication
                .load(Ordering::Acquire),
            0
        );

        unsafe {
            first.write(10);
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(10)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(20)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_cooperative_publishes_ready_batch_prefix() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        let mut first = unsafe { producer.reserve_write_batch(2) }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write_batch(3) }.expect("reserve second");
        let third = unsafe { producer.reserve_write() }.expect("reserve third");

        for (index, value) in [20, 21, 22].into_iter().enumerate() {
            unsafe {
                second.write(index, value);
            }
        }
        drop(second);

        unsafe {
            third.write(30);
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));

        for (index, value) in [10, 11].into_iter().enumerate() {
            unsafe {
                first.write(index, value);
            }
        }
        drop(first);

        let mut values = Vec::new();
        while let Some(value) = unsafe { read_item(&mut consumer) }.unwrap() {
            values.push(value);
        }
        assert_eq!(values, vec![10, 11, 20, 21, 22, 30]);
    }

    #[test]
    fn test_cooperative_publication_packs_item_and_descriptor_cursor() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        producer
            .try_write(10, Ordering::Relaxed)
            .expect("write first");
        producer
            .try_write(20, Ordering::Relaxed)
            .expect("write second");

        let publication = unsafe { producer.queue.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(publication), 2);
        assert_eq!(cooperative_packed_descriptor(publication), 2);
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(10)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(20)));
    }

    #[test]
    fn test_cooperative_publish_retries_after_stale_packed_cursor() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        let first = unsafe { producer.reserve_write() }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");
        unsafe {
            first.write(10);
        }

        let header = producer.queue.header;
        let second_descriptor_position = second.descriptor;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_descriptor_at(header, second_descriptor_position)
                .as_ref()
        };
        unsafe { second.as_mut_ptr().write(20) };

        let current_publication = unsafe { header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(current_publication), 1);
        assert_eq!(cooperative_packed_descriptor(current_publication), 1);

        // Simulate a helper that loaded the old packed cursor before the first
        // descriptor was published. The old descriptor is no longer ready, so
        // the CAS failure path must refresh and retry instead of stopping.
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
        core::mem::forget(second);
        let published = unsafe {
            SharedQueueHeader::try_publish_cooperative_from(
                header,
                cooperative_pack_cursor_pair(0, 0),
            )
        };

        assert_eq!(published, 1);
        let publication = unsafe { header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(publication), 2);
        assert_eq!(cooperative_packed_descriptor(publication), 2);
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(10)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(20)));
    }

    #[test]
    fn test_cooperative_descriptor_exhaustion_returns_none() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            );

        let first = unsafe { producer.reserve_write() }.expect("reserve first");
        let second = unsafe { producer.reserve_write() }.expect("reserve second");
        assert!(unsafe { producer.reserve_write() }.is_none());

        unsafe {
            first.write(10);
            second.write(20);
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(10)));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(20)));

        let third = unsafe { producer.reserve_write() }.expect("descriptor reused");
        unsafe {
            third.write(30);
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(30)));
    }

    #[test]
    fn test_cooperative_direct_read_detects_slot_reserved_for_overwrite() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        unsafe { write_item(&producer, 1) };
        let direct = unsafe { consumer.try_read_direct() }.unwrap().unwrap();
        let batch = unsafe { producer.reserve_write_batch(BUFFER_CAPACITY) }
            .expect("reserve wraparound batch");

        assert_eq!(direct.validate(), Err(1));
        assert_eq!(direct.commit(), Err(1));
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        core::mem::forget(batch);
    }

    #[test]
    fn test_cooperative_overrun_repositions_consumer() {
        let (_file, producer, mut consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_BUFFER_SIZE,
                COOPERATIVE_OPTIONS,
            );

        for i in 0..(BUFFER_CAPACITY as u64 + 2) {
            unsafe { write_item(&producer, i) };
        }

        assert_eq!(unsafe { read_item(&mut consumer) }, Err(2));
        for expected in 2..(BUFFER_CAPACITY as u64 + 2) {
            assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(expected)));
        }
        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
    }

    #[test]
    fn test_cooperative_producer_recover_as_exclusive() {
        let (file, producer, mut consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        for item in 0..BUFFER_CAPACITY as Item {
            unsafe { write_item(&producer, item) };
        }

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve write");
        unsafe {
            *guard.as_mut_ptr() = 99;
        }
        core::mem::forget(guard);

        unsafe {
            producer.recover_as_exclusive();
        }
        consumer.sync_to_latest();
        let mut joined =
            unsafe { CooperativeConsumer::<Item>::join(&file) }.expect("join after recovery");

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(None));
        assert_eq!(unsafe { read_item(&mut joined) }, Ok(None));
        unsafe { write_item(&producer, 2) };

        assert_eq!(unsafe { read_item(&mut consumer) }, Ok(Some(2)));
        assert_eq!(unsafe { read_item(&mut joined) }, Ok(Some(2)));
    }

    #[test]
    fn test_cooperative_pair_reuses_descriptor_slots_after_wrap() {
        let (producer, mut consumer) =
            cooperative_pair::<u64>(4, COOPERATIVE_OPTIONS).expect("pair failed");

        for value in 0..16 {
            producer
                .try_write(value, Ordering::Relaxed)
                .expect("write failed");
            assert_eq!(consumer.try_read(Ordering::Relaxed), Ok(Some(value)));
        }
        assert_eq!(consumer.try_read(Ordering::Relaxed), Ok(None));
    }
}
