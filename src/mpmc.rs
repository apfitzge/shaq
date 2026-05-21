//! DPDK-style bounded MPMC ring queue

use crate::{
    error::{Error, WaitError},
    normalized_capacity,
    shmem::Region,
    CacheAlignedAtomicSize, CacheAlignedAtomicU32, VERSION,
};
use core::{
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::{fs::File, num::NonZeroUsize, sync::Arc, time::Duration};

/// Unique identifier for MPMC queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqmpmc");
const CONSUMER_WAIT_SPIN_ATTEMPTS: usize = 2048;
const DEFAULT_COOPERATIVE_DESCRIPTOR_CAPACITY: usize = 4096;
const COOPERATIVE_MAX_CAPACITY: usize = 1usize << 31;
pub const MODE_CLASSIC: u32 = 0;
pub const MODE_COOPERATIVE: u32 = 1;

/// Producer publication strategy for an MPMC queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum PublicationMode {
    /// Current behavior: each producer publishes its own reservation in FIFO
    /// order, spinning until earlier producers have published.
    Classic = MODE_CLASSIC,
    /// Producers and consumers mark reservations ready and cooperatively
    /// advance the ready FIFO prefix, so later guard drops do not spin behind
    /// earlier outstanding guards.
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

/// Creation options for an MPMC queue.
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

/// Classic MPMC producer, kept as the default `Producer` type.
pub type Producer<T> = ModeProducer<T, { MODE_CLASSIC }>;
/// Classic MPMC consumer, kept as the default `Consumer` type.
pub type Consumer<T> = ModeConsumer<T, { MODE_CLASSIC }>;
/// Producer using classic in-order publication.
pub type ClassicProducer<T> = ModeProducer<T, { MODE_CLASSIC }>;
/// Producer using cooperative descriptor publication.
pub type CooperativeProducer<T> = ModeProducer<T, { MODE_COOPERATIVE }>;
/// Consumer joined to a classic MPMC queue.
pub type ClassicConsumer<T> = ModeConsumer<T, { MODE_CLASSIC }>;
/// Consumer using cooperative descriptor release.
pub type CooperativeConsumer<T> = ModeConsumer<T, { MODE_COOPERATIVE }>;

/// MPMC producer parameterized by publication mode.
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
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
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
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
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
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Consumer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    pub fn join_as_consumer(&self) -> ModeConsumer<T, MODE> {
        ModeConsumer {
            queue: self.queue.clone(),
        }
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

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        // SAFETY: On successful reservation the item is written below.
        let guard = match unsafe { self.reserve_write() } {
            Some(guard) => guard,
            None => return Err(item),
        };
        guard.write(item);
        Ok(())
    }

    /// Writes items from a slice into the queue.
    ///
    /// Returns `Err()` if there is not enough space.
    pub fn try_write_slice(&self, items: &[T]) -> bool
    where
        T: Copy,
    {
        if items.is_empty() {
            return true;
        }

        // SAFETY: if successful we write all items below.
        let mut guard = match unsafe { self.reserve_write_batch(items.len()) } {
            Some(guard) => guard,
            None => return false,
        };

        for (index, item) in items.iter().copied().enumerate() {
            // SAFETY: index is not out of bounds.
            unsafe { guard.write(index, item) };
        }
        true
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
    /// - The caller must initialize the reserved slot before the guard is dropped.
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
    /// - The caller must initialize all reserved slots before the batch is dropped.
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
    /// running `Drop` reusable without exposing their slots to consumers.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   producer process is still live.
    /// - This must only be used when joining as the sole producer process for
    ///   the shared queue.
    /// - Racing with any live producer process or thread may corrupt the queue.
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
            self.queue.clear_producer_descriptors();
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
unsafe impl<T: Sync, const MODE: u32> Sync for ModeProducer<T, MODE> {}

/// MPMC consumer parameterized by publication mode.
pub struct ModeConsumer<T, const MODE: u32> {
    queue: SharedQueue<T, MODE>,
}

impl<T, const MODE: u32> ModeConsumer<T, MODE> {
    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::create_with_options(file, file_size, Options::default()) }
    }

    /// Creates a new consumer for the shared queue using explicit options.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
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

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> ModeProducer<T, MODE> {
        ModeProducer {
            queue: self.queue.clone(),
        }
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

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&self) -> Option<T> {
        self.reserve_read().map(ReadGuard::read)
    }

    /// Attempts to read a value from the queue, waiting up to `timeout` for
    /// a producer to publish data.
    pub fn read_timeout(&self, timeout: Duration) -> Result<T, WaitError> {
        self.reserve_read_timeout(timeout).map(ReadGuard::read)
    }

    /// Attempts to reserve a value from the queue, returning a guard.
    /// The slot is released back to producers when the guard is dropped.
    ///
    /// Other [`Consumer`]s may read in parallel, but capacity is returned in
    /// reservation order. In classic mode, holding a [`ReadGuard`] can make
    /// later consumers spin while releasing; cooperative mode avoids that
    /// spin, though the release frontier still waits for earlier guards.
    #[must_use]
    pub fn reserve_read(&self) -> Option<ReadGuard<'_, T, MODE>> {
        self.queue
            .reserve_read()
            .map(|(cell, reservation)| ReadGuard {
                header: self.queue.header,
                cell,
                start: reservation.start,
                descriptor: reservation.descriptor,
                _marker: PhantomData,
            })
    }

    /// Attempts to reserve a value from the queue, waiting up to `timeout` for
    /// a producer to publish data.
    pub fn reserve_read_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ReadGuard<'_, T, MODE>, WaitError> {
        self.reserve_with_timeout(timeout, || self.reserve_read())
    }

    /// Attempts to reserve up to `max` values from the queue.
    /// The slots are released back to producers when the batch is dropped.
    ///
    ///
    /// Other [`Consumer`]s may read in parallel, but capacity is returned in
    /// reservation order. In classic mode, holding a [`ReadBatch`] can make
    /// later consumers spin while releasing; cooperative mode avoids that
    /// spin, though the release frontier still waits for earlier batches.
    #[must_use]
    pub fn reserve_read_batch(&self, max: usize) -> Option<ReadBatch<'_, T, MODE>> {
        let reservation = self.queue.reserve_read_batch(max)?;
        Some(ReadBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start: reservation.start,
            descriptor: reservation.descriptor,
            count: reservation.count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }

    /// Attempts to reserve up to `max` values from the queue, waiting up to
    /// `timeout` for a producer to publish data.
    ///
    /// Returns `Ok(None)` immediately when `max == 0`.
    pub fn reserve_read_batch_timeout(
        &self,
        max: usize,
        timeout: Duration,
    ) -> Result<Option<ReadBatch<'_, T, MODE>>, WaitError> {
        if max == 0 {
            return Ok(None);
        }

        self.reserve_with_timeout(timeout, || self.reserve_read_batch(max))
            .map(Some)
    }

    #[inline]
    fn reserve_with_timeout<R>(
        &self,
        timeout: Duration,
        mut reserve: impl FnMut() -> Option<R>,
    ) -> Result<R, WaitError> {
        let deadline = crate::futex::WaitDeadline::timeout(timeout);
        loop {
            if let Some(reserved) = reserve() {
                return Ok(reserved);
            }

            deadline.remaining()?;
            for _ in 0..CONSUMER_WAIT_SPIN_ATTEMPTS {
                core::hint::spin_loop();
                if let Some(reserved) = reserve() {
                    return Ok(reserved);
                }
            }

            let waiter = self.queue.register_consumer_waiter();
            if let Some(reserved) = reserve() {
                return Ok(reserved);
            }

            waiter.wait(deadline.remaining()?)?;
        }
    }

    /// Makes reserved-but-not-released reads left behind by a previous
    /// consumer process available to be read again.
    ///
    /// This rolls `consumer_reservation` back to `consumer_release`, making
    /// previously claimed items readable again by the new consumer process
    /// after their guards were lost without running `Drop`.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   consumer process is still live.
    /// - This must only be used when joining as the sole consumer process for
    ///   the shared queue.
    /// - Racing with any live consumer process or thread may corrupt the queue.
    /// - If `T` requires freeing of memory or other resources, this may cause
    ///   double-free if the previous consumer had processed some items but not
    ///   released them before crashing.
    pub unsafe fn recover_as_exclusive(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let release = header.consumer_release.load(Ordering::Acquire);
        if MODE == MODE_CLASSIC {
            header
                .consumer_reservation
                .store(release, Ordering::Release);
        } else {
            header
                .consumer_reservation
                .store(release, Ordering::Release);
            self.queue.clear_consumer_descriptors();
        }
    }

    /// Drops all reserved-but-not-released reads left behind by a previous
    /// consumer process.
    ///
    /// This advances `consumer_release` up to `consumer_reservation`,
    /// discarding items already claimed by the previous consumer process after
    /// their guards were lost without running `Drop` and making their capacity
    /// reusable by producers.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   consumer process is still live.
    /// - This must only be used when joining as the sole consumer process for
    ///   the shared queue.
    /// - Racing with any live consumer process or thread may corrupt the queue.
    pub unsafe fn recover_as_exclusive_lossy(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let reservation = header.consumer_reservation.load(Ordering::Acquire);
        if MODE == MODE_CLASSIC {
            header
                .consumer_release
                .store(reservation, Ordering::Release);
        } else {
            header
                .consumer_release
                .store(reservation, Ordering::Release);
            self.queue.clear_consumer_descriptors();
        }
    }
}

impl<T, const MODE: u32> Clone for ModeConsumer<T, MODE> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T: Send, const MODE: u32> Send for ModeConsumer<T, MODE> {}
unsafe impl<T: Sync, const MODE: u32> Sync for ModeConsumer<T, MODE> {}

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

/// Creates a new in-process MPMC queue pair backed by a heap allocation.
///
/// Values left buffered when the queue is dropped may be leaked instead of
/// having their destructors run.
pub fn pair<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), Error> {
    pair_with_options::<T, { MODE_CLASSIC }>(capacity, Options::default())
}

/// Creates a new in-process MPMC queue pair with explicit options.
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
    let consumer = unsafe { ModeConsumer::<T, MODE>::from_header(region, header) }?;
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

#[derive(Clone, Copy)]
struct ConsumerReservation {
    start: usize,
    count: usize,
    descriptor: usize,
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
        // descriptor position. The publication/release cursor is the reuse
        // frontier, so no descriptor-local free state is needed.
        self.state
            .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
        self.start.store(start, Ordering::Relaxed);
        self.count.store(count, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct CooperativeDescriptorRings {
    producer: NonNull<CooperativeDescriptor>,
    consumer: NonNull<CooperativeDescriptor>,
}

struct SharedQueue<T, const MODE: u32> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    cooperative_descriptors: Option<CooperativeDescriptorRings>,
    buffer_mask: usize,
    cooperative_descriptor_mask: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<Region>,
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
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    fn reserve_write(&self) -> Option<(NonNull<T>, ProducerReservation)> {
        let reservation = self.reserve_write_batch(1)?;
        let cell_index = reservation.start & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, reservation))
    }

    fn reserve_read(&self) -> Option<(NonNull<T>, ConsumerReservation)> {
        let reservation = self.reserve_read_batch(1)?;
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
            let consumer_release = header.consumer_release.load(Ordering::Acquire);
            let used = producer_reservation.wrapping_sub(consumer_release);
            let limit = capacity - count;
            if used > limit {
                return None;
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

        let descriptors = self.cooperative_descriptors?.producer;
        let descriptor_capacity = self.cooperative_descriptor_mask.wrapping_add(1);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut packed_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_reservation = cooperative_packed_item(packed_reservation);
            let descriptor_reservation = cooperative_packed_descriptor(packed_reservation);

            let consumer_release =
                cooperative_packed_item(header.consumer_release.load(Ordering::Acquire));
            let used_items = cooperative_cursor_sub(producer_reservation, consumer_release);
            let item_limit = capacity - count;
            if used_items > item_limit {
                return None;
            }

            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let descriptor_frontier = cooperative_packed_descriptor(producer_publication);
            let used_descriptors =
                cooperative_cursor_sub(descriptor_reservation, descriptor_frontier);
            if used_descriptors >= descriptor_capacity {
                // Descriptor capacity may be hidden behind ready producer
                // descriptors whose owners have not drained the prefix yet.
                // SAFETY: `self.header` points to a cooperative queue header.
                unsafe { SharedQueueHeader::try_publish_cooperative(self.header) };
                if header.producer_publication.load(Ordering::Acquire) != producer_publication {
                    continue;
                }
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

    fn reserve_read_batch(&self, max: usize) -> Option<ConsumerReservation> {
        if max == 0 {
            return None;
        }

        let capacity = self.capacity();
        let max = max.min(capacity);

        if MODE == MODE_CLASSIC {
            self.reserve_read_batch_classic(max, capacity)
        } else {
            self.reserve_read_batch_cooperative(max, capacity)
        }
    }

    fn reserve_read_batch_classic(
        &self,
        max: usize,
        capacity: usize,
    ) -> Option<ConsumerReservation> {
        debug_assert!(max != 0);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut consumer_reservation = header.consumer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let available = producer_publication.wrapping_sub(consumer_reservation);
            if available == 0 || available > capacity {
                return None;
            }

            let count = available.min(max);
            let new_reservation = consumer_reservation.wrapping_add(count);
            match header.consumer_reservation.compare_exchange_weak(
                consumer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(ConsumerReservation {
                        start: consumer_reservation,
                        count,
                        descriptor: 0,
                    });
                }
                Err(current) => {
                    consumer_reservation = current;
                }
            }
        }
    }

    fn reserve_read_batch_cooperative(
        &self,
        max: usize,
        capacity: usize,
    ) -> Option<ConsumerReservation> {
        debug_assert!(max != 0);

        let descriptors = self.cooperative_descriptors?.consumer;
        let descriptor_capacity = self.cooperative_descriptor_mask.wrapping_add(1);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut packed_reservation = header.consumer_reservation.load(Ordering::Relaxed);

        loop {
            let consumer_reservation = cooperative_packed_item(packed_reservation);
            let descriptor_reservation = cooperative_packed_descriptor(packed_reservation);

            let producer_publication =
                cooperative_packed_item(header.producer_publication.load(Ordering::Acquire));
            let available = cooperative_cursor_sub(producer_publication, consumer_reservation);
            if available == 0 || available > capacity {
                return None;
            }

            let consumer_release = header.consumer_release.load(Ordering::Acquire);
            let descriptor_release = cooperative_packed_descriptor(consumer_release);
            let used_descriptors =
                cooperative_cursor_sub(descriptor_reservation, descriptor_release);
            if used_descriptors >= descriptor_capacity {
                // Consumer descriptor capacity may be hidden behind ready
                // release descriptors whose owners have not drained the
                // prefix yet.
                // SAFETY: `self.header` points to a cooperative queue header.
                unsafe { SharedQueueHeader::try_release_cooperative(self.header) };
                if header.consumer_release.load(Ordering::Acquire) != consumer_release {
                    continue;
                }
                return None;
            }

            let count = available.min(max);
            let new_reservation = cooperative_pack_cursor_pair(
                cooperative_cursor_add(consumer_reservation, count),
                cooperative_cursor_add(descriptor_reservation, 1),
            );
            match header.consumer_reservation.compare_exchange_weak(
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
                    descriptor.initialize_reserved(consumer_reservation, count);
                    return Some(ConsumerReservation {
                        start: consumer_reservation,
                        count,
                        descriptor: descriptor_reservation,
                    });
                }
                Err(current) => {
                    packed_reservation = current;
                }
            }
        }
    }

    fn register_consumer_waiter(&self) -> ConsumerWaiter<'_> {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        ConsumerWaiter::new(&header.futex, &header.futex_waiters)
    }

    fn clear_producer_descriptors(&self) {
        if let Some(descriptors) = self.cooperative_descriptors {
            self.clear_descriptors(descriptors.producer);
        }
    }

    fn clear_consumer_descriptors(&self) {
        if let Some(descriptors) = self.cooperative_descriptors {
            self.clear_descriptors(descriptors.consumer);
        }
    }

    fn clear_descriptors(&self, descriptors: NonNull<CooperativeDescriptor>) {
        let descriptor_capacity = self.cooperative_descriptor_mask.wrapping_add(1);
        for index in 0..descriptor_capacity {
            // SAFETY: `index < descriptor_capacity`, so the descriptor is in bounds.
            let descriptor = unsafe { descriptors.add(index).as_ref() };
            descriptor
                .state
                .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
        }
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
            // - cooperative layout validation above proves both descriptor rings exist.
            let producer =
                unsafe { SharedQueueHeader::cooperative_producer_descriptors_from_header(header) };
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - cooperative layout validation above proves both descriptor rings exist.
            let consumer = unsafe {
                SharedQueueHeader::cooperative_consumer_descriptors_from_header(
                    header,
                    cooperative_descriptor_capacity,
                )
            };
            Some(CooperativeDescriptorRings { producer, consumer })
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
    /// item cursor to determine how many initialized items are readable.
    producer_publication: CacheAlignedAtomicSize,
    /// Consumer reservation cursor.
    ///
    /// Consumers atomically advance this with CAS to claim readable slots, but
    /// reclaimed capacity is not visible to producers until `consumer_release`
    /// is advanced.
    consumer_reservation: CacheAlignedAtomicSize,
    /// Consumer release cursor.
    ///
    /// In classic mode this stores the item cursor. In cooperative mode this
    /// stores the packed item and descriptor release cursors. Consumers advance
    /// this in-order after dropping/reading claimed slots. Producers use the
    /// item cursor to determine how much free space is available.
    consumer_release: CacheAlignedAtomicSize,
    /// Futex word used for consumer wait/wake coordination.
    ///
    /// Consumers register in [`SharedQueueHeader::futex_waiters`], record this
    /// sequence value, recheck readability, then enter [`libc::FUTEX_WAIT`].
    /// Producers increment this sequence before [`libc::FUTEX_WAKE`] so a
    /// publish racing with the wait call cannot be missed.
    futex: CacheAlignedAtomicU32,
    /// Number of consumers registered as possible futex sleepers.
    ///
    /// Producers read this before touching [`SharedQueueHeader::futex`] so the
    /// producer fast path skips futex wake syscalls when no consumer is
    /// waiting.
    futex_waiters: CacheAlignedAtomicU32,
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
                    + 2 * descriptor_capacity * core::mem::size_of::<CooperativeDescriptor>();
                after_descriptors.next_multiple_of(core::mem::align_of::<T>())
            }
        }
    }

    #[cfg(test)]
    fn calculate_buffer_size_in_items<T>(file_size: usize) -> Result<usize, Error> {
        Self::calculate_buffer_size_in_items_for_layout::<T>(file_size, PublicationMode::Classic, 0)
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

                Err(Error::InvalidBufferSize)
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
        header.consumer_reservation.store(0, Ordering::Release);
        header.consumer_release.store(0, Ordering::Release);
        header.futex.store(0, Ordering::Release);
        header.futex_waiters.store(0, Ordering::Release);
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
            // `descriptor_capacity` producer descriptor entries after the header.
            let descriptors =
                unsafe { Self::cooperative_producer_descriptors_from_header(header_ptr) };
            for index in 0..descriptor_capacity {
                // SAFETY: `index < descriptor_capacity`, so the descriptor is in bounds.
                unsafe {
                    descriptors
                        .add(index)
                        .as_ptr()
                        .write(CooperativeDescriptor::new())
                };
            }

            // SAFETY: The cooperative layout reserves exactly
            // `descriptor_capacity` consumer descriptor entries after producer descriptors.
            let descriptors = unsafe {
                Self::cooperative_consumer_descriptors_from_header(header_ptr, descriptor_capacity)
            };
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
        let region = Region::map_file(file, file_size)?;
        let header = Self::join_region::<T>(&region)?;
        Ok((region, header))
    }

    fn join_region<T>(region: &Arc<Region>) -> Result<NonNull<Self>, Error> {
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
                    region.size(),
                    publication_mode,
                    descriptor_capacity,
                )?
            {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok(header)
    }

    fn publication_mode(&self) -> Result<PublicationMode, Error> {
        PublicationMode::from_u32(self.publication_mode).ok_or(Error::InvalidBufferSize)
    }

    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must include the cooperative descriptor ring.
    unsafe fn cooperative_producer_descriptors_from_header(
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
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must include both cooperative descriptor rings.
    unsafe fn cooperative_consumer_descriptors_from_header(
        header: NonNull<SharedQueueHeader>,
        descriptor_capacity: usize,
    ) -> NonNull<CooperativeDescriptor> {
        let descriptors_offset = Self::cooperative_descriptors_offset()
            + descriptor_capacity * core::mem::size_of::<CooperativeDescriptor>();

        // SAFETY:
        // - descriptors_offset will not overflow isize.
        // - header allocation is large enough to hold both descriptor rings.
        let descriptors = unsafe { header.byte_add(descriptors_offset) };
        descriptors.cast()
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this producer.
    unsafe fn publish_producer_publication_classic(
        header_ptr: NonNull<Self>,
        start: usize,
        count: usize,
    ) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.producer_publication.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .producer_publication
            .store(start.wrapping_add(count), Ordering::Release);
        header.wake_consumers_if_waiting(count);
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
        let descriptor = unsafe {
            Self::cooperative_producer_descriptor_at(header_ptr, descriptor_position).as_ref()
        };
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
        let mut total_published = 0usize;
        let mut descriptors_published = 0usize;

        loop {
            let descriptor_position = cooperative_packed_descriptor(publication);
            let frontier = cooperative_packed_item(publication);

            // SAFETY: `header_ptr` points to a cooperative queue header.
            let descriptor = unsafe {
                Self::cooperative_producer_descriptor_at(header_ptr, descriptor_position).as_ref()
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
            total_published = total_published.wrapping_add(count);
            descriptors_published = descriptors_published.wrapping_add(1);
            publication = next_publication;
        }

        if total_published != 0 {
            header.wake_consumers_if_waiting(total_published);
        }
        descriptors_published
    }

    /// # Safety
    /// - `header` must point to a cooperative queue header.
    unsafe fn cooperative_producer_descriptor_at(
        header: NonNull<SharedQueueHeader>,
        descriptor_position: usize,
    ) -> NonNull<CooperativeDescriptor> {
        // SAFETY: `header` points to a cooperative queue header.
        let descriptors = unsafe { Self::cooperative_producer_descriptors_from_header(header) };
        // SAFETY: `header` is a valid shared-memory header.
        let header_ref = unsafe { header.as_ref() };
        let index = descriptor_position & header_ref.descriptor_mask as usize;
        // SAFETY: Mask ensures the descriptor index is in bounds.
        unsafe { descriptors.add(index) }
    }

    /// # Safety
    /// - `header` must point to a cooperative queue header.
    unsafe fn cooperative_consumer_descriptor_at(
        header: NonNull<SharedQueueHeader>,
        descriptor_position: usize,
    ) -> NonNull<CooperativeDescriptor> {
        // SAFETY: `header` is a valid shared-memory header.
        let header_ref = unsafe { header.as_ref() };
        let descriptor_capacity = (header_ref.descriptor_mask as usize).wrapping_add(1);
        // SAFETY: `header` points to a cooperative queue header.
        let descriptors = unsafe {
            Self::cooperative_consumer_descriptors_from_header(header, descriptor_capacity)
        };
        let index = descriptor_position & header_ref.descriptor_mask as usize;
        // SAFETY: Mask ensures the descriptor index is in bounds.
        unsafe { descriptors.add(index) }
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this consumer.
    unsafe fn publish_consumer_release(header_ptr: NonNull<Self>, start: usize, count: usize) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.consumer_release.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .consumer_release
            .store(start.wrapping_add(count), Ordering::Release);
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this consumer.
    unsafe fn publish_consumer_release_cooperative(
        header_ptr: NonNull<Self>,
        start: usize,
        count: usize,
        descriptor_position: usize,
    ) {
        // SAFETY: `header_ptr` points to a cooperative queue header.
        let descriptor = unsafe {
            Self::cooperative_consumer_descriptor_at(header_ptr, descriptor_position).as_ref()
        };
        debug_assert_eq!(descriptor.start.load(Ordering::Acquire), start);
        debug_assert_eq!(descriptor.count.load(Ordering::Acquire), count);
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);

        // SAFETY: `header_ptr` points to a cooperative queue header.
        unsafe { Self::try_release_cooperative(header_ptr) };
    }

    /// # Safety
    /// - `header_ptr` must point to a cooperative queue header.
    unsafe fn try_release_cooperative(header_ptr: NonNull<Self>) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        let release = header.consumer_release.load(Ordering::Acquire);

        // SAFETY: forwarded from this function's safety contract.
        unsafe { Self::try_release_cooperative_from(header_ptr, release) };
    }

    /// # Safety
    /// - `header_ptr` must point to a cooperative queue header.
    unsafe fn try_release_cooperative_from(header_ptr: NonNull<Self>, mut release: usize) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };

        loop {
            let descriptor_position = cooperative_packed_descriptor(release);
            let frontier = cooperative_packed_item(release);

            // SAFETY: `header_ptr` points to a cooperative queue header.
            let descriptor = unsafe {
                Self::cooperative_consumer_descriptor_at(header_ptr, descriptor_position).as_ref()
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
                let refreshed_release = header.consumer_release.load(Ordering::Acquire);
                if refreshed_release != release {
                    release = refreshed_release;
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
                let refreshed_release = header.consumer_release.load(Ordering::Acquire);
                if refreshed_release != release {
                    release = refreshed_release;
                    continue;
                }
                break;
            }

            let next = cooperative_cursor_add(frontier, count);
            let next_descriptor_position = cooperative_cursor_add(descriptor_position, 1);
            let next_release = cooperative_pack_cursor_pair(next, next_descriptor_position);
            header
                .consumer_release
                .store(next_release, Ordering::Release);
            release = next_release;
        }
    }

    /// Wakes registered consumers after new producer publication.
    ///
    /// This skips the futex wake syscall when no consumer has registered as a
    /// sleeper. If consumers are registered, the futex sequence is advanced
    /// before waking so consumers racing into [`libc::FUTEX_WAIT`] observe the
    /// state change instead of sleeping through the publication.
    fn wake_consumers_if_waiting(&self, published: usize) {
        let waiters = self.futex_waiters.load(Ordering::Acquire);
        if waiters == 0 {
            return;
        }

        self.futex.fetch_add(1, Ordering::Release);
        let wake_count = waiters.min(published.min(libc::c_int::MAX as usize) as u32);
        crate::futex::wake(&self.futex, wake_count);
    }
}

struct ConsumerWaiter<'a> {
    futex: &'a CacheAlignedAtomicU32,
    futex_waiters: &'a CacheAlignedAtomicU32,
    expected: u32,
}

impl<'a> ConsumerWaiter<'a> {
    fn new(futex: &'a CacheAlignedAtomicU32, futex_waiters: &'a CacheAlignedAtomicU32) -> Self {
        futex_waiters.fetch_add(1, Ordering::AcqRel);
        Self {
            futex,
            futex_waiters,
            expected: futex.load(Ordering::Acquire),
        }
    }

    fn wait(&self, timeout: Duration) -> Result<(), WaitError> {
        crate::futex::wait(self.futex, self.expected, timeout)
    }
}

impl Drop for ConsumerWaiter<'_> {
    fn drop(&mut self) {
        self.futex_waiters.fetch_sub(1, Ordering::AcqRel);
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
    /// Returns a mutable reference to the slot.
    ///
    /// # Safety
    /// - T must be be valid for any bytes.
    pub unsafe fn as_mut_ref(&mut self) -> &mut T {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_mut() }
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.cell.as_ptr()
    }

    pub fn write(self, value: T) {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.as_ptr().write(value) };
    }
}

impl<'a, T, const MODE: u32> Drop for WriteGuard<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_producer_publication_classic(self.header, self.start, 1);
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
pub struct ReadGuard<'a, T, const MODE: u32 = MODE_CLASSIC> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    descriptor: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T, const MODE: u32> ReadGuard<'a, T, MODE> {
    pub fn as_ptr(&self) -> *const T {
        // SAFETY: The cell was reserved for reading.
        self.cell.as_ptr()
    }

    pub fn read(self) -> T {
        // SAFETY: The cell was reserved for reading and holds an initialized value.
        unsafe { self.cell.as_ptr().read() }
    }
}

impl<'a, T, const MODE: u32> AsRef<T> for ReadGuard<'a, T, MODE> {
    /// Returns a shared reference to the reserved slot.
    fn as_ref(&self) -> &T {
        // SAFETY: The cell was reserved for reading and is initialized.
        unsafe { self.cell.as_ref() }
    }
}

impl<'a, T, const MODE: u32> Drop for ReadGuard<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved consumer slot.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_consumer_release(self.header, self.start, 1);
            } else {
                SharedQueueHeader::publish_consumer_release_cooperative(
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

    /// Returns a mutable reference to the reserved slot.
    ///
    /// # Safety
    /// - The slot is uninitialized; caller must fully initialize `T`.
    /// - `index < count`
    /// - `T` must be valid for any bytes.
    pub unsafe fn as_mut(&mut self, index: usize) -> &mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_mut() }
    }

    /// Returns a mutable pointer to the reserved slot.
    ///
    /// # Safety
    /// - The slot is uninitialized; caller must fully initialize `T`.
    /// - `index < count`
    pub unsafe fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Writes a value into the slot at index.
    ///
    /// # Safety
    /// - `index < count`
    pub unsafe fn write(&mut self, index: usize, value: T) {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing
        unsafe { self.buffer.add(position & self.buffer_mask).write(value) }
    }
}

impl<'a, T, const MODE: u32> Drop for WriteBatch<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_producer_publication_classic(
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
pub struct ReadBatch<'a, T, const MODE: u32 = MODE_CLASSIC> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    descriptor: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T, const MODE: u32> ReadBatch<'a, T, MODE> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a reference to the reserved slot.
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn as_ref(&self, index: usize) -> &T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading and is initialized.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ref() }
    }

    /// Returns a pointer to the reserved slot.
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Read the value at index
    ///
    /// # Safety
    /// - `index` must be less than `self.len()`
    pub unsafe fn read(&self, index: usize) -> T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for reading.
        unsafe { self.buffer.add(position & self.buffer_mask).read() }
    }
}

impl<'a, T, const MODE: u32> Drop for ReadBatch<'a, T, MODE> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved consumer slots.
        unsafe {
            if MODE == MODE_CLASSIC {
                SharedQueueHeader::publish_consumer_release(self.header, self.start, self.count);
            } else {
                SharedQueueHeader::publish_consumer_release_cooperative(
                    self.header,
                    self.start,
                    self.count,
                    self.descriptor,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::create_temp_shmem_file;
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };

    type Item = u64;
    const BUFFER_CAPACITY: usize = 512;
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
            8,
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

    fn wait_for_waiters<T, const MODE: u32>(producer: &ModeProducer<T, MODE>, waiters: u32) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while unsafe { producer.queue.header.as_ref() }
            .futex_waiters
            .load(Ordering::Acquire)
            < waiters
        {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for registered futex waiters"
            );
            std::thread::yield_now();
        }
    }

    fn futex_waiters<T, const MODE: u32>(consumer: &ModeConsumer<T, MODE>) -> u32 {
        unsafe { consumer.queue.header.as_ref() }
            .futex_waiters
            .load(Ordering::Acquire)
    }

    #[test]
    fn test_producer_consumer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let capacity =
            SharedQueueHeader::calculate_buffer_size_in_items::<Item>(BUFFER_SIZE).unwrap();

        for i in 0..capacity {
            assert_eq!(producer.try_write(i as Item), Ok(()));
        }
        assert!(producer.try_write(999).is_err());

        for i in 0..capacity {
            assert_eq!(consumer.try_read(), Some(i as Item));
        }
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_reserve_and_try_read_ptr() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve failed");
        unsafe {
            *guard.as_mut_ptr() = 42;
        }
        drop(guard);

        let guard = consumer.reserve_read().expect("try_read_ptr failed");
        unsafe {
            assert_eq!(*guard.as_ptr(), 42);
        }
        assert_eq!(*guard.as_ref(), 42);
    }

    #[test]
    fn test_reserve_batch_and_try_read_batch() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        let mut batch = unsafe { producer.reserve_write_batch(4) }.expect("reserve_batch failed");
        for index in 0..batch.len() {
            unsafe {
                *batch.as_mut_ptr(index) = index as u64;
            }
        }
        drop(batch);

        let batch = consumer
            .reserve_read_batch(4)
            .expect("try_read_batch failed");
        for index in 0..batch.len() {
            unsafe {
                assert_eq!(*batch.as_ptr(index), index as u64);
            }
            assert_eq!(unsafe { batch.read(index) }, index as u64);
        }
    }

    #[test]
    fn test_batch_write_exact_read_upto_max() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        unsafe {
            assert!(producer.reserve_write_batch(0).is_none());
            assert!(producer.reserve_write_batch(BUFFER_CAPACITY + 1).is_none());
        }

        for i in 0..4 {
            assert_eq!(producer.try_write(i as Item), Ok(()));
        }
        let batch = consumer
            .reserve_read_batch(5)
            .expect("try_read_batch up-to failed");
        assert_eq!(batch.len(), 4);
        for index in 0..batch.len() {
            // SAFETY: `batch` has exactly 4 readable items.
            unsafe {
                assert_eq!(*batch.as_ptr(index), index as u64);
            }
        }
    }

    #[test]
    fn test_try_write_slice() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        assert!(producer.try_write_slice(&[]));

        let values = [10, 11, 12, 13];
        assert!(producer.try_write_slice(&values));
        for value in values {
            assert_eq!(consumer.try_read(), Some(value));
        }
        assert_eq!(consumer.try_read(), None);
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

        assert_eq!(producer.queue.capacity(), 8);
        assert_eq!(producer.queue.cooperative_descriptor_mask + 1, 2);
        assert_eq!(header.descriptor_mask + 1, 2);
    }

    #[test]
    fn test_multiple_producers_consumers() {
        let (file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = unsafe { Producer::join(&file) }.expect("Failed to create producer2");
        let consumer2 = unsafe { Consumer::join(&file) }.expect("Failed to create consumer2");

        let capacity = BUFFER_CAPACITY;
        for i in 0..(capacity / 2) {
            assert_eq!(producer.try_write((i * 2) as Item), Ok(()));
            assert_eq!(producer2.try_write((i * 2 + 1) as Item), Ok(()));
        }

        let mut values = Vec::with_capacity(capacity);
        while values.len() < capacity {
            let mut progressed = false;
            if let Some(value) = consumer.try_read() {
                values.push(value);
                progressed = true;
            }
            if let Some(value) = consumer2.try_read() {
                values.push(value);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }

        assert_eq!(values.len(), capacity);
        values.sort_unstable();
        for (i, value) in values.iter().enumerate() {
            assert_eq!(*value, i as Item);
        }
    }

    #[test]
    fn test_read_timeout_waits_until_publication() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let (tx, rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            let value = consumer
                .read_timeout(Duration::from_secs(1))
                .expect("timeout read failed");
            tx.send(value).unwrap();
        });

        wait_for_waiters(&producer, 1);
        let mut guard = unsafe { producer.reserve_write() }.expect("reserve write");
        unsafe {
            guard.as_mut_ptr().write(42);
        }

        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());

        drop(guard);

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
        handle.join().unwrap();
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .futex_waiters
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn test_cooperative_later_producer_drop_does_not_wait() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        let mut first = unsafe { producer.reserve_write() }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");

        unsafe {
            second.as_mut_ptr().write(20);
        }
        drop(second);

        assert_eq!(consumer.try_read(), None);
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .producer_publication
                .load(Ordering::Acquire),
            0
        );

        unsafe {
            first.as_mut_ptr().write(10);
        }
        drop(first);

        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_cooperative_publishes_ready_batch_prefix() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        let mut first = unsafe { producer.reserve_write_batch(2) }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write_batch(3) }.expect("reserve second");
        let mut third = unsafe { producer.reserve_write() }.expect("reserve third");

        for (index, value) in [20, 21, 22].into_iter().enumerate() {
            unsafe {
                second.write(index, value);
            }
        }
        drop(second);

        unsafe {
            third.as_mut_ptr().write(30);
        }
        drop(third);

        assert_eq!(consumer.try_read(), None);

        for (index, value) in [10, 11].into_iter().enumerate() {
            unsafe {
                first.write(index, value);
            }
        }
        drop(first);

        let mut values = Vec::new();
        while let Some(value) = consumer.try_read() {
            values.push(value);
        }
        assert_eq!(values, vec![10, 11, 20, 21, 22, 30]);
    }

    #[test]
    fn test_cooperative_producer_descriptor_pressure_helps_ready_publication() {
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            );

        let mut first = unsafe { producer.reserve_write() }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");
        unsafe {
            first.as_mut_ptr().write(10);
            second.as_mut_ptr().write(20);
        }

        // Simulate a producer that made its descriptor ready but did not drain
        // the ready publication prefix.
        let header = producer.queue.header;
        let descriptor_position = first.descriptor;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_producer_descriptor_at(header, descriptor_position)
                .as_ref()
        };
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
        std::mem::forget(first);

        assert_eq!(
            unsafe { header.as_ref() }
                .producer_publication
                .load(Ordering::Acquire),
            0
        );

        let mut third = unsafe { producer.reserve_write() }.expect("descriptor reused");
        unsafe {
            third.as_mut_ptr().write(30);
        }
        drop(third);
        drop(second);

        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
        assert_eq!(consumer.try_read(), Some(30));
    }

    #[test]
    fn test_cooperative_publication_packs_item_and_descriptor_cursor() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        producer.try_write(10).expect("write first");
        producer.try_write(20).expect("write second");

        let publication = unsafe { producer.queue.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(publication), 2);
        assert_eq!(cooperative_packed_descriptor(publication), 2);
        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
    }

    #[test]
    fn test_cooperative_publish_retries_after_stale_packed_cursor() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        let mut first = unsafe { producer.reserve_write() }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");
        unsafe {
            first.as_mut_ptr().write(10);
            second.as_mut_ptr().write(20);
        }
        drop(first);

        let header = producer.queue.header;
        let second_descriptor_position = second.descriptor;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_producer_descriptor_at(
                header,
                second_descriptor_position,
            )
            .as_ref()
        };
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
        std::mem::forget(second);

        let current_publication = unsafe { header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(current_publication), 1);
        assert_eq!(cooperative_packed_descriptor(current_publication), 1);

        // Simulate a helper that loaded the old packed cursor before the first
        // descriptor was published. The old descriptor is no longer ready, so
        // the CAS failure path must refresh and retry instead of stopping.
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
        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
    }

    #[test]
    fn test_cooperative_reuses_producer_descriptor_from_publication_cursor() {
        let options = Options::new().with_cooperative_descriptor_capacity(1);
        let file_size = minimum_file_size_for_mode::<Item, { MODE_COOPERATIVE }>(4, options);
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(file_size, options);

        let mut first = unsafe { producer.reserve_write() }.expect("reserve first");
        unsafe {
            first.as_mut_ptr().write(10);
        }

        let header = producer.queue.header;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_producer_descriptor_at(header, first.descriptor).as_ref()
        };

        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
        unsafe { header.as_ref() }
            .producer_publication
            .store(cooperative_pack_cursor_pair(1, 1), Ordering::Release);
        std::mem::forget(first);

        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");
        unsafe {
            second.as_mut_ptr().write(20);
        }
        drop(second);

        assert_eq!(
            unsafe { header.as_ref() }
                .producer_reservation
                .load(Ordering::Acquire),
            cooperative_pack_cursor_pair(2, 2)
        );
        assert_eq!(
            unsafe { header.as_ref() }
                .producer_publication
                .load(Ordering::Acquire),
            cooperative_pack_cursor_pair(2, 2)
        );
        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
    }

    #[test]
    fn test_cooperative_descriptor_exhaustion_returns_none() {
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            );

        let mut first = unsafe { producer.reserve_write() }.expect("reserve first");
        let mut second = unsafe { producer.reserve_write() }.expect("reserve second");
        assert!(unsafe { producer.reserve_write() }.is_none());

        unsafe {
            first.as_mut_ptr().write(10);
            second.as_mut_ptr().write(20);
        }
        drop(first);
        drop(second);

        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));

        let mut third = unsafe { producer.reserve_write() }.expect("descriptor reused");
        unsafe {
            third.as_mut_ptr().write(30);
        }
        drop(third);
        assert_eq!(consumer.try_read(), Some(30));
    }

    #[test]
    fn test_cooperative_later_consumer_drop_does_not_wait() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        assert!(producer.try_write_slice(&[10, 20]));

        let first = consumer.reserve_read().expect("reserve first");
        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*first.as_ref(), 10);
        assert_eq!(*second.as_ref(), 20);

        drop(second);

        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .consumer_release
                .load(Ordering::Acquire),
            0
        );

        drop(first);

        let release = unsafe { producer.queue.header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(release), 2);
        assert_eq!(cooperative_packed_descriptor(release), 2);
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_cooperative_consumer_descriptor_pressure_helps_ready_release() {
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            );

        assert!(producer.try_write_slice(&[10, 20, 30]));
        let first = consumer.reserve_read().expect("reserve first");
        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*first.as_ref(), 10);
        assert_eq!(*second.as_ref(), 20);

        // Simulate a consumer that made its release descriptor ready but did
        // not drain the ready release prefix.
        let header = producer.queue.header;
        let descriptor_position = first.descriptor;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_consumer_descriptor_at(header, descriptor_position)
                .as_ref()
        };
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
        std::mem::forget(first);

        assert_eq!(
            unsafe { header.as_ref() }
                .consumer_release
                .load(Ordering::Acquire),
            0
        );

        let third = consumer.reserve_read().expect("descriptor reused");
        assert_eq!(*third.as_ref(), 30);
        let release = unsafe { header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(release), 1);
        assert_eq!(cooperative_packed_descriptor(release), 1);

        drop(third);
        drop(second);
        let release = unsafe { header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(release), 3);
        assert_eq!(cooperative_packed_descriptor(release), 3);
    }

    #[test]
    fn test_cooperative_release_packs_item_and_descriptor_cursor() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        assert!(producer.try_write_slice(&[10, 20]));
        let first = consumer.reserve_read().expect("reserve first");
        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*first.as_ref(), 10);
        assert_eq!(*second.as_ref(), 20);
        drop(first);
        drop(second);

        let release = unsafe { producer.queue.header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(release), 2);
        assert_eq!(cooperative_packed_descriptor(release), 2);
    }

    #[test]
    fn test_cooperative_release_retries_after_stale_packed_cursor() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        assert!(producer.try_write_slice(&[10, 20]));
        let first = consumer.reserve_read().expect("reserve first");
        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*first.as_ref(), 10);
        assert_eq!(*second.as_ref(), 20);
        drop(first);

        let header = producer.queue.header;
        let second_descriptor_position = second.descriptor;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_consumer_descriptor_at(
                header,
                second_descriptor_position,
            )
            .as_ref()
        };
        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_READY, Ordering::Release);
        std::mem::forget(second);

        let current_release = unsafe { header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(current_release), 1);
        assert_eq!(cooperative_packed_descriptor(current_release), 1);

        // Simulate a helper that loaded the old packed cursor before the first
        // descriptor was released. The old descriptor is no longer ready, so
        // the CAS failure path must refresh and retry instead of stopping.
        unsafe {
            SharedQueueHeader::try_release_cooperative_from(
                header,
                cooperative_pack_cursor_pair(0, 0),
            )
        };

        let release = unsafe { header.as_ref() }
            .consumer_release
            .load(Ordering::Acquire);
        assert_eq!(cooperative_packed_item(release), 2);
        assert_eq!(cooperative_packed_descriptor(release), 2);
    }

    #[test]
    fn test_cooperative_reuses_consumer_descriptor_from_release_cursor() {
        let options = Options::new().with_cooperative_descriptor_capacity(1);
        let file_size = minimum_file_size_for_mode::<Item, { MODE_COOPERATIVE }>(4, options);
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(file_size, options);

        assert!(producer.try_write_slice(&[10, 20]));
        let first = consumer.reserve_read().expect("reserve first");
        assert_eq!(*first.as_ref(), 10);

        let header = producer.queue.header;
        let descriptor = unsafe {
            SharedQueueHeader::cooperative_consumer_descriptor_at(header, first.descriptor).as_ref()
        };

        descriptor
            .state
            .store(COOPERATIVE_DESCRIPTOR_PENDING, Ordering::Release);
        unsafe { header.as_ref() }
            .consumer_release
            .store(cooperative_pack_cursor_pair(1, 1), Ordering::Release);
        std::mem::forget(first);

        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*second.as_ref(), 20);
        drop(second);

        assert_eq!(
            unsafe { header.as_ref() }
                .consumer_reservation
                .load(Ordering::Acquire),
            cooperative_pack_cursor_pair(2, 2)
        );
        assert_eq!(
            unsafe { header.as_ref() }
                .consumer_release
                .load(Ordering::Acquire),
            cooperative_pack_cursor_pair(2, 2)
        );
    }

    #[test]
    fn test_cooperative_consumer_descriptor_exhaustion_returns_none() {
        let (_file, producer, consumer) =
            create_test_queue_with_options::<Item, { MODE_COOPERATIVE }>(
                COOPERATIVE_SMALL_DESCRIPTOR_BUFFER_SIZE,
                COOPERATIVE_SMALL_DESCRIPTOR_OPTIONS,
            );

        assert!(producer.try_write_slice(&[10, 20, 30]));

        let first = consumer.reserve_read().expect("reserve first");
        let second = consumer.reserve_read().expect("reserve second");
        assert_eq!(*first.as_ref(), 10);
        assert_eq!(*second.as_ref(), 20);
        assert!(consumer.reserve_read().is_none());

        drop(first);

        let third = consumer.reserve_read().expect("descriptor reused");
        assert_eq!(*third.as_ref(), 30);

        drop(second);
        drop(third);
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_read_timeout_cleans_waiter() {
        let (_file, _producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        assert!(matches!(
            consumer.reserve_read_timeout(Duration::from_millis(1)),
            Err(WaitError::Timeout)
        ));
        assert_eq!(futex_waiters(&consumer), 0);

        assert!(matches!(
            consumer.reserve_read_batch_timeout(4, Duration::from_millis(1)),
            Err(WaitError::Timeout)
        ));
        assert_eq!(futex_waiters(&consumer), 0);

        assert!(consumer
            .reserve_read_batch_timeout(0, Duration::from_secs(1))
            .unwrap()
            .is_none());
        assert_eq!(futex_waiters(&consumer), 0);
    }

    #[test]
    fn test_publish_without_waiters_leaves_futex_unchanged() {
        let (_file, producer, _consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let header = unsafe { producer.queue.header.as_ref() };
        let initial_futex = header.futex.load(Ordering::Acquire);
        assert_eq!(header.futex_waiters.load(Ordering::Acquire), 0);

        producer.try_write(9).unwrap();

        let header = unsafe { producer.queue.header.as_ref() };
        assert_eq!(header.futex_waiters.load(Ordering::Acquire), 0);
        assert_eq!(header.futex.load(Ordering::Acquire), initial_futex);
    }

    #[test]
    fn test_multiple_read_timeout_consumers_wake() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let consumer2 = consumer.clone();
        let (tx, rx) = mpsc::channel();

        let handle1 = std::thread::spawn({
            let tx = tx.clone();
            move || {
                tx.send(
                    consumer
                        .read_timeout(Duration::from_secs(1))
                        .expect("consumer 1 read failed"),
                )
                .unwrap();
            }
        });
        let handle2 = std::thread::spawn(move || {
            tx.send(
                consumer2
                    .read_timeout(Duration::from_secs(1))
                    .expect("consumer 2 read failed"),
            )
            .unwrap();
        });

        wait_for_waiters(&producer, 2);
        producer.try_write(10).unwrap();
        producer.try_write(20).unwrap();

        let mut values = vec![
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ];
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);

        handle1.join().unwrap();
        handle2.join().unwrap();
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .futex_waiters
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn test_reserve_read_batch_timeout_waits_until_publication() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let (tx, rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            let batch = consumer
                .reserve_read_batch_timeout(4, Duration::from_secs(1))
                .expect("batch timeout failed")
                .expect("batch should be returned");
            let mut values = Vec::with_capacity(batch.len());
            for index in 0..batch.len() {
                values.push(unsafe { batch.read(index) });
            }
            tx.send(values).unwrap();
        });

        wait_for_waiters(&producer, 1);
        assert!(producer.try_write_slice(&[1, 2, 3]));

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            vec![1, 2, 3]
        );
        handle.join().unwrap();
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .futex_waiters
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn test_cooperative_read_timeout_wakes_consumer() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);
        let (tx, rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            let value = consumer
                .read_timeout(Duration::from_secs(1))
                .expect("timeout read failed");
            tx.send(value).unwrap();
        });

        wait_for_waiters(&producer, 1);
        producer.try_write(42).expect("write failed");

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
        handle.join().unwrap();
        assert_eq!(
            unsafe { producer.queue.header.as_ref() }
                .futex_waiters
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn test_clone_producer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let producer2 = producer.clone();

        producer.try_write(10).unwrap();
        producer2.try_write(20).unwrap();

        let mut values = Vec::new();
        while let Some(v) = consumer.try_read() {
            values.push(v);
        }
        values.sort_unstable();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_clone_consumer() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);
        let consumer2 = consumer.clone();

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let mut values = Vec::new();
        loop {
            let mut progressed = false;
            if let Some(v) = consumer.try_read() {
                values.push(v);
                progressed = true;
            }
            if let Some(v) = consumer2.try_read() {
                values.push(v);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        values.sort_unstable();
        assert_eq!(values, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_cross_role_joins() {
        let (_file, producer1, consumer1) = create_test_queue::<Item>(BUFFER_SIZE);
        let consumer2 = producer1.join_as_consumer();
        let producer2 = consumer2.join_as_producer();

        // Write two values.
        producer1.try_write(100).unwrap();
        producer2.try_write(200).unwrap();

        // Read two values.
        assert_eq!(consumer2.try_read().unwrap(), 100);
        assert_eq!(consumer1.try_read().unwrap(), 200);
    }

    #[test]
    fn test_drop_original_mapping_stays_alive() {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::<Item>::create(&file, BUFFER_SIZE) }.expect("create failed");
        let consumer = producer.join_as_consumer();
        let producer2 = producer.clone();

        // Drop the original producer — the mapping stays alive via Arc.
        drop(producer);

        producer2.try_write(42).unwrap();
        assert_eq!(consumer.try_read(), Some(42));
    }

    #[test]
    fn test_pair_creates_in_process_queue() {
        let (producer, consumer) = pair::<u64>(64).expect("pair failed");

        for value in [10, 20, 30, 40] {
            producer.try_write(value).expect("write failed");
        }

        for value in [10, 20, 30, 40] {
            assert_eq!(consumer.try_read(), Some(value));
        }
    }

    #[test]
    fn test_cooperative_pair_reuses_descriptor_slots_after_wrap() {
        let (producer, consumer) =
            cooperative_pair::<u64>(4, COOPERATIVE_OPTIONS).expect("pair failed");

        for value in 0..16 {
            producer.try_write(value).expect("write failed");
            assert_eq!(consumer.try_read(), Some(value));
        }
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_pair_clone_roles() {
        let (producer, consumer) = pair::<u64>(64).expect("pair failed");
        let producer2 = producer.clone();
        let consumer2 = consumer.clone();

        producer.try_write(1).expect("write failed");
        producer2.try_write(2).expect("write failed");

        let mut values = Vec::new();
        loop {
            let mut progressed = false;
            if let Some(value) = consumer.try_read() {
                values.push(value);
                progressed = true;
            }
            if let Some(value) = consumer2.try_read() {
                values.push(value);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }

        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn test_consumer_recover_as_exclusive_lossy() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let guard = consumer.reserve_read().expect("reserve read");
        assert_eq!(*guard.as_ref(), 0);
        core::mem::forget(guard);

        unsafe {
            consumer.recover_as_exclusive_lossy();
        }

        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(consumer.try_read(), Some(3));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_consumer_recover_as_exclusive() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let guard = consumer.reserve_read().expect("reserve read");
        assert_eq!(*guard.as_ref(), 0);
        core::mem::forget(guard);

        unsafe {
            consumer.recover_as_exclusive();
        }

        assert_eq!(consumer.try_read(), Some(0));
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(consumer.try_read(), Some(3));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_cooperative_consumer_recover_as_exclusive_lossy() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let guard = consumer.reserve_read().expect("reserve read");
        assert_eq!(*guard.as_ref(), 0);
        core::mem::forget(guard);

        unsafe {
            consumer.recover_as_exclusive_lossy();
        }

        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(consumer.try_read(), Some(3));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_cooperative_consumer_recover_as_exclusive() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        for i in 0..4 {
            producer.try_write(i).unwrap();
        }

        let guard = consumer.reserve_read().expect("reserve read");
        assert_eq!(*guard.as_ref(), 0);
        core::mem::forget(guard);

        unsafe {
            consumer.recover_as_exclusive();
        }

        assert_eq!(consumer.try_read(), Some(0));
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(consumer.try_read(), Some(3));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_producer_recover_as_exclusive() {
        let (_file, producer, consumer) = create_test_queue::<Item>(BUFFER_SIZE);

        producer.try_write(10).unwrap();

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve write");
        unsafe {
            guard.as_mut_ptr().write(99);
        }
        core::mem::forget(guard);

        unsafe {
            producer.recover_as_exclusive();
        }

        producer.try_write(20).unwrap();

        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn test_cooperative_producer_recover_as_exclusive() {
        let (_file, producer, consumer) = create_test_queue_with_options::<
            Item,
            { MODE_COOPERATIVE },
        >(COOPERATIVE_BUFFER_SIZE, COOPERATIVE_OPTIONS);

        producer.try_write(10).unwrap();

        let mut guard = unsafe { producer.reserve_write() }.expect("reserve write");
        unsafe {
            guard.as_mut_ptr().write(99);
        }
        core::mem::forget(guard);

        unsafe {
            producer.recover_as_exclusive();
        }

        producer.try_write(20).unwrap();

        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(consumer.try_read(), Some(20));
        assert_eq!(consumer.try_read(), None);
    }
}
