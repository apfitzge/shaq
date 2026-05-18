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
//! Broadcast payloads are treated as shared-memory bytes. Values overwritten in
//! the ring are not dropped, and high-level writes copy payload bytes into the
//! ring and forget the source value. By-value reads/writes and the atomic chunk
//! helpers therefore require byte-copyable shared-memory data: no `Drop`, no
//! uninitialized bytes including padding, and no process-local pointers unless
//! every process that reads them can validly use them. Raw pointer reads that
//! materialize `T` before validation additionally require every possible
//! old/new/mixed snapshot to be valid to temporarily inspect as `T`.

use crate::{error::Error, normalized_capacity, shmem::Region, CacheAlignedAtomicSize, VERSION};
use core::{
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ptr::{self, NonNull},
    sync::atomic::{fence, AtomicU64, AtomicU8, Ordering},
};
use std::{fs::File, sync::Arc};

/// Unique identifier for broadcast queue in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");

pub struct Producer<T> {
    queue: SharedQueue<T>,
}

impl<T> Producer<T> {
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
    /// - All producers and consumers for the same file must use the same `T` and
    ///   uphold the module-level payload contract. The queue does not validate
    ///   `T` across processes.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, file_size) }?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - `file` must refer to a live initialized broadcast queue and must not be
    ///   concurrently truncated or resized while joined.
    /// - All producers and consumers for the same file must use the same `T` and
    ///   uphold the module-level payload contract. The queue does not validate
    ///   `T` across processes.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization.
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
    pub fn join_as_consumer(&self) -> Consumer<T> {
        Consumer::from_queue(self.queue.clone())
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
    /// This may wait behind earlier producers that have reserved slots but not
    /// yet published them. Holding a [`WriteGuard`] or [`WriteBatch`] should be
    /// treated similarly to holding a lock on a critical section.
    ///
    /// The value is copied into the reserved slot with [`write_atomic_chunks`]
    /// before the slot is published to consumers. `ordering` applies to the
    /// payload stores only; queue publication still uses the queue's internal
    /// release ordering.
    ///
    /// This relies on the constructor payload contract for by-value writes.
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
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in order they were reserved. This call may wait behind earlier
    /// unpublished reservations. Holding a [`WriteGuard`] should be treated
    /// similarly to holding a lock on a critical section.
    ///
    /// # Safety
    /// - Before the guard is dropped, the slot bytes must be initialized and
    ///   valid to publish under the constructor payload contract.
    /// - Payload access that may race with consumers must use atomics or
    ///   external synchronization.
    #[must_use]
    pub unsafe fn reserve_write(&self) -> Option<WriteGuard<'_, T>> {
        self.queue
            .reserve_write()
            .map(|(cell, position)| WriteGuard {
                header: self.queue.header,
                cell,
                start: position,
                _marker: PhantomData,
            })
    }

    /// Reserves exactly `count` slots for writing.
    /// The slots are committed when the batch is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in the order they were reserved. This call may wait behind
    /// earlier unpublished reservations. Holding a [`WriteBatch`] should be
    /// treated similarly to holding a lock on a critical section.
    ///
    /// # Safety
    /// - Before the batch is dropped, all reserved slot bytes must be initialized
    ///   and valid to publish under the constructor payload contract.
    /// - Payload access that may race with consumers must use atomics or
    ///   external synchronization.
    #[must_use]
    pub unsafe fn reserve_write_batch(&self, count: usize) -> Option<WriteBatch<'_, T>> {
        let start = self.queue.reserve_write_batch(count)?;
        Some(WriteBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start,
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
        header
            .producer_reservation
            .store(publication, Ordering::Release);
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send + Sync> Sync for Producer<T> {}

pub struct Consumer<T> {
    queue: SharedQueue<T>,
    next: usize,
}

impl<T> Consumer<T> {
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
    /// - All producers and consumers for the same file must use the same `T` and
    ///   uphold the module-level payload contract. The queue does not validate
    ///   `T` across processes.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) = unsafe { SharedQueueHeader::create::<T>(file, file_size) }?;
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
    /// - All producers and consumers for the same file must use the same `T` and
    ///   uphold the module-level payload contract. The queue does not validate
    ///   `T` across processes.
    /// - Payload access that may race with an overwrite must use the atomic
    ///   payload APIs or external synchronization.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        let queue = unsafe { SharedQueue::from_header(region, header) }?;
        Ok(Self::from_queue(queue))
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Producer<T> {
        Producer {
            queue: self.queue.clone(),
        }
    }

    fn from_queue(queue: SharedQueue<T>) -> Self {
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
        let skipped = published.wrapping_sub(self.next);
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
    /// This relies on the constructor payload contract for by-value reads.
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
    /// The returned guard exposes a pointer into shared memory. Producers may
    /// overwrite that memory concurrently. Payload access through that pointer
    /// must use [`read_atomic_chunks`] or another atomic/external
    /// synchronization scheme if it may race with an overwrite. A racing atomic
    /// read may observe old data, new data, or a mixed old/new snapshot.
    ///
    /// Prefer [`Self::try_read`] for high-level reads. If using the raw pointer
    /// returned by [`DirectRead::as_ptr`], copy the payload with
    /// [`read_atomic_chunks`] or another payload-level atomic/external
    /// synchronization scheme before validation.
    ///
    /// # Safety
    /// - Payload access through the returned guard must follow the constructor
    ///   payload contract.
    /// - Copy or inspect the payload before calling [`DirectRead::validate`] or
    ///   [`DirectRead::commit`]. If validation reports a possible overwrite,
    ///   discard that already-taken snapshot.
    /// - Validation is not a pin. Do not read through the returned pointer
    ///   after validation or commit and assume the same slot contents remain.
    pub unsafe fn try_read_direct(&mut self) -> Result<Option<DirectRead<'_, T>>, usize> {
        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
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
    /// The returned guard exposes pointers into shared memory. Producers may
    /// overwrite that memory concurrently. Payload access through those pointers
    /// must use [`read_atomic_chunks`] or another atomic/external
    /// synchronization scheme if it may race with an overwrite. A racing atomic
    /// read may observe old data, new data, or a mixed old/new snapshot.
    ///
    /// If using the raw pointers returned by [`DirectReadBatch::as_ptr`], copy
    /// each payload with [`read_atomic_chunks`] or another payload-level
    /// atomic/external synchronization scheme before validation.
    ///
    /// # Safety
    /// - Payload access through the returned guard must follow the constructor
    ///   payload contract.
    /// - Copy or inspect the payloads before calling
    ///   [`DirectReadBatch::validate`] or [`DirectReadBatch::commit`]. If
    ///   validation reports a possible overwrite, discard those already-taken
    ///   snapshots.
    /// - Validation is not a pin. Do not read through the returned pointers
    ///   after validation or commit and assume the same slot contents remain.
    pub unsafe fn try_read_direct_batch(
        &mut self,
        max: usize,
    ) -> Result<Option<DirectReadBatch<'_, T>>, usize> {
        if max == 0 {
            return Ok(None);
        }

        let start = self.next;
        let published = self.queue.published();
        let available = published.wrapping_sub(start);
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

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            next: self.next,
        }
    }
}

unsafe impl<T: Send> Send for Consumer<T> {}
unsafe impl<T: Send + Sync> Sync for Consumer<T> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
pub const fn minimum_file_size<T>(capacity: usize) -> usize {
    let buffer_offset = SharedQueueHeader::buffer_offset::<T>();
    buffer_offset + normalized_capacity(capacity) * core::mem::size_of::<T>()
}

/// Calculates the minimum region size required for a queue with given capacity.
pub const fn minimum_region_size<T>(capacity: usize) -> usize {
    minimum_file_size::<T>(capacity)
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
/// - `ordering` must be valid for atomic loads. `Release` and `AcqRel` will
///   panic through the standard atomic APIs.
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
/// - If `dst` currently contains an initialized value, bytewise overwrite must
///   uphold the caller's ownership and validity invariants for that value.
/// - Any concurrent reads from the destination bytes must also use compatible
///   atomic accesses, or be protected by external synchronization.
/// - `ordering` must be valid for atomic stores. `Acquire` and `AcqRel` will
///   panic through the standard atomic APIs.
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

struct SharedQueue<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    buffer_mask: usize,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<Region>,
}

#[derive(Clone, Copy)]
struct WindowOverrun {
    skipped: usize,
    next: usize,
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            buffer: self.buffer,
            buffer_mask: self.buffer_mask,
            region: Arc::clone(&self.region),
        }
    }
}

impl<T> SharedQueue<T> {
    #[inline]
    fn overrun(&self, start: usize, cursor: usize) -> usize {
        cursor.wrapping_sub(start).wrapping_sub(self.capacity())
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    #[inline]
    fn published(&self) -> usize {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        unsafe { self.header.as_ref() }
            .producer_publication
            .load(Ordering::Acquire)
    }

    #[inline]
    fn reserved(&self) -> usize {
        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        unsafe { self.header.as_ref() }
            .producer_reservation
            .load(Ordering::Acquire)
    }

    #[inline]
    fn oldest_available_at_reserved(&self, reserved: usize) -> usize {
        reserved.saturating_sub(self.capacity())
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

    fn reserve_write(&self) -> Option<(NonNull<T>, usize)> {
        let position = self.reserve_write_batch(1)?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_write_batch(&self, count: usize) -> Option<usize> {
        if count == 0 {
            return None;
        }

        let capacity = self.capacity();
        if count > capacity {
            return None;
        }

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
                    return Some(producer_reservation);
                }
                Err(current) => {
                    producer_reservation = current;
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
        if reserved.wrapping_sub(start) > self.capacity() {
            return Err(self.overrun_at_reserved(start, reserved));
        }
        if reserved.wrapping_sub(start.wrapping_add(count)) > self.capacity() {
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
        if !buffer_size_in_items.is_power_of_two()
            || buffer_size_in_items == 0
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(region.size())?
                != buffer_size_in_items
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - allocation at `header` is large enough to hold the header and the buffer.
        let buffer = unsafe { Self::buffer_from_header(header) };
        Ok(Self {
            header,
            buffer,
            region,
            buffer_mask,
        })
    }

    /// Gets a pointer to the buffer following the header.
    ///
    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must be of sufficient size to hold the
    ///   header and padding bytes to align the trailing buffer of `T`.
    unsafe fn buffer_from_header(header: NonNull<SharedQueueHeader>) -> NonNull<T> {
        let buffer_offset = SharedQueueHeader::buffer_offset::<T>();

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

    /// Producer reservation cursor.
    ///
    /// Producers atomically advance this with CAS to claim slots, but claimed
    /// writes are not visible to consumers until `producer_publication` is
    /// advanced.
    producer_reservation: CacheAlignedAtomicSize,
    /// Producer publication cursor.
    ///
    /// Producers advance this in-order after filling reserved slots. Consumers
    /// use it to determine which values are currently retained in the ring.
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
    unsafe fn create<T>(file: &File, size: usize) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        file.set_len(size as u64)?;

        let buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(size)?;
        let region = Region::map_file(file, size)?;
        let header = region.addr().cast::<Self>();
        // SAFETY: The header is non-null and aligned properly.
        //         Alignment is guaranteed because mmap ensures that the
        //         memory is aligned to the page size, which is sufficient for the
        //         alignment of `SharedQueueHeader`.
        //         Access is exclusive because the caller guarantees this region
        //         is initialized at most once.
        unsafe { Self::initialize(header, buffer_size_in_items) };
        Ok((region, header))
    }

    const fn buffer_offset<T>() -> usize {
        const {
            assert!(
                core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
                "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
            )
        }

        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<T>())
    }

    const fn calculate_buffer_size_in_items<T>(file_size: usize) -> Result<usize, Error> {
        const {
            assert!(
                core::mem::size_of::<T>() > 0,
                "zero-sized types are not supported"
            )
        }

        let buffer_offset = Self::buffer_offset::<T>();
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

        // The buffer mask is stored as u32, so the capacity must fit.
        if buffer_size_in_items > u32::MAX as usize + 1 {
            return Err(Error::InvalidBufferSize);
        }

        Ok(buffer_size_in_items)
    }

    /// Initializes the shared queue header.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `header` allocation must be large enough to hold the header and the buffer.
    /// - `access` to `header` must be unique when this is called.
    unsafe fn initialize(mut header_ptr: NonNull<Self>, buffer_size_in_items: usize) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header_ptr.as_mut() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);
        header.buffer_mask = u32::try_from(buffer_size_in_items - 1).unwrap();
        header.version = VERSION;
        header.magic.store(MAGIC, Ordering::Release);
    }

    fn join<T>(file: &File) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        let file_size = file.metadata()?.len() as usize;
        let expected_buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(file_size)?;
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
            let buffer_size_in_items = (header.buffer_mask as usize).wrapping_add(1);
            if buffer_size_in_items != expected_buffer_size_in_items {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok((region, header))
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
}

#[must_use]
pub struct WriteGuard<'a, T> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteGuard<'a, T> {
    /// Returns a raw write pointer to the reserved slot.
    ///
    /// Prefer [`Self::write_atomic`] unless the caller needs custom in-place
    /// initialization.
    ///
    /// # Safety
    /// Caller must use the pointer in a way that upholds
    /// [`Producer::reserve_write`]'s safety requirements.
    pub unsafe fn as_mut_ptr(&mut self) -> *mut T {
        self.cell.as_ptr()
    }

    /// Writes a value into the reserved slot with atomic chunk stores.
    ///
    /// `ordering` applies to the payload stores only; queue publication still
    /// uses the queue's internal release ordering.
    ///
    /// This relies on the constructor payload contract for by-value writes.
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
    /// # Safety
    /// No additional requirements beyond the constructor payload contract.
    /// This method uses relaxed payload stores; prefer [`Self::write_atomic`]
    /// when choosing an explicit ordering.
    pub unsafe fn write(self, value: T) {
        self.write_atomic(value, Ordering::Relaxed);
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, 1);
        }
    }
}

#[must_use]
pub struct WriteBatch<'a, T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: usize,
    buffer_mask: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteBatch<'a, T> {
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
    /// - Caller must use the pointer in a way that upholds
    ///   [`Producer::reserve_write_batch`]'s safety requirements.
    pub unsafe fn as_mut_ptr(&mut self, index: usize) -> *mut T {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
        // SAFETY: The position was reserved for writing.
        unsafe { self.buffer.add(position & self.buffer_mask).as_ptr() }
    }

    /// Writes a value into the slot at `index` with atomic chunk stores.
    ///
    /// `ordering` applies to the payload stores only; queue publication still
    /// uses the queue's internal release ordering when the batch is dropped.
    ///
    /// This relies on the constructor payload contract for by-value writes.
    ///
    /// # Safety
    /// - `index < count`
    /// - `ordering` must be valid for atomic stores.
    pub unsafe fn write_atomic(&mut self, index: usize, value: T, ordering: Ordering) {
        debug_assert!(index < self.count);
        let position = self.start.wrapping_add(index);
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
    /// # Safety
    /// - `index < count`
    /// - No additional requirements beyond the constructor payload contract.
    ///   This method uses relaxed payload stores.
    pub unsafe fn write(&mut self, index: usize, value: T) {
        // SAFETY: Caller upholds the index requirement.
        unsafe { self.write_atomic(index, value, Ordering::Relaxed) }
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, self.count);
        }
    }
}

#[must_use]
pub struct DirectRead<'a, T> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T>,
    start: usize,
}

impl<'a, T> DirectRead<'a, T> {
    /// Returns a raw pointer into the broadcast ring.
    ///
    /// Producers may overwrite this slot concurrently. Copy or inspect the
    /// payload according to the constructor payload contract, then call
    /// [`Self::validate`] or [`Self::commit`] before trusting the snapshot.
    /// Validation is not a pin.
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
                *self.next = self.start.wrapping_add(1);
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
pub struct DirectReadBatch<'a, T> {
    next: &'a mut usize,
    queue: &'a SharedQueue<T>,
    start: usize,
    count: usize,
}

impl<'a, T> DirectReadBatch<'a, T> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns a pointer into the broadcast ring.
    ///
    /// Producers may overwrite this slot concurrently. Copy or inspect the
    /// payload according to the constructor payload contract, then call
    /// [`Self::validate`] or [`Self::commit`] before trusting the snapshot.
    /// Validation is not a pin.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    pub unsafe fn as_ptr(&self, index: usize) -> *const T {
        debug_assert!(index < self.count);
        self.queue.ptr_at(self.start.wrapping_add(index))
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
                *self.next = self.start.wrapping_add(self.count);
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

    fn create_test_queue<T>(file_size: usize) -> (File, Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().unwrap();
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("Failed to create producer");
        let consumer = unsafe { Consumer::join(&file) }.expect("Failed to join consumer");

        (file, producer, consumer)
    }

    unsafe fn write_item<T>(producer: &Producer<T>, item: T) {
        assert!(producer.try_write(item, Ordering::Relaxed).is_ok());
    }

    unsafe fn read_item<T>(consumer: &mut Consumer<T>) -> Result<Option<T>, usize> {
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
}
