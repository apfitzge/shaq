use core::ptr::{addr_of, addr_of_mut, NonNull};

const CACHELINE_SIZE: usize = 64;
pub(super) const PAYLOADS_PER_RING_ENTRY: usize = 2;

const NO_PAYLOAD: u32 = 0;

/// Per-payload metadata storage.
///
/// The two stack fields are addressed by stack offset, while
/// `retired_sequence` is addressed by payload offset. Keeping them together
/// preserves the old 16-byte-per-payload metadata budget.
#[repr(C)]
pub(super) struct PayloadMetadata {
    free_stack_slot: u32,
    retired_stack_slot: u32,
    retired_sequence: usize,
}

impl PayloadMetadata {
    const fn new(free_stack_slot: u32) -> Self {
        Self {
            free_stack_slot,
            retired_stack_slot: 0,
            retired_sequence: 0,
        }
    }
}

/// Ring payload handle: zero means no payload; otherwise `payload_index + 1`.
#[derive(Clone, Copy, PartialEq)]
#[repr(transparent)]
pub(super) struct PayloadHandle(pub(super) u64);

impl PayloadHandle {
    pub(super) const EMPTY: Self = Self(0);

    #[inline]
    fn new(payload_index: u32) -> Self {
        Self(stored_payload_index(payload_index) as u64)
    }

    #[inline]
    fn payload_index(self) -> Option<u32> {
        payload_index_from_stored(self.0 as u32)
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReservedPayloads {
    first_stack_offset: usize,
    first_payload_index: u32,
    count: usize,
}

impl ReservedPayloads {
    #[inline]
    pub(super) fn first_payload_index(self) -> u32 {
        self.first_payload_index
    }

    #[inline]
    pub(super) fn len(self) -> usize {
        self.count
    }
}

#[derive(Clone, Copy)]
pub(super) struct ProtectedPayload {
    payload_index: u32,
}

impl ProtectedPayload {
    pub(super) const EMPTY: Self = Self { payload_index: 0 };

    #[inline]
    fn new(payload_index: u32) -> Self {
        Self { payload_index }
    }

    #[inline]
    fn payload_index(self) -> u32 {
        self.payload_index
    }
}

#[derive(Clone, Copy)]
pub(super) struct RetiredPayload {
    lane: usize,
    sequence: usize,
}

impl RetiredPayload {
    #[inline]
    pub(super) fn new(lane: usize, sequence: usize) -> Self {
        Self { lane, sequence }
    }

    #[inline]
    pub(super) fn lane(self) -> usize {
        self.lane
    }

    #[inline]
    pub(super) fn sequence(self) -> usize {
        self.sequence
    }
}

pub(super) struct PayloadPool<T> {
    free_tops: NonNull<u32>,
    retired_tops: NonNull<u32>,
    payload_metadata: NonNull<PayloadMetadata>,
    payloads: NonNull<T>,
    lane_capacity: usize,
    lane_mask: usize,
    lane_shift: u32,
    metadata_lane_stride_bytes: usize,
    payload_lane_stride_bytes: usize,
    producer_slots: usize,
    capacity: usize,
}

impl<T> Clone for PayloadPool<T> {
    fn clone(&self) -> Self {
        Self {
            free_tops: self.free_tops,
            retired_tops: self.retired_tops,
            payload_metadata: self.payload_metadata,
            payloads: self.payloads,
            lane_capacity: self.lane_capacity,
            lane_mask: self.lane_mask,
            lane_shift: self.lane_shift,
            metadata_lane_stride_bytes: self.metadata_lane_stride_bytes,
            payload_lane_stride_bytes: self.payload_lane_stride_bytes,
            producer_slots: self.producer_slots,
            capacity: self.capacity,
        }
    }
}

impl<T> PayloadPool<T> {
    #[inline]
    pub(super) fn new(
        free_tops: NonNull<u32>,
        retired_tops: NonNull<u32>,
        payload_metadata: NonNull<PayloadMetadata>,
        payloads: NonNull<T>,
        lane_capacity: usize,
        metadata_lane_stride_bytes: usize,
        payload_lane_stride_bytes: usize,
        producer_slots: usize,
    ) -> Self {
        debug_assert!(lane_capacity.is_power_of_two());
        let capacity = lane_capacity
            .checked_mul(producer_slots)
            .expect("payload capacity overflow");
        Self {
            free_tops,
            retired_tops,
            payload_metadata,
            payloads,
            lane_capacity,
            lane_mask: lane_capacity - 1,
            lane_shift: lane_capacity.trailing_zeros(),
            metadata_lane_stride_bytes,
            payload_lane_stride_bytes,
            producer_slots,
            capacity,
        }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(super) fn reserve_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        self.take_free_payloads_exact(0, count)
    }

    #[inline(always)]
    pub(super) fn take_free_payloads_exact(
        &self,
        lane: usize,
        count: usize,
    ) -> Option<ReservedPayloads> {
        if count == 0 {
            return None;
        }

        self.pop_free_payloads_exact(lane, count)
    }

    #[cfg(test)]
    pub(super) fn cancel_reserved_payloads(&self, chain: ReservedPayloads) {
        self.restore_reserved_payloads(0, chain);
    }

    #[inline(always)]
    pub(super) fn restore_reserved_payloads(&self, lane: usize, chain: ReservedPayloads) {
        debug_assert!(chain.count != 0);
        debug_assert_eq!(self.read_free_top(lane), chain.first_stack_offset);
        self.write_free_top(lane, chain.first_stack_offset + chain.count);
    }

    #[inline(always)]
    pub(super) fn handle_for_payload(&self, payload_index: u32) -> PayloadHandle {
        debug_assert!((payload_index as usize) < self.capacity);
        PayloadHandle::new(payload_index)
    }

    pub(super) fn payload_for_protected_handle(
        &self,
        handle: PayloadHandle,
    ) -> Option<ProtectedPayload> {
        self.valid_payload_index(handle).map(ProtectedPayload::new)
    }

    #[cfg(test)]
    pub(super) fn retire(&self, handle: PayloadHandle, retired: RetiredPayload) {
        self.retire_in_lane(retired.lane(), handle, retired);
    }

    #[inline(always)]
    pub(super) fn retire_in_lane(
        &self,
        lane: usize,
        handle: PayloadHandle,
        retired: RetiredPayload,
    ) {
        let Some(payload_index) = self.valid_payload_index(handle) else {
            return;
        };
        debug_assert_eq!(lane, retired.lane());
        debug_assert_eq!(self.lane_for_payload(payload_index), lane);

        let retired_top = self.read_retired_top(lane);
        debug_assert!(retired_top < self.lane_capacity);
        self.write_retired_sequence(lane, payload_index, retired.sequence);
        self.write_retired_stack_slot(lane, retired_top, payload_index);
        self.write_retired_top(lane, retired_top + 1);
    }

    #[inline]
    pub(super) fn retired_payload_count(&self, lane: usize) -> usize {
        self.read_retired_top(lane)
    }

    #[cfg(test)]
    pub(super) fn reclaim_retired_payloads<F>(&self, is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        self.reclaim_retired_payloads_in_lane(0, is_protected);
    }

    pub(super) fn reclaim_retired_payloads_in_lane<F>(&self, lane: usize, mut is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        let retired_top = self.read_retired_top(lane);
        if retired_top == 0 {
            return;
        }

        let mut kept = 0usize;
        let mut free_top = self.read_free_top(lane);
        for offset in 0..retired_top {
            let payload_index = self.read_retired_stack_slot(lane, offset);
            let sequence = self.read_retired_sequence(lane, payload_index);

            if is_protected(RetiredPayload::new(lane, sequence)) {
                if kept != offset {
                    self.write_retired_stack_slot(lane, kept, payload_index);
                }
                kept += 1;
            } else {
                debug_assert!(free_top < self.lane_capacity);
                self.write_free_stack_slot(lane, free_top, payload_index);
                free_top += 1;
            }
        }

        self.write_free_top(lane, free_top);
        self.write_retired_top(lane, kept);
    }

    #[inline]
    pub(super) fn protected_payload_ptr(&self, payload: ProtectedPayload) -> NonNull<T> {
        self.payload_at(payload.payload_index())
    }

    #[inline]
    pub(super) fn payload_at(&self, payload_index: u32) -> NonNull<T> {
        debug_assert!((payload_index as usize) < self.capacity);
        let (lane, offset) = self.lane_and_offset(payload_index);
        self.payload_at_offset(lane, offset)
    }

    #[inline(always)]
    pub(super) fn payload_at_in_lane(&self, lane: usize, payload_index: u32) -> NonNull<T> {
        debug_assert!((payload_index as usize) < self.capacity);
        debug_assert_eq!(self.lane_for_payload(payload_index), lane);
        self.payload_at_offset(lane, self.payload_offset(payload_index))
    }

    #[inline(always)]
    pub(super) fn reserved_payload_at(
        &self,
        lane: usize,
        chain: ReservedPayloads,
        index: usize,
    ) -> u32 {
        debug_assert!(index < chain.count);
        let stack_offset = chain.first_stack_offset + chain.count - 1 - index;
        let payload_index = self.read_free_stack_slot(lane, stack_offset);
        debug_assert_eq!(self.lane_for_payload(payload_index), lane);
        payload_index
    }

    #[inline(always)]
    fn payload_at_offset(&self, lane: usize, offset: usize) -> NonNull<T> {
        debug_assert!(lane < self.producer_slots);
        debug_assert!(offset < self.lane_capacity);
        let byte_offset =
            lane * self.payload_lane_stride_bytes + offset * core::mem::size_of::<T>();
        // SAFETY: callers only pass validated payload indices.
        unsafe { self.payloads.cast::<u8>().byte_add(byte_offset).cast() }
    }

    pub(super) fn recover_as_exclusive(&self) {
        self.reset_stacks();
    }

    fn valid_payload_index(&self, handle: PayloadHandle) -> Option<u32> {
        let payload_index = handle.payload_index()?;
        if payload_index as usize >= self.capacity {
            None
        } else {
            Some(payload_index)
        }
    }

    #[inline]
    fn free_top(&self, lane: usize) -> NonNull<u32> {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: `free_tops` points into the live per-lane free-top table.
        unsafe {
            self.free_tops
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<u32>()
        }
    }

    #[inline]
    fn retired_top(&self, lane: usize) -> NonNull<u32> {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: `retired_tops` points into the live per-lane retired-top table.
        unsafe {
            self.retired_tops
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<u32>()
        }
    }

    #[inline]
    fn read_free_top(&self, lane: usize) -> usize {
        // SAFETY: this producer owns `lane`; consumers never read pool stack tops.
        unsafe { self.free_top(lane).as_ptr().read() as usize }
    }

    #[inline]
    fn write_free_top(&self, lane: usize, top: usize) {
        debug_assert!(top <= self.lane_capacity);
        // SAFETY: this producer owns `lane`; consumers never read pool stack tops.
        unsafe {
            self.free_top(lane)
                .as_ptr()
                .write(u32::try_from(top).expect("payload stack top fits u32"))
        };
    }

    #[inline]
    fn read_retired_top(&self, lane: usize) -> usize {
        // SAFETY: this producer owns `lane`; consumers never read pool stack tops.
        unsafe { self.retired_top(lane).as_ptr().read() as usize }
    }

    #[inline]
    fn write_retired_top(&self, lane: usize, top: usize) {
        debug_assert!(top <= self.lane_capacity);
        // SAFETY: this producer owns `lane`; consumers never read pool stack tops.
        unsafe {
            self.retired_top(lane)
                .as_ptr()
                .write(u32::try_from(top).expect("payload stack top fits u32"))
        };
    }

    #[inline]
    fn pop_free_payloads_exact(&self, lane: usize, count: usize) -> Option<ReservedPayloads> {
        debug_assert!(count != 0);
        debug_assert!(count <= self.lane_capacity);

        let top = self.read_free_top(lane);
        if top < count {
            return None;
        }

        let first_stack_offset = top - count;
        let first_payload_index = self.read_free_stack_slot(lane, top - 1);
        self.write_free_top(lane, first_stack_offset);
        Some(ReservedPayloads {
            first_stack_offset,
            first_payload_index,
            count,
        })
    }

    #[inline(always)]
    fn read_free_stack_slot(&self, lane: usize, stack_offset: usize) -> u32 {
        let metadata = self.metadata_at(lane, stack_offset);
        // SAFETY: this producer owns `lane`; consumers never read pool stacks.
        unsafe { addr_of!((*metadata.as_ptr()).free_stack_slot).read() }
    }

    #[inline(always)]
    fn write_free_stack_slot(&self, lane: usize, stack_offset: usize, payload_index: u32) {
        debug_assert_eq!(self.lane_for_payload(payload_index), lane);
        let metadata = self.metadata_at(lane, stack_offset);
        // SAFETY: this producer owns `lane`; consumers never read pool stacks.
        unsafe { addr_of_mut!((*metadata.as_ptr()).free_stack_slot).write(payload_index) };
    }

    #[inline(always)]
    fn read_retired_stack_slot(&self, lane: usize, stack_offset: usize) -> u32 {
        let metadata = self.metadata_at(lane, stack_offset);
        // SAFETY: this producer owns `lane`; consumers never read pool stacks.
        unsafe { addr_of!((*metadata.as_ptr()).retired_stack_slot).read() }
    }

    #[inline(always)]
    fn write_retired_stack_slot(&self, lane: usize, stack_offset: usize, payload_index: u32) {
        debug_assert_eq!(self.lane_for_payload(payload_index), lane);
        let metadata = self.metadata_at(lane, stack_offset);
        // SAFETY: this producer owns `lane`; consumers never read pool stacks.
        unsafe { addr_of_mut!((*metadata.as_ptr()).retired_stack_slot).write(payload_index) };
    }

    #[inline(always)]
    fn read_retired_sequence(&self, lane: usize, payload_index: u32) -> usize {
        let metadata = self.metadata_at(lane, self.payload_offset(payload_index));
        // SAFETY: this producer owns `lane`; consumers never read retired metadata.
        unsafe { addr_of!((*metadata.as_ptr()).retired_sequence).read() }
    }

    #[inline(always)]
    fn write_retired_sequence(&self, lane: usize, payload_index: u32, sequence: usize) {
        let metadata = self.metadata_at(lane, self.payload_offset(payload_index));
        // SAFETY: this producer owns `lane`; consumers never read retired metadata.
        unsafe { addr_of_mut!((*metadata.as_ptr()).retired_sequence).write(sequence) };
    }

    fn reset_stacks(&self) {
        for lane in 0..self.producer_slots {
            for stack_offset in 0..self.lane_capacity {
                let payload_index = self.payload_index(lane, self.lane_capacity - 1 - stack_offset);
                self.write_free_stack_slot(lane, stack_offset, payload_index);
                self.write_retired_stack_slot(lane, stack_offset, self.payload_index(lane, 0));
                self.write_retired_sequence(lane, self.payload_index(lane, stack_offset), 0);
            }

            self.write_free_top(lane, self.lane_capacity);
            self.write_retired_top(lane, 0);
        }
    }

    #[inline(always)]
    fn metadata_at(&self, lane: usize, offset: usize) -> NonNull<PayloadMetadata> {
        debug_assert!(lane < self.producer_slots);
        debug_assert!(offset < self.lane_capacity);
        let byte_offset = lane * self.metadata_lane_stride_bytes
            + offset * core::mem::size_of::<PayloadMetadata>();
        // SAFETY: callers only pass validated stack or payload offsets.
        unsafe {
            self.payload_metadata
                .cast::<u8>()
                .byte_add(byte_offset)
                .cast::<PayloadMetadata>()
        }
    }

    #[inline]
    fn lane_and_offset(&self, payload_index: u32) -> (usize, usize) {
        let payload_index = payload_index as usize;
        (
            payload_index >> self.lane_shift,
            payload_index & self.lane_mask,
        )
    }

    #[inline]
    fn lane_for_payload(&self, payload_index: u32) -> usize {
        (payload_index as usize) >> self.lane_shift
    }

    #[inline(always)]
    fn payload_offset(&self, payload_index: u32) -> usize {
        (payload_index as usize) & self.lane_mask
    }

    #[inline]
    fn payload_index(&self, lane: usize, offset: usize) -> u32 {
        debug_assert!(lane < self.producer_slots);
        debug_assert!(offset < self.lane_capacity);
        ((lane << self.lane_shift) | offset) as u32
    }
}

#[inline]
pub(super) fn capacity_for_ring_capacity(ring_capacity: usize) -> Option<usize> {
    let capacity = ring_capacity.checked_mul(PAYLOADS_PER_RING_ENTRY)?;
    if capacity == 0 || capacity > u32::MAX as usize {
        None
    } else {
        Some(capacity)
    }
}

#[inline]
pub(super) fn initial_free_top(lane_capacity: usize) -> u32 {
    u32::try_from(lane_capacity).expect("payload stack top fits u32")
}

#[inline]
pub(super) const fn initial_retired_top() -> u32 {
    0
}

pub(super) unsafe fn initialize_payload_metadata(
    payload_metadata: NonNull<PayloadMetadata>,
    lane_capacity: usize,
    metadata_lane_stride_bytes: usize,
    producer_slots: usize,
) {
    for lane in 0..producer_slots {
        for stack_offset in 0..lane_capacity {
            let payload_index =
                ((lane * lane_capacity) + (lane_capacity - 1 - stack_offset)) as u32;
            let byte_offset = lane * metadata_lane_stride_bytes
                + stack_offset * core::mem::size_of::<PayloadMetadata>();
            // SAFETY: caller guarantees `payload_metadata` points at the padded
            // per-lane metadata layout.
            unsafe {
                payload_metadata
                    .cast::<u8>()
                    .byte_add(byte_offset)
                    .cast::<PayloadMetadata>()
                    .as_ptr()
                    .write(PayloadMetadata::new(payload_index))
            };
        }
    }
}

/// Payload references stored in `PayloadHandle`.
/// Zero means no payload; non-zero values are `payload_index + 1`.
#[inline]
fn stored_payload_index(payload_index: u32) -> u32 {
    debug_assert!(payload_index != u32::MAX, "payload index overflow");
    payload_index.wrapping_add(1)
}

#[inline]
fn payload_index_from_stored(stored: u32) -> Option<u32> {
    if stored == NO_PAYLOAD {
        None
    } else {
        Some(payload_index_from_non_null_stored(stored))
    }
}

#[inline]
fn payload_index_from_non_null_stored(stored: u32) -> u32 {
    debug_assert!(stored != NO_PAYLOAD, "no payload");
    stored.wrapping_sub(1)
}

#[cfg(test)]
mod tests {
    use super::{
        initial_free_top, initial_retired_top, initialize_payload_metadata, PayloadHandle,
        PayloadMetadata, PayloadPool, RetiredPayload,
    };
    use core::{
        mem::{size_of, MaybeUninit},
        ptr::NonNull,
    };

    fn initialized_pool(
        capacity: usize,
    ) -> (u32, u32, Vec<PayloadMetadata>, Vec<MaybeUninit<u64>>) {
        let free_top = initial_free_top(capacity);
        let retired_top = initial_retired_top();
        let mut payload_metadata = Vec::with_capacity(capacity);
        let mut payloads = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            payloads.push(MaybeUninit::uninit());
        }

        let payload_metadata_ptr =
            NonNull::new(payload_metadata.as_mut_ptr()).expect("non-empty payload metadata slice");
        // SAFETY: `payload_metadata` has capacity for `capacity` entries and we
        // set its length immediately after initialization.
        unsafe {
            initialize_payload_metadata(
                payload_metadata_ptr,
                capacity,
                capacity * size_of::<PayloadMetadata>(),
                1,
            );
            payload_metadata.set_len(capacity);
        }

        (free_top, retired_top, payload_metadata, payloads)
    }

    fn pool<'a>(
        free_top: &'a mut u32,
        retired_top: &'a mut u32,
        payload_metadata: &'a [PayloadMetadata],
        payloads: &'a mut [MaybeUninit<u64>],
    ) -> PayloadPool<u64> {
        PayloadPool::new(
            NonNull::from(&mut *free_top),
            NonNull::from(&mut *retired_top),
            NonNull::new(payload_metadata.as_ptr().cast_mut())
                .expect("non-empty payload metadata slice"),
            NonNull::new(payloads.as_mut_ptr().cast()).expect("non-empty payload slice"),
            payload_metadata.len(),
            payload_metadata.len() * size_of::<PayloadMetadata>(),
            payload_metadata.len() * size_of::<u64>(),
            1,
        )
    }

    #[test]
    fn payload_metadata_uses_compact_layout() {
        assert_eq!(size_of::<PayloadMetadata>(), 16);
    }

    #[test]
    fn reserve_payloads_exact_removes_requested_prefix() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(4);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );

        let first = pool.reserve_payloads_exact(2).expect("first reserve");
        assert_eq!(first.first_payload_index(), 0);
        assert_eq!(pool.reserved_payload_at(0, first, 1), 1);
        assert_eq!(free_top, 2);

        let second = pool.reserve_payloads_exact(2).expect("second reserve");
        assert_eq!(second.first_payload_index(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn cancel_reserved_payloads_restores_stack_to_pool() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(4);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );

        let removed = pool.reserve_payloads_exact(2).expect("reserve");
        pool.cancel_reserved_payloads(removed);

        let all = pool.reserve_payloads_exact(4).expect("reserve all");
        assert_eq!(all.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn reclaim_retired_payloads_restores_reusable_payloads() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(4);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );
        let _reserved = pool.reserve_payloads_exact(4).expect("reserve all");

        pool.retire(pool.handle_for_payload(2), RetiredPayload::new(0, 12));
        pool.retire(pool.handle_for_payload(0), RetiredPayload::new(0, 10));
        pool.reclaim_retired_payloads(|_| false);

        let chain = pool.reserve_payloads_exact(2).expect("reserve released");
        assert_eq!(chain.len(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn protected_retired_payload_blocks_reuse_until_unprotected() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(1);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );

        let reserved = pool.reserve_payloads_exact(1).expect("reserve");
        let handle = pool.handle_for_payload(reserved.first_payload_index());
        pool.retire(handle, RetiredPayload::new(0, 5));
        pool.reclaim_retired_payloads(|retired| retired.sequence() == 5);
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.reclaim_retired_payloads(|_| false);
        assert!(pool.reserve_payloads_exact(1).is_some());
    }

    #[test]
    fn invalid_handle_does_not_resolve_to_payload() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(1);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );

        assert!(pool
            .payload_for_protected_handle(PayloadHandle::EMPTY)
            .is_none());
    }

    #[test]
    fn recover_as_exclusive_restores_all_payloads() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pool(4);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );
        assert!(pool.reserve_payloads_exact(4).is_some());
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.recover_as_exclusive();

        let chain = pool.reserve_payloads_exact(4).expect("reserve after reset");
        assert_eq!(chain.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }
}
