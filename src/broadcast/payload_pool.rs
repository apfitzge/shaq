use core::ptr::{addr_of, addr_of_mut, NonNull};

const CACHELINE_SIZE: usize = 64;
pub(super) const PAYLOADS_PER_RING_ENTRY: usize = 2;

pub(super) const EMPTY_PAYLOAD_HANDLE: u32 = u32::MAX;

/// Per-payload metadata storage.
///
/// The two stack fields are addressed by stack offset, while
/// `retired_sequence` is addressed by payload offset. Keeping them together
/// preserves the 16-byte-per-payload metadata budget.
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
    pub(super) payload_index: u32,
}

impl ProtectedPayload {
    pub(super) const EMPTY: Self = Self { payload_index: 0 };

    #[inline]
    fn new(payload_index: u32) -> Self {
        Self { payload_index }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RetiredPayload {
    pub(super) lane: usize,
    pub(super) sequence: usize,
}

impl RetiredPayload {
    #[inline]
    pub(super) fn new(lane: usize, sequence: usize) -> Self {
        Self { lane, sequence }
    }
}

pub(super) struct PayloadPool<T> {
    lane: usize,
    free_top: NonNull<u32>,
    retired_top: NonNull<u32>,
    metadata: NonNull<PayloadMetadata>,
    payloads: NonNull<T>,
    lane_capacity: usize,
    base_payload_index: u32,
}

impl<T> PayloadPool<T> {
    #[inline]
    pub(super) fn new(
        lane: usize,
        free_tops: NonNull<u32>,
        retired_tops: NonNull<u32>,
        payload_metadata: NonNull<PayloadMetadata>,
        payloads: NonNull<T>,
        lane_capacity: usize,
        producer_slots: usize,
    ) -> Self {
        debug_assert!(lane < producer_slots);
        debug_assert!(lane_capacity.is_power_of_two());
        debug_assert!(lane_capacity.checked_mul(producer_slots).is_some());
        let metadata_lane_stride_bytes = (lane_capacity * core::mem::size_of::<PayloadMetadata>())
            .next_multiple_of(CACHELINE_SIZE);
        let payload_lane_stride_bytes =
            (lane_capacity * core::mem::size_of::<T>()).next_multiple_of(CACHELINE_SIZE);

        let base_payload_index = lane
            .checked_mul(lane_capacity)
            .and_then(|index| u32::try_from(index).ok())
            .expect("payload index fits u32");

        // SAFETY: `lane` is bounded by `producer_slots`.
        let free_top = unsafe {
            free_tops
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<u32>()
        };
        // SAFETY: `lane` is bounded by `producer_slots`.
        let retired_top = unsafe {
            retired_tops
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<u32>()
        };
        // SAFETY: `lane` is bounded by `producer_slots`.
        let metadata = unsafe {
            payload_metadata
                .cast::<u8>()
                .byte_add(lane * metadata_lane_stride_bytes)
                .cast::<PayloadMetadata>()
        };
        // SAFETY: `lane` is bounded by `producer_slots`.
        let payloads = unsafe {
            payloads
                .cast::<u8>()
                .byte_add(lane * payload_lane_stride_bytes)
                .cast::<T>()
        };

        Self {
            lane,
            free_top,
            retired_top,
            metadata,
            payloads,
            lane_capacity,
            base_payload_index,
        }
    }

    #[cfg(test)]
    pub(super) fn reserve_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        self.take_free_payloads_exact(count)
    }

    #[inline(always)]
    pub(super) fn take_free_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        if count == 0 {
            return None;
        }

        self.pop_free_payloads_exact(count)
    }

    #[cfg(test)]
    pub(super) fn cancel_reserved_payloads(&self, chain: ReservedPayloads) {
        self.restore_reserved_payloads(chain);
    }

    #[inline(always)]
    pub(super) fn restore_reserved_payloads(&self, chain: ReservedPayloads) {
        debug_assert!(chain.count != 0);
        debug_assert_eq!(self.read_free_top(), chain.first_stack_offset);
        self.write_free_top(chain.first_stack_offset + chain.count);
    }

    #[inline(always)]
    pub(super) fn handle_for_payload(&self, payload_index: u32) -> u32 {
        debug_assert!(self.owns_payload(payload_index));
        debug_assert!(payload_index != EMPTY_PAYLOAD_HANDLE);
        payload_index
    }

    pub(super) fn payload_for_protected_handle(&self, handle: u32) -> Option<ProtectedPayload> {
        self.valid_payload_index(handle).map(ProtectedPayload::new)
    }

    #[cfg(test)]
    pub(super) fn retire(&self, handle: u32, retired: RetiredPayload) {
        self.retire_payload(handle, retired);
    }

    #[inline(always)]
    pub(super) fn retire_payload(&self, handle: u32, retired: RetiredPayload) {
        let Some(payload_index) = self.valid_payload_index(handle) else {
            return;
        };
        debug_assert_eq!(self.lane, retired.lane);

        let retired_top = self.read_retired_top();
        debug_assert!(retired_top < self.lane_capacity);
        self.write_retired_sequence(payload_index, retired.sequence);
        self.write_retired_stack_slot(retired_top, payload_index);
        self.write_retired_top(retired_top + 1);
    }

    #[inline]
    pub(super) fn retired_payload_count(&self) -> usize {
        self.read_retired_top()
    }

    #[cfg(test)]
    pub(super) fn reclaim_retired_payloads<F>(&self, is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        self.reclaim_retired(is_protected);
    }

    pub(super) fn reclaim_retired<F>(&self, mut is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        let retired_top = self.read_retired_top();
        if retired_top == 0 {
            return;
        }

        let mut kept = 0usize;
        let mut free_top = self.read_free_top();
        for offset in 0..retired_top {
            let payload_index = self.read_retired_stack_slot(offset);
            let sequence = self.read_retired_sequence(payload_index);

            if is_protected(RetiredPayload::new(self.lane, sequence)) {
                if kept != offset {
                    self.write_retired_stack_slot(kept, payload_index);
                }
                kept += 1;
            } else {
                debug_assert!(free_top < self.lane_capacity);
                self.write_free_stack_slot(free_top, payload_index);
                free_top += 1;
            }
        }

        self.write_free_top(free_top);
        self.write_retired_top(kept);
    }

    #[inline]
    pub(super) fn protected_payload_ptr(&self, payload: ProtectedPayload) -> NonNull<T> {
        self.payload_at(payload.payload_index)
    }

    #[inline(always)]
    pub(super) fn payload_at(&self, payload_index: u32) -> NonNull<T> {
        self.payload_at_offset(self.payload_offset(payload_index))
    }

    #[inline(always)]
    pub(super) fn reserved_payload_at(&self, chain: ReservedPayloads, index: usize) -> u32 {
        debug_assert!(index < chain.count);
        let stack_offset = chain.first_stack_offset + chain.count - 1 - index;
        let payload_index = self.read_free_stack_slot(stack_offset);
        debug_assert!(self.owns_payload(payload_index));
        payload_index
    }

    #[inline(always)]
    fn payload_at_offset(&self, offset: usize) -> NonNull<T> {
        debug_assert!(offset < self.lane_capacity);
        let byte_offset = offset * core::mem::size_of::<T>();
        // SAFETY: callers only pass validated payload offsets.
        unsafe { self.payloads.cast::<u8>().byte_add(byte_offset).cast() }
    }

    pub(super) fn recover_as_exclusive(&self) {
        self.reset_stacks();
    }

    fn valid_payload_index(&self, handle: u32) -> Option<u32> {
        if handle == EMPTY_PAYLOAD_HANDLE || !self.owns_payload(handle) {
            None
        } else {
            Some(handle)
        }
    }

    #[inline]
    fn read_free_top(&self) -> usize {
        // SAFETY: this producer owns the lane; consumers never read pool stack tops.
        unsafe { self.free_top.as_ptr().read() as usize }
    }

    #[inline]
    fn write_free_top(&self, top: usize) {
        debug_assert!(top <= self.lane_capacity);
        // SAFETY: this producer owns the lane; consumers never read pool stack tops.
        unsafe {
            self.free_top
                .as_ptr()
                .write(u32::try_from(top).expect("payload stack top fits u32"))
        };
    }

    #[inline]
    fn read_retired_top(&self) -> usize {
        // SAFETY: this producer owns the lane; consumers never read pool stack tops.
        unsafe { self.retired_top.as_ptr().read() as usize }
    }

    #[inline]
    fn write_retired_top(&self, top: usize) {
        debug_assert!(top <= self.lane_capacity);
        // SAFETY: this producer owns the lane; consumers never read pool stack tops.
        unsafe {
            self.retired_top
                .as_ptr()
                .write(u32::try_from(top).expect("payload stack top fits u32"))
        };
    }

    #[inline]
    fn pop_free_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        debug_assert!(count != 0);
        debug_assert!(count <= self.lane_capacity);

        let top = self.read_free_top();
        if top < count {
            return None;
        }

        let first_stack_offset = top - count;
        let first_payload_index = self.read_free_stack_slot(top - 1);
        self.write_free_top(first_stack_offset);
        Some(ReservedPayloads {
            first_stack_offset,
            first_payload_index,
            count,
        })
    }

    #[inline(always)]
    fn read_free_stack_slot(&self, stack_offset: usize) -> u32 {
        let metadata = self.metadata_at(stack_offset);
        // SAFETY: this producer owns the lane; consumers never read pool stacks.
        unsafe { addr_of!((*metadata.as_ptr()).free_stack_slot).read() }
    }

    #[inline(always)]
    fn write_free_stack_slot(&self, stack_offset: usize, payload_index: u32) {
        debug_assert!(self.owns_payload(payload_index));
        let metadata = self.metadata_at(stack_offset);
        // SAFETY: this producer owns the lane; consumers never read pool stacks.
        unsafe { addr_of_mut!((*metadata.as_ptr()).free_stack_slot).write(payload_index) };
    }

    #[inline(always)]
    fn read_retired_stack_slot(&self, stack_offset: usize) -> u32 {
        let metadata = self.metadata_at(stack_offset);
        // SAFETY: this producer owns the lane; consumers never read pool stacks.
        unsafe { addr_of!((*metadata.as_ptr()).retired_stack_slot).read() }
    }

    #[inline(always)]
    fn write_retired_stack_slot(&self, stack_offset: usize, payload_index: u32) {
        debug_assert!(self.owns_payload(payload_index));
        let metadata = self.metadata_at(stack_offset);
        // SAFETY: this producer owns the lane; consumers never read pool stacks.
        unsafe { addr_of_mut!((*metadata.as_ptr()).retired_stack_slot).write(payload_index) };
    }

    #[inline(always)]
    fn read_retired_sequence(&self, payload_index: u32) -> usize {
        let metadata = self.metadata_at(self.payload_offset(payload_index));
        // SAFETY: this producer owns the lane; consumers never read retired metadata.
        unsafe { addr_of!((*metadata.as_ptr()).retired_sequence).read() }
    }

    #[inline(always)]
    fn write_retired_sequence(&self, payload_index: u32, sequence: usize) {
        let metadata = self.metadata_at(self.payload_offset(payload_index));
        // SAFETY: this producer owns the lane; consumers never read retired metadata.
        unsafe { addr_of_mut!((*metadata.as_ptr()).retired_sequence).write(sequence) };
    }

    fn reset_stacks(&self) {
        for stack_offset in 0..self.lane_capacity {
            let payload_index = self.payload_index(self.lane_capacity - 1 - stack_offset);
            self.write_free_stack_slot(stack_offset, payload_index);
            self.write_retired_stack_slot(stack_offset, self.base_payload_index);
            self.write_retired_sequence(self.payload_index(stack_offset), 0);
        }

        self.write_free_top(self.lane_capacity);
        self.write_retired_top(0);
    }

    #[inline(always)]
    fn metadata_at(&self, offset: usize) -> NonNull<PayloadMetadata> {
        debug_assert!(offset < self.lane_capacity);
        let byte_offset = offset * core::mem::size_of::<PayloadMetadata>();
        // SAFETY: callers only pass validated stack or payload offsets.
        unsafe {
            self.metadata
                .cast::<u8>()
                .byte_add(byte_offset)
                .cast::<PayloadMetadata>()
        }
    }

    #[inline(always)]
    fn owns_payload(&self, payload_index: u32) -> bool {
        payload_index.wrapping_sub(self.base_payload_index) < self.lane_capacity as u32
    }

    #[inline(always)]
    fn payload_offset(&self, payload_index: u32) -> usize {
        debug_assert!(self.owns_payload(payload_index));
        payload_index.wrapping_sub(self.base_payload_index) as usize
    }

    #[inline]
    fn payload_index(&self, offset: usize) -> u32 {
        debug_assert!(offset < self.lane_capacity);
        self.base_payload_index
            .checked_add(u32::try_from(offset).expect("payload offset fits u32"))
            .expect("payload index fits u32")
    }
}

#[inline]
pub(super) fn capacity_for_ring_capacity(ring_capacity: usize) -> Option<usize> {
    let capacity = ring_capacity.checked_mul(PAYLOADS_PER_RING_ENTRY)?;
    if capacity == 0 || capacity > EMPTY_PAYLOAD_HANDLE as usize {
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

#[cfg(test)]
mod tests {
    use super::{
        initial_free_top, initial_retired_top, initialize_payload_metadata, PayloadMetadata,
        PayloadPool, RetiredPayload, EMPTY_PAYLOAD_HANDLE,
    };
    use core::{
        mem::{size_of, MaybeUninit},
        ptr::NonNull,
    };

    fn initialized_pools(
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
            0,
            NonNull::from(&mut *free_top),
            NonNull::from(&mut *retired_top),
            NonNull::new(payload_metadata.as_ptr().cast_mut())
                .expect("non-empty payload metadata slice"),
            NonNull::new(payloads.as_mut_ptr().cast()).expect("non-empty payload slice"),
            payload_metadata.len(),
            1,
        )
    }

    #[test]
    fn payload_metadata_uses_compact_layout() {
        assert_eq!(size_of::<PayloadMetadata>(), 16);
    }

    #[test]
    fn reserve_payloads_exact_removes_requested_prefix() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(4);
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
        assert_eq!(pool.reserved_payload_at(first, 1), 1);
        assert_eq!(free_top, 2);

        let second = pool.reserve_payloads_exact(2).expect("second reserve");
        assert_eq!(second.first_payload_index(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn cancel_reserved_payloads_restores_stack_to_pool() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(4);
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
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(4);
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
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(1);
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
        pool.reclaim_retired_payloads(|retired| retired.sequence == 5);
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.reclaim_retired_payloads(|_| false);
        assert!(pool.reserve_payloads_exact(1).is_some());
    }

    #[test]
    fn invalid_handle_does_not_resolve_to_payload() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(1);
        let mut free_top = free_top;
        let mut retired_top = retired_top;
        let pool = pool(
            &mut free_top,
            &mut retired_top,
            &payload_metadata,
            &mut payloads,
        );

        assert!(pool
            .payload_for_protected_handle(EMPTY_PAYLOAD_HANDLE)
            .is_none());
    }

    #[test]
    fn recover_as_exclusive_restores_all_payloads() {
        let (free_top, retired_top, payload_metadata, mut payloads) = initialized_pools(4);
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
