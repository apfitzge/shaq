use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

const CACHELINE_SIZE: usize = 64;
pub(super) const PAYLOADS_PER_RING_ENTRY: usize = 2;

const EMPTY_PAYLOAD: u32 = 0;

/// Payload descriptor. `generation` is copied into ring handles; `next_free`
/// links payloads through the free and retired lists.
#[repr(C)]
pub(super) struct PayloadHeader {
    generation: AtomicU32,
    next_free: AtomicU32,
    retired_lane: AtomicU32,
    retired_sequence: AtomicUsize,
}

impl PayloadHeader {
    const fn new(next_free: u32) -> Self {
        Self {
            generation: AtomicU32::new(0),
            next_free: AtomicU32::new(next_free),
            retired_lane: AtomicU32::new(0),
            retired_sequence: AtomicUsize::new(0),
        }
    }
}

/// Ring payload handle: high 32 bits are generation, low 32 bits are a nullable
/// payload index.
#[derive(Clone, Copy, PartialEq)]
#[repr(transparent)]
pub(super) struct PayloadHandle(pub(super) u64);

impl PayloadHandle {
    pub(super) const EMPTY: Self = Self(0);

    #[inline]
    fn new(payload_index: u32, generation: u32) -> Self {
        Self(((generation as u64) << 32) | encode_payload_index(payload_index) as u64)
    }

    #[inline]
    fn payload_index(self) -> Option<u32> {
        decode_payload_index(self.0 as u32)
    }

    #[inline]
    fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

/// Payload pool head: high 32 bits are the ABA tag, low 32 bits are a nullable
/// payload index encoded as `payload_index + 1`, with zero meaning empty.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct PayloadPoolHead(u64);

impl PayloadPoolHead {
    #[inline]
    const fn new(encoded_first: u32, tag: u32) -> Self {
        Self(((tag as u64) << 32) | encoded_first as u64)
    }

    #[inline]
    const fn encoded_first(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    const fn tag(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReservedPayloads {
    first_payload_index: u32,
    last_payload_index: u32,
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

pub(super) struct PayloadReleaseBatch {
    first_payload_index: Option<u32>,
    last_payload_index: Option<u32>,
    count: usize,
}

impl PayloadReleaseBatch {
    #[inline]
    pub(super) const fn new() -> Self {
        Self {
            first_payload_index: None,
            last_payload_index: None,
            count: 0,
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    pub(super) fn take(&mut self) -> Self {
        core::mem::replace(self, Self::new())
    }
}

pub(super) struct PayloadPool<T> {
    free_heads: NonNull<AtomicU64>,
    retired_heads: NonNull<AtomicU64>,
    payload_headers: NonNull<PayloadHeader>,
    payloads: NonNull<T>,
    lane_capacity: usize,
    lane_mask: usize,
    lane_shift: u32,
    header_lane_stride_bytes: usize,
    payload_lane_stride_bytes: usize,
    producer_slots: usize,
    capacity: usize,
}

impl<T> Clone for PayloadPool<T> {
    fn clone(&self) -> Self {
        Self {
            free_heads: self.free_heads,
            retired_heads: self.retired_heads,
            payload_headers: self.payload_headers,
            payloads: self.payloads,
            lane_capacity: self.lane_capacity,
            lane_mask: self.lane_mask,
            lane_shift: self.lane_shift,
            header_lane_stride_bytes: self.header_lane_stride_bytes,
            payload_lane_stride_bytes: self.payload_lane_stride_bytes,
            producer_slots: self.producer_slots,
            capacity: self.capacity,
        }
    }
}

impl<T> PayloadPool<T> {
    #[inline]
    pub(super) fn new(
        free_heads: NonNull<AtomicU64>,
        retired_heads: NonNull<AtomicU64>,
        payload_headers: NonNull<PayloadHeader>,
        payloads: NonNull<T>,
        lane_capacity: usize,
        header_lane_stride_bytes: usize,
        payload_lane_stride_bytes: usize,
        producer_slots: usize,
    ) -> Self {
        debug_assert!(lane_capacity.is_power_of_two());
        let capacity = lane_capacity
            .checked_mul(producer_slots)
            .expect("payload capacity overflow");
        Self {
            free_heads,
            retired_heads,
            payload_headers,
            payloads,
            lane_capacity,
            lane_mask: lane_capacity - 1,
            lane_shift: lane_capacity.trailing_zeros(),
            header_lane_stride_bytes,
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
        if count == 0 {
            return None;
        }

        let chain = self.take_free_payloads_exact(0, count)?;
        self.prepare_reserved_payloads(chain);
        Some(chain)
    }

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
        debug_assert!(chain.count != 0);
        self.push_free_payloads(0, chain.first_payload_index, chain.last_payload_index);
    }

    #[cfg(test)]
    pub(super) fn prepare_reserved_payloads(&self, chain: ReservedPayloads) {
        self.prepare_reserved_payloads_in_lane(0, chain);
    }

    #[inline(always)]
    pub(super) fn prepare_reserved_payloads_in_lane(&self, lane: usize, chain: ReservedPayloads) {
        let mut cursor = self.payload_cursor(lane, chain.first_payload_index);
        let last_index = chain.count.wrapping_sub(1);
        for index in 0..chain.count {
            self.prepare_reserved_payload_in_lane(lane, cursor.current());
            if index != last_index {
                cursor.advance();
            }
        }
    }

    #[inline]
    #[cfg(test)]
    pub(super) fn handle_for_payload(&self, payload_index: u32) -> PayloadHandle {
        self.handle_for_payload_in_lane(0, payload_index)
    }

    #[inline(always)]
    pub(super) fn handle_for_payload_in_lane(
        &self,
        lane: usize,
        payload_index: u32,
    ) -> PayloadHandle {
        let generation = self
            .payload_header_at(lane, self.payload_offset(payload_index))
            .generation
            .load(Ordering::Relaxed);
        PayloadHandle::new(payload_index, generation)
    }

    pub(super) fn payload_for_protected_handle(
        &self,
        handle: PayloadHandle,
    ) -> Option<ProtectedPayload> {
        self.valid_payload_index(handle).map(ProtectedPayload::new)
    }

    #[cfg(test)]
    fn payload_for_handle(&self, handle: PayloadHandle) -> Option<ProtectedPayload> {
        let payload_index = self.valid_payload_index(handle)?;
        let generation = self
            .payload_header(payload_index)
            .generation
            .load(Ordering::Acquire);
        if generation == handle.generation() {
            Some(ProtectedPayload::new(payload_index))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(super) fn retire_to_batch(
        &self,
        handle: PayloadHandle,
        retired: RetiredPayload,
        batch: &mut PayloadReleaseBatch,
    ) {
        self.retire_to_batch_in_lane(retired.lane(), handle, retired, batch);
    }

    #[inline(always)]
    pub(super) fn retire_to_batch_in_lane(
        &self,
        lane: usize,
        handle: PayloadHandle,
        retired: RetiredPayload,
        batch: &mut PayloadReleaseBatch,
    ) {
        let Some(payload_index) = self.valid_payload_index(handle) else {
            return;
        };
        debug_assert_eq!(lane, retired.lane());
        let payload_header = self.payload_header_at(lane, self.payload_offset(payload_index));
        if payload_header.generation.load(Ordering::Acquire) != handle.generation() {
            return;
        }

        payload_header
            .retired_lane
            .store(retired.lane as u32, Ordering::Relaxed);
        payload_header
            .retired_sequence
            .store(retired.sequence, Ordering::Relaxed);
        self.prepend_payload(lane, payload_index, batch);
    }

    #[cfg(test)]
    pub(super) fn reclaim_retired_batch<F>(&self, batch: PayloadReleaseBatch, is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        if let Some(first) = batch.first_payload_index {
            self.reclaim_payload_chain(first, is_protected);
        }
    }

    pub(super) fn reclaim_retired_batch_to_batches_in_lane<F>(
        &self,
        lane: usize,
        batch: PayloadReleaseBatch,
        is_protected: F,
        free_batch: &mut PayloadReleaseBatch,
        retired_batch: &mut PayloadReleaseBatch,
    ) where
        F: FnMut(RetiredPayload) -> bool,
    {
        if let Some(first) = batch.first_payload_index {
            self.partition_retired_payload_chain(
                lane,
                first,
                is_protected,
                free_batch,
                retired_batch,
            );
        }
    }

    #[cfg(test)]
    pub(super) fn reclaim_retired_payloads<F>(&self, is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        if let Some(first) = self.take_retired_payloads(0) {
            self.reclaim_payload_chain(first, is_protected);
        }
    }

    pub(super) fn reclaim_retired_payloads_to_batches<F>(
        &self,
        lane: usize,
        is_protected: F,
        free_batch: &mut PayloadReleaseBatch,
        retired_batch: &mut PayloadReleaseBatch,
    ) where
        F: FnMut(RetiredPayload) -> bool,
    {
        if let Some(first) = self.take_retired_payloads(lane) {
            self.partition_retired_payload_chain(
                lane,
                first,
                is_protected,
                free_batch,
                retired_batch,
            );
        }
    }

    pub(super) fn flush_retired_batch(&self, lane: usize, batch: PayloadReleaseBatch) {
        if let (Some(first), Some(last)) = (batch.first_payload_index, batch.last_payload_index) {
            self.push_retired_payloads(lane, first, last);
        }
    }

    pub(super) fn flush_release_batch(&self, lane: usize, batch: PayloadReleaseBatch) {
        if let (Some(first), Some(last)) = (batch.first_payload_index, batch.last_payload_index) {
            self.push_free_payloads(lane, first, last);
        }
    }

    pub(super) fn prepend_reserved_payloads_to_batch_in_lane(
        &self,
        lane: usize,
        chain: ReservedPayloads,
        batch: &mut PayloadReleaseBatch,
    ) {
        self.prepend_payload_chain(
            lane,
            chain.first_payload_index,
            chain.last_payload_index,
            chain.count,
            batch,
        );
    }

    #[inline(always)]
    pub(super) fn pop_batch_payloads_exact_in_lane(
        &self,
        lane: usize,
        batch: &mut PayloadReleaseBatch,
        count: usize,
    ) -> Option<ReservedPayloads> {
        if count == 0 || count > batch.count {
            return None;
        }

        let first_payload_index = batch.first_payload_index?;
        let mut last_payload_index = first_payload_index;
        let last_index = count.wrapping_sub(1);
        for index in 0..count {
            if index != last_index {
                last_payload_index = self.next_payload_index_in_lane(lane, last_payload_index);
            }
        }

        let next = self
            .payload_header_at(lane, self.payload_offset(last_payload_index))
            .next_free
            .load(Ordering::Relaxed);
        self.payload_header_at(lane, self.payload_offset(last_payload_index))
            .next_free
            .store(EMPTY_PAYLOAD, Ordering::Relaxed);

        batch.count -= count;
        batch.first_payload_index = decode_payload_index(next);
        if batch.count == 0 {
            batch.last_payload_index = None;
        }

        Some(ReservedPayloads {
            first_payload_index,
            last_payload_index,
            count,
        })
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
        self.payload_at_offset(lane, self.payload_offset(payload_index))
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

    #[inline]
    pub(super) fn payload_cursor(&self, lane: usize, first: u32) -> PayloadChainCursor<'_, T> {
        PayloadChainCursor::new(self, lane, first)
    }

    #[inline(always)]
    pub(super) fn next_payload_index_in_lane(&self, lane: usize, payload_index: u32) -> u32 {
        decode_non_empty_payload_index(
            self.payload_header_at(lane, self.payload_offset(payload_index))
                .next_free
                .load(Ordering::Relaxed),
        )
    }

    pub(super) fn recover_as_exclusive(&self) {
        for index in 0..self.capacity {
            self.payload_header(index as u32)
                .retired_lane
                .store(0, Ordering::Relaxed);
            self.payload_header(index as u32)
                .retired_sequence
                .store(0, Ordering::Relaxed);
        }

        self.reset_free_links();
    }

    #[inline(always)]
    fn prepare_reserved_payload_in_lane(&self, lane: usize, payload_index: u32) {
        let payload_header = self.payload_header_at(lane, self.payload_offset(payload_index));
        let mut generation = payload_header
            .generation
            .load(Ordering::Relaxed)
            .wrapping_add(1);
        if generation == 0 {
            generation = 1;
        }
        payload_header
            .generation
            .store(generation, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn reclaim_payload_chain<F>(&self, first_payload_index: u32, mut is_protected: F)
    where
        F: FnMut(RetiredPayload) -> bool,
    {
        let mut free_batch = PayloadReleaseBatch::new();
        let mut retired_batch = PayloadReleaseBatch::new();
        self.partition_retired_payload_chain(
            0,
            first_payload_index,
            &mut is_protected,
            &mut free_batch,
            &mut retired_batch,
        );

        self.flush_release_batch(0, free_batch);
        self.flush_retired_batch(0, retired_batch);
    }

    fn partition_retired_payload_chain<F>(
        &self,
        lane: usize,
        first_payload_index: u32,
        mut is_protected: F,
        free_batch: &mut PayloadReleaseBatch,
        retired_batch: &mut PayloadReleaseBatch,
    ) where
        F: FnMut(RetiredPayload) -> bool,
    {
        let mut encoded = encode_payload_index(first_payload_index);

        while encoded != EMPTY_PAYLOAD {
            let payload_index = decode_non_empty_payload_index(encoded);
            let payload_header = self.payload_header_at(lane, self.payload_offset(payload_index));
            let next = payload_header.next_free.load(Ordering::Relaxed);
            let retired_lane = payload_header.retired_lane.load(Ordering::Relaxed) as usize;
            let sequence = payload_header.retired_sequence.load(Ordering::Relaxed);

            if is_protected(RetiredPayload::new(retired_lane, sequence)) {
                self.prepend_payload(lane, payload_index, retired_batch);
            } else {
                self.prepend_payload(lane, payload_index, free_batch);
            }

            encoded = next;
        }
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
    fn free_head(&self, lane: usize) -> &AtomicU64 {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: `free_heads` points into the live per-lane free-head table.
        unsafe {
            self.free_heads
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<AtomicU64>()
                .as_ref()
        }
    }

    #[inline]
    fn retired_head(&self, lane: usize) -> &AtomicU64 {
        debug_assert!(lane < self.producer_slots);
        // SAFETY: `retired_heads` points into the live per-lane retired-head table.
        unsafe {
            self.retired_heads
                .cast::<u8>()
                .byte_add(lane * CACHELINE_SIZE)
                .cast::<AtomicU64>()
                .as_ref()
        }
    }

    #[inline]
    fn pop_free_payloads_exact(&self, lane: usize, count: usize) -> Option<ReservedPayloads> {
        debug_assert!(count != 0);

        let free_head = self.free_head(lane);
        self.pop_free_payloads_exact_from_head(
            lane,
            free_head,
            count,
            PayloadPoolHead(free_head.load(Ordering::Acquire)),
        )
    }

    fn pop_free_payloads_exact_from_head(
        &self,
        lane: usize,
        free_head: &AtomicU64,
        count: usize,
        mut head: PayloadPoolHead,
    ) -> Option<ReservedPayloads> {
        'retry: loop {
            let first = head.encoded_first();
            if first == EMPTY_PAYLOAD {
                return None;
            }

            let mut encoded = first;
            let mut last_payload_index = 0;
            for _ in 0..count {
                if encoded == EMPTY_PAYLOAD {
                    // Another producer may have popped this prefix and cleared
                    // its old tail after we loaded `head`. Only report
                    // exhaustion if the shared head is still the one we walked.
                    let current = PayloadPoolHead(free_head.load(Ordering::Acquire));
                    if current.0 == head.0 {
                        return None;
                    }
                    head = current;
                    continue 'retry;
                }
                let payload_index = decode_non_empty_payload_index(encoded);
                assert!(
                    (payload_index as usize) < self.capacity,
                    "corrupt free payload chain"
                );
                last_payload_index = payload_index;
                encoded = self
                    .payload_header_at(lane, self.payload_offset(payload_index))
                    .next_free
                    .load(Ordering::Relaxed);
            }

            let new_head = PayloadPoolHead::new(encoded, head.tag().wrapping_add(1));
            match free_head.compare_exchange_weak(
                head.0,
                new_head.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let first_payload_index = decode_non_empty_payload_index(first);
                    self.payload_header_at(lane, self.payload_offset(last_payload_index))
                        .next_free
                        .store(EMPTY_PAYLOAD, Ordering::Relaxed);
                    return Some(ReservedPayloads {
                        first_payload_index,
                        last_payload_index,
                        count,
                    });
                }
                Err(current) => head = PayloadPoolHead(current),
            }
        }
    }

    fn take_retired_payloads(&self, lane: usize) -> Option<u32> {
        let retired_head = self.retired_head(lane);
        let mut head = PayloadPoolHead(retired_head.load(Ordering::Acquire));

        loop {
            let first = head.encoded_first();
            if first == EMPTY_PAYLOAD {
                return None;
            }

            let new_head = PayloadPoolHead::new(EMPTY_PAYLOAD, head.tag().wrapping_add(1));
            match retired_head.compare_exchange_weak(
                head.0,
                new_head.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(decode_non_empty_payload_index(first)),
                Err(current) => head = PayloadPoolHead(current),
            }
        }
    }

    #[inline]
    fn push_free_payloads(&self, lane: usize, first_payload_index: u32, last_payload_index: u32) {
        self.push_payloads(
            lane,
            self.free_head(lane),
            first_payload_index,
            last_payload_index,
        );
    }

    #[inline]
    fn push_retired_payloads(
        &self,
        lane: usize,
        first_payload_index: u32,
        last_payload_index: u32,
    ) {
        self.push_payloads(
            lane,
            self.retired_head(lane),
            first_payload_index,
            last_payload_index,
        );
    }

    fn push_payloads(
        &self,
        lane: usize,
        head: &AtomicU64,
        first_payload_index: u32,
        last_payload_index: u32,
    ) {
        let first = encode_payload_index(first_payload_index);
        let mut current = PayloadPoolHead(head.load(Ordering::Acquire));

        loop {
            self.payload_header_at(lane, self.payload_offset(last_payload_index))
                .next_free
                .store(current.encoded_first(), Ordering::Relaxed);
            let new_head = PayloadPoolHead::new(first, current.tag().wrapping_add(1));
            match head.compare_exchange_weak(
                current.0,
                new_head.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = PayloadPoolHead(next),
            }
        }
    }

    #[inline]
    fn prepend_payload(&self, lane: usize, payload_index: u32, batch: &mut PayloadReleaseBatch) {
        self.payload_header_at(lane, self.payload_offset(payload_index))
            .next_free
            .store(
                batch
                    .first_payload_index
                    .map(encode_payload_index)
                    .unwrap_or(EMPTY_PAYLOAD),
                Ordering::Relaxed,
            );
        if batch.last_payload_index.is_none() {
            batch.last_payload_index = Some(payload_index);
        }
        batch.first_payload_index = Some(payload_index);
        batch.count += 1;
    }

    fn prepend_payload_chain(
        &self,
        lane: usize,
        first_payload_index: u32,
        last_payload_index: u32,
        count: usize,
        batch: &mut PayloadReleaseBatch,
    ) {
        debug_assert!(count != 0);
        self.payload_header_at(lane, self.payload_offset(last_payload_index))
            .next_free
            .store(
                batch
                    .first_payload_index
                    .map(encode_payload_index)
                    .unwrap_or(EMPTY_PAYLOAD),
                Ordering::Relaxed,
            );
        if batch.last_payload_index.is_none() {
            batch.last_payload_index = Some(last_payload_index);
        }
        batch.first_payload_index = Some(first_payload_index);
        batch.count += count;
    }

    fn reset_free_links(&self) {
        for lane in 0..self.producer_slots {
            for offset in 0..self.lane_capacity {
                let payload_index = self.payload_index(lane, offset);
                self.payload_header(payload_index).next_free.store(
                    next_free_after_lane(lane, offset, self.lane_capacity),
                    Ordering::Relaxed,
                );
            }
        }

        for lane in 0..self.producer_slots {
            let free_head = self.free_head(lane);
            let current_free = PayloadPoolHead(free_head.load(Ordering::Acquire));
            free_head.store(
                PayloadPoolHead::new(
                    first_for_lane(lane, self.lane_capacity),
                    current_free.tag().wrapping_add(1),
                )
                .0,
                Ordering::Release,
            );

            let retired_head = self.retired_head(lane);
            let current_retired = PayloadPoolHead(retired_head.load(Ordering::Acquire));
            retired_head.store(
                PayloadPoolHead::new(EMPTY_PAYLOAD, current_retired.tag().wrapping_add(1)).0,
                Ordering::Release,
            );
        }
    }

    #[inline]
    fn payload_header(&self, payload_index: u32) -> &PayloadHeader {
        debug_assert!((payload_index as usize) < self.capacity);
        let (lane, offset) = self.lane_and_offset(payload_index);
        self.payload_header_at(lane, offset)
    }

    #[inline(always)]
    fn payload_header_at(&self, lane: usize, offset: usize) -> &PayloadHeader {
        debug_assert!(lane < self.producer_slots);
        debug_assert!(offset < self.lane_capacity);
        let byte_offset =
            lane * self.header_lane_stride_bytes + offset * core::mem::size_of::<PayloadHeader>();
        // SAFETY: callers only pass validated payload indices.
        unsafe {
            self.payload_headers
                .cast::<u8>()
                .byte_add(byte_offset)
                .cast::<PayloadHeader>()
                .as_ref()
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

pub(super) struct PayloadChainCursor<'a, T> {
    pool: &'a PayloadPool<T>,
    lane: usize,
    current: u32,
}

impl<'a, T> PayloadChainCursor<'a, T> {
    #[inline(always)]
    fn new(pool: &'a PayloadPool<T>, lane: usize, first: u32) -> Self {
        Self {
            pool,
            lane,
            current: first,
        }
    }

    #[inline(always)]
    pub(super) fn current(&self) -> u32 {
        self.current
    }

    #[inline(always)]
    pub(super) fn advance(&mut self) {
        self.current = self
            .pool
            .next_payload_index_in_lane(self.lane, self.current);
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
pub(super) fn initial_free_head(lane: usize, lane_capacity: usize) -> u64 {
    PayloadPoolHead::new(first_for_lane(lane, lane_capacity), 0).0
}

#[inline]
pub(super) const fn initial_retired_head() -> u64 {
    PayloadPoolHead::new(EMPTY_PAYLOAD, 0).0
}

pub(super) unsafe fn initialize_payload_headers(
    payload_headers: NonNull<PayloadHeader>,
    lane_capacity: usize,
    header_lane_stride_bytes: usize,
    producer_slots: usize,
) {
    for lane in 0..producer_slots {
        for offset in 0..lane_capacity {
            let byte_offset =
                lane * header_lane_stride_bytes + offset * core::mem::size_of::<PayloadHeader>();
            // SAFETY: caller guarantees `payload_headers` points at the padded
            // per-lane header layout.
            unsafe {
                payload_headers
                    .cast::<u8>()
                    .byte_add(byte_offset)
                    .cast::<PayloadHeader>()
                    .as_ptr()
                    .write(PayloadHeader::new(next_free_after_lane(
                        lane,
                        offset,
                        lane_capacity,
                    )))
            };
        }
    }
}

/// Nullable payload index stored in `PayloadHandle`, `PayloadPoolHead`, and free-list links.
/// Zero means empty; non-zero values are `payload_index + 1`.
#[inline]
fn encode_payload_index(payload_index: u32) -> u32 {
    debug_assert!(payload_index != u32::MAX, "payload index overflow");
    payload_index.wrapping_add(1)
}

#[inline]
fn decode_payload_index(encoded: u32) -> Option<u32> {
    if encoded == EMPTY_PAYLOAD {
        None
    } else {
        Some(decode_non_empty_payload_index(encoded))
    }
}

#[inline]
fn decode_non_empty_payload_index(encoded: u32) -> u32 {
    debug_assert!(encoded != EMPTY_PAYLOAD, "empty encoded payload index");
    encoded.wrapping_sub(1)
}

#[inline]
fn next_free_after_lane(lane: usize, offset: usize, lane_capacity: usize) -> u32 {
    if offset + 1 == lane_capacity {
        EMPTY_PAYLOAD
    } else {
        encode_payload_index((lane * lane_capacity + offset + 1) as u32)
    }
}

#[inline]
fn first_for_lane(lane: usize, lane_capacity: usize) -> u32 {
    if lane_capacity == 0 {
        EMPTY_PAYLOAD
    } else {
        encode_payload_index((lane * lane_capacity) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        initial_free_head, initial_retired_head, initialize_payload_headers, PayloadHandle,
        PayloadHeader, PayloadPool, PayloadPoolHead, PayloadReleaseBatch, RetiredPayload,
    };
    use core::{
        mem::{size_of, MaybeUninit},
        ptr::NonNull,
        sync::atomic::{AtomicU64, Ordering},
    };

    fn initialized_pool(
        capacity: usize,
    ) -> (
        AtomicU64,
        AtomicU64,
        Vec<PayloadHeader>,
        Vec<MaybeUninit<u64>>,
    ) {
        let free_head = AtomicU64::new(initial_free_head(0, capacity));
        let retired_head = AtomicU64::new(initial_retired_head());
        let mut payload_headers = Vec::with_capacity(capacity);
        let mut payloads = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            payloads.push(MaybeUninit::uninit());
        }

        let payload_header_ptr =
            NonNull::new(payload_headers.as_mut_ptr()).expect("non-empty payload header slice");
        // SAFETY: `payload_headers` has capacity for `capacity` headers and we set
        // its length immediately after initialization.
        unsafe {
            initialize_payload_headers(
                payload_header_ptr,
                capacity,
                capacity * size_of::<PayloadHeader>(),
                1,
            );
            payload_headers.set_len(capacity);
        }

        (free_head, retired_head, payload_headers, payloads)
    }

    fn pool<'a>(
        free_head: &'a AtomicU64,
        retired_head: &'a AtomicU64,
        payload_headers: &'a [PayloadHeader],
        payloads: &'a mut [MaybeUninit<u64>],
    ) -> PayloadPool<u64> {
        PayloadPool::new(
            NonNull::from(free_head),
            NonNull::from(retired_head),
            NonNull::new(payload_headers.as_ptr().cast_mut())
                .expect("non-empty payload header slice"),
            NonNull::new(payloads.as_mut_ptr().cast()).expect("non-empty payload slice"),
            payload_headers.len(),
            payload_headers.len() * size_of::<PayloadHeader>(),
            payload_headers.len() * size_of::<u64>(),
            1,
        )
    }

    #[test]
    fn payload_header_uses_compact_layout() {
        assert_eq!(size_of::<PayloadHeader>(), 24);
    }

    #[test]
    fn reserve_payloads_exact_removes_requested_prefix() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);

        let first = pool.reserve_payloads_exact(2).expect("first reserve");
        assert_eq!(first.first_payload_index(), 0);
        assert_eq!(payload_headers[1].next_free.load(Ordering::Relaxed), 0);

        let second = pool.reserve_payloads_exact(2).expect("second reserve");
        assert_eq!(second.first_payload_index(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn stale_short_free_chain_retries_current_head() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(8);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);
        let stale_head = PayloadPoolHead(initial_free_head(0, 8));

        let prefix = pool.reserve_payloads_exact(3).expect("reserve prefix");
        assert_eq!(prefix.first_payload_index(), 0);

        let current = pool
            .pop_free_payloads_exact_from_head(0, &free_head, 4, stale_head)
            .expect("reserve from current head after stale short chain");
        assert_eq!(current.first_payload_index(), 3);

        let last = pool.reserve_payloads_exact(1).expect("last payload");
        assert_eq!(last.first_payload_index(), 7);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn cancel_reserved_payloads_restores_chain_to_pool() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);

        let removed = pool.reserve_payloads_exact(2).expect("reserve");
        pool.cancel_reserved_payloads(removed);

        let all = pool.reserve_payloads_exact(4).expect("reserve all");
        assert_eq!(all.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn reclaim_retired_payloads_restores_reusable_payloads() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);
        let _reserved = pool.reserve_payloads_exact(4).expect("reserve all");

        let mut batch = PayloadReleaseBatch::new();
        pool.retire_to_batch(
            pool.handle_for_payload(2),
            RetiredPayload::new(0, 12),
            &mut batch,
        );
        pool.retire_to_batch(
            pool.handle_for_payload(0),
            RetiredPayload::new(0, 10),
            &mut batch,
        );
        pool.reclaim_retired_batch(batch, |_| false);

        let chain = pool.reserve_payloads_exact(2).expect("reserve released");
        assert_eq!(chain.len(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn protected_retired_payload_blocks_reuse_until_unprotected() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(1);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);

        let reserved = pool.reserve_payloads_exact(1).expect("reserve");
        let handle = pool.handle_for_payload(reserved.first_payload_index());
        let mut batch = PayloadReleaseBatch::new();
        pool.retire_to_batch(handle, RetiredPayload::new(0, 5), &mut batch);
        pool.reclaim_retired_batch(batch, |retired| retired.sequence() == 5);
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.reclaim_retired_payloads(|_| false);
        assert!(pool.reserve_payloads_exact(1).is_some());
    }

    #[test]
    fn invalid_handle_does_not_resolve_to_payload() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(1);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);

        assert!(pool.payload_for_handle(PayloadHandle::EMPTY).is_none());
    }

    #[test]
    fn recover_as_exclusive_restores_all_payloads() {
        let (free_head, retired_head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&free_head, &retired_head, &payload_headers, &mut payloads);
        assert!(pool.reserve_payloads_exact(4).is_some());
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.recover_as_exclusive();

        let chain = pool.reserve_payloads_exact(4).expect("reserve after reset");
        assert_eq!(chain.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }
}
