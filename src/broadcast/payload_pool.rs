use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

pub(super) const PAYLOADS_PER_RING_ENTRY: usize = 2;

const EMPTY_PAYLOAD: u32 = 0;

/// Payload descriptor. `generation` is copied into ring handles; `next_free`
/// links payloads through the free and retired lists.
#[repr(C)]
pub(super) struct PayloadHeader {
    generation: AtomicU32,
    next_free: AtomicU32,
    retired_sequence: AtomicUsize,
}

impl PayloadHeader {
    const fn new(next_free: u32) -> Self {
        Self {
            generation: AtomicU32::new(0),
            next_free: AtomicU32::new(next_free),
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
    #[inline]
    fn new(payload_index: u32) -> Self {
        Self { payload_index }
    }

    #[inline]
    fn payload_index(self) -> u32 {
        self.payload_index
    }
}

pub(super) struct PayloadReleaseBatch {
    first_payload_index: Option<u32>,
    last_payload_index: Option<u32>,
}

impl PayloadReleaseBatch {
    #[inline]
    pub(super) const fn new() -> Self {
        Self {
            first_payload_index: None,
            last_payload_index: None,
        }
    }
}

pub(super) struct PayloadPool<T> {
    free_head: NonNull<AtomicU64>,
    retired_head: NonNull<AtomicU64>,
    payload_headers: NonNull<PayloadHeader>,
    payloads: NonNull<T>,
    capacity: usize,
}

impl<T> Clone for PayloadPool<T> {
    fn clone(&self) -> Self {
        Self {
            free_head: self.free_head,
            retired_head: self.retired_head,
            payload_headers: self.payload_headers,
            payloads: self.payloads,
            capacity: self.capacity,
        }
    }
}

impl<T> PayloadPool<T> {
    #[inline]
    pub(super) fn new(
        free_head: NonNull<AtomicU64>,
        retired_head: NonNull<AtomicU64>,
        payload_headers: NonNull<PayloadHeader>,
        payloads: NonNull<T>,
        capacity: usize,
    ) -> Self {
        Self {
            free_head,
            retired_head,
            payload_headers,
            payloads,
            capacity,
        }
    }

    #[inline]
    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn reserve_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        if count == 0 {
            return None;
        }

        let chain = self.pop_free_payloads_exact(count)?;
        let mut cursor = self.payload_cursor(chain.first_payload_index);
        let last_index = count.wrapping_sub(1);
        for index in 0..count {
            self.prepare_reserved_payload(cursor.current());
            if index != last_index {
                cursor.advance();
            }
        }

        Some(chain)
    }

    pub(super) fn cancel_reserved_payloads(&self, chain: ReservedPayloads) {
        debug_assert!(chain.count != 0);
        self.push_free_payloads(chain.first_payload_index, chain.last_payload_index);
    }

    #[inline]
    pub(super) fn handle_for_payload(&self, payload_index: u32) -> PayloadHandle {
        let generation = self
            .payload_header(payload_index)
            .generation
            .load(Ordering::Relaxed);
        PayloadHandle::new(payload_index, generation)
    }

    pub(super) fn payload_for_handle(&self, handle: PayloadHandle) -> Option<ProtectedPayload> {
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

    pub(super) fn retire_to_batch(
        &self,
        handle: PayloadHandle,
        sequence: usize,
        batch: &mut PayloadReleaseBatch,
    ) {
        let Some(payload_index) = self.valid_payload_index(handle) else {
            return;
        };
        let payload_header = self.payload_header(payload_index);
        if payload_header.generation.load(Ordering::Acquire) != handle.generation() {
            return;
        }

        payload_header
            .retired_sequence
            .store(sequence, Ordering::Relaxed);
        self.prepend_payload(payload_index, batch);
    }

    pub(super) fn reclaim_retired_batch<F>(&self, batch: PayloadReleaseBatch, is_protected: F)
    where
        F: FnMut(usize) -> bool,
    {
        if let Some(first) = batch.first_payload_index {
            self.reclaim_payload_chain(first, is_protected);
        }
    }

    pub(super) fn reclaim_retired_payloads<F>(&self, is_protected: F)
    where
        F: FnMut(usize) -> bool,
    {
        if let Some(first) = self.take_retired_payloads() {
            self.reclaim_payload_chain(first, is_protected);
        }
    }

    pub(super) fn flush_retired_batch(&self, batch: PayloadReleaseBatch) {
        if let (Some(first), Some(last)) = (batch.first_payload_index, batch.last_payload_index) {
            self.push_retired_payloads(first, last);
        }
    }

    pub(super) fn flush_release_batch(&self, batch: PayloadReleaseBatch) {
        if let (Some(first), Some(last)) = (batch.first_payload_index, batch.last_payload_index) {
            self.push_free_payloads(first, last);
        }
    }

    #[inline]
    pub(super) fn protected_payload_ptr(&self, payload: ProtectedPayload) -> NonNull<T> {
        self.payload_at(payload.payload_index())
    }

    #[inline]
    pub(super) fn payload_at(&self, payload_index: u32) -> NonNull<T> {
        debug_assert!((payload_index as usize) < self.capacity);
        // SAFETY: callers only pass validated payload indices.
        unsafe { self.payloads.add(payload_index as usize) }
    }

    #[inline]
    pub(super) fn payload_cursor(&self, first: u32) -> PayloadChainCursor<'_, T> {
        PayloadChainCursor::new(self, first)
    }

    #[inline]
    pub(super) fn next_payload_index(&self, payload_index: u32) -> u32 {
        decode_non_empty_payload_index(
            self.payload_header(payload_index)
                .next_free
                .load(Ordering::Relaxed),
        )
    }

    pub(super) fn recover_as_exclusive(&self) {
        for index in 0..self.capacity {
            self.payload_header(index as u32)
                .retired_sequence
                .store(0, Ordering::Relaxed);
        }

        self.reset_free_links();
    }

    #[inline]
    fn prepare_reserved_payload(&self, payload_index: u32) {
        let payload_header = self.payload_header(payload_index);
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

    fn reclaim_payload_chain<F>(&self, first_payload_index: u32, mut is_protected: F)
    where
        F: FnMut(usize) -> bool,
    {
        let mut free_batch = PayloadReleaseBatch::new();
        let mut retired_batch = PayloadReleaseBatch::new();
        let mut encoded = encode_payload_index(first_payload_index);

        while encoded != EMPTY_PAYLOAD {
            let payload_index = decode_non_empty_payload_index(encoded);
            let payload_header = self.payload_header(payload_index);
            let next = payload_header.next_free.load(Ordering::Relaxed);
            let sequence = payload_header.retired_sequence.load(Ordering::Relaxed);

            if is_protected(sequence) {
                self.prepend_payload(payload_index, &mut retired_batch);
            } else {
                self.prepend_payload(payload_index, &mut free_batch);
            }

            encoded = next;
        }

        self.flush_release_batch(free_batch);
        self.flush_retired_batch(retired_batch);
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
    fn free_head(&self) -> &AtomicU64 {
        // SAFETY: `free_head` points into the live shared queue header.
        unsafe { self.free_head.as_ref() }
    }

    #[inline]
    fn retired_head(&self) -> &AtomicU64 {
        // SAFETY: `retired_head` points into the live shared queue header.
        unsafe { self.retired_head.as_ref() }
    }

    #[inline]
    fn pop_free_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        debug_assert!(count != 0);

        let free_head = self.free_head();
        self.pop_free_payloads_exact_from_head(
            free_head,
            count,
            PayloadPoolHead(free_head.load(Ordering::Acquire)),
        )
    }

    fn pop_free_payloads_exact_from_head(
        &self,
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
                    .payload_header(payload_index)
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
                    self.payload_header(last_payload_index)
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

    fn take_retired_payloads(&self) -> Option<u32> {
        let retired_head = self.retired_head();
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
    fn push_free_payloads(&self, first_payload_index: u32, last_payload_index: u32) {
        self.push_payloads(self.free_head(), first_payload_index, last_payload_index);
    }

    #[inline]
    fn push_retired_payloads(&self, first_payload_index: u32, last_payload_index: u32) {
        self.push_payloads(self.retired_head(), first_payload_index, last_payload_index);
    }

    fn push_payloads(&self, head: &AtomicU64, first_payload_index: u32, last_payload_index: u32) {
        let first = encode_payload_index(first_payload_index);
        let mut current = PayloadPoolHead(head.load(Ordering::Acquire));

        loop {
            self.payload_header(last_payload_index)
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
    fn prepend_payload(&self, payload_index: u32, batch: &mut PayloadReleaseBatch) {
        self.payload_header(payload_index).next_free.store(
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
    }

    fn reset_free_links(&self) {
        for index in 0..self.capacity {
            self.payload_header(index as u32)
                .next_free
                .store(next_free_after(index, self.capacity), Ordering::Relaxed);
        }

        let free_head = self.free_head();
        let current_free = PayloadPoolHead(free_head.load(Ordering::Acquire));
        free_head.store(
            PayloadPoolHead::new(
                first_for_capacity(self.capacity),
                current_free.tag().wrapping_add(1),
            )
            .0,
            Ordering::Release,
        );

        let retired_head = self.retired_head();
        let current_retired = PayloadPoolHead(retired_head.load(Ordering::Acquire));
        retired_head.store(
            PayloadPoolHead::new(EMPTY_PAYLOAD, current_retired.tag().wrapping_add(1)).0,
            Ordering::Release,
        );
    }

    #[inline]
    fn payload_header(&self, payload_index: u32) -> &PayloadHeader {
        debug_assert!((payload_index as usize) < self.capacity);
        // SAFETY: callers only pass validated payload indices.
        unsafe { self.payload_headers.add(payload_index as usize).as_ref() }
    }
}

pub(super) struct PayloadChainCursor<'a, T> {
    pool: &'a PayloadPool<T>,
    current: u32,
}

impl<'a, T> PayloadChainCursor<'a, T> {
    #[inline]
    fn new(pool: &'a PayloadPool<T>, first: u32) -> Self {
        Self {
            pool,
            current: first,
        }
    }

    #[inline]
    pub(super) fn current(&self) -> u32 {
        self.current
    }

    #[inline]
    pub(super) fn advance(&mut self) {
        self.current = self.pool.next_payload_index(self.current);
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
pub(super) fn initial_free_head(capacity: usize) -> u64 {
    PayloadPoolHead::new(first_for_capacity(capacity), 0).0
}

#[inline]
pub(super) const fn initial_retired_head() -> u64 {
    PayloadPoolHead::new(EMPTY_PAYLOAD, 0).0
}

pub(super) unsafe fn initialize_payload_headers(
    payload_headers: NonNull<PayloadHeader>,
    capacity: usize,
) {
    for index in 0..capacity {
        // SAFETY: caller guarantees `payload_headers` points at `capacity` headers.
        unsafe {
            payload_headers
                .add(index)
                .as_ptr()
                .write(PayloadHeader::new(next_free_after(index, capacity)))
        };
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
fn next_free_after(index: usize, capacity: usize) -> u32 {
    if index + 1 == capacity {
        EMPTY_PAYLOAD
    } else {
        encode_payload_index((index + 1) as u32)
    }
}

#[inline]
fn first_for_capacity(capacity: usize) -> u32 {
    if capacity == 0 {
        EMPTY_PAYLOAD
    } else {
        encode_payload_index(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        initial_free_head, initial_retired_head, initialize_payload_headers, PayloadHandle,
        PayloadHeader, PayloadPool, PayloadPoolHead, PayloadReleaseBatch,
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
        let free_head = AtomicU64::new(initial_free_head(capacity));
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
            initialize_payload_headers(payload_header_ptr, capacity);
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
        )
    }

    #[test]
    fn payload_header_uses_compact_layout() {
        assert_eq!(size_of::<PayloadHeader>(), 16);
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
        let stale_head = PayloadPoolHead(initial_free_head(8));

        let prefix = pool.reserve_payloads_exact(3).expect("reserve prefix");
        assert_eq!(prefix.first_payload_index(), 0);

        let current = pool
            .pop_free_payloads_exact_from_head(&free_head, 4, stale_head)
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
        pool.retire_to_batch(pool.handle_for_payload(2), 12, &mut batch);
        pool.retire_to_batch(pool.handle_for_payload(0), 10, &mut batch);
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
        pool.retire_to_batch(handle, 5, &mut batch);
        pool.reclaim_retired_batch(batch, |sequence| sequence == 5);
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
