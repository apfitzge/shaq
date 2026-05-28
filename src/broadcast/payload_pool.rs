use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

pub(super) const PAYLOADS_PER_RING_ENTRY: usize = 2;

const EMPTY_PAYLOAD: u32 = 0;

/// Payload descriptor. `state` is owned by the payload lifetime protocol;
/// `next_free` is owned by the pool's free-payload implementation.
#[repr(C)]
pub(super) struct PayloadHeader {
    /// High 32 bits: generation. Low 32 bits: reference count.
    state: AtomicU64,
    next_free: AtomicU32,
}

impl PayloadHeader {
    const fn new(next_free: u32) -> Self {
        Self {
            state: AtomicU64::new(0),
            next_free: AtomicU32::new(next_free),
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

/// Payload lifetime state: high 32 bits are generation, low 32 bits are the
/// active reader/reference count.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct PayloadLifetimeState(u64);

impl PayloadLifetimeState {
    #[inline]
    const fn new(generation: u32, refcnt: u32) -> Self {
        Self(((generation as u64) << 32) | refcnt as u64)
    }

    #[inline]
    fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    fn refcnt(self) -> u32 {
        self.0 as u32
    }
}

/// Payload pool free head: high 32 bits are the ABA tag, low 32 bits are a
/// nullable payload index encoded as `payload_index + 1`, with zero meaning empty.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct PayloadPoolFreeHead(u64);

impl PayloadPoolFreeHead {
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
pub(super) struct PinnedPayload {
    handle: PayloadHandle,
    payload_index: u32,
}

impl PinnedPayload {
    #[inline]
    fn new(handle: PayloadHandle, payload_index: u32) -> Self {
        Self {
            handle,
            payload_index,
        }
    }

    #[inline]
    fn payload_index(self) -> u32 {
        self.payload_index
    }

    #[inline]
    pub(super) fn handle(self) -> PayloadHandle {
        self.handle
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
    payload_headers: NonNull<PayloadHeader>,
    payloads: NonNull<T>,
    capacity: usize,
}

impl<T> Clone for PayloadPool<T> {
    fn clone(&self) -> Self {
        Self {
            free_head: self.free_head,
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
        payload_headers: NonNull<PayloadHeader>,
        payloads: NonNull<T>,
        capacity: usize,
    ) -> Self {
        Self {
            free_head,
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

        let mut cursor = self.payload_cursor(chain.first_payload_index);
        let last_index = chain.count.wrapping_sub(1);
        for index in 0..chain.count {
            let payload_index = cursor.current();
            let payload_header = self.payload_header(payload_index);
            let generation =
                PayloadLifetimeState(payload_header.state.load(Ordering::Relaxed)).generation();
            payload_header.state.store(
                PayloadLifetimeState::new(generation, 0).0,
                Ordering::Relaxed,
            );
            if index != last_index {
                cursor.advance();
            }
        }
        self.push_free_payloads(chain.first_payload_index, chain.last_payload_index);
    }

    #[inline]
    pub(super) fn handle_for_payload(&self, payload_index: u32) -> PayloadHandle {
        let state = PayloadLifetimeState(
            self.payload_header(payload_index)
                .state
                .load(Ordering::Relaxed),
        );
        PayloadHandle::new(payload_index, state.generation())
    }

    pub(super) fn pin(&self, handle: PayloadHandle) -> Option<PinnedPayload> {
        let payload_index = handle.payload_index()?;
        if payload_index as usize >= self.capacity {
            return None;
        }

        let generation = handle.generation();
        let payload_header = self.payload_header(payload_index);
        let mut state = PayloadLifetimeState(payload_header.state.load(Ordering::Acquire));
        loop {
            if state.generation() != generation {
                return None;
            }
            let refcnt = state.refcnt();
            if refcnt == 0 || refcnt == u32::MAX {
                return None;
            }
            let new_state = PayloadLifetimeState::new(generation, refcnt.wrapping_add(1));
            match payload_header.state.compare_exchange_weak(
                state.0,
                new_state.0,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(PinnedPayload::new(handle, payload_index)),
                Err(current) => state = PayloadLifetimeState(current),
            }
        }
    }

    pub(super) fn release_handle(&self, handle: PayloadHandle) {
        if let Some(payload_index) = self.release_candidate(handle) {
            self.push_free_payloads(payload_index, payload_index);
        }
    }

    pub(super) fn release_pinned_payloads(&self, payloads: &[PinnedPayload]) {
        let mut batch = PayloadReleaseBatch::new();
        for payload in payloads {
            self.release_to_batch(payload.handle, &mut batch);
        }
        self.flush_release_batch(batch);
    }

    pub(super) fn release_to_batch(&self, handle: PayloadHandle, batch: &mut PayloadReleaseBatch) {
        if let Some(payload_index) = self.release_candidate(handle) {
            self.prepend_free_payload(payload_index, batch);
        }
    }

    pub(super) fn flush_release_batch(&self, batch: PayloadReleaseBatch) {
        if let (Some(first), Some(last)) = (batch.first_payload_index, batch.last_payload_index) {
            self.push_free_payloads(first, last);
        }
    }

    #[inline]
    pub(super) fn pinned_payload_ptr(&self, payload: PinnedPayload) -> NonNull<T> {
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
            let payload_header = self.payload_header(index as u32);
            let generation =
                PayloadLifetimeState(payload_header.state.load(Ordering::Relaxed)).generation();
            payload_header.state.store(
                PayloadLifetimeState::new(generation, 0).0,
                Ordering::Relaxed,
            );
        }

        self.reset_free_links();
    }

    #[inline]
    fn prepare_reserved_payload(&self, payload_index: u32) {
        let payload_header = self.payload_header(payload_index);
        let mut generation = PayloadLifetimeState(payload_header.state.load(Ordering::Relaxed))
            .generation()
            .wrapping_add(1);
        if generation == 0 {
            generation = 1;
        }
        payload_header.state.store(
            PayloadLifetimeState::new(generation, 1).0,
            Ordering::Relaxed,
        );
    }

    fn release_candidate(&self, handle: PayloadHandle) -> Option<u32> {
        let payload_index = handle.payload_index()?;
        if payload_index as usize >= self.capacity {
            return None;
        }

        let payload_header = self.payload_header(payload_index);
        let previous = PayloadLifetimeState(payload_header.state.fetch_sub(1, Ordering::AcqRel));
        let previous_refcnt = previous.refcnt();
        debug_assert!(previous_refcnt != 0);
        if previous_refcnt == 1 {
            Some(payload_index)
        } else {
            None
        }
    }

    #[inline]
    fn free_head(&self) -> &AtomicU64 {
        // SAFETY: `free_head` points into the live shared queue header.
        unsafe { self.free_head.as_ref() }
    }

    #[inline]
    fn pop_free_payloads_exact(&self, count: usize) -> Option<ReservedPayloads> {
        debug_assert!(count != 0);

        let free_head = self.free_head();
        self.pop_free_payloads_exact_from_head(
            free_head,
            count,
            PayloadPoolFreeHead(free_head.load(Ordering::Acquire)),
        )
    }

    fn pop_free_payloads_exact_from_head(
        &self,
        free_head: &AtomicU64,
        count: usize,
        mut head: PayloadPoolFreeHead,
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
                    let current = PayloadPoolFreeHead(free_head.load(Ordering::Acquire));
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

            let new_head = PayloadPoolFreeHead::new(encoded, head.tag().wrapping_add(1));
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
                Err(current) => head = PayloadPoolFreeHead(current),
            }
        }
    }

    #[inline]
    fn push_free_payloads(&self, first_payload_index: u32, last_payload_index: u32) {
        let first = encode_payload_index(first_payload_index);
        let free_head = self.free_head();
        let mut head = PayloadPoolFreeHead(free_head.load(Ordering::Acquire));

        loop {
            self.payload_header(last_payload_index)
                .next_free
                .store(head.encoded_first(), Ordering::Relaxed);
            let new_head = PayloadPoolFreeHead::new(first, head.tag().wrapping_add(1));
            match free_head.compare_exchange_weak(
                head.0,
                new_head.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => head = PayloadPoolFreeHead(current),
            }
        }
    }

    #[inline]
    fn prepend_free_payload(&self, payload_index: u32, batch: &mut PayloadReleaseBatch) {
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
        let current = PayloadPoolFreeHead(free_head.load(Ordering::Acquire));
        free_head.store(
            PayloadPoolFreeHead::new(
                first_for_capacity(self.capacity),
                current.tag().wrapping_add(1),
            )
            .0,
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
    PayloadPoolFreeHead::new(first_for_capacity(capacity), 0).0
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

/// Nullable payload index stored in `PayloadHandle`, `PayloadPoolFreeHead`, and free-list links.
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
        initial_free_head, initialize_payload_headers, PayloadHandle, PayloadHeader, PayloadPool,
        PayloadPoolFreeHead, PayloadReleaseBatch,
    };
    use core::{
        mem::{size_of, MaybeUninit},
        ptr::NonNull,
        sync::atomic::{AtomicU64, Ordering},
    };

    fn initialized_pool(capacity: usize) -> (AtomicU64, Vec<PayloadHeader>, Vec<MaybeUninit<u64>>) {
        let head = AtomicU64::new(initial_free_head(capacity));
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

        (head, payload_headers, payloads)
    }

    fn pool<'a>(
        head: &'a AtomicU64,
        payload_headers: &'a [PayloadHeader],
        payloads: &'a mut [MaybeUninit<u64>],
    ) -> PayloadPool<u64> {
        PayloadPool::new(
            NonNull::from(head),
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
        let (head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&head, &payload_headers, &mut payloads);

        let first = pool.reserve_payloads_exact(2).expect("first reserve");
        assert_eq!(first.first_payload_index(), 0);
        assert_eq!(payload_headers[1].next_free.load(Ordering::Relaxed), 0);

        let second = pool.reserve_payloads_exact(2).expect("second reserve");
        assert_eq!(second.first_payload_index(), 2);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn stale_short_free_chain_retries_current_head() {
        let (head, payload_headers, mut payloads) = initialized_pool(8);
        let pool = pool(&head, &payload_headers, &mut payloads);
        let stale_head = PayloadPoolFreeHead(initial_free_head(8));

        let prefix = pool.reserve_payloads_exact(3).expect("reserve prefix");
        assert_eq!(prefix.first_payload_index(), 0);

        let current = pool
            .pop_free_payloads_exact_from_head(&head, 4, stale_head)
            .expect("reserve from current head after stale short chain");
        assert_eq!(current.first_payload_index(), 3);

        let last = pool.reserve_payloads_exact(1).expect("last payload");
        assert_eq!(last.first_payload_index(), 7);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn cancel_reserved_payloads_restores_chain_to_pool() {
        let (head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&head, &payload_headers, &mut payloads);

        let removed = pool.reserve_payloads_exact(2).expect("reserve");
        pool.cancel_reserved_payloads(removed);

        let all = pool.reserve_payloads_exact(4).expect("reserve all");
        assert_eq!(all.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn release_batch_restores_reusable_payloads_together() {
        let (head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&head, &payload_headers, &mut payloads);
        assert!(pool.reserve_payloads_exact(4).is_some());

        let mut batch = PayloadReleaseBatch::new();
        pool.release_to_batch(pool.handle_for_payload(2), &mut batch);
        pool.release_to_batch(pool.handle_for_payload(0), &mut batch);
        pool.flush_release_batch(batch);

        let chain = pool.reserve_payloads_exact(2).expect("reserve released");
        assert_eq!(chain.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }

    #[test]
    fn pin_blocks_reuse_until_released() {
        let (head, payload_headers, mut payloads) = initialized_pool(1);
        let pool = pool(&head, &payload_headers, &mut payloads);

        let reserved = pool.reserve_payloads_exact(1).expect("reserve");
        let handle = pool.handle_for_payload(reserved.first_payload_index());
        let pinned = pool.pin(handle).expect("pin");

        pool.release_handle(handle);
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.release_handle(pinned.handle());
        assert!(pool.reserve_payloads_exact(1).is_some());
    }

    #[test]
    fn invalid_handle_does_not_pin() {
        let (head, payload_headers, mut payloads) = initialized_pool(1);
        let pool = pool(&head, &payload_headers, &mut payloads);

        assert!(pool.pin(PayloadHandle::EMPTY).is_none());
    }

    #[test]
    fn recover_as_exclusive_restores_all_payloads() {
        let (head, payload_headers, mut payloads) = initialized_pool(4);
        let pool = pool(&head, &payload_headers, &mut payloads);
        assert!(pool.reserve_payloads_exact(4).is_some());
        assert!(pool.reserve_payloads_exact(1).is_none());

        pool.recover_as_exclusive();

        let chain = pool.reserve_payloads_exact(4).expect("reserve after reset");
        assert_eq!(chain.first_payload_index(), 0);
        assert!(pool.reserve_payloads_exact(1).is_none());
    }
}
