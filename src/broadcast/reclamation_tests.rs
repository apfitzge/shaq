use super::*;

fn queues(capacity: usize, consumer_slots: usize) -> Vec<Broadcast<u64>> {
    let config = BroadcastConfig {
        capacity,
        producer_slots: 2,
        consumer_slots,
    };
    let layout = config.layout::<u64>().unwrap();
    let region = Region::alloc(NonZeroUsize::new(layout.size()).unwrap()).unwrap();
    // SAFETY: fresh aligned storage with portable u64 payloads, initialized once.
    let queue =
        unsafe { SharedQueue::create_in_region::<u64>(&region, &config, DEFAULT_QUEUE_IDENTIFIER) }
            .unwrap();
    let queues = vec![Broadcast::from_queue(queue)];
    #[cfg(not(miri))]
    let queues = {
        let mut queues = queues;
        let file = crate::shmem::create_temp_shmem_file().unwrap();
        file.set_len((layout.align() + layout.size()) as u64)
            .unwrap();
        // SAFETY: fresh bounded storage, matching u64 layout, no resizing while joined.
        queues.push(
            unsafe {
                Broadcast::create_at(&file, layout.align() as u64, layout.size() as u64, config)
            }
            .unwrap(),
        );
        queues
    };
    queues
}

fn count(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

#[test]
fn sequence_identity_tracks_commit_cancellation_and_prefixes_per_lane() {
    for queue in queues(8, 0) {
        let mut first = queue.producer(ProducerId::new(1)).unwrap();
        let mut second = queue.producer(ProducerId::new(1)).unwrap();
        let first_index = first.index();
        let second_index = second.index();
        assert_ne!(first_index, second_index);
        let prepared = first.try_prepare_write().unwrap();
        assert_eq!(prepared.sequence(), 0);
        assert_eq!(prepared.producer_index(), first_index);
        drop(prepared);
        let prepared = first.try_prepare_write().unwrap();
        assert_eq!(prepared.sequence(), 0);
        prepared.commit(10);
        let batch = first.try_prepare_write_batch(count(4)).unwrap();
        assert_eq!(batch.start_sequence(), 1);
        assert_eq!(batch.producer_index(), first_index);
        batch.commit_from_slice(&[11, 12]);
        let batch = first.try_prepare_write_batch(count(4)).unwrap();
        assert_eq!(batch.start_sequence(), 3);
        std::mem::forget(batch);
        assert_eq!(first.try_prepare_write().unwrap().sequence(), 3);
        let prepared = second.try_prepare_write().unwrap();
        assert_eq!(prepared.sequence(), 0);
        assert_eq!(prepared.producer_index(), second_index);
        prepared.commit(20);
        assert_eq!(first.reclaimable_before(), 3);
        assert_eq!(second.reclaimable_before(), 1);
    }
}

#[test]
fn without_consumers_watermark_is_publication_even_while_prepared() {
    for slots in [0, 2] {
        for queue in queues(8, slots) {
            let mut producer = queue.producer(ProducerId::new(1)).unwrap();
            assert_eq!(producer.reclaimable_before(), 0);
            producer
                .try_prepare_write_batch(count(3))
                .unwrap()
                .commit_from_slice(&[1, 2, 3]);
            // Progress is useful well before the broadcast ring fills.
            assert_eq!(producer.reclaimable_before(), 3);
            let mut prepared = producer.try_prepare_write().unwrap();
            prepared.as_mut().write(99);
            assert_eq!(prepared.sequence(), 3);
            assert_eq!(prepared.reclaimable_before(), 3);
            drop(prepared);
            let mut batch = producer.try_prepare_write_batch(count(8)).unwrap();
            batch.as_mut(0).write(99);
            assert_eq!(batch.start_sequence(), 3);
            assert_eq!(batch.reclaimable_before(), 3);
            drop(batch);
            assert_eq!(producer.reclaimable_before(), 3);
        }
    }
}

#[test]
fn held_reads_and_slowest_consumer_pin_reclamation() {
    for queue in queues(8, 2) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut typed = queue.consumer().unwrap();
        // SAFETY: u64 initializes every payload byte.
        let mut slices = unsafe { queue.slice_consumer() }.unwrap();
        assert!(producer.try_write_slice(&[1, 2, 3]));
        let read = typed.try_reserve_read().unwrap();
        let slice_batch = slices.try_reserve_read_batch(count(3)).unwrap();
        assert_eq!(producer.reclaimable_before(), 0);
        assert_eq!(*read.as_ref(), 1);
        drop(read);
        assert_eq!(producer.reclaimable_before(), 0);
        drop(slice_batch);
        assert_eq!(producer.reclaimable_before(), 1);
        let read_batch = typed.try_reserve_read_batch(count(2)).unwrap();
        assert_eq!(producer.reclaimable_before(), 1);
        drop(read_batch);
        assert_eq!(producer.reclaimable_before(), 3);
        producer.try_prepare_write().unwrap().commit(4);
        drop(typed);
        assert_eq!(producer.reclaimable_before(), 3);
        let read = slices.try_read().unwrap();
        assert_eq!(producer.reclaimable_before(), 3);
        assert_eq!(read.as_slice(), 4u64.to_ne_bytes());
        drop(read);
        assert_eq!(producer.reclaimable_before(), 4);
    }
}

#[test]
fn joins_during_preparation_and_reused_slots_preserve_prior_bounds() {
    for queue in queues(8, 1) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        producer.try_prepare_write().unwrap().commit(1);
        let prepared = producer.try_prepare_write().unwrap();
        let saved_bound = prepared.reclaimable_before();
        assert_eq!(saved_bound, 1);
        let mut consumer = queue.consumer().unwrap();
        let index = consumer.index();
        assert_eq!(prepared.reclaimable_before(), saved_bound);
        prepared.commit(2);
        assert_eq!(producer.reclaimable_before(), 1);
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(producer.reclaimable_before(), 2);
        producer.try_prepare_write().unwrap().commit(3);
        assert_eq!(producer.reclaimable_before(), 2);
        drop(consumer);
        assert_eq!(producer.reclaimable_before(), 3);
        let mut consumer = queue.consumer().unwrap();
        assert_eq!(consumer.index(), index);
        assert_eq!(consumer.try_read(), None);
        assert_eq!(producer.reclaimable_before(), 3);
        let batch = producer.try_prepare_write_batch(count(4)).unwrap();
        assert_eq!(batch.reclaimable_before(), 3);
        batch.commit_from_slice(&[4, 5]);
        assert_eq!(producer.reclaimable_before(), 3);
        assert_eq!(consumer.try_read(), Some(4));
        assert_eq!(producer.reclaimable_before(), 4);
        drop(consumer);
        assert_eq!(producer.reclaimable_before(), 5);
    }
}

#[test]
fn unpublished_legacy_reservation_is_not_reclaimable_publication() {
    for queue in queues(8, 0) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        // SAFETY: initialize the legacy reservation before any subsequent write.
        let mut guard = unsafe { producer.try_reserve_write() }.unwrap();
        guard.as_mut().write(1);
        std::mem::forget(guard);
        assert_eq!(producer.reclaimable_before(), 0);
        let prepared = producer.try_prepare_write().unwrap();
        assert_eq!(prepared.sequence(), 1);
        assert_eq!(prepared.reclaimable_before(), 0);
        prepared.commit(2);
        assert_eq!(producer.reclaimable_before(), 2);
    }
}

#[test]
fn consumers_constrain_each_lane_independently() {
    for queue in queues(8, 1) {
        let mut first = queue.producer(ProducerId::new(1)).unwrap();
        let mut second = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        first
            .try_prepare_write_batch(count(3))
            .unwrap()
            .commit_from_slice(&[10, 11, 12]);
        second.try_prepare_write().unwrap().commit(20);
        assert_eq!(first.reclaimable_before(), 0);
        assert_eq!(second.reclaimable_before(), 0);
        assert_eq!(consumer.try_read(), Some(10));
        assert_eq!(first.reclaimable_before(), 1);
        assert_eq!(second.reclaimable_before(), 0);
        assert_eq!(consumer.try_read(), Some(20));
        assert_eq!(first.reclaimable_before(), 1);
        assert_eq!(second.reclaimable_before(), 1);
        drop(consumer);
        assert_eq!(first.reclaimable_before(), 3);
        assert_eq!(second.reclaimable_before(), 1);
    }
}

#[test]
fn delayed_provisional_join_can_lower_scan_but_cannot_read_reclaimed_prefix() {
    use std::cell::{Cell, RefCell};
    for queue in queues(8, 1) {
        let producer = RefCell::new(queue.producer(ProducerId::new(1)).unwrap());
        let index = queue.shared_queue.acquire_consumer_index().unwrap();
        let state = queue
            .shared_queue
            .producer_lanes()
            .next()
            .unwrap()
            .consumer_state();
        let calls = Cell::new(0);
        let saved_bound = Cell::new(0);
        // Use join's existing reservation reader to schedule publication after
        // its first sample, and inspect the real provisional limit on the next
        // sample. No production instrumentation or fabricated cursor is needed.
        let start = state.join(index, || {
            let sample = producer.borrow().lane.reserved();
            let call = calls.get();
            calls.set(call + 1);
            if call == 0 {
                assert!(producer.borrow_mut().try_write_slice(&[1, 2, 3]));
                saved_bound.set(producer.borrow().reclaimable_before());
                assert_eq!(saved_bound.get(), 3);
            } else {
                // The slot is still globally joining, with provisional cursor 0.
                assert_eq!(producer.borrow().reclaimable_before(), 0);
            }
            sample
        });
        assert_eq!(calls.get(), 2);
        assert_eq!(start, saved_bound.get());
        assert_eq!(producer.borrow().reclaimable_before(), 3);
        state.release(index);
        queue.shared_queue.release_consumer_index(index);
    }
}

#[cfg(not(miri))]
#[test]
fn concurrent_reclamation_never_overtakes_a_read_or_rejoining_consumer() {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize},
        thread,
        time::Instant,
    };
    const TIMEOUT: Duration = Duration::from_secs(10);
    for seed in 0..4 {
        for queue in queues(4, 2) {
            let done = Arc::new(AtomicBool::new(false));
            let reclaimed = Arc::new(AtomicUsize::new(0));
            let mut readers = Vec::new();
            for reader in 0..2 {
                let mut consumer = queue.consumer().unwrap();
                let queue = queue.clone();
                let done = Arc::clone(&done);
                let reclaimed = Arc::clone(&reclaimed);
                readers.push(thread::spawn(move || {
                    let deadline = Instant::now() + TIMEOUT;
                    let mut reads = 0;
                    let mut rounds = seed + reader;
                    while !done.load(Ordering::Acquire) {
                        for _ in 0..4 {
                            if let Some(guard) = consumer.try_reserve_read() {
                                let sequence = guard.consumer.next_for_lane(guard.lane.get());
                                assert_eq!(*guard.as_ref(), sequence as u64 + 1);
                                assert!(sequence >= reclaimed.load(Ordering::Acquire));
                                thread::yield_now();
                                assert!(sequence >= reclaimed.load(Ordering::Acquire));
                                assert_eq!(*guard.as_ref(), sequence as u64 + 1);
                                reads += 1;
                            }
                        }
                        rounds += 1;
                        if reads > 0 && rounds % 3 == 0 {
                            drop(consumer);
                            thread::yield_now();
                            consumer = queue.consumer().unwrap();
                        }
                        assert!(Instant::now() < deadline, "reclamation reader timed out");
                    }
                    reads
                }));
            }
            let mut producer = queue.producer(ProducerId::new(1)).unwrap();
            let deadline = Instant::now() + TIMEOUT;
            let mut publication = 0;
            let mut retained_bound = 0;
            while publication < 1000 {
                assert!(Instant::now() < deadline, "reclamation producer timed out");
                let n = (1000 - publication).min(1 + (publication + seed) % 4);
                if let Some(mut batch) = producer.try_prepare_write_batch(count(n)) {
                    assert_eq!(batch.start_sequence(), publication);
                    let bound = batch.reclaimable_before();
                    assert!(bound <= publication);
                    retained_bound = retained_bound.max(bound);
                    reclaimed.store(retained_bound, Ordering::Release);
                    for i in 0..n {
                        batch.as_mut(i).write((publication + i + 1) as u64);
                    }
                    thread::yield_now();
                    // SAFETY: all n cells were initialized above.
                    unsafe { batch.commit() };
                    publication += n;
                }
                let bound = producer.reclaimable_before();
                assert!(bound <= publication);
                retained_bound = retained_bound.max(bound);
                reclaimed.store(retained_bound, Ordering::Release);
                thread::yield_now();
            }
            done.store(true, Ordering::Release);
            for reader in readers {
                assert!(reader.join().unwrap() > 0);
            }
            assert_eq!(producer.reclaimable_before(), publication);
        }
    }
}
