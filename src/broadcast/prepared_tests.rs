use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn queues(capacity: usize, consumer_slots: usize) -> Vec<Broadcast<u64>> {
    let config = BroadcastConfig {
        capacity,
        producer_slots: 2,
        consumer_slots,
    };
    let layout = config.layout::<u64>().unwrap();
    let region = Region::alloc(NonZeroUsize::new(layout.size()).unwrap()).unwrap();
    // SAFETY: fresh allocation with portable u64 payloads, initialized once.
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
        // SAFETY: fresh bounded storage, portable u64 payloads, no resizing.
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

fn frontiers(queue: &Broadcast<u64>, expected: usize) {
    let lane = queue.shared_queue.producer_lanes().next().unwrap();
    assert_eq!(lane.reserved(), expected);
    assert_eq!(lane.published(), expected);
}

#[test]
fn single_commit_is_explicit_and_joins_observe_the_right_frontier() {
    for queue in queues(4, 3) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut before = queue.consumer().unwrap();
        let mut prepared = producer.try_prepare_write().unwrap();
        prepared.as_mut().write(42);
        let mut during = queue.consumer().unwrap();
        frontiers(&queue, 0);
        assert_eq!(before.try_read(), None);
        assert_eq!(during.try_read(), None);
        // SAFETY: the cell holds a valid u64 initialized above.
        unsafe { prepared.commit_initialized() };
        frontiers(&queue, 1);
        let mut after = queue.consumer().unwrap();
        assert_eq!(before.try_read(), Some(42));
        assert_eq!(during.try_read(), Some(42));
        assert_eq!(after.try_read(), None);
        producer.try_prepare_write().unwrap().commit(43);
        for consumer in [&mut before, &mut during, &mut after] {
            assert_eq!(consumer.try_read(), Some(43));
        }
    }
}

#[test]
fn drop_error_and_panic_cancel_without_a_sequence_gap() {
    for queue in queues(4, 1) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        for mode in 0..3 {
            match mode {
                0 => {
                    let mut prepared = producer.try_prepare_write().unwrap();
                    prepared.as_mut().write(999);
                }
                1 => {
                    fn serialize(producer: &mut Producer<u64>) -> Result<(), &'static str> {
                        let mut prepared = producer.try_prepare_write().unwrap();
                        prepared.as_mut().write(999);
                        Err("serialization failed")
                    }
                    let result = serialize(&mut producer);
                    assert!(result.is_err());
                }
                _ => {
                    assert!(catch_unwind(AssertUnwindSafe(|| {
                        let mut prepared = producer.try_prepare_write_batch(count(4)).unwrap();
                        prepared.as_mut(0).write(999);
                        panic!("partial serialization panicked");
                    }))
                    .is_err());
                }
            }
            frontiers(&queue, mode);
            assert_eq!(consumer.try_read(), None);
            producer.try_prepare_write().unwrap().commit(mode as u64);
            assert_eq!(consumer.try_read(), Some(mode as u64));
        }
    }
}

#[test]
fn forgotten_preparations_and_mixed_legacy_writes_do_not_leave_holes() {
    for queue in queues(4, 1) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        let mut prepared = producer.try_prepare_write().unwrap();
        prepared.as_mut().write(999);
        std::mem::forget(prepared);
        let mut batch = producer.try_prepare_write_batch(count(4)).unwrap();
        batch.as_mut(1).write(999);
        std::mem::forget(batch);
        frontiers(&queue, 0);
        assert!(producer.try_write(1).is_ok());
        producer.try_prepare_write().unwrap().commit(2);
        assert!(producer.try_write_slice(&[3, 4]));
        for value in 1..=4 {
            assert_eq!(consumer.try_read(), Some(value));
        }
    }
}

#[test]
fn initialized_forgotten_legacy_single_reservation_allows_later_writes() {
    for queue in queues(4, 1) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        // SAFETY: the cell is initialized before reusing the producer.
        let mut guard = unsafe { producer.try_reserve_write() }.unwrap();
        guard.as_mut().write(1);
        std::mem::forget(guard);
        assert_eq!(consumer.try_read(), None);
        producer.try_prepare_write().unwrap().commit(42);
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(42));
        assert_eq!(consumer.try_read(), None);
    }
}

#[test]
fn legacy_batch_defers_reservation_and_commits_on_drop() {
    for queue in queues(4, 2) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut before = queue.consumer().unwrap();
        // SAFETY: both cells are initialized before drop below.
        let mut batch = unsafe { producer.try_reserve_write_batch(count(2)) }.unwrap();
        assert_eq!(batch.len(), 2);
        for i in 0..2 {
            // SAFETY: i is in the two-cell batch.
            unsafe { batch.write(i, i as u64 + 1) };
        }
        frontiers(&queue, 0);
        let mut during = queue.consumer().unwrap();
        assert_eq!(before.try_read(), None);
        assert_eq!(during.try_read(), None);
        drop(batch);
        frontiers(&queue, 2);
        for consumer in [&mut before, &mut during] {
            assert_eq!(consumer.try_read(), Some(1));
            assert_eq!(consumer.try_read(), Some(2));
        }
        // SAFETY: the uninitialized batch is forgotten, never dropped or published.
        let batch = unsafe { producer.try_reserve_write_batch(count(2)) }.unwrap();
        std::mem::forget(batch);
        frontiers(&queue, 2);
        producer.try_prepare_write().unwrap().commit(42);
        for consumer in [&mut before, &mut during] {
            assert_eq!(consumer.try_read(), Some(42));
            assert_eq!(consumer.try_read(), None);
        }
    }
}

#[test]
fn full_batch_wraps_and_prefix_commit_reuses_its_suffix() {
    for queue in queues(4, 2) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        assert!(producer.try_write_slice(&[0, 1, 2]));
        for value in 0..3 {
            assert_eq!(consumer.try_read(), Some(value));
        }
        // The four prepared cells start at physical index 3 and wrap to 0.
        let mut batch = producer.try_prepare_write_batch(count(4)).unwrap();
        for i in (0..batch.len()).rev() {
            batch.as_mut(i).write(3 + i as u64);
        }
        // SAFETY: all four cells were initialized, even though out of order.
        unsafe { batch.commit() };
        for value in 3..7 {
            assert_eq!(consumer.try_read(), Some(value));
        }

        let mut batch = producer.try_prepare_write_batch(count(4)).unwrap();
        batch.as_mut(0).write(7);
        batch.as_mut(1).write(8);
        // The third cell's serialization fails; only the successful prefix is published.
        let mut late = queue.consumer().unwrap();
        // SAFETY: exactly the first two cells hold initialized u64 values.
        unsafe { batch.commit_prefix(2) };
        frontiers(&queue, 9);
        for reader in [&mut consumer, &mut late] {
            assert_eq!(reader.try_read(), Some(7));
            assert_eq!(reader.try_read(), Some(8));
            assert_eq!(reader.try_read(), None);
        }
        producer
            .try_prepare_write_batch(count(4))
            .unwrap()
            .commit_from_slice(&[9, 10]);
        for reader in [&mut consumer, &mut late] {
            assert_eq!(reader.try_read(), Some(9));
            assert_eq!(reader.try_read(), Some(10));
        }
        frontiers(&queue, 11);
    }
}

#[test]
fn zero_prefix_and_invalid_bounds_never_publish() {
    for queue in queues(4, 1) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        let batch = producer.try_prepare_write_batch(count(4)).unwrap();
        // SAFETY: the empty prefix requires no initialized cells.
        unsafe { batch.commit_prefix(0) };
        producer
            .try_prepare_write_batch(count(4))
            .unwrap()
            .commit_from_slice(&[]);
        for mode in 0..3 {
            assert!(catch_unwind(AssertUnwindSafe(|| {
                let mut batch = producer.try_prepare_write_batch(count(4)).unwrap();
                match mode {
                    0 => {
                        batch.as_mut(4).write(99);
                    }
                    1 => batch.commit_from_slice(&[1, 2, 3, 4, 5]),
                    _ => {
                        // SAFETY: invalid count is specified to panic before
                        // publishing or accessing any cells.
                        unsafe { batch.commit_prefix(5) };
                    }
                }
            }))
            .is_err());
            frontiers(&queue, 0);
            assert_eq!(consumer.try_read(), None);
        }
        producer.try_prepare_write().unwrap().commit(42);
        assert_eq!(consumer.try_read(), Some(42));
    }
}

#[test]
fn capacity_fails_before_caller_work_and_held_guards_pin_cells() {
    for queue in queues(2, 2) {
        let mut producer = queue.producer(ProducerId::new(1)).unwrap();
        let mut consumer = queue.consumer().unwrap();
        // SAFETY: u64 has no uninitialized padding.
        let mut slices = unsafe { queue.slice_consumer() }.unwrap();
        producer
            .try_prepare_write_batch(count(2))
            .unwrap()
            .commit_from_slice(&[1, 2]);
        let read = consumer.try_reserve_read_batch(count(2)).unwrap();
        let slice = slices.try_read().unwrap();
        let mut calls = 0;
        let attempt = (|| {
            let prepared = producer.try_prepare_write()?;
            calls += 1;
            prepared.commit(3);
            Some(())
        })();
        assert_eq!(attempt, None);
        assert_eq!(calls, 0);
        assert_eq!(slice.as_slice(), 1u64.to_ne_bytes());
        assert_eq!(read.as_slices(), (&[1, 2][..], &[][..]));
        drop(read);
        assert!(producer.try_prepare_write().is_none());
        drop(slice);
        producer.try_prepare_write().unwrap().commit(3);
        assert_eq!(consumer.try_read(), Some(3));
        drop(slices);
        producer
            .try_prepare_write_batch(count(2))
            .unwrap()
            .commit_from_slice(&[4, 5]);
    }
}

#[test]
fn no_consumers_and_independent_lanes() {
    for consumer_slots in [0, 2] {
        for queue in queues(4, consumer_slots) {
            let mut first = queue.producer(ProducerId::new(1)).unwrap();
            let mut second = queue.producer(ProducerId::new(1)).unwrap();
            assert!(first.try_prepare_write_batch(count(5)).is_none());
            for round in 0..10 {
                let prepared = first.try_prepare_write_batch(count(4)).unwrap();
                second.try_prepare_write().unwrap().commit(round);
                prepared.commit_from_slice(&[1, 2, 3, 4]);
            }
            assert_eq!(first.lane.published(), 40);
            assert_eq!(second.lane.published(), 10);
        }
    }
}

#[cfg(not(miri))]
mod concurrent {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc, Arc, Barrier,
        },
        thread,
        time::{Duration, Instant},
    };

    const TIMEOUT: Duration = Duration::from_secs(10);

    #[test]
    fn joins_during_preparation_survive_commit_drop_and_forget() {
        for mode in 0..3 {
            for queue in queues(4, 1) {
                let mut producer = queue.producer(ProducerId::new(1)).unwrap();
                assert!(producer.try_write_slice(&[0, 1, 2, 3]));
                let (ready_tx, ready_rx) = mpsc::sync_channel(0);
                let (resume_tx, resume_rx) = mpsc::sync_channel(0);
                let writing = thread::spawn(move || {
                    let mut prepared = producer.try_prepare_write_batch(count(4)).unwrap();
                    prepared.as_mut(0).write(99);
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(TIMEOUT).unwrap();
                    match mode {
                        0 => prepared.commit_from_slice(&[4, 5]),
                        1 => drop(prepared),
                        _ => std::mem::forget(prepared),
                    }
                    producer
                });
                ready_rx.recv_timeout(TIMEOUT).unwrap();
                let mut consumer = queue.consumer().unwrap();
                assert_eq!(consumer.core.next_for_lane(0), 4);
                resume_tx.send(()).unwrap();
                let mut producer = writing.join().unwrap();
                if mode == 0 {
                    assert_eq!(consumer.try_read(), Some(4));
                    assert_eq!(consumer.try_read(), Some(5));
                } else {
                    frontiers(&queue, 4);
                    assert_eq!(consumer.try_read(), None);
                }
                producer.try_prepare_write().unwrap().commit(42);
                assert_eq!(consumer.try_read(), Some(42));
            }
        }
    }

    #[test]
    fn racing_join_and_prefix_commit_preserve_future_writes() {
        for queue in queues(4, 2) {
            let mut producer = queue.producer(ProducerId::new(1)).unwrap();
            let mut existing = queue.consumer().unwrap();
            for round in 0..100 {
                let joiner = queue.clone();
                let barrier = Arc::new(Barrier::new(2));
                let joining_barrier = Arc::clone(&barrier);
                let joining = thread::spawn(move || {
                    joining_barrier.wait();
                    joiner.consumer().unwrap()
                });
                let prepared = producer.try_prepare_write_batch(count(4)).unwrap();
                barrier.wait();
                prepared.commit_from_slice(&[1, 2]);
                let mut late = joining.join().unwrap();
                assert_eq!(existing.try_read(), Some(1));
                assert_eq!(existing.try_read(), Some(2));
                // Joining can observe either side of the reservation store;
                // it must see the entire prefix or skip it, never a partial one.
                match late.try_read() {
                    Some(1) => assert_eq!(late.try_read(), Some(2)),
                    None => (),
                    value => panic!("unexpected prefix in round {round}: {value:?}"),
                }
                assert_eq!(late.try_read(), None);
                producer
                    .try_prepare_write_batch(count(2))
                    .unwrap()
                    .commit_from_slice(&[3, 4]);
                for reader in [&mut existing, &mut late] {
                    assert_eq!(reader.try_read(), Some(3));
                    assert_eq!(reader.try_read(), Some(4));
                }
            }
        }
    }

    fn vary_schedule(state: &mut usize) {
        *state = state.wrapping_mul(1664525).wrapping_add(1013904223);
        if *state & 3 != 0 {
            thread::yield_now();
        }
    }

    #[test]
    fn stress_join_slot_reuse_and_prepared_publication() {
        // Scheduling varies only around API calls and while guards are held.
        // Production atomics run without instrumentation. Values encode their
        // sequence so every read checks the physical ring cell's generation.
        for seed in 0..4 {
            for queue in queues(4, 2) {
                let done = Arc::new(AtomicBool::new(false));
                let mut readers = Vec::new();
                for reader in 0..2 {
                    let initial_consumer = queue.consumer().unwrap();
                    let queue = queue.clone();
                    let done = Arc::clone(&done);
                    readers.push(thread::spawn(move || {
                        let deadline = Instant::now() + TIMEOUT;
                        let mut schedule = seed + reader;
                        let mut consumer = initial_consumer;
                        let mut reads = 0;
                        while !done.load(Ordering::Acquire) {
                            for _ in 0..7 {
                                if let Some(guard) = consumer.try_reserve_read() {
                                    let sequence = guard.consumer.next_for_lane(guard.lane.get());
                                    vary_schedule(&mut schedule);
                                    assert_eq!(*guard.as_ref(), sequence as u64 + 1);
                                    reads += 1;
                                }
                            }
                            if reads > 0 {
                                drop(consumer);
                                vary_schedule(&mut schedule);
                                consumer = queue.consumer().unwrap();
                            }
                            assert!(Instant::now() < deadline, "reader stress timeout");
                        }
                        reads
                    }));
                }
                let mut producer = queue.producer(ProducerId::new(1)).unwrap();
                let deadline = Instant::now() + TIMEOUT;
                let mut schedule = seed;
                let mut sequence = 0;
                while sequence < 1000 {
                    let n = (1000 - sequence).min(4);
                    vary_schedule(&mut schedule);
                    let mut prepared = match producer.try_prepare_write_batch(count(n)) {
                        Some(prepared) => prepared,
                        None => {
                            assert!(Instant::now() < deadline, "producer stress timeout");
                            thread::yield_now();
                            continue;
                        }
                    };
                    for i in 0..n {
                        prepared.as_mut(i).write((sequence + i + 1) as u64);
                        vary_schedule(&mut schedule);
                    }
                    if sequence % 7 == 0 {
                        drop(prepared);
                        loop {
                            match producer.try_prepare_write() {
                                Some(prepared) => {
                                    prepared.commit(sequence as u64 + 1);
                                    break;
                                }
                                None => {
                                    assert!(Instant::now() < deadline, "producer stress timeout");
                                    thread::yield_now();
                                }
                            }
                        }
                        sequence += 1;
                    } else {
                        let prefix = 1 + (sequence + seed) % n;
                        // SAFETY: the entire prepared range is initialized above.
                        unsafe { prepared.commit_prefix(prefix) };
                        sequence += prefix;
                    }
                }
                done.store(true, Ordering::Release);
                for reader in readers {
                    assert!(reader.join().unwrap() > 0);
                }
                frontiers(&queue, 1000);
            }
        }
    }
}
