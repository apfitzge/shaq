//! Small models of the SPSC publication, release, and wait protocols.
//!
//! Raw buffer accesses are invisible to Loom. Shadow cells model payload reads
//! and writes inside the real queue's reservations, so missing synchronization
//! is detected without concurrently dereferencing untracked queue payloads.
//! These bounded models do not cover shared-memory initialization or timeouts.

use super::*;
use loom::{cell::UnsafeCell, sync::Arc, thread};

fn queue(capacity: usize, position: usize) -> (Producer<usize>, Consumer<usize>) {
    let size = minimum_region_size::<usize>(capacity);
    let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
    let header = region.addr().cast::<SharedQueueHeader>();
    // Zeroed memory does not construct atomic objects in Loom's runtime.
    let value = SharedQueueHeader {
        magic: AtomicU64::new(MAGIC),
        version: VERSION,
        buffer_mask: u32::try_from(capacity - 1).unwrap(),
        identifier: DEFAULT_QUEUE_IDENTIFIER,
        write: CacheAlignedAtomicSize::default(),
        read: CacheAlignedAtomicSize::default(),
        waiters: Waiters::default(),
    };
    value.write.store(position, Ordering::Relaxed);
    value.read.store(position, Ordering::Relaxed);
    // SAFETY: the fresh region is sized and aligned for this header and buffer.
    unsafe { header.write(value) };
    // SAFETY: the header was just initialized in this region.
    let producer = unsafe { Producer::from_header(region, header) }.unwrap();
    // SAFETY: this fresh queue has no other consumer.
    let consumer = unsafe { producer.join_as_consumer() }.unwrap();
    (producer, consumer)
}

fn write(batch: &mut WriteBatch<'_, usize>, cell: &UnsafeCell<usize>, value: usize) -> bool {
    if batch.try_write(0).is_err() {
        return false;
    }
    cell.with_mut(|ptr| {
        // SAFETY: a successful write reserves the corresponding physical slot
        // exclusively until the consumer releases it. Loom checks that ordering.
        unsafe { *ptr = value };
    });
    true
}

fn read(cell: &UnsafeCell<usize>) -> usize {
    cell.with(|ptr| {
        // SAFETY: callers hold the corresponding read reservation. Loom checks
        // that publication precedes this access and reuse follows its release.
        unsafe { *ptr }
    })
}

type Payloads = Arc<Vec<UnsafeCell<usize>>>;

fn queue_with_payloads(
    capacity: usize,
    position: usize,
) -> (Producer<usize>, Consumer<usize>, Payloads) {
    let (producer, consumer) = queue(capacity, position);
    let cells = Arc::new((0..capacity).map(|_| UnsafeCell::new(0)).collect());
    (producer, consumer, cells)
}

fn write_batch(producer: &mut Producer<usize>, cells: &[UnsafeCell<usize>], values: &[usize]) {
    let mut batch = producer.write_batch();
    for &value in values {
        let index = batch.producer.queue.mask(batch.producer.queue.cached_write);
        assert!(write(&mut batch, &cells[index], value));
    }
}

fn read_batch(
    consumer: &mut Consumer<usize>,
    cells: &[UnsafeCell<usize>],
    max: usize,
) -> Option<Vec<usize>> {
    let batch = consumer.try_reserve_read_batch(NonZeroUsize::new(max).unwrap())?;
    Some(
        (0..batch.len())
            .map(|offset| {
                let position = batch.reservation.start.wrapping_add(offset);
                let index = batch.reservation.consumer.queue.mask(position);
                read(&cells[index])
            })
            .collect(),
    )
}

#[test]
fn batch_publication_orders_payload() {
    loom::model(|| {
        // Both values fit contiguously, with no cursor or buffer wraparound.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, 0);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 2);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn batch_publication_orders_payload_across_cursor_wrap() {
    loom::model(|| {
        // The logical cursor advances from usize::MAX to zero.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, usize::MAX);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 1);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn batch_publication_orders_payload_across_buffer_wrap() {
    loom::model(|| {
        // Physical slots three and zero, while the logical cursor advances normally.
        let (mut producer, mut consumer, cells) = queue_with_payloads(4, 3);
        let writer_cells = cells.clone();
        let writer = thread::spawn(move || {
            write_batch(&mut producer, &writer_cells, &[10, 11]);
        });

        // Race publication: the complete batch must be visible or nothing yet.
        let items = read_batch(&mut consumer, &cells, 2);
        writer.join().unwrap();
        let items = items
            .or_else(|| read_batch(&mut consumer, &cells, 2))
            .unwrap();
        assert_eq!(items, [10, 11]);
        assert_eq!(consumer.queue.cached_read, 5);
        assert!(read_batch(&mut consumer, &cells, 2).is_none());
    });
}

#[test]
fn read_reservation_orders_slot_reuse_across_cursor_wrap() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, usize::MAX);
        let cell = Arc::new(UnsafeCell::new(0));
        assert!(write(&mut producer.write_batch(), &cell, 1));
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();

        let writer_cell = cell.clone();
        let writer = thread::spawn(move || {
            let written = write(&mut producer.write_batch(), &writer_cell, 2);
            (producer, written)
        });
        // Advancing the private read cursor must not release this live batch.
        assert_eq!(read(&cell), 1);
        drop(batch);
        let (mut producer, written) = writer.join().unwrap();
        if !written {
            assert!(write(&mut producer.write_batch(), &cell, 2));
        }
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();
        assert_eq!(read(&cell), 2);
        drop(batch);
        assert!(consumer.try_read().is_none());
    });
}

#[test]
fn consuming_iterator_keeps_entire_reservation_until_drop() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, 0);
        producer.try_write(1).unwrap();
        let batch = consumer.try_reserve_read_batch(NonZeroUsize::MIN).unwrap();
        let mut iter = batch.into_iter();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);

        let writer = thread::spawn(move || {
            // Even exhausting the iterator must not release its reservation.
            assert_eq!(producer.try_write(2), Err(2));
            producer
        });
        let mut producer = writer.join().unwrap();
        drop(iter);
        producer.try_write(2).unwrap();
        assert_eq!(consumer.try_read(), Some(2));
    });
}

#[test]
fn publication_wakes_waiting_consumer() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue(1, usize::MAX);
        let cell = Arc::new(UnsafeCell::new(0));
        let reader_cell = cell.clone();
        let reader = thread::spawn(move || {
            let header = consumer.queue.header;
            // SAFETY: the consumer keeps its initialized header alive.
            let header = unsafe { header.as_ref() };
            // Match reserve_read_batch_timeout, omitting its 2048 spin attempts
            // so Loom explores registration, recheck, and compare-and-sleep.
            let reservation = header
                .waiters
                .wait_for(&header.write, 0, Duration::MAX, || {
                    consumer.queue.load_write();
                    consumer.try_reserve_read_range(NonZeroUsize::MIN)
                })
                .unwrap();
            let batch = consumer.read_batch_for(reservation);
            assert_eq!(batch.len(), 1);
            assert_eq!(read(&reader_cell), 42);
        });

        assert!(write(&mut producer.write_batch(), &cell, 42));
        reader.join().unwrap();
    });
}
