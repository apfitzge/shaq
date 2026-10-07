//! Small models of broadcast protocols.
//! Cursor assertions check cell protection without tracking payload accesses.

use super::*;
use crate::sync::atomic::AtomicUsize;
use loom::thread;

const CAPACITY: usize = 1;

fn broadcast() -> Broadcast<usize> {
    let config = BroadcastConfig {
        capacity: CAPACITY,
        producer_slots: 1,
        consumer_slots: 1,
    };
    let layout = QueueLayout::new::<usize>(&config).unwrap();
    let region = Region::alloc(NonZeroUsize::new(layout.total).unwrap()).unwrap();
    // SAFETY: fresh heap allocation, initialized once with this queue layout.
    let queue = unsafe { SharedQueue::create_in_region::<usize>(&region, &config, 0) }.unwrap();
    Broadcast::from_queue(queue)
}

// Tests the reservation-based double-sample join handshake. A racing consumer
// must either start past earlier reservations or install a limit that prevents
// the producer from overwriting its unread cell, including while a guard is held.
#[test]
fn joining_consumer_protects_unread_cell() {
    loom::model(|| {
        let broadcast = broadcast();
        let mut producer = broadcast.producer().unwrap();
        let writer = thread::spawn(move || {
            // The first write establishes a frontier while the consumer joins.
            assert!(producer.try_write(0).is_ok());
            // Capacity one: a second write would reuse the same physical cell.
            let _ = producer.try_write(1);
        });

        // Join while the producer can reserve or publish either value.
        let mut consumer = broadcast.consumer().unwrap();
        let next = consumer.core.next_for_lane(0);
        // If data is available, hold the guard until writing finishes.
        // Do not dereference the payload: cursor checks detect an overwrite
        // without introducing a data race if the protocol is broken.
        let guard = consumer.try_reserve_read();
        writer.join().unwrap();

        // Joining must either skip earlier reservations or constrain the writer.
        // Completing the writer makes these samples observe its final cursors.
        let lane = broadcast.shared_queue.producer_lanes().next().unwrap();
        let reserved = lane.reserved();
        let published = lane.published();
        assert!(reserved >= next, "reservation precedes the join position");
        assert!(
            reserved - next <= CAPACITY,
            "producer reserved an unread cell"
        );
        assert!(
            published <= next + CAPACITY,
            "producer overwrote an unread cell"
        );

        // Only dropping the guard may release the cell through consumer progress.
        drop(guard);
    });
}

// Tests the release/acquire producer ownership handover. A replacement that
// acquires a released lane must observe the previous owner's reservation and
// publication cursors and continue from them rather than restarting at zero.
#[test]
fn replacement_producer_preserves_cursors() {
    loom::model(|| {
        let broadcast = broadcast();
        let mut producer = broadcast.producer().unwrap();
        let writer = thread::spawn(move || {
            assert!(producer.try_write(0).is_ok());
            // Release ownership after completing publication.
            drop(producer);
        });

        // Attempt acquisition concurrently; failure means the old owner is live.
        // On success, acquisition must observe its completed write.
        if let Ok(mut replacement) = broadcast.producer() {
            assert_eq!(replacement.lane.reserved(), 1);
            assert_eq!(replacement.lane.published(), 1);
            // No consumers: the replacement can reuse the cell at sequence one.
            assert!(replacement.try_write(1).is_ok());
            assert_eq!(replacement.lane.reserved(), 2);
            assert_eq!(replacement.lane.published(), 2);
        }

        writer.join().unwrap();
    });
}

// Tests the waiter registration and broadcast wake-counter protocol. Publication
// racing with registration, the condition recheck, or compare-and-sleep must
// either be observed before sleeping or wake the consumer; no wake may be lost.
#[test]
fn publication_wakes_waiter() {
    loom::model(|| {
        let waiters = Arc::new(Waiters::default());
        let published = Arc::new(AtomicUsize::new(0));
        let wake_word = Arc::new(AtomicUsize::new(0));
        let consumer = {
            let waiters = waiters.clone();
            let published = published.clone();
            let wake_word = wake_word.clone();
            thread::spawn(move || {
                waiters
                    .wait_for(&wake_word, 0, Duration::MAX, || {
                        (published.load(Ordering::Acquire) == 1).then_some(())
                    })
                    .unwrap();
            })
        };

        published.store(1, Ordering::Release);
        waiters.bump_and_wake(&wake_word);
        consumer.join().unwrap();
    });
}
