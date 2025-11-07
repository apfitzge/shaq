use shaq::mpmc::{Consumer, Producer};
use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

struct Item {
    data: [u8; 512],
}

fn main() {
    let (consumer1_core_id, consumer2_core_id, producer1_core_id, producer2_core_id) =
        core_affinity::get_core_ids()
            .map(|c| {
                // Get the last 4 cores - assuming these are sorted.
                (
                    Some(c[c.len() - 4]),
                    Some(c[c.len() - 3]),
                    Some(c[c.len() - 2]),
                    Some(c[c.len() - 1]),
                )
            })
            .unwrap_or_default();

    if let Some(consumer_core_id) = consumer1_core_id.as_ref() {
        println!("Consumer 1 core id: {}", consumer_core_id.id);
    }
    if let Some(consumer_core_id) = consumer2_core_id.as_ref() {
        println!("Consumer 2 core id: {}", consumer_core_id.id);
    }
    if let Some(producer_core_id) = producer1_core_id.as_ref() {
        println!("Producer 1 core id: {}", producer_core_id.id);
    }
    if let Some(producer_core_id) = producer2_core_id.as_ref() {
        println!("Producer 2 core id: {}", producer_core_id.id);
    }

    let exit = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let exit = exit.clone();
        move || exit.store(true, Ordering::Release)
    })
    .unwrap();

    let queue_path = "/tmp/shaq";
    println!("Cleaning queue file: {queue_path}");
    let _ = std::fs::remove_file(queue_path);
    let queue_file = File::options()
        .create_new(true)
        .read(true)
        .write(true)
        .open(queue_path)
        .unwrap();

    let producer_1 = Producer::<Item>::create(&queue_file, 16 * 1024 * 1024, 2, 0).unwrap();
    let producer_2 = Producer::<Item>::join(&queue_file, 1).unwrap();
    let consumer_1 = Consumer::<Item>::join(&queue_file).unwrap();
    let consumer_2 = Consumer::<Item>::join(&queue_file).unwrap();

    let items_received = Arc::new(AtomicU64::new(0));

    let consumer_1_hdl = std::thread::Builder::new()
        .name("shaqConsumer1".to_string())
        .spawn({
            let exit = exit.clone();
            let items_received = items_received.clone();
            move || {
                if let Some(id) = consumer1_core_id {
                    core_affinity::set_for_current(id);
                }
                run_consumer(consumer_1, items_received, exit);
            }
        })
        .unwrap();
    let consumer_2_hdl = std::thread::Builder::new()
        .name("shaqConsumer2".to_string())
        .spawn({
            let exit = exit.clone();
            let items_received = items_received.clone();
            move || {
                if let Some(id) = consumer2_core_id {
                    core_affinity::set_for_current(id);
                }
                run_consumer(consumer_2, items_received, exit);
            }
        })
        .unwrap();

    let producer_1_hdl = std::thread::Builder::new()
        .name("shaqProducer1".to_string())
        .spawn({
            let exit = exit.clone();
            move || {
                if let Some(id) = producer1_core_id {
                    core_affinity::set_for_current(id);
                }
                run_producer(producer_1, exit);
            }
        })
        .unwrap();
    let producer_2_hdl = std::thread::Builder::new()
        .name("shaqProducer2".to_string())
        .spawn({
            let exit = exit.clone();
            move || {
                if let Some(id) = producer2_core_id {
                    core_affinity::set_for_current(id);
                }
                run_producer(producer_2, exit);
            }
        })
        .unwrap();

    let mut last_report = Instant::now();
    while !exit.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now.duration_since(last_report).as_secs() > 0 {
            let items_produced = items_received.swap(0, Ordering::Relaxed);
            println!(
                "{}/s ( GiB/s: {:.2} )",
                items_produced,
                items_produced as f64 * (core::mem::size_of::<Item>() as f64)
                    / (1024.0 * 1024.0 * 1024.0)
            );

            last_report = now;
        }
    }

    consumer_1_hdl.join().unwrap();
    consumer_2_hdl.join().unwrap();
    producer_1_hdl.join().unwrap();
    producer_2_hdl.join().unwrap();

    println!("Cleaning up files");
    let _ = std::fs::remove_file(queue_path);
}

// Synchronize, Commit, Finalize every SYNC_CADENCE items.
const SYNC_CADENCE: usize = 1024;

fn run_producer(mut producer: Producer<Item>, exit: Arc<AtomicBool>) {
    while !exit.load(Ordering::Acquire) {
        producer.sync();
        for _ in 0..SYNC_CADENCE {
            if producer.try_write(Item { data: [42; 512] }).is_err() {
                break;
            }
        }
        producer.commit();
    }
}

fn run_consumer(
    mut consumer: Consumer<Item>,
    items_received: Arc<AtomicU64>,
    exit: Arc<AtomicBool>,
) {
    while !exit.load(Ordering::Acquire) {
        if let Some(mut session) = consumer.try_open_session() {
            while let Some(_item) = session.try_read() {
                items_received.fetch_add(1, Ordering::Relaxed);
            }
            session.finalize();
        }
    }
}
