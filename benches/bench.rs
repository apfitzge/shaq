use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use shaq::{create_mmap, join_mmap, Consumer, Producer, SharedQueue, HEADER_SIZE};

const MAP_SIZE: usize = 1024 * 1024 * 1024 - HEADER_SIZE;

fn bench_queue_with_size<const N: usize>(c: &mut Criterion) {
    let pid = std::process::id();
    let map_path = format!("/mnt/hugepages/shaq_bench_queue_{pid}");
    let _ = std::fs::remove_file(&map_path);

    let mmap = shaq::create_mmap(&map_path, 1024 * 1024 * 10 - shaq::HEADER_SIZE);
    let mut queue = SharedQueue::new(mmap);

    const NUM_ITEMS_PER_ITERATION: usize = 1024;
    let mut group = c.benchmark_group(format!("queue/{N}"));
    group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                assert!(queue.try_enqueue(&[5; N]));
                let dequeued_item = queue.try_dequeue().unwrap();
                black_box(dequeued_item);
            }
        })
    });
    let _ = std::fs::remove_file(&map_path);
}

fn bench_producer_consumer_with_size<const N: usize>(c: &mut Criterion) {
    let map_path = "/mnt/hugepages/shaq_bench_producer_consumer";
    let _ = std::fs::remove_file(map_path);
    let mut producer = {
        let mmap = create_mmap(map_path, MAP_SIZE);
        Producer::new(mmap)
    };
    let mut consumer = {
        let mmap = join_mmap(map_path);
        Consumer::new(mmap)
    };

    const NUM_ITEMS_PER_ITERATION: usize = 1024;
    let mut group = c.benchmark_group(format!("producer_consumer/{N}"));
    group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            producer.commit();
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                assert!(producer.try_enqueue(&[5; N]));
            }
            producer.commit();

            consumer.sync();
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                let dequeued_item = consumer.try_dequeue().unwrap();
                black_box(dequeued_item);
            }
            consumer.sync();
        })
    });
    let _ = std::fs::remove_file(map_path);
}

fn bench_queue(c: &mut Criterion) {
    bench_queue_with_size::<138>(c);
    bench_queue_with_size::<256>(c);
    bench_queue_with_size::<512>(c);
    bench_queue_with_size::<1024>(c);
    bench_queue_with_size::<1232>(c);
}

fn bench_producer_consumer(c: &mut Criterion) {
    bench_producer_consumer_with_size::<138>(c);
    bench_producer_consumer_with_size::<256>(c);
    bench_producer_consumer_with_size::<512>(c);
    bench_producer_consumer_with_size::<1024>(c);
    bench_producer_consumer_with_size::<1232>(c);
}

criterion_group!(benches, bench_queue, bench_producer_consumer);
criterion_main!(benches);
