use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use shaq::{create_mmap, SharedQueue, HEADER_SIZE};

pub struct Item<const N: usize>([u8; N]);
const MAP_SIZE: usize = 1024 * 1024 * 1024 - HEADER_SIZE;

fn bench_queue_with_size<const N: usize>(c: &mut Criterion) {
    let map_path = "/mnt/hugepages/shaq_bench";
    let _ = std::fs::remove_file(map_path);
    let mmap = create_mmap(map_path, MAP_SIZE);

    let mut queue = SharedQueue::new(mmap);

    let item = Item::<N>([5; N]);

    const NUM_ITEMS_PER_ITERATION: usize = 1024;
    let mut group = c.benchmark_group(format!("queue/{N}"));
    group.throughput(Throughput::Bytes((N * NUM_ITEMS_PER_ITERATION) as u64));
    group.bench_function("push_pop", |b| {
        b.iter(|| {
            for _ in 0..NUM_ITEMS_PER_ITERATION {
                let reserved_bytes = queue.reserve(core::mem::size_of::<Item<N>>()).unwrap();
                let item = unsafe { &mut *(reserved_bytes as *mut Item<N>) };
                *item = Item::<N>([5; N]);
                queue.commit_size(core::mem::size_of::<Item<N>>());

                // let item = black_box(&item);
                // assert!(queue.try_enqueue(&item.0));
                let dequeued_item = queue.try_dequeue().unwrap();
                black_box(dequeued_item);
            }
        })
    });
}

fn bench_queue(c: &mut Criterion) {
    bench_queue_with_size::<138>(c);
    bench_queue_with_size::<256>(c);
    bench_queue_with_size::<512>(c);
    bench_queue_with_size::<1024>(c);
    bench_queue_with_size::<1232>(c);
}

criterion_group!(benches, bench_queue);
criterion_main!(benches);
