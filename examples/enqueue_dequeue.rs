use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn main() {
    let exit = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler({
        let exit = exit.clone();
        move || exit.store(true, Ordering::Relaxed)
    })
    .unwrap();

    let map_path = "/mnt/hugepages/shaq_enqueue_dequeue";
    let _ = std::fs::remove_file(map_path);

    let (ptr, file_size) = shaq::create_mmap(map_path, 1024 * 1024 * 10 - shaq::HEADER_SIZE);
    let ptr = ptr as usize;

    let recver_hdl = std::thread::Builder::new()
        .name("shaqRecver".to_string())
        .spawn({
            let exit = exit.clone();
            move || {
                let recver = {
                    let mmap = shaq::join_mmap(map_path);
                    shaq::Consumer::new(mmap)
                };
                run_recver(recver, exit)
            }
        })
        .unwrap();

    let sender_hdl = std::thread::Builder::new()
        .name("shaqSender".to_string())
        .spawn(move || {
            let ptr = ptr as *mut u8;
            let sender = { shaq::Producer::new((ptr, file_size)) };
            run_sender(sender, exit);
        })
        .unwrap();

    recver_hdl.join().unwrap();
    sender_hdl.join().unwrap();

    let _ = std::fs::remove_file(map_path);
}

#[inline(never)]
fn run_sender(mut sender: shaq::Producer, exit: Arc<AtomicBool>) {
    const ITEM_SIZE: usize = 512;

    let mut sender_count = 0;
    let mut last_time = std::time::Instant::now();
    while !exit.load(Ordering::Relaxed) {
        // Push in batches.
        const BYTES_PER_BATCH: usize = 100_000;
        const MESSAGES_PER_BATCH: usize = BYTES_PER_BATCH / ITEM_SIZE;
        for _ in 0..MESSAGES_PER_BATCH {
            if !sender.try_enqueue(&[5; ITEM_SIZE]) {
                break;
            }
            sender_count += 1;
        }
        sender.commit();

        if sender_count >= 10_000_000 {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_time);
            last_time = now;
            println!(
                "{:.02} GB/sec - {:.0} items/sec",
                (sender_count * ITEM_SIZE) as f64 / (elapsed.as_secs_f64()) / 1e9,
                (sender_count as f64) / elapsed.as_secs_f64(),
            );
            sender_count = 0;
        }
    }
}

#[inline(never)]
fn run_recver(mut recver: shaq::Consumer, exit: Arc<AtomicBool>) {
    while !exit.load(Ordering::Relaxed) {
        recver.sync();
        loop {
            let x = recver.try_dequeue();
            if x.is_none() {
                break;
            }
        }
    }
}
