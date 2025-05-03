fn main() {
    let map_path = "/mnt/hugepages/shaq_enqueue_dequeue";
    let _ = std::fs::remove_file(map_path);

    let sender = {
        let mmap = shaq::create_mmap(map_path, 1024 * 1024 * 1024 - shaq::HEADER_SIZE);
        shaq::SharedQueue::new(mmap)
    };

    let recver = {
        let mmap = shaq::join_mmap(map_path);
        shaq::SharedQueue::new(mmap)
    };

    let recver_hdl = std::thread::Builder::new()
        .name("shaqRecver".to_string())
        .spawn(move || run_recver(recver))
        .unwrap();

    let sender_hdl = std::thread::Builder::new()
        .name("shaqSender".to_string())
        .spawn(move || {
            run_sender(sender);
        })
        .unwrap();

    recver_hdl.join().unwrap();
    sender_hdl.join().unwrap();
}

#[inline(never)]
fn run_sender(mut sender: shaq::SharedQueue) {
    const ITEM_SIZE: usize = 138;

    let mut sender_count = 0;
    let mut last_time = std::time::Instant::now();
    loop {
        sender.try_enqueue(&[5; ITEM_SIZE]);
        sender_count += 1;
        if sender_count == 10_000_000 {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_time);
            last_time = now;
            println!(
                "{:.02} GB/sec",
                (10_000_000 * ITEM_SIZE) as f64 / (elapsed.as_secs_f64()) / 1e9
            );
            sender_count = 0;
        }
    }
}

#[inline(never)]
fn run_recver(mut recver: shaq::SharedQueue) {
    loop {
        let _x = recver.try_dequeue();
    }
}
