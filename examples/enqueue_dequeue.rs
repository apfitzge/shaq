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

    let header_path = "/mnt/hugepages/shaq_enqueue_dequeue_header";
    let buffer_path = "/mnt/hugepages/shaq_enqueue_dequeue_buffer";
    let _ = std::fs::remove_file(header_path);
    let _ = std::fs::remove_file(buffer_path);

    let header_ptr = shaq::create_header_mmap(header_path).unwrap();
    let (buffer_ptr, file_size) =
        shaq::create_buffer_mmap(buffer_path, 1024 * 1024 * 1024).unwrap();
    let header_ptr = header_ptr as usize;
    let buffer_ptr = buffer_ptr as usize;

    let recver_hdl = std::thread::Builder::new()
        .name("shaqRecver".to_string())
        .spawn({
            let exit = exit.clone();
            move || {
                let recver = {
                    let header_mmap = shaq::join_header_mmap(header_path).unwrap();
                    let buffer_mmap = shaq::join_buffer_mmap(buffer_path).unwrap();
                    shaq::Consumer::new(header_mmap, buffer_mmap)
                };
                run_recver(recver, exit)
            }
        })
        .unwrap();

    let sender_hdl = std::thread::Builder::new()
        .name("shaqSender".to_string())
        .spawn(move || {
            let header_ptr = header_ptr as *mut u8;
            let buffer_ptr = buffer_ptr as *mut u8;
            let sender = { shaq::Producer::new(header_ptr, (buffer_ptr, file_size)) };
            run_sender(sender, exit);
        })
        .unwrap();

    recver_hdl.join().unwrap();
    sender_hdl.join().unwrap();

    let _ = std::fs::remove_file(header_path);
    let _ = std::fs::remove_file(buffer_path);
}

const ITEM_SIZE: usize = 512;
const BYTES_PER_BATCH: usize = 100_000;
const MESSAGES_PER_BATCH: usize = BYTES_PER_BATCH / ITEM_SIZE;

#[inline(never)]
fn run_sender(mut sender: shaq::Producer, exit: Arc<AtomicBool>) {
    let mut message_count = 0;
    let mut failed_reserves = 0;
    let mut batch_count = 0;
    let mut last_time = std::time::Instant::now();
    let mut value = 0;
    while !exit.load(Ordering::Relaxed) {
        // Push in batches.
        for _ in 0..MESSAGES_PER_BATCH {
            // Loop until we write the message.
            loop {
                let Some(ptr) = sender.reserve(ITEM_SIZE) else {
                    failed_reserves += 1;
                    continue;
                };

                write_item(ptr, value);
                value += 1;
                message_count += 1;
                break;
            }
        }
        sender.commit();

        batch_count += 1;
        if batch_count >= 100_000 {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_time);
            last_time = now;
            println!(
                "{:.02} GB/sec - {:.0} items/sec. ({} items, {} failed reserveds)",
                (message_count * ITEM_SIZE) as f64 / (elapsed.as_secs_f64()) / 1e9,
                (message_count as f64) / elapsed.as_secs_f64(),
                message_count,
                failed_reserves
            );
            message_count = 0;
            failed_reserves = 0;
            batch_count = 0;
        }
    }
}

#[repr(C, align(64))]
struct AlignedBuffer([u8; ITEM_SIZE]);

#[inline(never)]
fn write_item(ptr: *mut u8, value: u8) {
    unsafe {
        *ptr.cast() = AlignedBuffer([value; ITEM_SIZE]);
    }
}

#[inline(never)]
fn run_recver(mut recver: shaq::Consumer, exit: Arc<AtomicBool>) {
    let mut value = 0u8;
    while !exit.load(Ordering::Relaxed) {
        recver.sync();
        let mut num_messages = 0;
        loop {
            let Some(buffer) = recver.try_dequeue() else {
                break;
            };

            let b = buffer[0];
            if b != value {
                exit.store(true, Ordering::Relaxed);
                println!("Error: expected {}, got {} ({})", value, buffer[0], b);
                return;
            }
            value += 1;

            num_messages += 1;
            if num_messages == MESSAGES_PER_BATCH {
                recver.sync();
            }
        }
    }
}
