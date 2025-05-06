use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct Producer {
    queue: SharedQueue,
    head: usize,
    tail: usize,
}

impl Producer {
    pub fn new(header_ptr: *mut u8, buffer_ptr_and_file_size: (*mut u8, usize)) -> Self {
        let queue = SharedQueue::new(header_ptr, buffer_ptr_and_file_size);
        let head = queue.header().head.load(Ordering::Acquire);
        let tail = queue.header().tail.load(Ordering::Relaxed);
        Self { queue, head, tail }
    }

    /// Write data to the queue - the [`Consumer`] cannot see it until
    /// [`Self::commit`] is called.
    pub fn try_enqueue(&mut self, data: &[u8]) -> bool {
        let Some(reserved_buffer) = self.reserve(data.len()) else {
            return false;
        };

        unsafe {
            reserved_buffer.copy_from_nonoverlapping(data.as_ptr(), data.len());
        }
        true
    }

    /// Reserve a buffer for writing data. The [`Consumer`] cannot see it until
    /// [`Self::commit`] is called.
    pub fn reserve(&mut self, size: usize) -> Option<*mut u8> {
        let (reserved_buffer, tail) = self
            .queue
            .reserve_with_head_and_tail(size, self.head, self.tail)?;
        self.tail = tail;
        Some(reserved_buffer)
    }

    /// Commit the data to the queue. The [`Consumer`] can now see it.
    /// This is a release operation, so it must be called after all writes to
    /// the reserved buffer are done.
    pub fn commit(&mut self) {
        std::sync::atomic::fence(Ordering::Release);
        self.head = self.queue.header().head.load(Ordering::Acquire);
        self.queue.header().tail.store(self.tail, Ordering::Release);
        std::sync::atomic::fence(Ordering::Release);
    }
}

pub struct Consumer {
    queue: SharedQueue,
    head: usize,
    tail: usize,
}

impl Consumer {
    pub fn new(header_ptr: *mut u8, buffer_ptr_and_file_size: (*mut u8, usize)) -> Self {
        let queue = SharedQueue::new(header_ptr, buffer_ptr_and_file_size);
        let head = queue.header().head.load(Ordering::Acquire);
        let tail = queue.header().tail.load(Ordering::Relaxed);
        Self { queue, head, tail }
    }

    /// Try to dequeue a message from the queue. Returns `None` if there is no
    /// message available.
    /// This does not update the head of the shared queue so the space is not
    /// freed for the [`Producer`] to use until [`Self::sync`] is called.
    pub fn try_dequeue(&mut self) -> Option<&[u8]> {
        let (data_ptr, len, head) = self
            .queue
            .try_pop_with_head_and_tail(self.head, self.tail)?;
        self.head = head;
        Some(unsafe { core::slice::from_raw_parts(data_ptr, len) })
    }

    /// Sync the queue with the current head and tail.
    pub fn sync(&mut self) {
        std::sync::atomic::fence(Ordering::Release);
        self.tail = self.queue.header().tail.load(Ordering::Acquire);
        self.queue.header().head.store(self.head, Ordering::Release);
        std::sync::atomic::fence(Ordering::Release);
    }
}

#[repr(C)]
struct SharedQueueHeader {
    head: CacheAlignedAtomicSize,
    tail: CacheAlignedAtomicSize,
}

pub struct SharedQueue {
    header_ptr: *mut u8,
    buffer_ptr: *mut u8,
    buffer_size: usize,
    buffer_mask: usize,
}

impl SharedQueue {
    pub fn new(header_ptr: *mut u8, buffer_ptr_and_file_size: (*mut u8, usize)) -> Self {
        Self {
            header_ptr,
            buffer_ptr: buffer_ptr_and_file_size.0,
            buffer_size: buffer_ptr_and_file_size.1,
            buffer_mask: buffer_ptr_and_file_size.1 - 1,
        }
    }

    /// Get the size of the buffer - this may have been rounded during creation.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn header(&self) -> &SharedQueueHeader {
        unsafe { &*(self.header_ptr as *const SharedQueueHeader) }
    }

    fn buffer_ptr(&mut self) -> *mut u8 {
        self.buffer_ptr
    }

    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }

    /// Reserve a buffer using a passed value of `head` and `tail`.
    /// If the reservation is successful, return the pointer to the buffer,
    /// and the next tail value.
    fn reserve_with_head_and_tail(
        &mut self,
        size: usize,
        head: usize,
        tail: usize,
    ) -> Option<(*mut u8, usize)> {
        // Size written at the current `tail`.
        const _: () = assert!(core::mem::size_of::<u32>() < CACHELINE_SIZE);
        let cache_aligned_payload_tail = tail.wrapping_add(CACHELINE_SIZE);

        // The end of the payload would naturally fall at:
        // `cache_aligned_payload_tail + size`.
        // However, we round to the next cacheline boundary.
        let next_tail = round_to_next_cacheline(cache_aligned_payload_tail.wrapping_add(size));

        // Round contiguous size to the next 64 byte boundary.
        let contiguous_size = next_tail.wrapping_sub(tail);
        let remaining = self.buffer_size.wrapping_sub(tail.wrapping_sub(head));
        if remaining < contiguous_size {
            return None;
        }

        let starting_pos = self.mask(tail);
        let buf = self.buffer_ptr();
        unsafe {
            buf.add(starting_pos).cast::<u32>().write(size as u32);
        }

        // Return a pointer to the data.
        let reserved_buffer = unsafe { buf.add(starting_pos).add(CACHELINE_SIZE) };
        Some((reserved_buffer, next_tail))
    }

    fn commit_tail(&mut self, tail: usize) {
        std::sync::atomic::fence(Ordering::Release);
        self.header().tail.store(tail, Ordering::Release);
        std::sync::atomic::fence(Ordering::Release);
    }

    pub fn try_enqueue(&mut self, data: &[u8]) -> bool {
        let head = self.header().head.load(Ordering::Acquire);
        let tail = self.header().tail.load(Ordering::Relaxed);

        let Some((reserved_buffer, tail)) = self.reserve_with_head_and_tail(data.len(), head, tail)
        else {
            return false;
        };

        unsafe {
            reserved_buffer.copy_from_nonoverlapping(data.as_ptr(), data.len());
        }
        self.commit_tail(tail);
        true
    }

    pub fn try_dequeue<'a>(&'a mut self) -> Option<&'a [u8]> {
        let tail = self.header().tail.load(Ordering::Acquire);
        let head = self.header().head.load(Ordering::Relaxed);

        let (data, len, head) = self.try_pop_with_head_and_tail(head, tail)?;
        std::sync::atomic::fence(Ordering::Release);
        self.header().head.store(head, Ordering::Release);
        Some(unsafe { core::slice::from_raw_parts(data, len) })
    }

    /// Try to pop a message from the queue, returning the message, length, and new head.
    fn try_pop_with_head_and_tail(
        &mut self,
        head: usize,
        tail: usize,
    ) -> Option<(*const u8, usize, usize)> {
        if head == tail {
            return None;
        }
        let buf = self.buffer_ptr();

        // Get the current position and read the length.
        let pos = self.mask(head);
        let len = unsafe { buf.add(pos).cast::<u32>().read() } as usize;

        // Cache-aligned payload will live at the next cacheline boundary.
        let cache_aligned_payload_head = head.wrapping_add(CACHELINE_SIZE);
        let next_head = round_to_next_cacheline(cache_aligned_payload_head.wrapping_add(len));

        // Payload lives at next cacheline boundary.
        let data_ptr = unsafe { buf.add(pos).add(CACHELINE_SIZE) };

        Some((data_ptr, len, next_head))
    }
}

const CACHELINE_SIZE: usize = 64;
const fn round_to_next_cacheline(size: usize) -> usize {
    (size + CACHELINE_SIZE - 1) & !(CACHELINE_SIZE - 1)
}

/// `AtomicUsize` with 128-byte alignment for better performance.
#[derive(Default)]
#[repr(C, align(128))]
pub struct CacheAlignedAtomicSize {
    inner: AtomicUsize,
}

impl core::ops::Deref for CacheAlignedAtomicSize {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn create_file(path: impl AsRef<Path>, size: usize) -> (File, usize) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.as_ref())
        .unwrap();
    let use_hugepages = use_hugepages(path.as_ref());
    let page_size = if use_hugepages {
        1 << 21
    } else {
        nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
            .unwrap()
            .unwrap() as usize
    };

    let file_size = (size + page_size - 1) & !(page_size - 1);
    let file_size = file_size.next_power_of_two();
    assert!(file_size % page_size == 0);

    file.set_len((file_size) as u64).unwrap();

    (file, file_size)
}

fn join_file(path: impl AsRef<Path>) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}

pub fn create_header_mmap(path: impl AsRef<Path>) -> *mut u8 {
    let (file, file_size) = create_file(path.as_ref(), core::mem::size_of::<SharedQueueHeader>());
    let mmap = map(&file, file_size);

    unsafe {
        mmap.write_bytes(0, file_size);
    }

    mmap
}

pub fn join_header_mmap(path: impl AsRef<Path>) -> *mut u8 {
    let file = join_file(path.as_ref());
    let file_size = file.metadata().unwrap().len() as usize;
    map(&file, file_size)
}

pub fn create_buffer_mmap(path: impl AsRef<Path>, buffer_size: usize) -> (*mut u8, usize) {
    let (file, file_size) = create_file(path.as_ref(), buffer_size);

    let mmap = mirror_map(&file, file_size, use_hugepages(path));
    unsafe {
        mmap.write_bytes(0, file_size);
    }
    (mmap, file_size)
}

pub fn join_buffer_mmap(path: impl AsRef<Path>) -> (*mut u8, usize) {
    let file = join_file(path.as_ref());
    let file_size = file.metadata().unwrap().len() as usize;
    (
        mirror_map(&file, file_size, use_hugepages(path.as_ref())),
        file_size,
    )
}

fn use_hugepages(path: impl AsRef<Path>) -> bool {
    path.as_ref().starts_with("/mnt/hugepages")
}

fn map(file: &File, file_size: usize) -> *mut u8 {
    let addr = unsafe {
        nix::libc::mmap(
            core::ptr::null_mut(),
            file_size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            nix::libc::MAP_SHARED, // TODO: support hugepages?
            file.as_raw_fd(),
            0,
        )
    };

    if addr == nix::libc::MAP_FAILED {
        panic!("mmap failed: {}", std::io::Error::last_os_error());
    }

    addr.cast()
}

fn mirror_map(file: &File, file_size: usize, use_hugepages: bool) -> *mut u8 {
    let anon_flags = if use_hugepages {
        nix::libc::MAP_PRIVATE | nix::libc::MAP_ANONYMOUS | nix::libc::MAP_HUGETLB
    } else {
        nix::libc::MAP_PRIVATE | nix::libc::MAP_ANONYMOUS
    };

    let addr = unsafe {
        nix::libc::mmap(
            core::ptr::null_mut(),
            file_size * 2,
            nix::libc::PROT_NONE,
            anon_flags,
            -1,
            0,
        )
    };

    if addr == nix::libc::MAP_FAILED {
        panic!("reserve failed: {}", std::io::Error::last_os_error());
    }

    let addr1 = addr;
    let addr2 = (addr as usize + file_size) as *mut nix::libc::c_void;

    let flags = if use_hugepages {
        nix::libc::MAP_SHARED
            | nix::libc::MAP_FIXED
            | nix::libc::MAP_HUGETLB
            | nix::libc::MAP_POPULATE
    } else {
        nix::libc::MAP_SHARED | nix::libc::MAP_FIXED | nix::libc::MAP_POPULATE
    };

    let first = unsafe {
        nix::libc::mmap(
            addr1,
            file_size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            flags,
            file.as_raw_fd(),
            0,
        )
    };

    if first == nix::libc::MAP_FAILED {
        panic!("first map failed: {}", std::io::Error::last_os_error());
    }

    let second = unsafe {
        nix::libc::mmap(
            addr2,
            file_size,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            flags,
            file.as_raw_fd(),
            0,
        )
    };

    if second == nix::libc::MAP_FAILED {
        panic!("second map failed: {}", std::io::Error::last_os_error());
    }

    first.cast()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_queue_simple_message() {
        let header_path = "/tmp/test_shared_queue_simple_message_header";
        let buffer_path = "/tmp/test_shared_queue_simple_message_buffer";
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);

        let mut queue = SharedQueue::new(
            create_header_mmap(header_path),
            create_buffer_mmap(buffer_path, 1024),
        );

        let original_data = b"hello world";
        assert!(queue.try_enqueue(original_data));

        let expected_position = CACHELINE_SIZE + round_to_next_cacheline(original_data.len());
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position
        );
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        let dequeued_data = queue.try_dequeue().unwrap();
        assert_eq!(dequeued_data, original_data);
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            expected_position
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position
        );

        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
    }

    #[test]
    fn test_shared_queue_variable_sized_messages() {
        let header_path = "/tmp/test_shared_queue_variable_sized_messages_header";
        let buffer_path = "/tmp/test_shared_queue_variable_sized_messages_buffer";
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
        let mut queue = SharedQueue::new(
            create_header_mmap(header_path),
            create_buffer_mmap(buffer_path, 1024),
        );

        let message1 = b"hello world";
        let message2 = b"wasup";

        assert!(queue.try_enqueue(message1));
        assert!(queue.try_enqueue(message2));

        let message1_len = CACHELINE_SIZE + round_to_next_cacheline(message1.len());
        let message2_len = CACHELINE_SIZE + round_to_next_cacheline(message2.len());

        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        assert_eq!(queue.try_dequeue().unwrap(), message1);
        assert_eq!(queue.try_dequeue().unwrap(), message2);
        assert!(queue.try_dequeue().is_none());

        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
    }

    #[test]
    fn test_shared_queue_wrap_around() {
        let header_path = "/tmp/test_shared_queue_wrap_around_header";
        let buffer_path = "/tmp/test_shared_queue_wrap_around_buffer";
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
        let mut queue = SharedQueue::new(
            create_header_mmap(header_path),
            create_buffer_mmap(buffer_path, 1024),
        );
        let buffer_size = queue.buffer_size();

        // Put message in and pop out so that there is room at the beginnning of the buffer.
        let message_size = buffer_size - 64;
        assert!(queue.try_enqueue(&vec![5; message_size]));
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);
        let expected_position = CACHELINE_SIZE + round_to_next_cacheline(message_size);
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position
        );
        assert!(queue.try_dequeue().is_some());
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            expected_position
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position
        );

        // Not enough space to write contiguously - but mirrored mmap allos wrap around anyway.
        let data = (0..buffer_size - 120).map(|i| i as u8).collect::<Vec<u8>>();
        assert!(queue.try_enqueue(&data));

        // The queue should be full now.
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            expected_position
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position + CACHELINE_SIZE + round_to_next_cacheline(buffer_size - 120)
        );

        // Dequeue the data.
        let dequeued_data = queue.try_dequeue().unwrap();
        assert_eq!(dequeued_data, &data[..]);
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            expected_position + CACHELINE_SIZE + round_to_next_cacheline(buffer_size - 120)
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            expected_position + CACHELINE_SIZE + round_to_next_cacheline(buffer_size - 120)
        );
        assert!(queue.try_dequeue().is_none());

        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
    }

    #[test]
    fn test_shared_queue_separate_instances() {
        let header_path = "/tmp/test_shared_queue_separate_instances_header";
        let buffer_path = "/tmp/test_shared_queue_separate_instances_buffer";
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);

        let mut sender = SharedQueue::new(
            create_header_mmap(header_path),
            create_buffer_mmap(buffer_path, 1024),
        );
        let mut recver =
            SharedQueue::new(join_header_mmap(header_path), join_buffer_mmap(buffer_path));

        let message1 = b"hello world";
        let message2 = b"wasup";

        assert!(sender.try_enqueue(message1));
        assert!(sender.try_enqueue(message2));

        let message1_len = CACHELINE_SIZE + round_to_next_cacheline(message1.len());
        let message2_len = CACHELINE_SIZE + round_to_next_cacheline(message2.len());

        assert_eq!(
            sender.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(sender.header().head.load(Ordering::Acquire), 0);
        assert_eq!(
            recver.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(recver.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        assert_eq!(recver.try_dequeue().unwrap(), message1);
        assert_eq!(recver.try_dequeue().unwrap(), message2);
        assert!(recver.try_dequeue().is_none());

        assert_eq!(
            sender.header().head.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(
            sender.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(
            recver.header().head.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        assert_eq!(
            recver.header().tail.load(Ordering::Acquire),
            message1_len + message2_len,
        );
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);
    }

    #[test]
    fn test_producer_consumer_simple_message() {
        let header_path = "/tmp/test_producer_consumer_simple_message_header";
        let buffer_path = "/tmp/test_producer_consumer_simple_message_buffer";
        let _ = std::fs::remove_file(header_path);
        let _ = std::fs::remove_file(buffer_path);

        let mut producer = {
            let header_mmap = create_header_mmap(header_path);
            let buffer_mmap = create_buffer_mmap(buffer_path, 1024);
            Producer::new(header_mmap, buffer_mmap)
        };
        let mut consumer = {
            let header_mmap = join_header_mmap(header_path);
            let buffer_mmap = join_buffer_mmap(buffer_path);
            Consumer::new(header_mmap, buffer_mmap)
        };

        let original_data = b"hello world";
        let expected_position = CACHELINE_SIZE + round_to_next_cacheline(original_data.len());
        assert!(producer.try_enqueue(original_data));

        assert_eq!(producer.queue.header().tail.load(Ordering::Acquire), 0);
        assert_eq!(producer.tail, expected_position);
        assert_eq!(producer.head, 0);

        // The producer has not committed the data yet, so the consumer cannot see it.
        assert!(consumer.try_dequeue().is_none());
        assert_eq!(consumer.tail, 0);
        assert_eq!(consumer.head, 0);
        assert_eq!(consumer.queue.header().head.load(Ordering::Acquire), 0);
        assert_eq!(consumer.queue.header().tail.load(Ordering::Acquire), 0);

        // Commit the data to the queue.
        producer.commit();
        assert_eq!(
            producer.queue.header().tail.load(Ordering::Acquire),
            expected_position
        );
        assert_eq!(producer.tail, expected_position);
        assert_eq!(producer.head, 0);
        assert_eq!(producer.queue.header().head.load(Ordering::Acquire), 0);

        // The consumer can still not see the data since it has not synced the tail.
        assert!(consumer.try_dequeue().is_none());
        assert_eq!(consumer.tail, 0);
        assert_eq!(consumer.head, 0);

        // Sync the queue with the current head and tail.
        consumer.sync();

        // Dequeue the data.
        let dequeued_data = consumer.try_dequeue().unwrap();
        assert_eq!(dequeued_data, original_data);
        assert_eq!(consumer.tail, expected_position);
        assert_eq!(consumer.head, expected_position);
        assert_eq!(consumer.queue.header().head.load(Ordering::Acquire), 0);
        assert_eq!(
            consumer.queue.header().tail.load(Ordering::Acquire),
            expected_position
        );

        consumer.sync();
        assert_eq!(
            consumer.head,
            consumer.queue.header().head.load(Ordering::Acquire)
        );
    }
}
