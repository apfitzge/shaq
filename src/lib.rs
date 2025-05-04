use std::{
    fs::{File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const HEADER_SIZE: usize = core::mem::size_of::<SharedQueueHeader>();

pub struct Producer {
    queue: SharedQueue,
    head: usize,
    tail: usize,
}

impl Producer {
    pub fn new(ptr_and_file_size: (*mut u8, usize)) -> Self {
        let queue = SharedQueue::new(ptr_and_file_size);
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
    pub fn new(ptr_and_file_size: (*mut u8, usize)) -> Self {
        let queue = SharedQueue::new(ptr_and_file_size);
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
    ptr: *mut u8,
    buffer_start: usize,
    buffer_size: usize,
}

impl SharedQueue {
    pub fn new(ptr_and_file_size: (*mut u8, usize)) -> Self {
        let ptr = ptr_and_file_size.0;
        let file_size = ptr_and_file_size.1;

        let buffer_size = file_size - core::mem::size_of::<SharedQueueHeader>();
        Self {
            ptr,
            buffer_start: core::mem::size_of::<SharedQueueHeader>(),
            buffer_size,
        }
    }

    /// Get the size of the buffer - this may have been rounded during creation.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn header(&self) -> &SharedQueueHeader {
        unsafe { &*(self.ptr as *const SharedQueueHeader) }
    }

    fn buffer_ptr(&mut self) -> *mut u8 {
        unsafe { self.ptr.add(self.buffer_start) }
    }

    fn mask(&self, index: usize) -> usize {
        // TODO: Make sure buffer_size is a power of 2 so we can just do shift.
        index % self.buffer_size
    }

    /// Reserve a buffer using a passed value of `head` and `tail`.
    /// If the reservation is successful, return the pointer to the buffer,
    /// and the next tail value.
    fn reserve_with_head_and_tail(
        &mut self,
        size: usize,
        head: usize,
        mut tail: usize,
    ) -> Option<(*mut u8, usize)> {
        let contiguous_size = size.wrapping_add(core::mem::size_of::<u32>());
        let remaining = self.buffer_size.wrapping_sub(tail.wrapping_sub(head));

        if remaining < contiguous_size {
            return None;
        }

        let starting_pos = self.mask(tail);
        let buf = self.buffer_ptr();
        unsafe {
            buf.add(starting_pos)
                .cast::<u32>()
                .write_unaligned(size as u32);
        }

        // Return a pointer to the data.
        let reserved_buffer = unsafe { buf.add(starting_pos + core::mem::size_of::<u32>()) };
        tail = tail.wrapping_add(contiguous_size);
        Some((reserved_buffer, tail))
    }

    fn commit_tail(&mut self, tail: usize) {
        std::sync::atomic::fence(Ordering::Release);
        self.header().tail.store(tail, Ordering::Release);
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
        mut head: usize,
        tail: usize,
    ) -> Option<(*const u8, usize, usize)> {
        if head == tail {
            return None;
        }

        let pos = self.mask(head);
        let buf = self.buffer_ptr();

        let len = unsafe { buf.add(pos).cast::<u32>().read_unaligned() } as usize;

        let payload_pos = pos + core::mem::size_of::<u32>();
        let data_ptr = unsafe { buf.add(payload_pos) };
        head = head.wrapping_add(len + core::mem::size_of::<u32>());
        Some((data_ptr, len, head))
    }
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

pub fn create_mmap(path: impl AsRef<Path>, buffer_size: usize) -> (*mut u8, usize) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();

    let use_hugepages = use_hugepages(path.as_ref());
    let page_size = if use_hugepages {
        1 << 21
    } else {
        nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
            .unwrap()
            .unwrap() as usize
    };

    let file_size = core::mem::size_of::<SharedQueueHeader>() + buffer_size;
    let file_size = (file_size + page_size - 1) & !(page_size - 1);

    file.set_len((file_size) as u64).unwrap();

    (mirror_map(&file, file_size, use_hugepages), file_size)
}

pub fn join_mmap(path: impl AsRef<Path>) -> (*mut u8, usize) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let file_size = file.metadata().unwrap().len() as usize;
    (
        mirror_map(&file, file_size, use_hugepages(path.as_ref())),
        file_size,
    )
}

fn use_hugepages(path: impl AsRef<Path>) -> bool {
    path.as_ref().starts_with("/mnt/hugepages")
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
        nix::libc::MAP_SHARED | nix::libc::MAP_FIXED | nix::libc::MAP_HUGETLB
    } else {
        nix::libc::MAP_SHARED | nix::libc::MAP_FIXED
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
        let path = "/tmp/test_shared_queue_simple_message";
        let _ = std::fs::remove_file(path);
        let mut queue = SharedQueue::new(create_mmap(path, 1024));

        let original_data = b"hello world";
        let original_data_len = original_data.len();
        assert!(queue.try_enqueue(original_data));

        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            original_data_len + 4
        );
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        let dequeued_data = queue.try_dequeue().unwrap();
        assert_eq!(dequeued_data, original_data);
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            original_data_len + 4
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            original_data_len + 4
        );
    }

    #[test]
    fn test_shared_queue_variable_sized_messages() {
        let path = "/tmp/test_shared_queue_variable_sized_messages";
        let _ = std::fs::remove_file(path);
        let mut queue = SharedQueue::new(create_mmap(path, 1024));

        let message1 = b"hello world";
        let message2 = b"wasup";

        assert!(queue.try_enqueue(message1));
        assert!(queue.try_enqueue(message2));

        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        assert_eq!(queue.try_dequeue().unwrap(), message1);
        assert_eq!(queue.try_dequeue().unwrap(), message2);
        assert!(queue.try_dequeue().is_none());

        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
    }

    #[test]
    fn test_shared_queue_wrap_around() {
        let path = "/tmp/test_shared_queue_wrap_around";
        let _ = std::fs::remove_file(path);
        let mut queue = SharedQueue::new(create_mmap(path, 128));
        let buffer_size = queue.buffer_size();

        // Put message in and pop out so that there is room at the beginnning of the buffer.
        assert!(queue.try_enqueue(&vec![5; buffer_size - 64]));
        assert_eq!(queue.header().head.load(Ordering::Acquire), 0);
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            buffer_size - 60
        );
        assert!(queue.try_dequeue().is_some());
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            buffer_size - 60
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            buffer_size - 60
        );

        // Not enough space to write contiguously - but mirrored mmap allos wrap around anyway.
        let data = (0..buffer_size - 4).map(|i| i as u8).collect::<Vec<u8>>();
        assert!(queue.try_enqueue(&data));

        // The queue should be full now.
        assert_eq!(
            queue.header().head.load(Ordering::Acquire),
            buffer_size - 60
        );
        assert_eq!(
            queue.header().tail.load(Ordering::Acquire),
            2 * buffer_size - 60
        );
    }

    #[test]
    fn test_shared_queue_separate_instances() {
        let path = "/tmp/test_shared_queue_separate_instances";
        let _ = std::fs::remove_file(path);
        let mut sender = SharedQueue::new(create_mmap(path, 1024));
        let mut recver = SharedQueue::new(join_mmap(path));

        let message1 = b"hello world";
        let message2 = b"wasup";

        assert!(sender.try_enqueue(message1));
        assert!(sender.try_enqueue(message2));

        assert_eq!(
            sender.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(sender.header().head.load(Ordering::Acquire), 0);
        assert_eq!(
            recver.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(recver.header().head.load(Ordering::Acquire), 0);

        // Dequeue the data.
        assert_eq!(recver.try_dequeue().unwrap(), message1);
        assert_eq!(recver.try_dequeue().unwrap(), message2);
        assert!(recver.try_dequeue().is_none());

        assert_eq!(
            sender.header().head.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(
            sender.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(
            recver.header().head.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
        assert_eq!(
            recver.header().tail.load(Ordering::Acquire),
            message1.len() + message2.len() + 4 + 4,
        );
    }

    #[test]
    fn test_producer_consumer_simple_messagee() {
        let path = "/tmp/test_producer_consumer_simple_messagee";
        let _ = std::fs::remove_file(path);
        let mut producer = {
            let mmap = create_mmap(path, 1024);
            Producer::new(mmap)
        };
        let mut consumer = {
            let mmap = join_mmap(path);
            Consumer::new(mmap)
        };

        let original_data = b"hello world";
        let original_data_len = original_data.len();
        assert!(producer.try_enqueue(original_data));

        assert_eq!(producer.queue.header().tail.load(Ordering::Acquire), 0);
        assert_eq!(producer.tail, original_data_len + 4);
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
            original_data_len + 4
        );
        assert_eq!(producer.tail, original_data_len + 4);
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
        assert_eq!(consumer.tail, original_data_len + 4);
        assert_eq!(consumer.head, original_data_len + 4);
        assert_eq!(consumer.queue.header().head.load(Ordering::Acquire), 0);
        assert_eq!(
            consumer.queue.header().tail.load(Ordering::Acquire),
            original_data_len + 4
        );

        consumer.sync();
        assert_eq!(
            consumer.head,
            consumer.queue.header().head.load(Ordering::Acquire)
        );
    }
}
