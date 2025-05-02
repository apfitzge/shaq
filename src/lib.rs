use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

const WRAP_MARKER: u32 = 0xFFFF_FFFF;

#[repr(C)]
struct SharedQueueHeader {
    head: CacheAlignedAtomicSize,
    tail: CacheAlignedAtomicSize,
}

struct SharedQueue {
    mmap: MmapMut,
    buffer_start: usize,
    buffer_size: usize,
}

impl SharedQueue {
    pub fn new(mmap: MmapMut) -> Self {
        let buffer_size = mmap.len() - core::mem::size_of::<SharedQueueHeader>();
        Self {
            mmap,
            buffer_start: core::mem::size_of::<SharedQueueHeader>(),
            buffer_size,
        }
    }

    fn header(&self) -> &SharedQueueHeader {
        unsafe { &*(self.mmap.as_ptr() as *const SharedQueueHeader) }
    }

    fn buffer_ptr(&mut self) -> *mut u8 {
        unsafe { self.mmap.as_mut_ptr().add(self.buffer_start) }
    }

    fn mask(&self, index: usize) -> usize {
        // TODO: Make sure buffer_size is a power of 2 so we can just do shift.
        index % self.buffer_size
    }

    pub fn try_enqueue(&mut self, data: &[u8]) -> bool {
        let len = data.len();
        let total = len + 4;

        let head = self.header().head.load(Ordering::Acquire);
        let tail = self.header().tail.load(Ordering::Relaxed);

        let free_space = if tail >= head {
            self.buffer_size - (tail - head)
        } else {
            head - tail
        };

        // Additional 4 for wrap marker if needed.
        if total + 4 > free_space {
            return false; // not enough space
        }

        let pos = self.mask(tail);
        let remaining = self.buffer_size - pos;
        let buf = self.buffer_ptr();

        if remaining < total {
            // Write a wrap marker.
            // TODO: is this guaranteed to be aligned?
            //       IF not we should probably make it so.
            unsafe {
                buf.add(pos).cast::<u32>().write(WRAP_MARKER);
            }
            std::sync::atomic::fence(Ordering::Release);
            self.header()
                .tail
                .store(self.mask(tail + remaining), Ordering::Release);
            return self.try_enqueue(data); // Try again at the beginning.
        }

        // Write the length.
        // TODO: is this guaranteed to be aligned?
        //       IF not we should probably make it so.
        unsafe {
            // TODO: do not use `as u32` casting!
            buf.add(pos).cast::<u32>().write(len as u32);
            core::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf.add(pos + core::mem::size_of::<u32>()),
                data.len(),
            );
        }

        std::sync::atomic::fence(Ordering::Release);

        self.header()
            .tail
            .store(self.mask(tail + total), Ordering::Release);

        true
    }

    pub fn try_dequeue(&mut self) -> Option<&[u8]> {
        let tail = self.header().tail.load(Ordering::Acquire);
        let mut head = self.header().head.load(Ordering::Relaxed);

        if head == tail {
            return None;
        }

        let mut pos = self.mask(head);
        let buf = self.buffer_ptr();

        let marker = unsafe { buf.cast::<u32>().read_unaligned() };
        if marker == WRAP_MARKER {
            // move head to 0 and retry.
            std::sync::atomic::fence(Ordering::Acquire);
            head = self.mask(head + (self.buffer_size - pos)); // move to 0
            self.header().head.store(head, Ordering::Release);
            pos = self.mask(head);
        }

        let len = unsafe { buf.add(pos).cast::<u32>().read_unaligned() } as usize;
        let payload_pos = pos + core::mem::size_of::<u32>();

        assert!(
            payload_pos + len <= self.buffer_size,
            "message would wrap buffer"
        );

        let data = unsafe { core::slice::from_raw_parts(buf.add(payload_pos), len) };

        std::sync::atomic::fence(Ordering::Acquire);
        self.header()
            .head
            .store(self.mask(payload_pos + len), Ordering::Release);
        Some(data)
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

pub fn create_mmap(path: impl AsRef<Path>, buffer_size: usize) -> MmapMut {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.set_len((core::mem::size_of::<SharedQueueHeader>() + buffer_size) as u64)
        .unwrap();
    unsafe { MmapOptions::new().map_mut(&file).unwrap() }
}

pub fn join_mmap(path: impl AsRef<Path>) -> MmapMut {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    unsafe { MmapOptions::new().map_mut(&file).unwrap() }
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
        let mut queue = SharedQueue::new(create_mmap(path, 1000));

        // Put message in and pop out so that there is room at the beginnning of the buffer.
        assert!(queue.try_enqueue(&[5; 500]));
        assert!(queue.try_dequeue().is_some());

        // 500 remaining bytes.
        assert!(!queue.try_enqueue(&[5; 500])); // not enough space because we cannot write the wrap marker.
        assert!(!queue.try_enqueue(&[5; 500])); // not enough space for the wrap marker and the message.
        assert!(queue.try_enqueue(&[5; 496])); // enough space for the wrap marker and the message.
        assert!(!queue.try_enqueue(&[0; 1])); // not enough space for even a single byte (actually 5).
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
}
