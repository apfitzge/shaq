use memmap2::{MmapMut, MmapOptions};
use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const HEADER_SIZE: usize = core::mem::size_of::<SharedQueueHeader>();

const WRAP_MARKER: u32 = 0xFFFF_FFFF;

#[repr(C)]
struct SharedQueueHeader {
    head: CacheAlignedAtomicSize,
    tail: CacheAlignedAtomicSize,
}

pub struct SharedQueue {
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

    /// Get a buffer of `size` bytes to write into.
    pub fn reserve(&mut self, size: usize) -> Option<*mut u8> {
        let contiguous_size = size.wrapping_add(core::mem::size_of::<u32>());
        let head = self.header().head.load(Ordering::Acquire);
        let mut tail = self.header().tail.load(Ordering::Relaxed);

        let remaining = self.buffer_size.wrapping_sub(tail.wrapping_sub(head));

        // This does NOT consider if we must wrap around.
        if remaining < contiguous_size {
            return None;
        }

        let buf = self.buffer_ptr();

        // We must now consider if we need to wrap around.
        let mut starting_pos = self.mask(tail);
        let remaining_before_wrap = self.buffer_size.wrapping_sub(starting_pos);

        // If we have exactly enough space to write the (len, message) then
        // we do not need to leave room for a wrap marker.
        let must_wrap = match remaining_before_wrap.cmp(&contiguous_size) {
            std::cmp::Ordering::Less => {
                // We must wrap around - write a wrap marker.
                true
            }
            std::cmp::Ordering::Equal => {
                // We have exactly enough space to write the (len, message).
                // We do NOT need to write a wrap marker.
                false
            }
            std::cmp::Ordering::Greater => {
                // We have enough space to write the (len, message) but we
                // must make sure we have enough space for a wrap marker
                // after.
                remaining_before_wrap.wrapping_sub(contiguous_size) < core::mem::size_of::<u32>()
            }
        };

        if must_wrap {
            // If we must wrap. Before we do so we must make sure we have enough room
            // AFTER we wrap around.
            let remaining_after_wrap = remaining.wrapping_sub(remaining_before_wrap);
            if remaining_after_wrap < contiguous_size {
                return None;
            }

            // Write the wrap-around marker.
            assert!(starting_pos + core::mem::size_of::<u32>() <= self.buffer_size);
            unsafe {
                buf.add(starting_pos)
                    .cast::<u32>()
                    .write_unaligned(WRAP_MARKER);
            }
            std::sync::atomic::fence(Ordering::Release);
            tail = tail.wrapping_add(remaining_before_wrap);
            starting_pos = 0;
            // Immediately commit the tail so we do not need to cache this info for `commit`.
            self.header().tail.store(tail, Ordering::Release);
        }

        // If wrapped around, local variables have been updated.
        // Otherwise, we are still at the same position.
        // Write the size.
        assert!(starting_pos + core::mem::size_of::<u32>() <= self.buffer_size);
        unsafe {
            buf.add(starting_pos)
                .cast::<u32>()
                .write_unaligned(size as u32);
        }
        // Return a reference to the data.
        Some(unsafe { buf.add(starting_pos + core::mem::size_of::<u32>()) })
    }

    pub fn commit_size(&mut self, size: usize) {
        std::sync::atomic::fence(Ordering::Release);
        let tail = self.header().tail.load(Ordering::Relaxed);
        let new_tail = tail.wrapping_add(size + core::mem::size_of::<u32>());
        self.header().tail.store(new_tail, Ordering::Release);
    }

    pub fn try_enqueue(&mut self, data: &[u8]) -> bool {
        let Some(reserved_buffer) = self.reserve(data.len()) else {
            return false;
        };
        unsafe {
            reserved_buffer.copy_from_nonoverlapping(data.as_ptr(), data.len());
        }
        self.commit_size(data.len());
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

        let marker = unsafe { buf.add(pos).cast::<u32>().read_unaligned() };
        if marker == WRAP_MARKER {
            // move head to 0 and retry.
            std::sync::atomic::fence(Ordering::Acquire);

            // wrap-around marker moves head to next increment of buffer-size.
            head = head.wrapping_add(self.buffer_size - self.mask(head));
            self.header().head.store(head, Ordering::Release);
            pos = self.mask(head);
        }

        let len = unsafe { buf.add(pos).cast::<u32>().read_unaligned() } as usize;
        let payload_pos = pos + core::mem::size_of::<u32>();

        assert!(
            payload_pos + len <= self.buffer_size,
            "message would wrap buffer. {} {} {}",
            payload_pos,
            len,
            self.buffer_size
        );

        let data = unsafe { core::slice::from_raw_parts(buf.add(payload_pos), len) };

        std::sync::atomic::fence(Ordering::Acquire);
        self.header().head.store(
            head.wrapping_add(len + core::mem::size_of::<u32>()),
            Ordering::Release,
        );
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
        let mut queue = SharedQueue::new(create_mmap(path, 128));

        // Put message in and pop out so that there is room at the beginnning of the buffer.
        assert!(queue.try_enqueue(&[5; 64]));
        assert!(queue.try_dequeue().is_some());

        // The head is at 68 bytes (64 + 4 for length).

        // There are 60 bytes at the beginning of the buffer, and 64 bytes remaining at the end.
        assert!(!queue.try_enqueue(&[5; 100])); // Not enough space to write contiguously.
        assert!(!queue.try_enqueue(&[5; 68])); // Not enough space to write contiguously with the size.
        assert!(queue.try_enqueue(&[5; 64])); // This should work - we do not need to wrap around.

        // The head is now back at 68.
        assert_eq!(queue.header().head.load(Ordering::Acquire) % 128, 68);
        assert_eq!(queue.header().tail.load(Ordering::Acquire) % 128, 68);
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
