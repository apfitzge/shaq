use crate::{
    error::Error,
    mpmc::layout::{buffers_offset, layout},
    shmem::map_file,
    CacheAlignedAtomicSize, VERSION,
};
use core::{
    ptr::NonNull,
    sync::atomic::{AtomicU8, Ordering},
};
use std::fs::File;

pub struct Consumer<T: Sized> {
    global_header: NonNull<SharedQueueHeader>,
    last_producer_index: usize,

    file_size: usize,
    _phantom: core::marker::PhantomData<T>,
}

impl<T: Sized> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Tests do not mmap so skip unmapping in tests.
        #[cfg(test)]
        {
            return;
        }

        #[allow(unreachable_code)]
        // SAFETY: global_header is mmapped and of size `file_size`.
        unsafe {
            libc::munmap(self.global_header.as_ptr().cast(), self.file_size);
        }
    }
}

fn calculate_per_producer_capacity<T: Sized>(size: usize, num_producers: usize) -> usize {
    let remaining_space_for_buffers = size.saturating_sub(buffers_offset::<T>(num_producers));
    let maximum_bytes_per_producer = remaining_space_for_buffers / num_producers;
    let maximum_items_per_producer = maximum_bytes_per_producer / core::mem::size_of::<T>();
    if !maximum_items_per_producer.is_power_of_two() {
        // If not a power of two, round down to the previous power of two.
        maximum_items_per_producer.next_power_of_two() >> 1
    } else {
        maximum_items_per_producer
    }
}

impl<T: Sized> Consumer<T> {
    pub fn create(file: &File, size: usize, num_producers: usize) -> Result<Self, Error> {
        assert!(num_producers > 0);
        assert!(core::mem::size_of::<T>() > 0);
        let per_producer_capacity = calculate_per_producer_capacity::<T>(size, num_producers);
        file.set_len(size as u64)?;

        let global_header = map_file(file, size)?.cast::<SharedQueueHeader>();
        unsafe {
            SharedQueueHeader::initialize::<T>(global_header, num_producers, per_producer_capacity)
        };

        Ok(unsafe { Self::from_header(global_header, size) })
    }

    pub fn join(file: &File) -> Result<Self, Error> {
        let size = file.metadata()?.len();
        let header = map_file(file, size as usize)?.cast::<SharedQueueHeader>();
        Ok(unsafe { Self::from_header(header, size as usize) })
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size` or greater.
    unsafe fn from_header(header: NonNull<SharedQueueHeader>, file_size: usize) -> Self {
        Self {
            global_header: header,
            last_producer_index: 0,
            file_size,
            _phantom: core::marker::PhantomData,
        }
    }

    pub fn try_open_session(&mut self) -> Option<ConsumerSession<T>> {
        let header = unsafe { self.global_header.as_ref() };
        for offset in 0..header.num_producers {
            let mut index = self.last_producer_index + offset;
            if index >= header.num_producers {
                index = 0;
            }

            let producer_header_ptr = unsafe {
                self.global_header
                    .byte_add(header.headers_offset)
                    .cast::<SharedProducerHeader>()
                    .add(index)
            };
            let producer_header = unsafe { producer_header_ptr.as_ref() };
            if let Some((write, read)) = producer_header.try_take_ownership() {
                let buffer = unsafe {
                    self.global_header
                        .byte_add(header.buffers_offset)
                        .cast::<T>()
                        .add(index * header.per_producer_capacity)
                };

                // Set last index so we begin search here next time.
                self.last_producer_index = index;
                return Some(ConsumerSession {
                    producer_header: producer_header_ptr,
                    buffer,
                    buffer_mask: header.per_producer_capacity - 1,
                    cached_write: write,
                    cached_read: read,
                });
            }
        }

        None
    }
}

unsafe impl<T: Sized> Send for Consumer<T> {}

pub struct ConsumerSession<T: Sized> {
    producer_header: NonNull<SharedProducerHeader>,
    buffer: NonNull<T>,

    buffer_mask: usize,
    cached_write: usize,
    cached_read: usize,
}

impl<T: Sized> Drop for ConsumerSession<T> {
    fn drop(&mut self) {
        unsafe { self.producer_header.as_ref() }.release_ownership();
    }
}

impl<T: Sized> ConsumerSession<T> {
    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.buffer_mask + 1
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.cached_write.wrapping_sub(self.cached_read)
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.cached_write == self.cached_read
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    /// Returns a reference to the value if available.
    pub fn try_read(&mut self) -> Option<&T> {
        // SAFETY: `try_read_ptr` returns a pointer to properly aligned
        //         location for `T`.
        //         IF producer properly wrote items, or T is POD, it is
        //         safe to convert to reference here.
        self.try_read_ptr().map(|p| unsafe { p.as_ref() })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    /// Returns a pointer to the value if available.
    ///
    /// All read items should be processed and pointers discarded before
    /// calling `finalize`.
    pub fn try_read_ptr(&mut self) -> Option<NonNull<T>> {
        if self.cached_read == self.cached_write {
            return None; // Queue is empty
        }

        let read_index = self.mask(self.cached_read);
        // SAFETY: read_index is guaranteed to be within bounds given the mask.
        let read_ptr = unsafe { self.buffer.add(read_index) };
        self.cached_read = self.cached_read.wrapping_add(1);

        Some(read_ptr)
    }

    /// Publishes the read position, making it visible to the producer.
    /// All previously read items MUST be processed before this is called.
    pub fn finalize(&mut self) {
        unsafe { self.producer_header.as_ref() }
            .read
            .store(self.cached_read, Ordering::Release);
    }

    /// Synchronizes the consumer's cached write position with the queue's write position.
    pub fn sync(&mut self) {
        self.cached_write = unsafe { self.producer_header.as_ref() }
            .write
            .load(Ordering::Acquire);
    }

    #[inline(always)]
    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }
}

pub struct Producer<T: Sized> {
    global_header: NonNull<SharedQueueHeader>,
    producer_header: NonNull<SharedProducerHeader>,
    buffer: NonNull<T>,

    buffer_mask: usize,
    file_size: usize,
    cached_write: usize,
    cached_read: usize,
}

impl<T: Sized> Drop for Producer<T> {
    fn drop(&mut self) {
        // Tests do not mmap so skip unmapping in tests.
        #[cfg(test)]
        {
            return;
        }

        #[allow(unreachable_code)]
        // SAFETY: global_header is mmapped and of size `file_size`.
        unsafe {
            libc::munmap(self.global_header.as_ptr().cast(), self.file_size);
        }
    }
}

impl<T: Sized> Producer<T> {
    pub fn create(
        file: &File,
        size: usize,
        num_producers: usize,
        producer_index: usize,
    ) -> Result<Self, Error> {
        assert!(num_producers > 0);
        assert!(core::mem::size_of::<T>() > 0);
        let per_producer_capacity = calculate_per_producer_capacity::<T>(size, num_producers);
        file.set_len(size as u64)?;

        let global_header = map_file(file, size)?.cast::<SharedQueueHeader>();
        unsafe {
            SharedQueueHeader::initialize::<T>(global_header, num_producers, per_producer_capacity)
        };

        Ok(unsafe { Self::from_header(global_header, size, producer_index) })
    }

    pub fn join(file: &File, producer_index: usize) -> Result<Self, Error> {
        let size = file.metadata()?.len();
        let header = map_file(file, size as usize)?.cast::<SharedQueueHeader>();
        Ok(unsafe { Self::from_header(header, size as usize, producer_index) })
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation at `header` must be of size `file_size` or greater.
    unsafe fn from_header(
        header: NonNull<SharedQueueHeader>,
        file_size: usize,
        producer_index: usize,
    ) -> Self {
        let header_ref = unsafe { header.as_ref() };
        let producer_header = header
            .byte_add(header_ref.headers_offset)
            .cast::<SharedProducerHeader>()
            .add(producer_index);
        let buffer = header
            .byte_add(header_ref.buffers_offset)
            .cast::<T>()
            .add(producer_index * header_ref.per_producer_capacity);

        let producer_header_ref = unsafe { producer_header.as_ref() };
        let cached_write = producer_header_ref.write.load(Ordering::Acquire);
        let cached_read = producer_header_ref.read.load(Ordering::Acquire);

        Self {
            global_header: header,
            producer_header,
            buffer,
            buffer_mask: header_ref.per_producer_capacity - 1,
            file_size,
            cached_write,
            cached_read,
        }
    }

    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.buffer_mask + 1
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.cached_write.wrapping_sub(self.cached_read)
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.cached_write == self.cached_read
    }

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&mut self, item: T) -> Result<(), T> {
        // SAFETY: pointer is written below if successfully reserved.
        match unsafe { self.reserve() } {
            Some(p) => {
                // SAFETY: `reserve` returns a properly aligned ptr with enough
                //         space to write T.
                unsafe { p.write(item) };
                Ok(())
            }
            None => Err(item),
        }
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    /// Returns a pointer to the reserved position.
    ///
    /// # Safety
    /// All reserved positions must be fully initialized before calling `commit`.
    /// Pointers should be dropped before calling `commit`.
    pub unsafe fn reserve(&mut self) -> Option<NonNull<T>> {
        // If write is >= read + buffer_size, the queue is written one iteration
        // ahead of the consumer, and we cannot reserve more space.
        if self.cached_write.wrapping_sub(self.cached_read) >= self.capacity() {
            return None;
        }

        let reserved_index = self.mask(self.cached_write);

        // SAFETY: The reserved index is guaranteed to be within bounds given the mask.
        let reserved_ptr = unsafe { self.buffer.add(reserved_index) };
        self.cached_write = self.cached_write.wrapping_add(1);

        Some(reserved_ptr)
    }

    /// Commits the reserved position, making it visible to the consumer.
    pub fn commit(&self) {
        unsafe { self.producer_header.as_ref() }
            .write
            .store(self.cached_write, Ordering::Release);
    }

    /// Synchronize the producer's cached read position with the queue's read
    /// position.
    pub fn sync(&mut self) {
        self.cached_read = unsafe { self.producer_header.as_ref() }
            .read
            .load(Ordering::Acquire);
    }

    #[inline(always)]
    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }
}

unsafe impl<T: Sized> Send for Producer<T> {}

#[repr(C)]
struct SharedQueueHeader {
    num_producers: usize,
    per_producer_capacity: usize,
    headers_offset: usize,
    buffers_offset: usize,

    version: AtomicU8,
}

impl SharedQueueHeader {
    /// Initializes the shared queue header.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `header` allocation must be large enough to hold the header and the buffer.
    unsafe fn initialize<T: Sized>(
        mut header: NonNull<Self>,
        num_producers: usize,
        per_producer_capacity: usize,
    ) {
        let layout = layout::<T>(num_producers);

        let headers_ptr = header
            .byte_add(layout.producer_headers_offset)
            .cast::<SharedProducerHeader>();
        for header_index in 0..num_producers {
            let producer_header = headers_ptr.add(header_index).as_mut();
            producer_header.write.store(0, Ordering::Release);
            producer_header.read.store(0, Ordering::Release);
            producer_header.owned.store(AVAILABLE, Ordering::Release);
        }

        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header.as_mut() };
        header.num_producers = num_producers;
        header.per_producer_capacity = per_producer_capacity;
        header.headers_offset = layout.producer_headers_offset;

        header.buffers_offset = layout.producer_buffer_offset;
        header.version.store(VERSION, Ordering::SeqCst);
    }
}

const AVAILABLE: usize = 0;
const TAKEN: usize = 1;

#[repr(C)]
struct SharedProducerHeader {
    write: CacheAlignedAtomicSize,
    read: CacheAlignedAtomicSize,
    owned: CacheAlignedAtomicSize,
}

impl SharedProducerHeader {
    fn try_take_ownership(&self) -> Option<(usize, usize)> {
        let write = self.write.load(Ordering::Acquire);
        let read = self.read.load(Ordering::Acquire);
        if write != read {
            if self
                .owned
                .compare_exchange(AVAILABLE, TAKEN, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some((write, read));
            }
        }

        None
    }

    fn release_ownership(&self) {
        self.owned.store(AVAILABLE, Ordering::Release);
    }
}

mod layout {
    use super::*;

    pub struct Layout {
        pub producer_headers_offset: usize,
        pub producer_buffer_offset: usize,
    }

    pub fn layout<T: Sized>(num_producers: usize) -> Layout {
        let producer_buffer_offset = buffers_offset::<T>(num_producers);

        Layout {
            producer_headers_offset: producer_headers_offset(),
            producer_buffer_offset,
        }
    }

    pub const fn producer_headers_offset() -> usize {
        round_to_next_alignment_of(
            core::mem::size_of::<SharedQueueHeader>(),
            core::mem::align_of::<SharedProducerHeader>(),
        )
    }

    pub const fn buffers_offset<T: Sized>(num_producers: usize) -> usize {
        round_to_next_alignment_of(
            producer_headers_offset()
                + num_producers * core::mem::size_of::<SharedProducerHeader>(),
            core::mem::align_of::<T>(),
        )
    }

    const fn round_to_next_alignment_of(value: usize, alignment: usize) -> usize {
        debug_assert!(
            alignment.is_power_of_two(),
            "alignment must be a power of two"
        );
        (value + alignment - 1) & !(alignment - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_queue<T: Sized>(
        buffer: &mut [u8],
        num_producers: usize,
        producer_capacity: usize,
        num_consumers: usize,
    ) -> (Vec<Producer<T>>, Vec<Consumer<T>>) {
        let file_size = buffer.len();

        let header = NonNull::new(buffer.as_mut_ptr().cast()).expect("Failed to create header");
        unsafe { SharedQueueHeader::initialize::<T>(header, num_producers, producer_capacity) };

        let producers = (0..num_producers)
            .map(|producer_index| unsafe {
                Producer::from_header(header, file_size, producer_index)
            })
            .collect();
        let consumers = (0..num_consumers)
            .map(|_| unsafe { Consumer::from_header(header, file_size) })
            .collect();

        (producers, consumers)
    }

    fn drain_session<T: Sized + Copy>(session: &mut ConsumerSession<T>) -> Vec<T> {
        let mut received = vec![];
        while let Some(x) = session.try_read() {
            received.push(*x);
        }
        session.finalize();
        received
    }

    fn sync_producers<T: Sized>(producers: &mut [Producer<T>]) {
        producers.iter_mut().for_each(|producer| producer.sync());
    }

    fn commit_producers<T: Sized>(producers: &mut [Producer<T>]) {
        producers.iter_mut().for_each(|producer| producer.commit());
    }

    #[test]
    fn test_mpmc() {
        let mut buffer = [0u8; 1024 * 16];
        let (mut producers, mut consumers) = create_test_queue::<usize>(&mut buffer, 2, 64, 2);

        producers[0].try_write(1).unwrap();
        producers[1].try_write(2).unwrap();
        producers[0].try_write(3).unwrap();
        commit_producers(&mut producers);

        let mut session = consumers[0].try_open_session().unwrap();
        let received = drain_session(&mut session);
        drop(session);
        assert_eq!(received, vec![1, 3]);
        let mut session = consumers[0].try_open_session().unwrap();
        let received = drain_session(&mut session);
        drop(session);
        assert_eq!(received, vec![2]);
        assert!(consumers[0].try_open_session().is_none());

        sync_producers(&mut producers);
        producers[0].try_write(1).unwrap();
        producers[1].try_write(2).unwrap();
        commit_producers(&mut producers);

        let mut session1 = consumers[0].try_open_session().unwrap();
        let mut session2 = consumers[1].try_open_session().unwrap();
        assert!(consumers[0].try_open_session().is_none()); // no more sessions currently available
        assert_eq!(drain_session(&mut session1), vec![2]); // begin at most recently accessed producer
        assert_eq!(drain_session(&mut session2), vec![1]);
    }
}
