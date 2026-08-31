use super::{
    Consumer as QueueConsumer, Producer as QueueProducer, RawReadBatch, ReadBatch, ReadGuard,
    WriteBatch, WriteGuard,
};
use crate::{
    channel::{channel_sides, ChannelSide, ChannelWake},
    error::{Error, ReadTimeoutError, TryReadError, TryWriteError},
};
use std::{num::NonZeroUsize, time::Duration};

/// Creates a bounded, disconnect-aware in-process MPMC channel.
///
/// This has the same heap-backed queue behavior as [`super::pair`], while its
/// operations distinguish a full or empty queue from a dropped peer.
pub fn channel<T: Send>(capacity: usize) -> Result<(Sender<T>, Receiver<T>), Error> {
    let (producer, consumer) = super::pair(capacity)?;
    let wake = ChannelWake::new();
    let (sender_side, receiver_side) = channel_sides(wake);
    Ok((
        Sender {
            producer,
            side: sender_side,
        },
        Receiver {
            consumer,
            side: receiver_side,
        },
    ))
}

/// Sending side of a disconnect-aware, heap-backed MPMC queue.
pub struct Sender<T> {
    // Keep the raw endpoint before the side token so it is dropped before the
    // final sender is reported as disconnected.
    producer: QueueProducer<T>,
    side: ChannelSide,
}

impl<T> Sender<T> {
    /// Writes an item, distinguishing a full queue from dropped receivers.
    pub fn try_write(&self, item: T) -> Result<(), TryWriteError<T>> {
        if !self.side.is_peer_connected() {
            return Err(TryWriteError::Disconnected(item));
        }

        self.producer
            .try_write_with_wake(item, Some(self.side.wake()))
            .map_err(TryWriteError::Full)
    }

    /// Writes a slice, distinguishing a full queue from dropped receivers.
    pub fn try_write_slice<'a>(&self, items: &'a [T]) -> Result<(), TryWriteError<&'a [T]>>
    where
        T: Copy,
    {
        if items.is_empty() {
            return Ok(());
        }
        if !self.side.is_peer_connected() {
            return Err(TryWriteError::Disconnected(items));
        }

        if self
            .producer
            .try_write_slice_with_wake(items, Some(self.side.wake()))
        {
            Ok(())
        } else {
            Err(TryWriteError::Full(items))
        }
    }

    /// Reserves a slot for writing.
    ///
    /// The slot is published when the returned guard is dropped.
    ///
    /// # Safety
    /// The caller must leave the reserved slot initialized with a valid `T`
    /// before the guard is dropped.
    pub unsafe fn try_reserve_write(&self) -> Result<WriteGuard<'_, T>, TryWriteError<()>> {
        if !self.side.is_peer_connected() {
            return Err(TryWriteError::Disconnected(()));
        }

        // SAFETY: The caller accepts the underlying guard initialization contract.
        unsafe {
            self.producer
                .try_reserve_write_with_wake(Some(self.side.wake()))
        }
        .ok_or(TryWriteError::Full(()))
    }

    /// Reserves exactly `count` slots for writing.
    ///
    /// The slots are published when the returned batch is dropped.
    ///
    /// # Safety
    /// The caller must leave every reserved slot initialized with a valid `T`
    /// before the batch is dropped.
    pub unsafe fn try_reserve_write_batch(
        &self,
        count: NonZeroUsize,
    ) -> Result<WriteBatch<'_, T>, TryWriteError<()>> {
        if !self.side.is_peer_connected() {
            return Err(TryWriteError::Disconnected(()));
        }

        // SAFETY: The caller accepts the underlying batch initialization contract.
        unsafe {
            self.producer
                .try_reserve_write_batch_with_wake(count, Some(self.side.wake()))
        }
        .ok_or(TryWriteError::Full(()))
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            side: self.side.clone(),
        }
    }
}

/// Receiving side of a disconnect-aware, heap-backed MPMC queue.
pub struct Receiver<T> {
    // Keep the raw endpoint before the side token so it is dropped before the
    // final receiver is reported as disconnected.
    consumer: QueueConsumer<T>,
    side: ChannelSide,
}

impl<T> Receiver<T> {
    /// Attempts to read one item, distinguishing empty from disconnected.
    pub fn try_read(&self) -> Result<T, TryReadError> {
        let connected = self.side.is_peer_connected();
        match self.consumer.try_read() {
            Some(item) => Ok(item),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Reads one item, waiting until data, sender disconnect, or timeout.
    pub fn read_timeout(&self, timeout: Duration) -> Result<T, ReadTimeoutError> {
        self.reserve_read_timeout(timeout)
            .map(ReadGuard::into_inner)
    }

    /// Attempts to reserve one value for reading.
    pub fn try_reserve_read(&self) -> Result<ReadGuard<'_, T>, TryReadError> {
        let connected = self.side.is_peer_connected();
        match self.consumer.try_reserve_read() {
            Some(guard) => Ok(guard),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Reserves one value, waiting until data or timeout.
    ///
    /// Dropping the final sender wakes this wait immediately.
    pub fn reserve_read_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ReadGuard<'_, T>, ReadTimeoutError> {
        self.wait_for_read(timeout, || self.consumer.try_reserve_read())
    }

    /// Attempts to reserve up to `max` values for reading.
    pub fn try_reserve_read_batch(
        &self,
        max: NonZeroUsize,
    ) -> Result<ReadBatch<'_, T>, TryReadError> {
        let connected = self.side.is_peer_connected();
        match self.consumer.try_reserve_read_batch(max) {
            Some(batch) => Ok(batch),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Attempts to reserve up to `max` values without destructor cleanup.
    ///
    /// # Safety
    /// The caller must satisfy [`super::Consumer::try_reserve_read_batch_raw`]'s
    /// ownership and cleanup requirements.
    pub unsafe fn try_reserve_read_batch_raw(
        &self,
        max: NonZeroUsize,
    ) -> Result<RawReadBatch<'_, T>, TryReadError> {
        let connected = self.side.is_peer_connected();
        // SAFETY: The caller accepts the raw read-batch contract.
        match unsafe { self.consumer.try_reserve_read_batch_raw(max) } {
            Some(batch) => Ok(batch),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Reserves up to `max` values, waiting until data or timeout.
    ///
    /// Dropping the final sender wakes this wait immediately.
    pub fn reserve_read_batch_timeout(
        &self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, ReadTimeoutError> {
        self.wait_for_read(timeout, || self.consumer.try_reserve_read_batch(max))
    }

    /// Reserves a raw batch, waiting until data or timeout.
    ///
    /// Dropping the final sender wakes this wait immediately.
    ///
    /// # Safety
    /// The caller must satisfy [`Self::try_reserve_read_batch_raw`]'s safety
    /// requirements for a successfully returned batch.
    pub unsafe fn reserve_read_batch_raw_timeout(
        &self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<RawReadBatch<'_, T>, ReadTimeoutError> {
        self.wait_for_read(timeout, || {
            // SAFETY: The caller accepts the raw read-batch contract.
            unsafe { self.consumer.try_reserve_read_batch_raw(max) }
        })
    }

    fn wait_for_read<R>(
        &self,
        timeout: Duration,
        check: impl FnMut() -> Option<R>,
    ) -> Result<R, ReadTimeoutError> {
        self.side.wait_for(timeout, check)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer.clone(),
            side: self.side.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, thread, time::Instant};

    #[test]
    fn reports_full_empty_and_disconnect_while_draining() {
        let (sender, receiver) = channel(1).unwrap();

        assert_eq!(receiver.try_read(), Err(TryReadError::Empty));
        sender.try_write(10).unwrap();
        assert_eq!(sender.try_write(11), Err(TryWriteError::Full(11)));
        assert_eq!(receiver.try_read(), Ok(10));

        sender.try_write(12).unwrap();
        drop(sender);
        assert_eq!(receiver.try_read(), Ok(12));
        assert_eq!(receiver.try_read(), Err(TryReadError::Disconnected));
    }

    #[test]
    fn receiver_disconnect_waits_for_last_sender_clone() {
        let (sender, receiver) = channel::<u64>(2).unwrap();
        let sender_clone = sender.clone();

        drop(sender);
        assert_eq!(receiver.try_read(), Err(TryReadError::Empty));
        drop(sender_clone);
        assert_eq!(receiver.try_read(), Err(TryReadError::Disconnected));
    }

    #[test]
    fn sender_disconnect_waits_for_last_receiver_clone() {
        let (sender, receiver) = channel(2).unwrap();
        let receiver_clone = receiver.clone();

        drop(receiver);
        sender.try_write(1).unwrap();
        drop(receiver_clone);
        assert_eq!(sender.try_write(2), Err(TryWriteError::Disconnected(2)));
    }

    #[test]
    fn write_reservation_checks_connection_only_when_created() {
        let (sender, receiver) = channel(1).unwrap();
        // SAFETY: The guard is initialized below before it is dropped.
        let guard = unsafe { sender.try_reserve_write() }.unwrap();

        drop(receiver);
        guard.write(42);
        // A new operation observes the disconnect.
        assert!(matches!(
            // SAFETY: No guard is returned from the disconnected channel.
            unsafe { sender.try_reserve_write() },
            Err(TryWriteError::Disconnected(()))
        ));
    }

    #[test]
    fn existing_write_guard_and_batch_publish_to_timed_reader() {
        let (sender, receiver) = channel(4).unwrap();
        let waiting_receiver = receiver.clone();
        let reader = thread::spawn(move || waiting_receiver.read_timeout(Duration::from_secs(1)));

        // SAFETY: `write` initializes the reserved slot before publishing it.
        unsafe { sender.try_reserve_write() }.unwrap().write(1);
        assert_eq!(reader.join().unwrap(), Ok(1));

        {
            // SAFETY: Both reserved slots are initialized before the batch drops.
            let mut batch =
                unsafe { sender.try_reserve_write_batch(NonZeroUsize::new(2).unwrap()) }.unwrap();
            // SAFETY: Index zero is within this two-slot batch.
            unsafe { batch.write(0, 2) };
            // SAFETY: Index one is within this two-slot batch.
            unsafe { batch.as_mut(1).write(3) };
        }
        assert_eq!(receiver.try_read(), Ok(2));
        assert_eq!(receiver.try_read(), Ok(3));
    }

    #[test]
    fn read_reservations_report_timeout_and_disconnect() {
        let (sender, receiver) = channel::<u64>(2).unwrap();
        assert!(matches!(
            receiver.try_reserve_read(),
            Err(TryReadError::Empty)
        ));
        assert!(matches!(
            receiver.reserve_read_batch_timeout(NonZeroUsize::MIN, Duration::ZERO),
            Err(ReadTimeoutError::Timeout)
        ));

        sender.try_write_slice(&[1, 2]).unwrap();
        let batch = receiver
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .unwrap();
        assert_eq!(batch.as_slices(), (&[1, 2][..], &[][..]));
        drop(batch);

        drop(sender);
        // SAFETY: No batch is returned on this empty, disconnected queue.
        let raw_result = unsafe { receiver.try_reserve_read_batch_raw(NonZeroUsize::MIN) };
        assert!(matches!(raw_result, Err(TryReadError::Disconnected)));
    }

    #[test]
    fn slice_write_returns_input_and_empty_slice_is_a_noop() {
        let (sender, receiver) = channel(2).unwrap();
        sender.try_write_slice(&[1, 2]).unwrap();
        let rejected = [3];
        assert_eq!(
            sender.try_write_slice(&rejected),
            Err(TryWriteError::Full(rejected.as_slice()))
        );

        drop(receiver);
        assert_eq!(sender.try_write_slice(&[]), Ok(()));
        assert_eq!(
            sender.try_write_slice(&rejected),
            Err(TryWriteError::Disconnected(rejected.as_slice()))
        );
    }

    #[test]
    fn timed_read_wakes_on_final_sender_disconnect() {
        let (sender, receiver) = channel::<u64>(1).unwrap();
        let sender_clone = sender.clone();
        let timeout = Duration::from_secs(2);
        let reader = thread::spawn(move || receiver.read_timeout(timeout));

        drop(sender);
        let dropped_at = Instant::now();
        thread::yield_now();
        drop(sender_clone);
        assert_eq!(reader.join().unwrap(), Err(ReadTimeoutError::Disconnected));
        assert!(dropped_at.elapsed() < timeout / 2);
    }

    #[test]
    fn sender_and_receiver_are_send_and_sync_for_send_not_sync_items() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Sender<Cell<u64>>>();
        assert_send_sync::<Receiver<Cell<u64>>>();
    }
}
