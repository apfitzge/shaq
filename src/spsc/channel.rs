use super::{Consumer as QueueConsumer, Producer as QueueProducer, ReadBatch, WriteBatch};
use crate::{
    channel::{channel_sides, ChannelSide},
    error::{Error, ReadTimeoutError, TryReadError, TryWriteError, WaitError},
};
use std::{num::NonZeroUsize, time::Duration};

/// Creates a bounded, disconnect-aware in-process SPSC channel.
///
/// This has the same heap-backed queue behavior as [`super::pair`], while its
/// operations distinguish a full or empty queue from a dropped peer.
pub fn channel<T: Send>(capacity: usize) -> Result<(Sender<T>, Receiver<T>), Error> {
    let (producer, consumer) = super::pair(capacity)?;
    let (sender_side, receiver_side) = channel_sides();
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

/// Sending side of a disconnect-aware, heap-backed SPSC queue.
pub struct Sender<T> {
    // Keep the raw endpoint before the side token so it is dropped before this
    // sender is reported as disconnected.
    producer: QueueProducer<T>,
    side: ChannelSide,
}

impl<T> Sender<T> {
    /// Returns the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }

    /// Returns the current length of the queue.
    pub fn len(&self) -> usize {
        self.producer.len()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.producer.is_empty()
    }

    /// Starts a batched write, or reports that the receiver is disconnected.
    ///
    /// Connection state is checked only when the batch is created. Once
    /// returned, the ordinary queue batch remains valid until it is dropped.
    pub fn write_batch(&mut self) -> Result<WriteBatch<'_, T>, TryWriteError<()>> {
        if self.side.is_peer_connected() {
            Ok(self.producer.write_batch())
        } else {
            Err(TryWriteError::Disconnected(()))
        }
    }

    /// Writes an item, distinguishing a full queue from a dropped receiver.
    pub fn try_write(&mut self, item: T) -> Result<(), TryWriteError<T>> {
        if !self.side.is_peer_connected() {
            return Err(TryWriteError::Disconnected(item));
        }

        self.producer.try_write(item).map_err(TryWriteError::Full)
    }
}

// SAFETY: The wrapped SPSC producer is Send when T is Send, and the channel
// side contains only thread-safe atomics and Arc/Weak ownership tokens.
unsafe impl<T: Send> Send for Sender<T> {}

/// Receiving side of a disconnect-aware, heap-backed SPSC queue.
pub struct Receiver<T> {
    // Keep the raw endpoint before the side token so it is dropped before this
    // receiver is reported as disconnected.
    consumer: QueueConsumer<T>,
    side: ChannelSide,
}

impl<T> Receiver<T> {
    /// Returns the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.consumer.capacity()
    }

    /// Returns the current length of the queue.
    pub fn len(&self) -> usize {
        self.consumer.len()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.consumer.is_empty()
    }

    /// Attempts to read one item, distinguishing empty from disconnected.
    pub fn try_read(&mut self) -> Result<T, TryReadError> {
        let connected = self.side.is_peer_connected();
        match self.consumer.try_read() {
            Some(item) => Ok(item),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Attempts to reserve up to `max` values for reading.
    pub fn try_reserve_read_batch(
        &mut self,
        max: NonZeroUsize,
    ) -> Result<ReadBatch<'_, T>, TryReadError> {
        let connected = self.side.is_peer_connected();
        match self.consumer.try_reserve_read_batch(max) {
            Some(batch) => Ok(batch),
            None if connected => Err(TryReadError::Empty),
            None => Err(TryReadError::Disconnected),
        }
    }

    /// Waits until data is readable or `timeout` expires.
    ///
    /// A sender disconnect does not wake the queue's futex. After a timeout,
    /// this method returns [`ReadTimeoutError::Disconnected`] if every sender
    /// has been dropped; otherwise it returns [`ReadTimeoutError::Timeout`].
    pub fn wait_readable_timeout(&mut self, timeout: Duration) -> Result<(), ReadTimeoutError> {
        match self.consumer.wait_readable_timeout(timeout) {
            Ok(()) => Ok(()),
            Err(WaitError::Timeout) if self.side.is_peer_connected() => {
                Err(ReadTimeoutError::Timeout)
            }
            Err(WaitError::Timeout) => {
                self.consumer.sync();
                if self.consumer.is_empty() {
                    Err(ReadTimeoutError::Disconnected)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Reads one item, waiting until data or timeout.
    ///
    /// Disconnection is checked after the underlying queue wait times out.
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<T, ReadTimeoutError> {
        let batch = self.reserve_read_batch_timeout(NonZeroUsize::MIN, timeout)?;
        Ok(batch
            .into_iter()
            .next()
            .expect("a successful one-item reservation is non-empty"))
    }

    /// Reserves up to `max` values, waiting until data or timeout.
    ///
    /// Disconnection is checked after the underlying queue wait times out.
    pub fn reserve_read_batch_timeout(
        &mut self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, ReadTimeoutError> {
        match self.consumer.wait_readable_timeout(timeout) {
            Ok(()) => Ok(self
                .consumer
                .try_reserve_read_batch(max)
                .expect("the unique consumer observed readable data")),
            Err(WaitError::Timeout) if self.side.is_peer_connected() => {
                Err(ReadTimeoutError::Timeout)
            }
            Err(WaitError::Timeout) => self
                .try_reserve_read_batch(max)
                .map_err(|_| ReadTimeoutError::Disconnected),
        }
    }
}

// SAFETY: The wrapped SPSC consumer is Send when T is Send, and the channel
// side contains only thread-safe atomics and Arc/Weak ownership tokens.
unsafe impl<T: Send> Send for Receiver<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn reports_full_empty_and_disconnect_while_draining() {
        let (mut sender, mut receiver) = channel(1).unwrap();

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
    fn sender_observes_receiver_disconnect() {
        let (mut sender, receiver) = channel::<u64>(1).unwrap();
        drop(receiver);
        assert_eq!(sender.try_write(42), Err(TryWriteError::Disconnected(42)));
    }

    #[test]
    fn write_batch_checks_connection_only_when_created() {
        let (mut sender, receiver) = channel(1).unwrap();
        let mut batch = sender.write_batch().unwrap();

        drop(receiver);
        assert_eq!(batch.try_write(42), Ok(()));
    }

    #[test]
    fn existing_write_batch_publishes_to_timed_reader() {
        let (mut sender, mut receiver) = channel(2).unwrap();
        let reader = thread::spawn(move || receiver.read_timeout(Duration::from_secs(1)));

        {
            let mut batch = sender.write_batch().unwrap();
            batch.try_write(1).unwrap();
            // SAFETY: The returned slot is initialized before the batch drops.
            unsafe { batch.try_as_mut().unwrap().write(2) };
        }

        assert_eq!(reader.join().unwrap(), Ok(1));
    }

    #[test]
    fn timed_read_reports_disconnect_after_queue_timeout() {
        let (sender, mut receiver) = channel::<u64>(1).unwrap();
        let reader = thread::spawn(move || receiver.read_timeout(Duration::from_millis(10)));

        thread::yield_now();
        drop(sender);
        assert_eq!(reader.join().unwrap(), Err(ReadTimeoutError::Disconnected));
    }

    #[test]
    fn read_batch_reports_empty_then_disconnected() {
        let (sender, mut receiver) = channel::<u64>(2).unwrap();
        assert_eq!(
            receiver.wait_readable_timeout(Duration::ZERO),
            Err(ReadTimeoutError::Timeout)
        );
        assert!(matches!(
            receiver.try_reserve_read_batch(NonZeroUsize::MIN),
            Err(TryReadError::Empty)
        ));
        drop(sender);
        assert!(matches!(
            receiver.try_reserve_read_batch(NonZeroUsize::MIN),
            Err(TryReadError::Disconnected)
        ));
    }
}
