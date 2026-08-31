use crate::{
    error::{ReadTimeoutError, WaitError},
    futex::{Waiters, SPIN_ATTEMPTS},
    CacheAlignedAtomicSize,
};
use core::sync::atomic::AtomicUsize;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};

/// Heap-only futex state used by disconnect-aware channels.
///
/// Both fields occupy their own cache line: wait registration does not
/// contend with wake-generation updates.
#[derive(Default)]
#[repr(C, align(64))]
pub(crate) struct ChannelWake {
    waiters: Waiters,
    generation: CacheAlignedAtomicSize,
}

impl ChannelWake {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn notify_or_cursor(
        channel_wake: Option<&Self>,
        cursor_waiters: &Waiters,
        cursor: &AtomicUsize,
        count: usize,
    ) {
        if let Some(wake) = channel_wake {
            wake.waiters.bump_and_wake_count(&wake.generation, count);
        } else {
            cursor_waiters.wake(cursor, count);
        }
    }

    fn notify_all(&self) {
        self.waiters.bump_and_wake(&self.generation);
    }

    fn wait_for<T>(
        &self,
        timeout: Duration,
        check: impl FnMut() -> Option<T>,
    ) -> Result<T, WaitError> {
        self.waiters
            .wait_for(&self.generation, SPIN_ATTEMPTS, timeout, check)
    }
}

struct Side {
    wake: Arc<ChannelWake>,
}

impl Drop for Side {
    fn drop(&mut self) {
        // `Side` is dropped only after the final clone of one endpoint is
        // gone. Wake every channel waiter so it can observe that disconnect.
        self.wake.notify_all();
    }
}

/// One endpoint's ownership token and view of its peer's token.
pub(crate) struct ChannelSide {
    own: Arc<Side>,
    peer: Weak<Side>,
}

impl ChannelSide {
    pub(crate) fn is_peer_connected(&self) -> bool {
        self.peer.strong_count() != 0
    }

    pub(crate) fn wake(&self) -> &ChannelWake {
        &self.own.wake
    }

    /// Waits for `check` to succeed, the peer to disconnect, or the timeout.
    ///
    /// Connection is sampled before `check`: if the peer is already gone,
    /// `check` still gets one final acquire of the queue publication state so
    /// buffered values are drained before disconnection is reported.
    pub(crate) fn wait_for<T>(
        &self,
        timeout: Duration,
        mut check: impl FnMut() -> Option<T>,
    ) -> Result<T, ReadTimeoutError> {
        enum Outcome<T> {
            Ready(T),
            Disconnected,
        }

        let mut check_channel = || {
            let connected = self.is_peer_connected();
            match check() {
                Some(value) => Some(Outcome::Ready(value)),
                None if connected => None,
                None => Some(Outcome::Disconnected),
            }
        };

        match self.own.wake.wait_for(timeout, &mut check_channel) {
            Ok(Outcome::Ready(value)) => Ok(value),
            Ok(Outcome::Disconnected) => Err(ReadTimeoutError::Disconnected),
            Err(WaitError::Timeout) => match check_channel() {
                Some(Outcome::Ready(value)) => Ok(value),
                Some(Outcome::Disconnected) => Err(ReadTimeoutError::Disconnected),
                None => Err(ReadTimeoutError::Timeout),
            },
        }
    }
}

impl Clone for ChannelSide {
    fn clone(&self) -> Self {
        Self {
            own: Arc::clone(&self.own),
            peer: self.peer.clone(),
        }
    }
}

pub(crate) fn channel_sides(wake: Arc<ChannelWake>) -> (ChannelSide, ChannelSide) {
    let first = Arc::new(Side {
        wake: Arc::clone(&wake),
    });
    let second = Arc::new(Side { wake });

    (
        ChannelSide {
            own: Arc::clone(&first),
            peer: Arc::downgrade(&second),
        },
        ChannelSide {
            own: second,
            peer: Arc::downgrade(&first),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_wake_uses_separate_cache_lines() {
        assert_eq!(core::mem::align_of::<ChannelWake>(), 64);
        assert_eq!(core::mem::size_of::<ChannelWake>(), 128);
    }
}
