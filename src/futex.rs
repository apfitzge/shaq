use crate::error::WaitError;
use core::sync::atomic::AtomicU32;
use std::time::{Duration, Instant};

/// Waits until `futex` no longer equals `expected`, a wake is received, or
/// `timeout` elapses.
///
/// [`libc::EAGAIN`] is treated as success because it means the value changed
/// before the kernel parked the thread. [`libc::EINTR`] is retried with the
/// remaining [`Duration`]. Unexpected futex errors indicate a violated
/// caller/platform invariant and panic.
pub(crate) fn wait(futex: &AtomicU32, expected: u32, timeout: Duration) -> Result<(), WaitError> {
    let start = Instant::now();
    let deadline = start.checked_add(timeout);

    loop {
        let remaining = match deadline {
            Some(deadline) => deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(WaitError::Timeout)?,
            None => timeout,
        };

        let timeout_storage = duration_to_timespec(remaining);
        let timeout_ptr = &timeout_storage as *const libc::timespec;

        // SAFETY:
        // - `futex.as_ptr()` is a valid pointer to a 4-byte aligned atomic.
        // - `timeout_ptr` is null or points to a live `timespec`.
        // - `FUTEX_WAIT` only blocks if the value still equals `expected`.
        let result = unsafe {
            libc::syscall(
                libc::SYS_futex,
                futex.as_ptr(),
                libc::FUTEX_WAIT,
                expected as libc::c_int,
                timeout_ptr,
            )
        };

        if result == 0 {
            return Ok(());
        }

        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EAGAIN) => return Ok(()),
            Some(libc::EINTR) => continue,
            Some(libc::ETIMEDOUT) => return Err(WaitError::Timeout),
            _ => panic!("unexpected futex wait error: {err}"),
        }
    }
}

/// Wakes up to `count` waiters blocked on `futex`.
///
/// Callers should only invoke this after their own wait-state check proves at
/// least one sleeper may exist. The wake count is not returned because current
/// queue logic only needs a best-effort notification.
pub(crate) fn wake(futex: &AtomicU32, count: u32) {
    debug_assert!(count > 0);
    debug_assert!(count <= libc::c_int::MAX as u32);

    // SAFETY: `futex.as_ptr()` is a valid pointer to a 4-byte aligned atomic.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex.as_ptr(),
            libc::FUTEX_WAKE,
            count as libc::c_int,
        )
    };
    debug_assert!(
        result >= 0,
        "unexpected futex wake error: {}",
        std::io::Error::last_os_error()
    );
}

/// Converts a [`Duration`] to the relative [`libc::timespec`] timeout format
/// expected by futex.
#[inline]
const fn duration_to_timespec(duration: Duration) -> libc::timespec {
    debug_assert!(duration.as_secs() <= libc::time_t::MAX as u64);
    libc::timespec {
        tv_sec: duration.as_secs() as libc::time_t,
        tv_nsec: duration.subsec_nanos() as libc::c_long,
    }
}
