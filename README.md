# shaq

[![Rust CI](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml/badge.svg)](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml)

shaq is a **SHAred Queue**: a simple shared-memory SPSC (Single Producer Single Consumer) and MPMC (Multi Producer Multi Consumer) FIFO queue.
It is designed for efficient inter-thread or inter-process communication using lock-free queues.

`shaq` now supports two backing modes:

- File-backed shared memory via `create` / `join`, for inter-process communication.
- In-process heap-backed queues via `spsc::pair` and `mpmc::pair`, for channel-style usage without file backing.

For in-process users that also need peer-disconnection detection, `spsc::channel`
and `mpmc::channel` return `Sender` / `Receiver` wrappers with the same queue
operations. Nonblocking operations distinguish `Full` or `Empty` from
`Disconnected`. Timed channel reads use a heap-only wake sidecar, so publishing
data or dropping the final sender wakes them immediately. The raw `pair` APIs
continue to wait directly on the queue's published cursor and remain available
when lifecycle tracking is unnecessary.
