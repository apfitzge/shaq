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
`Disconnected`. Timed reads use the queue's existing futex wait; if it times
out, they report `Disconnected` when the final sender has been dropped and
`Timeout` otherwise. A disconnect does not wake a timed read early. The raw
`pair` APIs remain available when lifecycle tracking is unnecessary.
