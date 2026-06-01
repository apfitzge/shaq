# shaq

[![Rust CI](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml/badge.svg)](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml)

shaq is a **SHAred Queue**: a simple shared-memory SPSC (Single Producer Single Consumer), MPMC (Multi Producer Multi Consumer) FIFO queue, and broadcast (MPMC).
It is designed for efficient inter-thread or inter-process communication using lock-free queues.

The broadcast queue is lossy: producers may overwrite retained slots without
waiting for consumers. See the `broadcast` module docs for the payload access
and validation contract.

Broadcast queues use runtime-configured producer and consumer slots via
`broadcast::BroadcastConfig`. Each producer slot has an independent publication
lane; consumers read a merged stream while preserving order within each lane.

`shaq` now supports two backing modes:

- File-backed shared memory via `create` / `join`, for inter-process communication.
- In-process heap-backed queues via `spsc::pair` and `mpmc::pair`, for channel-style usage without file backing.
