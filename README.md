# agent-resume

Checkpoint and resume long-running agent jobs from the last completed item.

Crash on item 47? Resume from item 48 on the next run. `agent-resume` is a
small, dependency-light Rust library for making batch/agent workloads
restartable: it tracks which work items have been completed, persists that
progress to a durable store, and lets a fresh process pick up exactly where the
previous one left off.

## What it does

You wrap a list of work items in a `Resumable` cursor backed by a `Sink`
(a checkpoint store). For each item you do your work and then call
`checkpoint()` to persist progress. If the process dies, a new `Resumable`
over the same store skips everything that was already completed.

Items are **at-least-once**: if the process dies *before* `checkpoint()` is
called for an item, that item is retried on restart. This is intentional —
design your per-item work to be idempotent.

### Core types

- **`Resumable`** — a checkpoint-aware cursor over a `Vec<serde_json::Value>`
  of work items. Provides `next_item()`, `checkpoint()`, `remaining_items()`,
  and accessors for `state()`, `turn()`, `completed_items()`, and `resumed()`.
- **`Checkpoint`** — one persisted row: turn counter, arbitrary JSON `state`,
  the list of completed item keys, and a timestamp. Serializes to a single
  JSONL line.
- **`Sink`** — the store contract (`append` + `load_latest`). Implement it to
  back checkpoints with any storage you like.
- **`InMemoryStore`** — a non-durable in-process store, useful for tests and
  demos.
- **`JsonlStore`** — an append-only JSONL file store, one checkpoint per line,
  with fsync-on-write by default (call `.no_fsync()` to disable, e.g. on
  tmpfs or in CI).

A custom **item key** function can be supplied so completion is tracked by a
stable identifier (e.g. `item["id"]`) rather than the full item value.

## Install

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
agent-resume = "0.1"
serde_json = "1"
```

## Usage

```rust
use agent_resume::{InMemoryStore, Resumable};
use serde_json::{json, Map, Value};
use std::sync::Arc;

let store = Arc::new(InMemoryStore::new());
let items: Vec<Value> = (0..5).map(|i| json!(i)).collect();

let mut r = Resumable::new(store.clone(), None, items.clone(), None);
assert!(!r.resumed());

while let Some(item) = r.next_item() {
    // do work with `item`...
    let mut st = Map::new();
    st.insert("last".into(), item);
    r.checkpoint(Some(st)).unwrap();
}

assert_eq!(r.turn(), 5);

// Simulate a restart: a new Resumable over the same store.
let mut r2 = Resumable::new(store, None, items, None);
assert!(r2.resumed());
assert!(r2.next_item().is_none()); // all items already done
```

### Durable checkpoints with `JsonlStore`

```rust
use agent_resume::{JsonlStore, Resumable};
use serde_json::{json, Value};
use std::sync::Arc;

let store = Arc::new(JsonlStore::new("checkpoints.jsonl"));
let items: Vec<Value> = (0..100).map(|i| json!(i)).collect();

let mut r = Resumable::new(store, None, items, None);
while let Some(item) = r.next_item() {
    // process item, then persist progress
    r.checkpoint(None).unwrap();
}
```

### Custom item keys

```rust
use serde_json::Value;

// Track completion by a stable `id` field instead of the whole item.
let key_fn: Box<dyn Fn(&Value) -> Value + Send + Sync> =
    Box::new(|v| v["id"].clone());
// pass `Some(key_fn)` as the fourth argument to `Resumable::new`.
```

## Tech stack

- **Language:** Rust (edition 2021)
- **Dependency:** [`serde_json`](https://crates.io/crates/serde_json) for the
  JSON state and JSONL serialization
- **License:** MIT

## Development

```sh
cargo build
cargo test
```

The crate ships with an extensive unit test suite covering checkpoint
serialization, resume semantics, custom item keys, and both store backends.
