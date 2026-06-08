# agent-resume

Checkpoint and resume long-running agent jobs from the last completed item.

Crash on item 47? Resume from item 48 on the next run.

`agent-resume` is a tiny, dependency-light Rust crate for making batch jobs —
LLM pipelines, scrapers, ETL passes, any "process a list of items" loop —
**restartable**. After each item you call `checkpoint(...)`, which durably
records progress. If the process dies, a fresh run reads the latest checkpoint
and skips everything already finished.

- **At-least-once semantics.** If the process dies *before* `checkpoint()` is
  called for an item, that item is retried on restart. Make your per-item work
  idempotent (or tolerant of one extra attempt).
- **Pluggable storage.** Ships with an in-memory store (tests/demos) and a
  durable append-only JSONL file store. Bring your own by implementing the
  [`Sink`](#the-sink-trait) trait (Postgres, S3, Redis, ...).
- **Arbitrary resumable state.** Alongside the completed-item set you can carry
  a free-form JSON `state` map (token counts, running totals, cursors).

## Installation

Add it to your `Cargo.toml`:

```toml
[dependencies]
agent-resume = "0.1"
serde_json = "1"
```

`serde_json::Value` is part of the public API, so you will usually want
`serde_json` as a direct dependency too.

## Quick start

```rust
use agent_resume::{JsonlStore, Resumable};
use serde_json::{json, Map, Value};
use std::sync::Arc;

fn main() {
    // Durable, append-only checkpoint file. Re-run this program after a crash
    // and it resumes from where it left off.
    let store = Arc::new(JsonlStore::new("checkpoints.jsonl"));

    let items: Vec<Value> = (0..100).map(|i| json!(i)).collect();
    let mut job = Resumable::new(store, None, items, None);

    if job.resumed() {
        println!("resuming at turn {}", job.turn());
    }

    while let Some(item) = job.next_item() {
        // ... do the real work for `item` here ...
        let result = item.as_u64().unwrap() * 2;

        // Persist progress. The item just returned by `next_item()` is marked
        // complete; on a later run it will be skipped.
        let mut state = Map::new();
        state.insert("last_result".into(), json!(result));
        job.checkpoint(Some(state)).expect("checkpoint write failed");
    }

    println!("done in {} turns", job.turn());
}
```

## How it works

A `Resumable` is a cursor over a `Vec<serde_json::Value>` of work items:

1. `next_item()` returns the next item whose key is not yet in the completed
   set, advancing the cursor. It returns `None` once everything is done.
2. `checkpoint(new_state)` marks the most recently yielded item complete,
   bumps the turn counter, optionally replaces the carried `state`, and writes
   a [`Checkpoint`](#checkpoint) row to the store.
3. On construction, `Resumable::new(...)` calls `store.load_latest()`. If a
   checkpoint exists, its `turn`, `state`, and completed-item set are restored
   and `resumed()` returns `true`.

### Item keys

By default an item's identity is the entire `Value`. If your items carry a
stable id, pass an [`ItemKeyFn`](#itemkeyfn) so resumption keys off that field
instead of the whole payload:

```rust
use agent_resume::{InMemoryStore, ItemKeyFn, Resumable};
use serde_json::{json, Value};
use std::sync::Arc;

let work = vec![
    json!({"id": "a", "payload": 1}),
    json!({"id": "b", "payload": 2}),
];
let store = Arc::new(InMemoryStore::new());

// Identity = the "id" field, not the full object.
let key_fn: ItemKeyFn = Box::new(|v| v["id"].clone());
let mut job = Resumable::new(store, None, work, Some(key_fn));

while let Some(item) = job.next_item() {
    // process item...
    job.checkpoint(None).unwrap();
}
```

> Keys must be derived only from fields that are stable across runs. If two
> distinct items produce the same key, the second is treated as already done
> once the first is checkpointed.

## API overview

### `Resumable`

| Method | Description |
| --- | --- |
| `Resumable::new(store, initial_state, work_items, item_key)` | Construct a cursor, restoring from the latest checkpoint if one exists. |
| `next_item() -> Option<Value>` | Next unfinished item, or `None` when all are done. |
| `checkpoint(new_state) -> Result<Checkpoint, ResumeError>` | Persist progress after completing the current item. |
| `state() -> &Map<String, Value>` | The carried, free-form state map. |
| `turn() -> u64` | Number of checkpoints written so far. |
| `completed_items() -> &[Value]` | Keys of items already finished. |
| `remaining_items() -> Vec<Value>` | Items not yet completed. |
| `resumed() -> bool` | `true` if this run restored from a prior checkpoint. |

### `Checkpoint`

A single persisted row: `turn`, the `state` map, the set of completed item
keys (`completed_items`), and a `timestamp` (seconds since the Unix epoch).
`to_json()` / `from_json()` serialize it to and from a single JSONL line.

### The `Sink` trait

Storage backends implement two methods:

```rust
pub trait Sink: Send + Sync {
    fn append(&self, checkpoint: &Checkpoint) -> Result<(), ResumeError>;
    fn load_latest(&self) -> Result<Checkpoint, ResumeError>;
}
```

Built-in implementations:

- **`InMemoryStore`** — keeps checkpoints in a `Mutex<Vec<_>>`. Not durable
  across processes; use it for tests and demos.
- **`JsonlStore`** — append-only JSONL file, one checkpoint per line, with
  `fsync`-on-write by default (call `.no_fsync()` to trade durability for
  speed). `load_latest()` returns the last line; corrupt lines surface as
  `ResumeError::CheckpointCorrupt { line_number, .. }`.

### `ItemKeyFn`

```rust
pub type ItemKeyFn = Box<dyn Fn(&Value) -> Value + Send + Sync>;
```

Extracts a stable identity key from a work item. Defaults to the identity
function (the item itself).

### `ResumeError`

The error type returned by store and checkpoint operations:
`NoCheckpoint`, `CheckpointCorrupt { message, line_number }`, `Io`, `Json`, and
`NewlineInPayload`. It implements `std::error::Error` and `From` for
`std::io::Error` and `serde_json::Error`.

## Development

```sh
cargo build
cargo test            # unit + integration + doc tests
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## License

Licensed under the [MIT License](LICENSE).
