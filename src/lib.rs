/*!
agent-resume: checkpoint and resume long-running agent jobs.

Crash on item 47? Resume from item 48 next run.

Items are **at-least-once**: if the process dies before `checkpoint()` is
called, that item will be retried on restart. This is intentional.

```rust
use agent_resume::{InMemoryStore, Resumable};
use serde_json::{json, Map, Value};
use std::sync::Arc;

let store = Arc::new(InMemoryStore::new());
let items: Vec<Value> = (0..5).map(|i| json!(i)).collect();

let mut r = Resumable::new(store.clone(), None, items.clone(), None);
assert!(!r.resumed());

while let Some(item) = r.next_item() {
    // do work with item...
    let mut st = Map::new();
    st.insert("last".into(), item);
    r.checkpoint(Some(st)).unwrap();
}

assert_eq!(r.turn(), 5);

// Simulate restart: new Resumable over same store.
let mut r2 = Resumable::new(store, None, items, None);
assert!(r2.resumed());
assert!(r2.next_item().is_none()); // all done
```
*/

use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// ---- errors ---------------------------------------------------------------

#[derive(Debug)]
pub enum ResumeError {
    NoCheckpoint(String),
    CheckpointCorrupt { message: String, line_number: Option<usize> },
    Io(std::io::Error),
    Json(serde_json::Error),
    NewlineInPayload,
}

impl std::fmt::Display for ResumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResumeError::NoCheckpoint(m) => write!(f, "no checkpoint: {m}"),
            ResumeError::CheckpointCorrupt { message, line_number } => {
                if let Some(n) = line_number {
                    write!(f, "checkpoint corrupt at line {n}: {message}")
                } else {
                    write!(f, "checkpoint corrupt: {message}")
                }
            }
            ResumeError::Io(e) => write!(f, "IO error: {e}"),
            ResumeError::Json(e) => write!(f, "JSON error: {e}"),
            ResumeError::NewlineInPayload => write!(f, "checkpoint payload contains a newline"),
        }
    }
}

impl std::error::Error for ResumeError {}

impl From<std::io::Error> for ResumeError {
    fn from(e: std::io::Error) -> Self {
        ResumeError::Io(e)
    }
}
impl From<serde_json::Error> for ResumeError {
    fn from(e: serde_json::Error) -> Self {
        ResumeError::Json(e)
    }
}

// ---- Checkpoint -----------------------------------------------------------

/// One row in a checkpoint store.
#[derive(Debug, Clone, PartialEq)]
pub struct Checkpoint {
    pub turn: u64,
    pub state: Map<String, Value>,
    pub completed_items: Vec<Value>,
    pub timestamp: f64,
}

impl Checkpoint {
    pub fn new(turn: u64, state: Map<String, Value>, completed_items: Vec<Value>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        Self { turn, state, completed_items, timestamp }
    }

    /// Serialize to a single JSONL line (no embedded newlines).
    pub fn to_json(&self) -> String {
        let v = serde_json::json!({
            "turn": self.turn,
            "state": self.state,
            "completed_items": self.completed_items,
            "timestamp": self.timestamp,
        });
        serde_json::to_string(&v).unwrap_or_else(|_| "{}".into())
    }

    /// Parse a single JSONL line back into a Checkpoint.
    pub fn from_json(raw: &str) -> Result<Self, ResumeError> {
        let v: Value = serde_json::from_str(raw)?;
        let turn = v["turn"].as_u64().ok_or_else(|| ResumeError::Json(
            serde_json::from_str::<Value>("null").unwrap_err()
        ))?;
        let state = v["state"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        let completed_items = v["completed_items"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let timestamp = v["timestamp"].as_f64().unwrap_or(0.0);
        Ok(Checkpoint { turn, state, completed_items, timestamp })
    }
}

// ---- Sink trait -----------------------------------------------------------

/// Minimal store contract. Implement `append` and `load_latest`.
pub trait Sink: Send + Sync {
    fn append(&self, checkpoint: &Checkpoint) -> Result<(), ResumeError>;
    fn load_latest(&self) -> Result<Checkpoint, ResumeError>;
}

// ---- InMemoryStore --------------------------------------------------------

/// In-memory store. Not durable across processes; use for tests and demos.
#[derive(Default)]
pub struct InMemoryStore {
    rows: Mutex<Vec<Checkpoint>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// All checkpoints written so far (test helper).
    pub fn all(&self) -> Vec<Checkpoint> {
        self.rows.lock().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.rows.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.lock().unwrap().is_empty()
    }
}

impl Sink for InMemoryStore {
    fn append(&self, checkpoint: &Checkpoint) -> Result<(), ResumeError> {
        self.rows.lock().unwrap().push(checkpoint.clone());
        Ok(())
    }

    fn load_latest(&self) -> Result<Checkpoint, ResumeError> {
        let rows = self.rows.lock().unwrap();
        rows.last().cloned().ok_or_else(|| ResumeError::NoCheckpoint("no checkpoints in memory".into()))
    }
}

// ---- JsonlStore -----------------------------------------------------------

/// Append-only JSONL file. One Checkpoint per line. fsync-on-write by default.
pub struct JsonlStore {
    pub path: PathBuf,
    fsync: bool,
    lock: Mutex<()>,
}

impl JsonlStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path: PathBuf = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        Self { path, fsync: true, lock: Mutex::new(()) }
    }

    pub fn no_fsync(mut self) -> Self {
        self.fsync = false;
        self
    }

    /// Stream all checkpoints in write order.
    pub fn iter_checkpoints(&self) -> Result<Vec<Checkpoint>, ResumeError> {
        if !self.path.exists() {
            return Ok(vec![]);
        }
        let f = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(f);
        let mut out = Vec::new();
        for (lineno, raw) in reader.lines().enumerate() {
            let line = raw?;
            if line.trim().is_empty() {
                continue;
            }
            let ckpt = Checkpoint::from_json(&line).map_err(|e| {
                ResumeError::CheckpointCorrupt {
                    message: e.to_string(),
                    line_number: Some(lineno + 1),
                }
            })?;
            out.push(ckpt);
        }
        Ok(out)
    }

    pub fn len(&self) -> Result<usize, ResumeError> {
        Ok(self.iter_checkpoints()?.len())
    }
}

impl Sink for JsonlStore {
    fn append(&self, checkpoint: &Checkpoint) -> Result<(), ResumeError> {
        let line = checkpoint.to_json();
        if line.contains('\n') {
            return Err(ResumeError::NewlineInPayload);
        }
        let _guard = self.lock.lock().unwrap();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()?;
        if self.fsync {
            let _ = f.sync_data(); // ignore fsync failures (tmpfs, CI)
        }
        Ok(())
    }

    fn load_latest(&self) -> Result<Checkpoint, ResumeError> {
        if !self.path.exists()
            || self.path.metadata().map(|m| m.len() == 0).unwrap_or(true)
        {
            return Err(ResumeError::NoCheckpoint(format!(
                "{} is empty or missing",
                self.path.display()
            )));
        }
        let ckpts = self.iter_checkpoints()?;
        ckpts.into_iter().last().ok_or_else(|| {
            ResumeError::NoCheckpoint(format!("{} contains no checkpoints", self.path.display()))
        })
    }
}

// ---- Resumable ------------------------------------------------------------

/// Checkpoint-aware cursor over a list of work items.
///
/// Call `next_item()` to get the next unfinished item, then call
/// `checkpoint(new_state)` to persist progress before moving on.
pub struct Resumable {
    store: Arc<dyn Sink>,
    item_key: Box<dyn Fn(&Value) -> Value + Send + Sync>,
    work_items: Vec<Value>,
    state: Map<String, Value>,
    turn: u64,
    completed: Vec<Value>,
    resumed: bool,
    current_idx: usize,
}

impl Resumable {
    /// Create a new `Resumable`.
    ///
    /// `item_key`: optional key extractor. Defaults to identity (the item itself).
    pub fn new(
        store: Arc<dyn Sink>,
        initial_state: Option<Map<String, Value>>,
        work_items: Vec<Value>,
        item_key: Option<Box<dyn Fn(&Value) -> Value + Send + Sync>>,
    ) -> Self {
        let key_fn: Box<dyn Fn(&Value) -> Value + Send + Sync> = item_key
            .unwrap_or_else(|| Box::new(|v: &Value| v.clone()));

        let (state, turn, completed, resumed) = match store.load_latest() {
            Ok(ckpt) => (ckpt.state, ckpt.turn, ckpt.completed_items, true),
            Err(_) => (initial_state.unwrap_or_default(), 0, vec![], false),
        };

        Self {
            store,
            item_key: key_fn,
            work_items,
            state,
            turn,
            completed,
            resumed,
            current_idx: 0,
        }
    }

    // ---- state accessors ------------------------------------------------

    pub fn state(&self) -> &Map<String, Value> {
        &self.state
    }

    pub fn turn(&self) -> u64 {
        self.turn
    }

    pub fn completed_items(&self) -> &[Value] {
        &self.completed
    }

    pub fn resumed(&self) -> bool {
        self.resumed
    }

    pub fn remaining_items(&self) -> Vec<Value> {
        let done: HashSet<String> = self
            .completed
            .iter()
            .map(|v| v.to_string())
            .collect();
        self.work_items
            .iter()
            .filter(|item| !done.contains(&(self.item_key)(item).to_string()))
            .cloned()
            .collect()
    }

    // ---- iteration ------------------------------------------------------

    /// Returns the next unfinished work item, or `None` if all are done.
    pub fn next_item(&mut self) -> Option<Value> {
        let done: HashSet<String> = self
            .completed
            .iter()
            .map(|v| v.to_string())
            .collect();
        while self.current_idx < self.work_items.len() {
            let item = &self.work_items[self.current_idx];
            let key = (self.item_key)(item);
            self.current_idx += 1;
            if !done.contains(&key.to_string()) {
                return Some(item.clone());
            }
        }
        None
    }

    // ---- checkpoint -----------------------------------------------------

    /// Persist progress. Call after completing each item.
    ///
    /// If `new_state` is `Some`, replaces the in-memory state. The item
    /// most recently returned by `next_item()` is marked complete.
    pub fn checkpoint(
        &mut self,
        new_state: Option<Map<String, Value>>,
    ) -> Result<Checkpoint, ResumeError> {
        if let Some(s) = new_state {
            self.state = s;
        }
        // Mark the item at current_idx - 1 as complete (the last yielded one).
        if self.current_idx > 0 {
            let idx = self.current_idx - 1;
            if idx < self.work_items.len() {
                let key = (self.item_key)(&self.work_items[idx]);
                let key_str = key.to_string();
                if !self.completed.iter().any(|c| c.to_string() == key_str) {
                    self.completed.push(key);
                }
            }
        }
        self.turn += 1;
        let ckpt = Checkpoint::new(
            self.turn,
            self.state.clone(),
            self.completed.clone(),
        );
        self.store.append(&ckpt)?;
        Ok(ckpt)
    }
}

// ---- tests ----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn items(n: u64) -> Vec<Value> {
        (0..n).map(|i| json!(i)).collect()
    }

    fn mem_store() -> Arc<InMemoryStore> {
        Arc::new(InMemoryStore::new())
    }

    #[test]
    fn checkpoint_to_from_json() {
        let mut m = Map::new();
        m.insert("x".into(), json!(1));
        let ckpt = Checkpoint { turn: 3, state: m, completed_items: vec![json!(0)], timestamp: 42.0 };
        let line = ckpt.to_json();
        let restored = Checkpoint::from_json(&line).unwrap();
        assert_eq!(restored.turn, 3);
        assert_eq!(restored.state["x"], json!(1));
        assert_eq!(restored.completed_items, vec![json!(0)]);
    }

    #[test]
    fn in_memory_store_basic() {
        let store = mem_store();
        assert!(store.load_latest().is_err());
        let ckpt = Checkpoint::new(1, Map::new(), vec![]);
        store.append(&ckpt).unwrap();
        assert_eq!(store.load_latest().unwrap().turn, 1);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn fresh_run_not_resumed() {
        let store = mem_store();
        let r = Resumable::new(store, None, items(3), None);
        assert!(!r.resumed());
        assert_eq!(r.turn(), 0);
    }

    #[test]
    fn iterates_all_items() {
        let store = mem_store();
        let mut r = Resumable::new(store.clone(), None, items(3), None);
        let mut seen = vec![];
        while let Some(item) = r.next_item() {
            seen.push(item.as_u64().unwrap());
            r.checkpoint(None).unwrap();
        }
        assert_eq!(seen, vec![0, 1, 2]);
        assert_eq!(r.turn(), 3);
    }

    #[test]
    fn resume_skips_completed() {
        let store = mem_store();
        {
            let mut r = Resumable::new(store.clone(), None, items(5), None);
            for _ in 0..3 {
                r.next_item().unwrap();
                r.checkpoint(None).unwrap();
            }
        }
        let mut r2 = Resumable::new(store, None, items(5), None);
        assert!(r2.resumed());
        let remaining: Vec<u64> = std::iter::from_fn(|| r2.next_item())
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(remaining, vec![3, 4]);
    }

    #[test]
    fn state_preserved_across_resume() {
        let store = mem_store();
        {
            let mut r = Resumable::new(store.clone(), None, items(3), None);
            r.next_item();
            let mut st = Map::new();
            st.insert("count".into(), json!(42));
            r.checkpoint(Some(st)).unwrap();
        }
        let r2 = Resumable::new(store, None, items(3), None);
        assert_eq!(r2.state()["count"], json!(42));
    }

    #[test]
    fn remaining_items() {
        let store = mem_store();
        let mut r = Resumable::new(store, None, items(4), None);
        r.next_item();
        r.checkpoint(None).unwrap();
        let rem = r.remaining_items();
        assert_eq!(rem, vec![json!(1), json!(2), json!(3)]);
    }

    #[test]
    fn turn_increments() {
        let store = mem_store();
        let mut r = Resumable::new(store, None, items(3), None);
        for _ in 0..3 {
            r.next_item();
            r.checkpoint(None).unwrap();
        }
        assert_eq!(r.turn(), 3);
    }

    #[test]
    fn no_items_nothing_to_do() {
        let store = mem_store();
        let mut r = Resumable::new(store, None, vec![], None);
        assert!(r.next_item().is_none());
    }

    #[test]
    fn all_items_already_done_resume() {
        let store = mem_store();
        {
            let mut r = Resumable::new(store.clone(), None, items(2), None);
            while let Some(_) = r.next_item() {
                r.checkpoint(None).unwrap();
            }
        }
        let mut r2 = Resumable::new(store, None, items(2), None);
        assert!(r2.next_item().is_none());
    }

    #[test]
    fn custom_item_key() {
        let store = mem_store();
        let work_items = vec![
            json!({"id": 1, "data": "a"}),
            json!({"id": 2, "data": "b"}),
            json!({"id": 3, "data": "c"}),
        ];
        let key_fn: Box<dyn Fn(&Value) -> Value + Send + Sync> =
            Box::new(|v| v["id"].clone());
        {
            let mut r = Resumable::new(store.clone(), None, work_items.clone(), Some(key_fn));
            r.next_item();
            r.checkpoint(None).unwrap();
        }
        let key_fn2: Box<dyn Fn(&Value) -> Value + Send + Sync> =
            Box::new(|v| v["id"].clone());
        let mut r2 = Resumable::new(store, None, work_items, Some(key_fn2));
        let ids: Vec<u64> = std::iter::from_fn(|| r2.next_item())
            .map(|v| v["id"].as_u64().unwrap())
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn jsonl_store_roundtrip() {
        let path = std::env::temp_dir().join("agent_resume_test.jsonl");
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(JsonlStore::new(&path).no_fsync());
        {
            let mut r = Resumable::new(store.clone(), None, items(4), None);
            for _ in 0..2 {
                r.next_item();
                r.checkpoint(None).unwrap();
            }
        }
        {
            let mut r2 = Resumable::new(store, None, items(4), None);
            assert!(r2.resumed());
            let remaining: Vec<u64> = std::iter::from_fn(|| r2.next_item())
                .map(|v| v.as_u64().unwrap())
                .collect();
            assert_eq!(remaining, vec![2, 3]);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_store_len() {
        let path = std::env::temp_dir().join("agent_resume_len.jsonl");
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(JsonlStore::new(&path).no_fsync());
        let mut r = Resumable::new(store, None, items(3), None);
        while let Some(_) = r.next_item() {
            r.checkpoint(None).unwrap();
        }
        let js = JsonlStore::new(&path);
        assert_eq!(js.len().unwrap(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn jsonl_store_missing_file() {
        let store = JsonlStore::new("/tmp/no_such_agent_resume.jsonl");
        assert!(matches!(store.load_latest(), Err(ResumeError::NoCheckpoint(_))));
    }

    #[test]
    fn completed_items_accessible() {
        let store = mem_store();
        let mut r = Resumable::new(store, None, items(3), None);
        r.next_item();
        r.checkpoint(None).unwrap();
        assert_eq!(r.completed_items().len(), 1);
    }

    #[test]
    fn checkpoint_from_json_bad() {
        assert!(matches!(
            Checkpoint::from_json("not json"),
            Err(ResumeError::Json(_))
        ));
    }
}
