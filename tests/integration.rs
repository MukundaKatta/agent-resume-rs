//! Integration tests exercising the public API of `agent-resume`.
//!
//! These run against the crate as an external consumer would, so they only
//! touch items re-exported from the crate root.

use agent_resume::{
    Checkpoint, InMemoryStore, ItemKeyFn, JsonlStore, Resumable, ResumeError, Sink,
};
use serde_json::{json, Map, Value};
use std::sync::Arc;

fn items(n: u64) -> Vec<Value> {
    (0..n).map(|i| json!(i)).collect()
}

/// A unique temp path per test so parallel test runs do not collide.
fn temp_path(tag: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("agent_resume_it_{tag}_{pid}_{nanos}.jsonl"))
}

#[test]
fn full_run_completes_every_item_once() {
    let store = Arc::new(InMemoryStore::new());
    let mut r = Resumable::new(store.clone(), None, items(5), None);
    assert!(!r.resumed());

    let mut processed = Vec::new();
    while let Some(item) = r.next_item() {
        processed.push(item.as_u64().unwrap());
        r.checkpoint(None).unwrap();
    }

    assert_eq!(processed, vec![0, 1, 2, 3, 4]);
    assert_eq!(r.turn(), 5);
    // One checkpoint row was written per completed item.
    assert_eq!(store.len(), 5);
}

#[test]
fn resume_after_simulated_crash_skips_done_items() {
    let store = Arc::new(InMemoryStore::new());

    // First "process" handles the first two items, then "crashes".
    {
        let mut r = Resumable::new(store.clone(), None, items(5), None);
        r.next_item();
        r.checkpoint(None).unwrap();
        r.next_item();
        r.checkpoint(None).unwrap();
    }

    // A fresh Resumable over the same store resumes where we left off.
    let mut r2 = Resumable::new(store, None, items(5), None);
    assert!(r2.resumed());
    let remaining: Vec<u64> = std::iter::from_fn(|| r2.next_item())
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(remaining, vec![2, 3, 4]);
}

#[test]
fn state_survives_resume() {
    let store = Arc::new(InMemoryStore::new());
    {
        let mut r = Resumable::new(store.clone(), None, items(3), None);
        r.next_item();
        let mut st = Map::new();
        st.insert("tokens_used".into(), json!(1234));
        r.checkpoint(Some(st)).unwrap();
    }
    let r2 = Resumable::new(store, None, items(3), None);
    assert_eq!(r2.state()["tokens_used"], json!(1234));
}

#[test]
fn custom_item_key_dedupes_on_id() {
    let work = vec![
        json!({"id": "a", "payload": 1}),
        json!({"id": "b", "payload": 2}),
        json!({"id": "c", "payload": 3}),
    ];
    let store = Arc::new(InMemoryStore::new());

    let key_fn: ItemKeyFn = Box::new(|v| v["id"].clone());
    {
        let mut r = Resumable::new(store.clone(), None, work.clone(), Some(key_fn));
        r.next_item();
        r.checkpoint(None).unwrap();
    }

    // Even though the payloads differ, resumption keys off "id".
    let key_fn2: ItemKeyFn = Box::new(|v| v["id"].clone());
    let mut r2 = Resumable::new(store, None, work, Some(key_fn2));
    let ids: Vec<String> = std::iter::from_fn(|| r2.next_item())
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["b", "c"]);
}

#[test]
fn jsonl_store_durably_resumes_across_instances() {
    let path = temp_path("durable");
    let _ = std::fs::remove_file(&path);

    {
        let store = Arc::new(JsonlStore::new(&path).no_fsync());
        let mut r = Resumable::new(store, None, items(4), None);
        r.next_item();
        r.checkpoint(None).unwrap();
    }

    {
        let store = Arc::new(JsonlStore::new(&path).no_fsync());
        let mut r = Resumable::new(store, None, items(4), None);
        assert!(r.resumed());
        let remaining: Vec<u64> = std::iter::from_fn(|| r.next_item())
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(remaining, vec![1, 2, 3]);
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn jsonl_store_len_and_is_empty() {
    let path = temp_path("len");
    let _ = std::fs::remove_file(&path);

    let empty = JsonlStore::new(&path);
    assert_eq!(empty.len().unwrap(), 0);
    assert!(empty.is_empty().unwrap());

    {
        let store = Arc::new(JsonlStore::new(&path).no_fsync());
        let mut r = Resumable::new(store, None, items(3), None);
        while r.next_item().is_some() {
            r.checkpoint(None).unwrap();
        }
    }

    let store = JsonlStore::new(&path);
    assert_eq!(store.len().unwrap(), 3);
    assert!(!store.is_empty().unwrap());

    let _ = std::fs::remove_file(&path);
}

#[test]
fn jsonl_store_reports_no_checkpoint_for_missing_file() {
    let path = temp_path("missing");
    let _ = std::fs::remove_file(&path);
    let store = JsonlStore::new(&path);
    assert!(matches!(
        store.load_latest(),
        Err(ResumeError::NoCheckpoint(_))
    ));
}

#[test]
fn jsonl_store_detects_corrupt_line() {
    let path = temp_path("corrupt");
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, "this is not json\n").unwrap();

    let store = JsonlStore::new(&path);
    match store.load_latest() {
        Err(ResumeError::CheckpointCorrupt { line_number, .. }) => {
            assert_eq!(line_number, Some(1));
        }
        other => panic!("expected CheckpointCorrupt, got {other:?}"),
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn checkpoint_json_roundtrip_via_public_api() {
    let mut state = Map::new();
    state.insert("phase".into(), json!("retrieval"));
    let ckpt = Checkpoint::new(7, state, vec![json!("doc-1"), json!("doc-2")]);

    let line = ckpt.to_json();
    assert!(!line.contains('\n'), "JSONL line must be single-line");

    let restored = Checkpoint::from_json(&line).unwrap();
    assert_eq!(restored.turn, 7);
    assert_eq!(restored.state["phase"], json!("retrieval"));
    assert_eq!(
        restored.completed_items,
        vec![json!("doc-1"), json!("doc-2")]
    );
}

#[test]
fn empty_work_list_has_nothing_to_do() {
    let store = Arc::new(InMemoryStore::new());
    let mut r = Resumable::new(store, None, vec![], None);
    assert!(r.next_item().is_none());
    assert_eq!(r.remaining_items().len(), 0);
}

#[test]
fn remaining_items_reflects_progress() {
    let store = Arc::new(InMemoryStore::new());
    let mut r = Resumable::new(store, None, items(4), None);
    assert_eq!(r.remaining_items(), items(4));
    r.next_item();
    r.checkpoint(None).unwrap();
    assert_eq!(r.remaining_items(), vec![json!(1), json!(2), json!(3)]);
}
