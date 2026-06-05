use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use keratin_log::{KDurability, Keratin, KeratinConfig, Message};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, watch};

use crate::{Result, StromaError};

fn io_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Io(e.to_string())
}

fn decode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Decode(e.to_string())
}

fn encode_err(e: impl std::fmt::Display) -> StromaError {
    StromaError::Encode(e.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalKey {
    pub namespace: String,
    pub key: String,
}

impl GlobalKey {
    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let key = Self {
            namespace: namespace.into(),
            key: key.into(),
        };
        key.validate()?;
        Ok(key)
    }

    fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(StromaError::InvalidArgument(
                "global key namespace must not be empty".into(),
            ));
        }
        if self.key.trim().is_empty() {
            return Err(StromaError::InvalidArgument(
                "global key must not be empty".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalValue {
    pub version: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    Stored { version: u64 },
    Conflict { current: Option<GlobalValue> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalRecord {
    key: GlobalKey,
    version: u64,
    bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct GlobalStore {
    log: Arc<Keratin>,
    put_lock: Mutex<()>,
    values: RwLock<HashMap<GlobalKey, GlobalValue>>,
    watchers: RwLock<HashMap<GlobalKey, watch::Sender<Option<GlobalValue>>>>,
    events_replayed: AtomicU64,
}

impl GlobalStore {
    pub async fn open(root: impl AsRef<Path>, cfg: KeratinConfig) -> Result<Self> {
        let log = Arc::new(Keratin::open(root, cfg).await.map_err(io_err)?);
        let mut values = HashMap::new();
        let mut events_replayed = 0_u64;

        let reader = log.reader();
        let mut next_offset = 0;
        loop {
            let records = reader.scan_from(next_offset, 1024).map_err(io_err)?;
            if records.is_empty() {
                break;
            }

            for record in records {
                next_offset = record.offset + 1;
                let event = GlobalRecord::decode(&record.payload)?;
                event.key.validate()?;
                let current = values
                    .get(&event.key)
                    .map(|value: &GlobalValue| value.version);
                if let Some(current) = current
                    && event.version <= current
                {
                    return Err(StromaError::Corruption(format!(
                        "global key {}/{} version regressed from {} to {}",
                        event.key.namespace, event.key.key, current, event.version
                    )));
                }
                values.insert(
                    event.key,
                    GlobalValue {
                        version: event.version,
                        bytes: event.bytes,
                    },
                );
                events_replayed += 1;
            }
        }

        Ok(Self {
            log,
            put_lock: Mutex::new(()),
            values: RwLock::new(values),
            watchers: RwLock::new(HashMap::new()),
            events_replayed: AtomicU64::new(events_replayed),
        })
    }

    pub async fn get(&self, key: &GlobalKey) -> Result<Option<GlobalValue>> {
        key.validate()?;
        Ok(self.values.read().await.get(key).cloned())
    }

    /// Stores a value if `expected_version` matches the current version.
    ///
    /// Missing keys have version `0`. Passing `None` makes the put
    /// unconditional.
    pub async fn put(
        &self,
        key: GlobalKey,
        bytes: Vec<u8>,
        expected_version: Option<u64>,
    ) -> Result<PutOutcome> {
        key.validate()?;

        let _put_guard = self.put_lock.lock().await;
        let current = self.values.read().await.get(&key).cloned();
        let current_version = current.as_ref().map_or(0, |value| value.version);

        if let Some(expected) = expected_version
            && expected != current_version
        {
            return Ok(PutOutcome::Conflict { current });
        }

        let version = current_version
            .checked_add(1)
            .ok_or_else(|| StromaError::Internal("global key version overflow".into()))?;
        let record = GlobalRecord {
            key: key.clone(),
            version,
            bytes,
        };
        let payload = record.encode()?;

        self.log
            .append(
                Message {
                    flags: 0,
                    headers: vec![],
                    payload,
                },
                Some(KDurability::AfterFsync),
            )
            .await
            .map_err(io_err)?;

        let value = GlobalValue {
            version,
            bytes: record.bytes,
        };
        let watchers = {
            let mut values = self.values.write().await;
            let current_after = values.get(&key).map_or(0, |value| value.version);
            if current_after != current_version {
                return Err(StromaError::Internal(format!(
                    "global key {}/{} changed during serialized put",
                    key.namespace, key.key
                )));
            }
            values.insert(key.clone(), value.clone());
            self.watchers.read().await.get(&key).cloned()
        };

        if let Some(sender) = watchers {
            let _ = sender.send(Some(value.clone()));
        }

        Ok(PutOutcome::Stored {
            version: value.version,
        })
    }

    pub async fn put_unconditional(&self, key: GlobalKey, bytes: Vec<u8>) -> Result<u64> {
        match self.put(key, bytes, None).await? {
            PutOutcome::Stored { version } => Ok(version),
            PutOutcome::Conflict { .. } => Err(StromaError::Internal(
                "unconditional global put returned conflict".into(),
            )),
        }
    }

    pub async fn watch(&self, key: GlobalKey) -> Result<watch::Receiver<Option<GlobalValue>>> {
        key.validate()?;

        if let Some(sender) = self.watchers.read().await.get(&key).cloned() {
            return Ok(sender.subscribe());
        }

        let _put_guard = self.put_lock.lock().await;
        if let Some(sender) = self.watchers.read().await.get(&key).cloned() {
            return Ok(sender.subscribe());
        }

        let current = self.values.read().await.get(&key).cloned();
        let mut watchers = self.watchers.write().await;
        let sender = watchers
            .entry(key)
            .or_insert_with(|| watch::channel(current).0);
        Ok(sender.subscribe())
    }

    pub fn events_replayed(&self) -> u64 {
        self.events_replayed.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.log.shutdown().await.map_err(io_err)
    }
}

impl GlobalRecord {
    fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(encode_err)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(decode_err)
    }
}

#[cfg(test)]
mod tests {
    use keratin_log::{KeratinConfig, test_dir};

    use super::*;

    #[tokio::test]
    async fn put_get_and_recover_global_value() {
        let dir = test_dir!("global_store_recover");
        let key = GlobalKey::new("fibril.runtime", "idle_queue_cleanup").unwrap();

        let store = GlobalStore::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();

        let outcome = store
            .put(key.clone(), b"one".to_vec(), Some(0))
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored { version: 1 });
        let outcome = store
            .put(key.clone(), b"two".to_vec(), Some(1))
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored { version: 2 });
        assert_eq!(
            store.get(&key).await.unwrap(),
            Some(GlobalValue {
                version: 2,
                bytes: b"two".to_vec(),
            })
        );
        store.shutdown().await.unwrap();
        drop(store);

        let recovered = GlobalStore::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        assert_eq!(recovered.events_replayed(), 2);
        assert_eq!(
            recovered.get(&key).await.unwrap(),
            Some(GlobalValue {
                version: 2,
                bytes: b"two".to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn compare_and_swap_reports_conflict() {
        let dir = test_dir!("global_store_cas");
        let store = GlobalStore::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        let key = GlobalKey::new("n", "k").unwrap();

        assert_eq!(
            store
                .put(key.clone(), b"one".to_vec(), Some(0))
                .await
                .unwrap(),
            PutOutcome::Stored { version: 1 }
        );

        assert_eq!(
            store
                .put(key.clone(), b"stale".to_vec(), Some(0))
                .await
                .unwrap(),
            PutOutcome::Conflict {
                current: Some(GlobalValue {
                    version: 1,
                    bytes: b"one".to_vec(),
                }),
            }
        );
        assert_eq!(
            store.get(&key).await.unwrap(),
            Some(GlobalValue {
                version: 1,
                bytes: b"one".to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn watch_receives_committed_values() {
        let dir = test_dir!("global_store_watch");
        let store = GlobalStore::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        let key = GlobalKey::new("n", "k").unwrap();
        let mut rx = store.watch(key.clone()).await.unwrap();
        assert_eq!(*rx.borrow(), None);

        store.put(key, b"value".to_vec(), Some(0)).await.unwrap();
        rx.changed().await.unwrap();

        assert_eq!(
            *rx.borrow(),
            Some(GlobalValue {
                version: 1,
                bytes: b"value".to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn watch_after_put_starts_at_current_value() {
        let dir = test_dir!("global_store_watch_current");
        let store = GlobalStore::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        let key = GlobalKey::new("n", "k").unwrap();

        store
            .put(key.clone(), b"value".to_vec(), Some(0))
            .await
            .unwrap();

        let rx = store.watch(key).await.unwrap();
        assert_eq!(
            *rx.borrow(),
            Some(GlobalValue {
                version: 1,
                bytes: b"value".to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn stroma_exposes_and_recovers_global_store() {
        let dir = test_dir!("stroma_global_store_recover");
        let key = GlobalKey::new("fibril.runtime", "settings").unwrap();

        let stroma = crate::Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            crate::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        stroma
            .global_store()
            .await
            .unwrap()
            .put(key.clone(), b"settings-v1".to_vec(), Some(0))
            .await
            .unwrap();
        stroma.shutdown().await.unwrap();
        drop(stroma);

        let recovered = crate::Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            crate::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            recovered
                .global_store()
                .await
                .unwrap()
                .get(&key)
                .await
                .unwrap(),
            Some(GlobalValue {
                version: 1,
                bytes: b"settings-v1".to_vec(),
            })
        );
    }
}
