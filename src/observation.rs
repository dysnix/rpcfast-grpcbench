use crate::config::Protocol;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
pub struct EndpointKey {
    pub protocol: String,
    pub alias: String,
}

impl EndpointKey {
    pub fn new(protocol: Protocol, alias: impl Into<String>) -> Self {
        Self {
            protocol: protocol.as_str().to_string(),
            alias: alias.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Timing {
    pub received_at: DateTime<Utc>,
    pub batch_received_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct EndpointObservation {
    pub first: Timing,
    pub duplicates: u64,
}

#[derive(Debug, Default, Clone)]
pub struct TxObservation {
    pub endpoints: HashMap<EndpointKey, EndpointObservation>,
}

#[derive(Debug, Clone)]
pub struct ObservationStore {
    inner: Arc<Mutex<HashMap<String, TxObservation>>>,
}

#[derive(Debug, Clone, Copy)]
pub enum RunPhase {
    Warmup,
    Measure,
}

impl ObservationStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn record(
        &self,
        phase: RunPhase,
        signature: String,
        endpoint: EndpointKey,
        timing: Timing,
    ) {
        if matches!(phase, RunPhase::Warmup) {
            return;
        }

        let mut guard = self.inner.lock().await;
        match guard
            .entry(signature)
            .or_default()
            .endpoints
            .entry(endpoint)
        {
            Entry::Vacant(entry) => {
                entry.insert(EndpointObservation {
                    first: timing,
                    duplicates: 0,
                });
            }
            Entry::Occupied(mut entry) => {
                let endpoint_obs = entry.get_mut();
                if timing.received_at < endpoint_obs.first.received_at {
                    endpoint_obs.first = timing;
                } else {
                    endpoint_obs.duplicates += 1;
                }
            }
        }
    }

    pub async fn snapshot(&self) -> HashMap<String, TxObservation> {
        self.inner.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Protocol;

    #[tokio::test]
    async fn first_observation_is_not_counted_as_duplicate() {
        let store = ObservationStore::new();
        let endpoint = EndpointKey::new(Protocol::Yellowstone, "ys");
        let received_at = Utc::now();

        store
            .record(
                RunPhase::Measure,
                "sig".to_string(),
                endpoint.clone(),
                Timing {
                    received_at,
                    batch_received_at: None,
                },
            )
            .await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot["sig"].endpoints[&endpoint].duplicates, 0);
    }

    #[tokio::test]
    async fn repeated_observation_increments_duplicate_count() {
        let store = ObservationStore::new();
        let endpoint = EndpointKey::new(Protocol::Yellowstone, "ys");
        let received_at = Utc::now();

        for offset in [0, 1] {
            store
                .record(
                    RunPhase::Measure,
                    "sig".to_string(),
                    endpoint.clone(),
                    Timing {
                        received_at: received_at + chrono::Duration::microseconds(offset),
                        batch_received_at: None,
                    },
                )
                .await;
        }

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot["sig"].endpoints[&endpoint].duplicates, 1);
    }
}
