use crate::config::Protocol;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

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
    /// Monotonic timestamp used for all latency and ordering calculations.
    pub received_at: Instant,
    /// Wall-clock timestamp retained only for human-readable report bounds.
    pub received_at_utc: DateTime<Utc>,
    /// Monotonic timestamp captured when the containing batch was received.
    pub batch_received_at: Option<Instant>,
    /// Zero-based transaction position in the containing stream batch.
    pub batch_position: Option<usize>,
}

impl Timing {
    pub fn now(batch_received_at: Option<Instant>, batch_position: Option<usize>) -> Self {
        Self::at(
            Instant::now(),
            Utc::now(),
            batch_received_at,
            batch_position,
        )
    }

    pub fn at(
        received_at: Instant,
        received_at_utc: DateTime<Utc>,
        batch_received_at: Option<Instant>,
        batch_position: Option<usize>,
    ) -> Self {
        Self {
            received_at,
            received_at_utc,
            batch_received_at,
            batch_position,
        }
    }
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

#[derive(Debug)]
struct Observation {
    signature: String,
    endpoint: EndpointKey,
    timing: Timing,
}

#[derive(Debug, Clone)]
pub struct ObservationStore {
    sender: UnboundedSender<Observation>,
}

pub struct ObservationAggregator {
    receiver: UnboundedReceiver<Observation>,
}

#[derive(Debug, Clone, Copy)]
pub enum RunPhase {
    Warmup,
    Measure,
}

impl ObservationStore {
    pub fn channel() -> (Self, ObservationAggregator) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, ObservationAggregator { receiver })
    }

    /// Enqueue an observation without yielding the collector task.
    pub fn record(
        &self,
        phase: RunPhase,
        signature: String,
        endpoint: EndpointKey,
        timing: Timing,
    ) {
        if matches!(phase, RunPhase::Warmup) {
            return;
        }

        // The receiver lives for the whole benchmark. If it has unexpectedly
        // exited, dropping the observation is preferable to delaying receipt.
        let _ = self.sender.send(Observation {
            signature,
            endpoint,
            timing,
        });
    }
}

impl ObservationAggregator {
    pub async fn run(mut self) -> HashMap<String, TxObservation> {
        let mut observations = HashMap::<String, TxObservation>::new();
        while let Some(observation) = self.receiver.recv().await {
            match observations
                .entry(observation.signature)
                .or_default()
                .endpoints
                .entry(observation.endpoint)
            {
                Entry::Vacant(entry) => {
                    entry.insert(EndpointObservation {
                        first: observation.timing,
                        duplicates: 0,
                    });
                }
                Entry::Occupied(mut entry) => {
                    let endpoint_obs = entry.get_mut();
                    if observation.timing.received_at < endpoint_obs.first.received_at {
                        endpoint_obs.first = observation.timing;
                    } else {
                        endpoint_obs.duplicates += 1;
                    }
                }
            }
        }
        observations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Protocol;

    #[tokio::test]
    async fn first_observation_is_not_counted_as_duplicate() {
        let (store, aggregator) = ObservationStore::channel();
        let endpoint = EndpointKey::new(Protocol::Yellowstone, "ys");

        store.record(
            RunPhase::Measure,
            "sig".to_string(),
            endpoint.clone(),
            Timing::now(None, None),
        );
        drop(store);

        let snapshot = aggregator.run().await;
        assert_eq!(snapshot["sig"].endpoints[&endpoint].duplicates, 0);
    }

    #[tokio::test]
    async fn repeated_observation_increments_duplicate_count() {
        let (store, aggregator) = ObservationStore::channel();
        let endpoint = EndpointKey::new(Protocol::Yellowstone, "ys");

        for _ in 0..2 {
            store.record(
                RunPhase::Measure,
                "sig".to_string(),
                endpoint.clone(),
                Timing::now(None, None),
            );
        }
        drop(store);

        let snapshot = aggregator.run().await;
        assert_eq!(snapshot["sig"].endpoints[&endpoint].duplicates, 1);
    }

    #[tokio::test]
    async fn warmup_observations_are_not_enqueued() {
        let (store, aggregator) = ObservationStore::channel();
        store.record(
            RunPhase::Warmup,
            "sig".to_string(),
            EndpointKey::new(Protocol::Yellowstone, "ys"),
            Timing::now(None, None),
        );
        drop(store);

        assert!(aggregator.run().await.is_empty());
    }

    #[test]
    fn batch_transactions_can_share_one_usable_timestamp() {
        let usable_at = Instant::now();
        let usable_at_utc = Utc::now();
        let batch_received_at = usable_at
            .checked_sub(std::time::Duration::from_micros(10))
            .expect("earlier batch timestamp");

        let first = Timing::at(usable_at, usable_at_utc, Some(batch_received_at), Some(0));
        let later_position =
            Timing::at(usable_at, usable_at_utc, Some(batch_received_at), Some(32));

        assert_eq!(first.received_at, later_position.received_at);
        assert_eq!(first.received_at_utc, later_position.received_at_utc);
        assert_ne!(first.batch_position, later_position.batch_position);
    }
}
