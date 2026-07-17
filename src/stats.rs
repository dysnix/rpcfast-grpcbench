use crate::config::Endpoint;
use crate::observation::{EndpointKey, TxObservation};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Serialize)]
pub struct BenchmarkStats {
    pub endpoint_summaries: Vec<EndpointSummary>,
    pub pairwise: Vec<PairwiseSummary>,
    pub total_unique_signatures: usize,
    pub race_eligible_signatures: usize,
    pub full_coverage_signatures: usize,
    pub configured_endpoint_count: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct EndpointSummary {
    pub alias: String,
    pub protocol: String,
    pub protocol_label: String,
    pub unique_signatures: u64,
    pub repeated_signatures: u64,
    pub wins: u64,
    pub full_coverage_wins: u64,
    pub seen_in_races: u64,
    pub coverage_pct: f64,
    pub win_pct: f64,
    pub median_lag_us: Option<i64>,
    pub p95_lag_us: Option<i64>,
    pub max_lag_us: Option<i64>,
    pub decode_latency_median_us: Option<i64>,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct PairwiseSummary {
    pub faster_alias: String,
    pub faster_protocol: String,
    pub slower_alias: String,
    pub slower_protocol: String,
    pub shared_signatures: u64,
    pub wins: u64,
    pub win_pct: f64,
    pub median_lag_us: Option<i64>,
    pub p95_lag_us: Option<i64>,
    pub median_behind_us: Option<i64>,
    pub p95_behind_us: Option<i64>,
}

#[derive(Default)]
struct EndpointAccumulator {
    summary: EndpointSummary,
    lag_us: Vec<i64>,
    decode_latency_us: Vec<i64>,
}

#[derive(Default)]
struct PairAccumulator {
    shared: u64,
    left_wins: u64,
    right_wins: u64,
    left_lead_us: Vec<i64>,
}

pub fn compute_stats(
    endpoints: &[Endpoint],
    observations: &HashMap<String, TxObservation>,
) -> BenchmarkStats {
    let mut endpoint_acc = BTreeMap::<EndpointKey, EndpointAccumulator>::new();
    for endpoint in endpoints {
        let key = EndpointKey::new(endpoint.protocol, endpoint.alias.clone());
        endpoint_acc.insert(
            key,
            EndpointAccumulator {
                summary: EndpointSummary {
                    alias: endpoint.alias.clone(),
                    protocol: endpoint.protocol.as_str().to_string(),
                    protocol_label: endpoint.protocol.label().to_string(),
                    ..EndpointSummary::default()
                },
                ..EndpointAccumulator::default()
            },
        );
    }

    let mut pair_acc = BTreeMap::<(EndpointKey, EndpointKey), PairAccumulator>::new();
    let mut race_eligible = 0usize;
    let mut full_coverage = 0usize;

    for obs in observations.values() {
        for (endpoint, timing) in &obs.endpoints {
            let acc = endpoint_acc.entry(endpoint.clone()).or_default();
            acc.summary.unique_signatures += 1;
            acc.summary.repeated_signatures += timing.duplicates;
            acc.summary.first_seen = min_opt(acc.summary.first_seen, timing.first.received_at);
            acc.summary.last_seen = max_opt(acc.summary.last_seen, timing.first.received_at);

            if let Some(batch_received_at) = timing.first.batch_received_at {
                acc.decode_latency_us.push(duration_us(
                    timing
                        .first
                        .received_at
                        .signed_duration_since(batch_received_at),
                ));
            }
        }

        if obs.endpoints.len() < 2 {
            continue;
        }

        race_eligible += 1;
        if obs.endpoints.len() == endpoints.len() {
            full_coverage += 1;
        }

        let mut sightings: Vec<_> = obs.endpoints.iter().collect();
        sightings.sort_by_key(|(_, timing)| timing.first.received_at);
        let winner_time = sightings[0].1.first.received_at;
        let winner_key = sightings[0].0.clone();

        if let Some(acc) = endpoint_acc.get_mut(&winner_key) {
            acc.summary.wins += 1;
            if obs.endpoints.len() == endpoints.len() {
                acc.summary.full_coverage_wins += 1;
            }
        }

        for (endpoint, timing) in &sightings {
            if let Some(acc) = endpoint_acc.get_mut(endpoint) {
                acc.summary.seen_in_races += 1;
                acc.lag_us.push(duration_us(
                    timing.first.received_at.signed_duration_since(winner_time),
                ));
            }
        }

        for left_idx in 0..sightings.len() {
            for right_idx in (left_idx + 1)..sightings.len() {
                let first = sightings[left_idx];
                let second = sightings[right_idx];
                let (left, right) = if first.0 <= second.0 {
                    (first, second)
                } else {
                    (second, first)
                };
                let left_lead_us = duration_us(
                    right
                        .1
                        .first
                        .received_at
                        .signed_duration_since(left.1.first.received_at),
                );
                let pair = pair_acc
                    .entry((left.0.clone(), right.0.clone()))
                    .or_default();
                pair.shared += 1;
                if left_lead_us > 0 {
                    pair.left_wins += 1;
                } else if left_lead_us < 0 {
                    pair.right_wins += 1;
                }
                pair.left_lead_us.push(left_lead_us);
            }
        }
    }

    let mut endpoint_summaries = Vec::new();
    for (_, mut acc) in endpoint_acc {
        acc.lag_us.sort_unstable();
        acc.decode_latency_us.sort_unstable();
        acc.summary.coverage_pct = pct(acc.summary.unique_signatures, observations.len() as u64);
        acc.summary.win_pct = pct(acc.summary.wins, race_eligible as u64);
        acc.summary.median_lag_us = percentile_sorted(&acc.lag_us, 0.50);
        acc.summary.p95_lag_us = percentile_sorted(&acc.lag_us, 0.95);
        acc.summary.max_lag_us = acc.lag_us.last().copied();
        acc.summary.decode_latency_median_us = percentile_sorted(&acc.decode_latency_us, 0.50);
        endpoint_summaries.push(acc.summary);
    }
    endpoint_summaries.sort_by(|a, b| b.wins.cmp(&a.wins).then_with(|| a.alias.cmp(&b.alias)));

    let mut pairwise = Vec::new();
    for ((left, right), mut acc) in pair_acc {
        let left_is_winner = acc.left_wins >= acc.right_wins;
        let (faster, slower, wins, mut lead_us) = if left_is_winner {
            (left, right, acc.left_wins, acc.left_lead_us)
        } else {
            let right_lead_us = acc.left_lead_us.drain(..).map(|value| -value).collect();
            (right, left, acc.right_wins, right_lead_us)
        };
        let mut behind_us: Vec<_> = lead_us
            .iter()
            .filter(|value| **value < 0)
            .map(|value| -value)
            .collect();
        lead_us.sort_unstable();
        behind_us.sort_unstable();
        pairwise.push(PairwiseSummary {
            faster_alias: faster.alias,
            faster_protocol: faster.protocol,
            slower_alias: slower.alias,
            slower_protocol: slower.protocol,
            shared_signatures: acc.shared,
            wins,
            win_pct: pct(wins, acc.shared),
            median_lag_us: percentile_sorted(&lead_us, 0.50),
            p95_lag_us: percentile_sorted(&lead_us, 0.95),
            median_behind_us: percentile_sorted(&behind_us, 0.50),
            p95_behind_us: percentile_sorted(&behind_us, 0.95),
        });
    }
    pairwise.sort_by(|a, b| {
        b.wins
            .cmp(&a.wins)
            .then_with(|| a.faster_alias.cmp(&b.faster_alias))
            .then_with(|| a.slower_alias.cmp(&b.slower_alias))
    });

    BenchmarkStats {
        endpoint_summaries,
        pairwise,
        total_unique_signatures: observations.len(),
        race_eligible_signatures: race_eligible,
        full_coverage_signatures: full_coverage,
        configured_endpoint_count: endpoints.len(),
    }
}

fn duration_us(duration: chrono::Duration) -> i64 {
    duration.num_microseconds().unwrap_or(i64::MAX)
}

fn pct(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 * 100.0 / total as f64
    }
}

fn percentile_sorted(values: &[i64], percentile: f64) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let idx = ((values.len() - 1) as f64 * percentile).round() as usize;
    values.get(idx).copied()
}

fn min_opt(current: Option<DateTime<Utc>>, next: DateTime<Utc>) -> Option<DateTime<Utc>> {
    Some(current.map_or(next, |current| current.min(next)))
}

fn max_opt(current: Option<DateTime<Utc>>, next: DateTime<Utc>) -> Option<DateTime<Utc>> {
    Some(current.map_or(next, |current| current.max(next)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Protocol;
    use crate::observation::{EndpointObservation, Timing};

    #[test]
    fn shared_signature_race_counts_winner_and_lag() {
        let endpoints = vec![
            endpoint("ys", Protocol::Yellowstone),
            endpoint("ap", Protocol::ApertureTxstream),
        ];
        let t0 = Utc::now();
        let mut observations = HashMap::new();
        observations.insert(
            "sig".to_string(),
            TxObservation {
                endpoints: HashMap::from([
                    (
                        EndpointKey::new(Protocol::Yellowstone, "ys"),
                        EndpointObservation {
                            first: timing(t0),
                            duplicates: 0,
                        },
                    ),
                    (
                        EndpointKey::new(Protocol::ApertureTxstream, "ap"),
                        EndpointObservation {
                            first: timing(t0 + chrono::Duration::microseconds(500)),
                            duplicates: 1,
                        },
                    ),
                ]),
            },
        );

        let stats = compute_stats(&endpoints, &observations);

        assert_eq!(stats.race_eligible_signatures, 1);
        assert_eq!(stats.endpoint_summaries[0].alias, "ys");
        assert_eq!(stats.endpoint_summaries[0].wins, 1);
        assert_eq!(stats.endpoint_summaries[1].repeated_signatures, 1);
        assert_eq!(stats.endpoint_summaries[1].median_lag_us, Some(500));
    }

    #[test]
    fn pairwise_summary_aggregates_unordered_pairs_with_signed_lead() {
        let endpoints = vec![
            endpoint("ys", Protocol::Yellowstone),
            endpoint("ap", Protocol::ApertureTxstream),
        ];
        let t0 = Utc::now();
        let mut observations = HashMap::new();
        observations.insert(
            "sig-ys-wins-1".to_string(),
            tx_observation([
                (Protocol::Yellowstone, "ys", t0),
                (
                    Protocol::ApertureTxstream,
                    "ap",
                    t0 + chrono::Duration::microseconds(500),
                ),
            ]),
        );
        observations.insert(
            "sig-ys-wins-2".to_string(),
            tx_observation([
                (
                    Protocol::Yellowstone,
                    "ys",
                    t0 + chrono::Duration::microseconds(100),
                ),
                (
                    Protocol::ApertureTxstream,
                    "ap",
                    t0 + chrono::Duration::microseconds(300),
                ),
            ]),
        );
        observations.insert(
            "sig-ap-wins".to_string(),
            tx_observation([
                (
                    Protocol::Yellowstone,
                    "ys",
                    t0 + chrono::Duration::microseconds(200),
                ),
                (
                    Protocol::ApertureTxstream,
                    "ap",
                    t0 + chrono::Duration::microseconds(100),
                ),
            ]),
        );

        let stats = compute_stats(&endpoints, &observations);

        assert_eq!(stats.pairwise.len(), 1);
        let pair = &stats.pairwise[0];
        assert_eq!(pair.faster_alias, "ys");
        assert_eq!(pair.slower_alias, "ap");
        assert_eq!(pair.shared_signatures, 3);
        assert_eq!(pair.wins, 2);
        assert_eq!(pair.win_pct, 100.0 * 2.0 / 3.0);
        assert_eq!(pair.median_lag_us, Some(200));
        assert_eq!(pair.p95_lag_us, Some(500));
        assert_eq!(pair.median_behind_us, Some(100));
        assert_eq!(pair.p95_behind_us, Some(100));
    }

    fn endpoint(alias: &str, protocol: Protocol) -> Endpoint {
        Endpoint {
            alias: alias.to_string(),
            protocol,
            url: "http://localhost:1".to_string(),
            token: String::new(),
            signatures_only: true,
            include_simulation: false,
        }
    }

    fn timing(received_at: DateTime<Utc>) -> Timing {
        Timing {
            received_at,
            batch_received_at: None,
        }
    }

    fn tx_observation<const N: usize>(
        entries: [(Protocol, &'static str, DateTime<Utc>); N],
    ) -> TxObservation {
        TxObservation {
            endpoints: entries
                .into_iter()
                .map(|(protocol, alias, received_at)| {
                    (
                        EndpointKey::new(protocol, alias),
                        EndpointObservation {
                            first: timing(received_at),
                            duplicates: 0,
                        },
                    )
                })
                .collect(),
        }
    }
}
