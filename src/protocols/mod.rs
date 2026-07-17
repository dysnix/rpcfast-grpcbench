use crate::config::{Endpoint, Protocol};
use crate::observation::{ObservationStore, RunPhase};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub mod aperture;
pub mod shredstream;
pub mod yellowstone;

#[derive(Clone)]
pub struct CollectorContext {
    pub store: ObservationStore,
    pub measuring: Arc<AtomicBool>,
    pub cancel: CancellationToken,
    pub buffer_size: usize,
    pub no_tx_timeout: Duration,
    pub account_include: Arc<Vec<String>>,
}

impl CollectorContext {
    pub fn phase(&self) -> RunPhase {
        if self.measuring.load(Ordering::Relaxed) {
            RunPhase::Measure
        } else {
            RunPhase::Warmup
        }
    }
}

pub fn spawn_endpoint(endpoint: Endpoint, ctx: CollectorContext) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match endpoint.protocol {
            Protocol::Yellowstone => yellowstone::run(endpoint, ctx).await,
            Protocol::JitoShredstream => shredstream::run(endpoint, ctx).await,
            Protocol::ApertureTxstream => aperture::run(endpoint, ctx).await,
        }
    })
}
