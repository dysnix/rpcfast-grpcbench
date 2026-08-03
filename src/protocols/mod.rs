use crate::config::{Endpoint, Protocol};
use crate::observation::{ObservationStore, RunPhase};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub mod aperture;
pub mod arpc;
mod grpc;
pub mod jetstream;
pub mod shreder_binary;
pub mod shredstream;
pub mod yellowstone;

pub(crate) fn http2_window_size(buffer_size: usize) -> u32 {
    u32::try_from(buffer_size).unwrap_or(u32::MAX)
}

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
            Protocol::ShrederBinary => shreder_binary::run(endpoint, ctx).await,
            Protocol::Arpc => arpc::run(endpoint, ctx).await,
            Protocol::Jetstream => jetstream::run(endpoint, ctx).await,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::http2_window_size;

    #[test]
    fn http2_window_size_saturates_at_protocol_limit() {
        assert_eq!(http2_window_size(4 * 1024 * 1024), 4 * 1024 * 1024);
        assert_eq!(http2_window_size(usize::MAX), u32::MAX);
    }
}
