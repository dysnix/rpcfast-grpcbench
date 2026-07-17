use super::CollectorContext;
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

pub async fn run(endpoint: Endpoint, ctx: CollectorContext) {
    let mut retry_delay = Duration::from_secs(2);
    while !ctx.cancel.is_cancelled() {
        match subscribe_once(&endpoint, &ctx).await {
            Ok(()) => retry_delay = Duration::from_secs(2),
            Err(error) => {
                if ctx.cancel.is_cancelled() {
                    break;
                }
                error!(
                    endpoint = endpoint.alias,
                    protocol = Protocol::Yellowstone.as_str(),
                    "Yellowstone collector failed: {error:#}. Retrying in {}s",
                    retry_delay.as_secs()
                );
                tokio::select! {
                    _ = tokio::time::sleep(retry_delay) => {}
                    _ = ctx.cancel.cancelled() => break,
                }
                retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn subscribe_once(endpoint: &Endpoint, ctx: &CollectorContext) -> Result<()> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.url.clone())?
        .tls_config(ClientTlsConfig::new().with_enabled_roots())?
        .buffer_size(ctx.buffer_size)
        .tcp_nodelay(true);

    if !endpoint.token.is_empty() {
        builder = builder.x_token(Some(endpoint.token.clone()))?;
    }

    let mut client = builder
        .connect()
        .await
        .with_context(|| format!("connect {}", endpoint.url))?;

    let mut filter = SubscribeRequestFilterTransactions::default();
    if !ctx.account_include.is_empty() {
        filter.account_include = ctx.account_include.as_ref().clone();
    }

    let subscribe_request = SubscribeRequest {
        transactions: HashMap::from([("bench".to_string(), filter)]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(subscribe_request))
        .await?;
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        "Yellowstone stream connected"
    );

    let endpoint_key = EndpointKey::new(Protocol::Yellowstone, endpoint.alias.clone());
    let mut last_tx = Instant::now();

    loop {
        let next = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(ctx.no_tx_timeout) => {
                if last_tx.elapsed() >= ctx.no_tx_timeout {
                    anyhow::bail!("no transactions for {:?}", ctx.no_tx_timeout);
                }
                continue;
            }
            next = stream.next() => next,
        };

        let Some(update) = next else {
            anyhow::bail!("server closed stream");
        };
        let update = update?;
        match update.update_oneof {
            Some(UpdateOneof::Transaction(tx_update)) => {
                let received_at = Utc::now();
                let Some(tx) = tx_update.transaction else {
                    warn!(
                        endpoint = endpoint.alias,
                        "Yellowstone transaction update had no transaction"
                    );
                    continue;
                };
                let signature = Signature::try_from(tx.signature.as_slice())
                    .context("decode Yellowstone signature")?
                    .to_string();
                last_tx = Instant::now();
                debug!(endpoint = endpoint.alias, signature, "Yellowstone tx");
                ctx.store
                    .record(
                        ctx.phase(),
                        signature,
                        endpoint_key.clone(),
                        Timing {
                            received_at,
                            batch_received_at: None,
                        },
                    )
                    .await;
            }
            Some(UpdateOneof::Ping(_)) => {
                subscribe_tx
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await
                    .context("respond to Yellowstone ping")?;
            }
            _ => {}
        }
    }
}
