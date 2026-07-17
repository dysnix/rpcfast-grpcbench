use super::CollectorContext;
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use anyhow::{Context, Result};
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, SubscribeFilters, VoteFilter,
};
use chrono::Utc;
use futures::StreamExt;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

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
                    protocol = Protocol::ApertureTxstream.as_str(),
                    "Aperture collector failed: {error:#}. Retrying in {}s",
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
    let client = ApertureGrpcClient::new(client_config(endpoint, ctx.buffer_size));
    let filters = subscribe_filters(
        &ctx.account_include,
        endpoint.signatures_only,
        endpoint.include_simulation,
    )?;
    let mut stream = Box::pin(client.subscribe_with_reconnect(filters));
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        "Aperture txstream collector started"
    );

    let endpoint_key = EndpointKey::new(Protocol::ApertureTxstream, endpoint.alias.clone());
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

        let Some(transaction) = next else {
            anyhow::bail!("server closed stream");
        };
        let received_at = Utc::now();
        let tx = transaction.context("receive Aperture transaction")?;

        if tx.signatures.is_empty() {
            warn!(
                endpoint = endpoint.alias,
                slot = tx.slot,
                index = tx.index,
                "Aperture tx had no signatures"
            );
            continue;
        }

        last_tx = Instant::now();
        for signature_bytes in tx.signatures {
            let signature = Signature::try_from(signature_bytes.as_slice())
                .context("decode Aperture signature")?
                .to_string();
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
    }
}

fn client_config(endpoint: &Endpoint, buffer_size: usize) -> ApertureClientConfig {
    let window_size = u32::try_from(buffer_size).unwrap_or(u32::MAX);
    let mut config = ApertureClientConfig::new(endpoint.url.clone());
    config.max_decoding_message_size = buffer_size;
    config.max_encoding_message_size = buffer_size;
    config.initial_stream_window_size = Some(window_size);
    config.initial_connection_window_size = Some(window_size);
    config.user_agent = Some("rpcfast-grpcbench/aperture-txstream".to_string());
    if !endpoint.token.is_empty() {
        config.x_token = Some(endpoint.token.clone());
    }
    config
}

fn subscribe_filters(
    accounts: &[String],
    signatures_only: bool,
    include_simulation: bool,
) -> Result<SubscribeFilters> {
    let mut filters = SubscribeFilters::default()
        .vote(VoteFilter::All)
        .with_signatures_only(signatures_only)
        .with_include_simulation(include_simulation);
    for account in accounts {
        filters = filters.include_account(
            Pubkey::from_str(account)
                .with_context(|| format!("invalid account pubkey {account}"))?
                .to_bytes(),
        );
    }
    Ok(filters)
}

#[cfg(test)]
mod tests {
    use super::subscribe_filters;

    #[test]
    fn subscribe_filters_forward_include_simulation() {
        let filters = subscribe_filters(&[], true, true).expect("build filters");
        assert!(filters.signatures_only);
        assert!(filters.include_simulation);
    }
}
