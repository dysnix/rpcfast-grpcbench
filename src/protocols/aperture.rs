use super::CollectorContext;
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use anyhow::{Context, Result};
use aperture_grpc_client::{
    ApertureClientConfig, ApertureGrpcClient, DecodedTransaction, SubscribeFilters, VoteFilter,
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
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        batch_mode = endpoint.batch_mode,
        "Aperture txstream collector started"
    );

    let endpoint_key = EndpointKey::new(Protocol::ApertureTxstream, endpoint.alias.clone());
    if endpoint.batch_mode {
        let stream = client.subscribe_batches_with_reconnect(filters);
        collect_batches(endpoint, ctx, endpoint_key, Box::pin(stream)).await
    } else {
        let stream = client.subscribe_with_reconnect(filters);
        collect_transactions(endpoint, ctx, endpoint_key, Box::pin(stream)).await
    }
}

async fn collect_transactions<S>(
    endpoint: &Endpoint,
    ctx: &CollectorContext,
    endpoint_key: EndpointKey,
    mut stream: std::pin::Pin<Box<S>>,
) -> Result<()>
where
    S: futures::Stream<
            Item = Result<DecodedTransaction, aperture_grpc_client::ApertureGrpcClientError>,
        > + ?Sized,
{
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
        let tx = transaction.context("receive Aperture transaction")?;
        if record_transaction(endpoint, ctx, &endpoint_key, tx, Timing::now(None, None))? {
            last_tx = Instant::now();
        }
    }
}

async fn collect_batches<S>(
    endpoint: &Endpoint,
    ctx: &CollectorContext,
    endpoint_key: EndpointKey,
    mut stream: std::pin::Pin<Box<S>>,
) -> Result<()>
where
    S: futures::Stream<
            Item = Result<
                aperture_grpc_client::DecodedTransactionBatch,
                aperture_grpc_client::ApertureGrpcClientError,
            >,
        > + ?Sized,
{
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

        let batch_received_at = Instant::now();
        let Some(batch) = next else {
            anyhow::bail!("server closed stream");
        };
        let batch = batch.context("receive Aperture transaction batch")?;
        // The gRPC stream yields a fully protobuf-decoded batch. Use one
        // timestamp for every transaction that became usable together.
        let batch_usable_at = Instant::now();
        let batch_usable_at_utc = Utc::now();
        for (batch_position, tx) in batch.transactions.into_iter().enumerate() {
            if record_transaction(
                endpoint,
                ctx,
                &endpoint_key,
                tx,
                Timing::at(
                    batch_usable_at,
                    batch_usable_at_utc,
                    Some(batch_received_at),
                    Some(batch_position),
                ),
            )? {
                last_tx = Instant::now();
            }
        }
    }
}

fn record_transaction(
    endpoint: &Endpoint,
    ctx: &CollectorContext,
    endpoint_key: &EndpointKey,
    tx: DecodedTransaction,
    timing: Timing,
) -> Result<bool> {
    let Some(signature) = primary_signature(&tx)? else {
        warn!(
            endpoint = endpoint.alias,
            slot = tx.slot,
            index = tx.index,
            "Aperture tx had no signatures"
        );
        return Ok(false);
    };

    ctx.store
        .record(ctx.phase(), signature, endpoint_key.clone(), timing);
    Ok(true)
}

fn primary_signature(tx: &DecodedTransaction) -> Result<Option<String>> {
    // A Solana transaction is identified by its primary (first) signature.
    tx.signatures
        .first()
        .map(|signature_bytes| {
            Signature::try_from(signature_bytes.as_slice())
                .context("decode Aperture primary signature")
                .map(|signature| signature.to_string())
        })
        .transpose()
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
    use super::{primary_signature, subscribe_filters};
    use aperture_grpc_client::DecodedTransaction;
    use solana_sdk::signature::Signature;

    #[test]
    fn subscribe_filters_forward_include_simulation() {
        let filters = subscribe_filters(&[], true, true).expect("build filters");
        assert!(filters.signatures_only);
        assert!(filters.include_simulation);
    }

    #[test]
    fn only_primary_signature_is_selected() {
        let primary = Signature::from([1_u8; 64]);
        let secondary = Signature::from([2_u8; 64]);
        let tx = DecodedTransaction {
            signatures: vec![primary.as_ref().to_vec(), secondary.as_ref().to_vec()],
            ..DecodedTransaction::default()
        };

        assert_eq!(
            primary_signature(&tx).expect("decode signature"),
            Some(primary.to_string())
        );
    }
}
