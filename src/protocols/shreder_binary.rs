use super::{grpc, CollectorContext};
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use crate::proto::shreder_binary::{
    shreder_binary_service_client::ShrederBinaryServiceClient, SubscribeBinaryTransactionsRequest,
    SubscribeRequestFilterBinaryTransactions,
};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{channel::mpsc::unbounded, SinkExt};
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

pub async fn run(endpoint: Endpoint, ctx: CollectorContext) {
    let mut retry_delay = Duration::from_secs(2);
    while !ctx.cancel.is_cancelled() {
        let result = tokio::select! {
            _ = ctx.cancel.cancelled() => break,
            result = subscribe_once(&endpoint, &ctx) => result,
        };
        match result {
            Ok(()) => retry_delay = Duration::from_secs(2),
            Err(error) => {
                if ctx.cancel.is_cancelled() {
                    break;
                }
                if grpc::is_auth_error(&error) {
                    error!(
                        endpoint = endpoint.alias,
                        protocol = Protocol::ShrederBinary.as_str(),
                        "Shreder Binary authentication failed: {error:#}. Not retrying"
                    );
                    break;
                }
                error!(
                    endpoint = endpoint.alias,
                    protocol = Protocol::ShrederBinary.as_str(),
                    "Shreder Binary collector failed: {error:#}. Retrying in {}s",
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
    let channel = grpc::build_channel(&endpoint.url, ctx.buffer_size).await?;
    let token = grpc::parse_x_token(&endpoint.token)?;
    let mut client = ShrederBinaryServiceClient::with_interceptor(channel, move |request| {
        Ok(grpc::with_x_token(request, token.as_ref()))
    });

    let request = SubscribeBinaryTransactionsRequest {
        transactions: HashMap::from([(
            "bench".to_string(),
            SubscribeRequestFilterBinaryTransactions {
                account_include: ctx.account_include.as_ref().clone(),
                account_exclude: Vec::new(),
                account_required: Vec::new(),
            },
        )]),
    };
    let (mut subscribe_tx, subscribe_rx) = unbounded();
    subscribe_tx.send(request).await?;
    let mut stream = client
        .subscribe_binary_transactions(subscribe_rx)
        .await
        .context("subscribe to Shreder Binary transactions")?
        .into_inner();
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        "Shreder Binary stream connected"
    );

    let endpoint_key = EndpointKey::new(Protocol::ShrederBinary, endpoint.alias.clone());
    let mut last_tx = Instant::now();

    loop {
        let message = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(ctx.no_tx_timeout) => {
                if last_tx.elapsed() >= ctx.no_tx_timeout {
                    anyhow::bail!("no transactions for {:?}", ctx.no_tx_timeout);
                }
                continue;
            }
            message = stream.message() => message,
        };
        let received_at = Utc::now();
        let Some(message) = message.context("receive Shreder Binary message")? else {
            anyhow::bail!("server closed stream");
        };
        let Some(transaction) = message.transaction.and_then(|update| update.transaction) else {
            continue;
        };
        let Some(signature_bytes) = transaction.signatures.first() else {
            warn!(
                endpoint = endpoint.alias,
                "Shreder Binary transaction had no signatures"
            );
            continue;
        };
        let signature = Signature::try_from(signature_bytes.as_slice())
            .context("decode Shreder Binary signature")?
            .to_string();
        last_tx = Instant::now();
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
