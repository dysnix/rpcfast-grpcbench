use super::{grpc, CollectorContext};
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use crate::proto::jetstream::{
    jetstream_client::JetstreamClient, subscribe_update::UpdateOneof, SubscribeRequest,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
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
                        protocol = Protocol::Jetstream.as_str(),
                        "Jetstream authentication failed: {error:#}. Not retrying"
                    );
                    break;
                }
                error!(
                    endpoint = endpoint.alias,
                    protocol = Protocol::Jetstream.as_str(),
                    "Jetstream collector failed: {error:#}. Retrying in {}s",
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
    let mut client = JetstreamClient::with_interceptor(channel, move |request| {
        Ok(grpc::with_x_token(request, token.as_ref()))
    });

    let request = subscribe_request(&ctx.account_include, None);
    let (mut subscribe_tx, subscribe_rx) = unbounded();
    subscribe_tx.send(request).await?;
    let mut stream = client
        .subscribe(subscribe_rx)
        .await
        .context("subscribe to Jetstream transactions")?
        .into_inner();
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        "Jetstream stream connected"
    );

    let endpoint_key = EndpointKey::new(Protocol::Jetstream, endpoint.alias.clone());
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
        let Some(message) = message.context("receive Jetstream message")? else {
            anyhow::bail!("server closed stream");
        };
        match message.update_oneof {
            Some(UpdateOneof::Transaction(update)) => {
                let Some(transaction) = update.transaction else {
                    continue;
                };
                let signature_bytes = if transaction.signature.is_empty() {
                    let Some(signature) = transaction.signatures.first() else {
                        warn!(
                            endpoint = endpoint.alias,
                            "Jetstream transaction had no signature"
                        );
                        continue;
                    };
                    signature.as_slice()
                } else {
                    transaction.signature.as_slice()
                };
                let signature = Signature::try_from(signature_bytes)
                    .context("decode Jetstream signature")?
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
            Some(UpdateOneof::Ping(_)) => {
                subscribe_tx
                    .send(subscribe_request(&ctx.account_include, Some(1)))
                    .await
                    .context("respond to Jetstream ping")?;
            }
            _ => {}
        }
    }
}

fn subscribe_request(accounts: &[String], ping_id: Option<i32>) -> SubscribeRequest {
    SubscribeRequest {
        transactions: if ping_id.is_none() {
            HashMap::from([(
                "bench".to_string(),
                SubscribeRequestFilterTransactions {
                    account_include: accounts.to_vec(),
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                },
            )])
        } else {
            HashMap::new()
        },
        accounts: HashMap::new(),
        ping: ping_id.map(|id| SubscribeRequestPing { id }),
    }
}

#[cfg(test)]
mod tests {
    use super::subscribe_request;

    #[test]
    fn initial_request_forwards_shared_account_filter() {
        let request = subscribe_request(&["account".to_string()], None);
        assert_eq!(
            request.transactions["bench"].account_include,
            vec!["account"]
        );
        assert!(request.ping.is_none());
    }
}
