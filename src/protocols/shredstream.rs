use super::CollectorContext;
use crate::config::{Endpoint, Protocol};
use crate::observation::{EndpointKey, Timing};
use crate::proto::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};
use anyhow::{Context, Result};
use chrono::Utc;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::{Channel, ClientTlsConfig},
    Request,
};
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
                    protocol = Protocol::JitoShredstream.as_str(),
                    "ShredStream collector failed: {error:#}. Retrying in {}s",
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
    let channel = build_channel(&endpoint.url, ctx.buffer_size).await?;
    let token = endpoint.token.clone();
    let mut client =
        ShredstreamProxyClient::with_interceptor(channel, move |mut req: Request<()>| {
            if !token.is_empty() {
                let value = token.parse::<MetadataValue<Ascii>>().map_err(|err| {
                    tonic::Status::invalid_argument(format!("invalid x-token: {err}"))
                })?;
                req.metadata_mut().insert("x-token", value);
            }
            Ok(req)
        });

    let mut stream = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await
        .context("subscribe entries")?
        .into_inner();
    info!(
        endpoint = endpoint.alias,
        url = endpoint.url,
        "ShredStream stream connected"
    );

    let account_filter = account_filter(&ctx.account_include)?;
    let endpoint_key = EndpointKey::new(Protocol::JitoShredstream, endpoint.alias.clone());
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

        let batch_received_at = Utc::now();
        let Some(entry_batch) = message.context("receive ShredStream message")? else {
            anyhow::bail!("server closed stream");
        };

        let entries =
            match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&entry_batch.entries) {
                Ok(entries) => entries,
                Err(error) => {
                    warn!(
                        endpoint = endpoint.alias,
                        "failed to decode ShredStream entries: {error}"
                    );
                    continue;
                }
            };

        for entry in entries {
            for transaction in entry.transactions {
                if let Some(filter) = &account_filter {
                    let matched = transaction
                        .message
                        .static_account_keys()
                        .iter()
                        .any(|account| filter.contains(account));
                    if !matched {
                        continue;
                    }
                }

                let Some(signature) = transaction.signatures.first() else {
                    continue;
                };
                last_tx = Instant::now();
                ctx.store
                    .record(
                        ctx.phase(),
                        signature.to_string(),
                        endpoint_key.clone(),
                        Timing {
                            received_at: Utc::now(),
                            batch_received_at: Some(batch_received_at),
                        },
                    )
                    .await;
            }
        }
    }
}

async fn build_channel(url: &str, buffer_size: usize) -> Result<Channel> {
    let host = host_from_url(url)?;
    let mut endpoint = Channel::from_shared(url.to_string())?
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .buffer_size(buffer_size);
    if url.starts_with("https://") {
        endpoint = endpoint.tls_config(
            ClientTlsConfig::new()
                .domain_name(host)
                .with_enabled_roots(),
        )?;
    }
    Ok(endpoint.connect().await?)
}

fn host_from_url(url: &str) -> Result<String> {
    let (_, rest) = url
        .split_once("://")
        .context("invalid url: missing scheme")?;
    let authority = rest
        .split('/')
        .next()
        .context("invalid url: missing authority")?;
    Ok(authority
        .trim_start_matches('[')
        .split(']')
        .next()
        .unwrap_or(authority)
        .split(':')
        .next()
        .unwrap_or(authority)
        .to_string())
}

fn account_filter(accounts: &[String]) -> Result<Option<HashSet<Pubkey>>> {
    if accounts.is_empty() {
        return Ok(None);
    }

    accounts
        .iter()
        .map(|account| {
            Pubkey::from_str(account).with_context(|| format!("invalid account pubkey {account}"))
        })
        .collect::<Result<HashSet<_>>>()
        .map(Some)
}
