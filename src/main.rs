use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use config::read_config;
use observation::ObservationStore;
use protocols::{spawn_endpoint, CollectorContext};
use report::{render_report, render_terminal_report, ReportMeta};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod config;
mod observation;
mod proto;
mod protocols;
mod report;
mod stats;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// TOML config path.
    #[arg(short, long, default_value = "grpcbench.toml")]
    config: PathBuf,

    /// Override benchmark measurement duration, e.g. 60s, 2m.
    #[arg(short, long, value_parser = parse_duration)]
    duration: Option<Duration>,

    /// Override warmup duration, e.g. 5s.
    #[arg(long, value_parser = parse_duration)]
    warmup: Option<Duration>,

    /// HTML report output path.
    #[arg(short, long, default_value = "grpcbench-report.html")]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    let config = read_config(&args.config)?;
    let endpoints = config.endpoints()?;
    let duration = args
        .duration
        .or(config.duration)
        .unwrap_or_else(|| Duration::from_secs(60));
    let warmup = args
        .warmup
        .or(config.warmup)
        .unwrap_or_else(|| Duration::from_secs(5));

    info!(
        endpoints = endpoints.len(),
        duration = ?duration,
        warmup = ?warmup,
        "starting benchmark"
    );

    let store = ObservationStore::new();
    let measuring = Arc::new(AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let ctx = CollectorContext {
        store: store.clone(),
        measuring: Arc::clone(&measuring),
        cancel: cancel.clone(),
        buffer_size: config.buffer_size,
        no_tx_timeout: config.no_tx_timeout,
        account_include: Arc::new(config.account_include.clone()),
    };

    let mut handles = Vec::new();
    for endpoint in endpoints.clone() {
        handles.push(spawn_endpoint(endpoint, ctx.clone()));
    }

    if !warmup.is_zero() {
        info!(warmup = ?warmup, "warming streams");
        tokio::select! {
            _ = tokio::time::sleep(warmup) => {}
            _ = tokio::signal::ctrl_c() => {
                warn!("interrupted during warmup; stopping without report");
                shutdown_collectors(cancel, handles).await;
                return Ok(());
            }
        }
    }

    let started_at = Utc::now();
    measuring.store(true, Ordering::Relaxed);
    info!(duration = ?duration, "measurement window started");

    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = tokio::signal::ctrl_c() => {
            warn!("interrupted during measurement; stopping without report");
            shutdown_collectors(cancel, handles).await;
            return Ok(());
        }
    }

    let finished_at = Utc::now();
    shutdown_collectors(cancel, handles).await;

    let observations = store.snapshot().await;
    let stats = stats::compute_stats(&endpoints, &observations);
    let meta = ReportMeta {
        generated_at: Utc::now(),
        started_at,
        finished_at,
        duration_secs: finished_at
            .signed_duration_since(started_at)
            .num_seconds()
            .max(0) as u64,
        warmup_secs: warmup.as_secs(),
        endpoint_count: endpoints.len(),
        account_include: config.account_include,
    };
    let html = render_report(&meta, &endpoints, &stats);
    std::fs::write(&args.output, html)
        .with_context(|| format!("write report {}", args.output.display()))?;

    println!(
        "{}",
        render_terminal_report(
            &meta,
            &endpoints,
            &stats,
            &args.output.display().to_string()
        )
    );

    Ok(())
}

async fn shutdown_collectors(cancel: CancellationToken, handles: Vec<tokio::task::JoinHandle<()>>) {
    cancel.cancel();
    for handle in handles {
        if let Err(error) = handle.await {
            warn!("collector task ended unexpectedly: {error}");
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn parse_duration(value: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(value)
}
