use anyhow::{Context, Result};
use std::time::Duration;
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::{Channel, ClientTlsConfig},
    Code, Request, Status,
};

pub async fn build_channel(url: &str, buffer_size: usize) -> Result<Channel> {
    let mut endpoint = Channel::from_shared(url.to_string())?
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true)
        .buffer_size(buffer_size);
    if url.starts_with("https://") {
        endpoint = endpoint.tls_config(
            ClientTlsConfig::new()
                .domain_name(host_from_url(url)?)
                .with_enabled_roots(),
        )?;
    }
    endpoint
        .connect()
        .await
        .with_context(|| format!("connect {url}"))
}

pub fn parse_x_token(token: &str) -> Result<Option<MetadataValue<Ascii>>> {
    if token.is_empty() {
        return Ok(None);
    }
    token
        .parse::<MetadataValue<Ascii>>()
        .context("invalid x-token")
        .map(Some)
}

pub fn with_x_token(mut request: Request<()>, token: Option<&MetadataValue<Ascii>>) -> Request<()> {
    if let Some(token) = token {
        request.metadata_mut().insert("x-token", token.clone());
    }
    request
}

pub fn is_auth_error(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source.downcast_ref::<Status>().is_some_and(|status| {
            matches!(
                status.code(),
                Code::Unauthenticated | Code::PermissionDenied
            )
        })
    })
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

#[cfg(test)]
mod tests {
    use super::{host_from_url, is_auth_error};
    use anyhow::Context;
    use tonic::Status;

    #[test]
    fn extracts_tls_host_without_port() {
        assert_eq!(
            host_from_url("https://fra.example.com:443/path").expect("host"),
            "fra.example.com"
        );
    }

    #[test]
    fn recognizes_wrapped_auth_status() {
        let error = Err::<(), _>(Status::unauthenticated("denied"))
            .context("subscribe")
            .expect_err("auth error");
        assert!(is_auth_error(&error));
    }
}
