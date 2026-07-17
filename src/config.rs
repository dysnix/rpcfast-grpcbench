use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum Protocol {
    Yellowstone,
    JitoShredstream,
    ApertureTxstream,
    ShrederBinary,
    Arpc,
    Jetstream,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yellowstone => "yellowstone",
            Self::JitoShredstream => "jito-shredstream",
            Self::ApertureTxstream => "aperture-txstream",
            Self::ShrederBinary => "shreder-binary",
            Self::Arpc => "arpc",
            Self::Jetstream => "jetstream",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Yellowstone => "Yellowstone",
            Self::JitoShredstream => "Jito ShredStream",
            Self::ApertureTxstream => "Aperture txstream",
            Self::ShrederBinary => "Shreder Binary",
            Self::Arpc => "aRPC",
            Self::Jetstream => "Jetstream",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub alias: String,
    pub protocol: Protocol,
    pub url: String,
    pub token: String,
    pub signatures_only: bool,
    pub include_simulation: bool,
    pub batch_mode: bool,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default, with = "humantime_serde")]
    pub duration: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    pub warmup: Option<Duration>,
    #[serde(default = "default_no_tx_timeout", with = "humantime_serde")]
    pub no_tx_timeout: Duration,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    #[serde(default)]
    pub account_include: Vec<String>,
    #[serde(default)]
    pub yellowstone: BTreeMap<String, EndpointConfig>,
    #[serde(default)]
    pub jito_shredstream: BTreeMap<String, EndpointConfig>,
    #[serde(default)]
    pub aperture_txstream: BTreeMap<String, ApertureEndpointConfig>,
    #[serde(default)]
    pub shreder_binary: BTreeMap<String, EndpointConfig>,
    #[serde(default)]
    pub arpc: BTreeMap<String, EndpointConfig>,
    #[serde(default)]
    pub jetstream: BTreeMap<String, EndpointConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointConfig {
    pub url: String,
    #[serde(default)]
    pub x_token: Option<String>,
    #[serde(default)]
    pub x_token_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApertureEndpointConfig {
    pub url: String,
    #[serde(default)]
    pub x_token: Option<String>,
    #[serde(default)]
    pub x_token_env: Option<String>,
    #[serde(default = "default_aperture_signatures_only")]
    pub signatures_only: bool,
    #[serde(default)]
    pub include_simulation: bool,
    #[serde(default)]
    pub batch_mode: bool,
}

fn default_no_tx_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_buffer_size() -> usize {
    4 * 1024 * 1024
}

fn default_aperture_signatures_only() -> bool {
    true
}

pub fn read_config(path: &Path) -> Result<Config> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let config: Config =
        toml::from_str(&content).with_context(|| format!("parse config {}", path.display()))?;
    config.validate()?;
    Ok(config)
}

impl Config {
    pub fn endpoints(&self) -> Result<Vec<Endpoint>> {
        let mut endpoints = Vec::new();

        for (alias, entry) in &self.yellowstone {
            endpoints.push(Endpoint {
                alias: alias.clone(),
                protocol: Protocol::Yellowstone,
                url: entry.url.clone(),
                token: resolve_token(entry.x_token.as_deref(), entry.x_token_env.as_deref())?,
                signatures_only: true,
                include_simulation: false,
                batch_mode: false,
            });
        }

        for (alias, entry) in &self.jito_shredstream {
            endpoints.push(Endpoint {
                alias: alias.clone(),
                protocol: Protocol::JitoShredstream,
                url: entry.url.clone(),
                token: resolve_token(entry.x_token.as_deref(), entry.x_token_env.as_deref())?,
                signatures_only: true,
                include_simulation: false,
                batch_mode: false,
            });
        }

        for (alias, entry) in &self.aperture_txstream {
            endpoints.push(Endpoint {
                alias: alias.clone(),
                protocol: Protocol::ApertureTxstream,
                url: entry.url.clone(),
                token: resolve_token(entry.x_token.as_deref(), entry.x_token_env.as_deref())?,
                signatures_only: entry.signatures_only,
                include_simulation: entry.include_simulation,
                batch_mode: entry.batch_mode,
            });
        }

        for (alias, entry) in &self.shreder_binary {
            endpoints.push(generic_endpoint(alias, entry, Protocol::ShrederBinary)?);
        }

        for (alias, entry) in &self.arpc {
            endpoints.push(generic_endpoint(alias, entry, Protocol::Arpc)?);
        }

        for (alias, entry) in &self.jetstream {
            endpoints.push(generic_endpoint(alias, entry, Protocol::Jetstream)?);
        }

        Ok(endpoints)
    }

    fn validate(&self) -> Result<()> {
        let endpoint_count = self.yellowstone.len()
            + self.jito_shredstream.len()
            + self.aperture_txstream.len()
            + self.shreder_binary.len()
            + self.arpc.len()
            + self.jetstream.len();
        if endpoint_count < 2 {
            return Err(anyhow!("configure at least two endpoints to compare"));
        }
        Ok(())
    }
}

fn generic_endpoint(alias: &str, entry: &EndpointConfig, protocol: Protocol) -> Result<Endpoint> {
    Ok(Endpoint {
        alias: alias.to_string(),
        protocol,
        url: entry.url.clone(),
        token: resolve_token(entry.x_token.as_deref(), entry.x_token_env.as_deref())?,
        signatures_only: true,
        include_simulation: false,
        batch_mode: false,
    })
}

fn resolve_token(value: Option<&str>, env_name: Option<&str>) -> Result<String> {
    if let Some(value) = value {
        return Ok(value.to_string());
    }

    match env_name {
        Some(name) => std::env::var(name)
            .with_context(|| format!("read token from environment variable {name}")),
        None => Ok(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Protocol};

    #[test]
    fn aperture_optional_flags_default_to_false_and_can_be_enabled() {
        let config: Config = toml::from_str(
            r#"
                [aperture_txstream.default]
                url = "https://default.example.com"

                [aperture_txstream.simulated]
                url = "https://simulated.example.com"
                include_simulation = true
                batch_mode = true
            "#,
        )
        .expect("parse config");

        let endpoints = config.endpoints().expect("resolve endpoints");
        assert!(!endpoints[0].include_simulation);
        assert!(!endpoints[0].batch_mode);
        assert!(endpoints[1].include_simulation);
        assert!(endpoints[1].batch_mode);
    }

    #[test]
    fn additional_protocol_sections_resolve_to_endpoints() {
        let config: Config = toml::from_str(
            r#"
                [shreder_binary.fra]
                url = "http://fra.binary.shreder.xyz:9991"

                [arpc.corvus]
                url = "http://arpc.fra.corvus-labs.io:20202"

                [jetstream.orbit]
                url = "http://fra.jetstream.orbitflare.com"
            "#,
        )
        .expect("parse config");

        let endpoints = config.endpoints().expect("resolve endpoints");
        assert_eq!(endpoints.len(), 3);
        assert_eq!(endpoints[0].protocol, Protocol::ShrederBinary);
        assert_eq!(endpoints[1].protocol, Protocol::Arpc);
        assert_eq!(endpoints[2].protocol, Protocol::Jetstream);
    }
}
