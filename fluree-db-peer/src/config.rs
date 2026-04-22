//! Peer configuration and validation
//!
//! Configuration for the query peer, including events endpoint connection,
//! storage read mode, and reconnect behavior.

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// Read mode for the peer
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum ReadMode {
    /// Read directly from shared storage (S3, NFS, local FS)
    #[default]
    SharedStorage,
    /// Read via authenticated storage or proxy (future)
    PrivateStorage,
}

/// Configuration for the query peer
#[derive(Parser, Debug, Clone)]
#[command(name = "fluree-peer", about = "Fluree DB query peer")]
pub struct PeerConfig {
    // === Events endpoint ===
    /// Transaction server events URL
    #[arg(
        long,
        env = "PEER_EVENTS_URL",
        default_value = "http://localhost:8090/fluree/events"
    )]
    pub events_url: String,

    /// Bearer token for events authentication (or @filepath to read from file)
    #[arg(long, env = "PEER_EVENTS_TOKEN")]
    pub events_token: Option<String>,

    /// Subscribed ledger aliases (repeatable, e.g. --ledger books:main --ledger users:main)
    #[arg(long = "ledger")]
    pub ledgers: Vec<String>,

    /// Subscribed graph source aliases (repeatable)
    #[arg(long = "graph-source")]
    pub graph_sources: Vec<String>,

    /// Subscribe to all ledgers and graph sources (required if no --ledger/--graph-source specified)
    #[arg(long, default_value = "false")]
    pub all: bool,

    // === Storage ===
    /// Read mode
    #[arg(long, default_value = "shared-storage", value_enum)]
    pub read_mode: ReadMode,

    /// Storage path (for file-based shared storage)
    #[arg(long, env = "PEER_STORAGE_PATH")]
    pub storage_path: Option<PathBuf>,

    // Future: S3 config, proxy URL, etc.

    // === Reconnect behavior ===
    /// Initial reconnect delay (ms)
    #[arg(long, default_value = "1000")]
    pub reconnect_initial_ms: u64,

    /// Max reconnect delay (ms)
    #[arg(long, default_value = "30000")]
    pub reconnect_max_ms: u64,

    /// Reconnect backoff multiplier
    #[arg(long, default_value = "2.0")]
    pub reconnect_multiplier: f64,
}

impl PeerConfig {
    /// Validate the configuration at startup
    pub fn validate(&self) -> Result<(), String> {
        // SharedStorage requires storage_path
        if self.read_mode == ReadMode::SharedStorage && self.storage_path.is_none() {
            return Err("read_mode=shared-storage requires --storage-path".to_string());
        }

        // Must subscribe to something
        if !self.all && self.ledgers.is_empty() && self.graph_sources.is_empty() {
            return Err("Must specify --all or at least one --ledger/--graph-source".to_string());
        }

        // Validate reconnect parameters
        if self.reconnect_initial_ms == 0 {
            return Err("reconnect_initial_ms must be > 0".to_string());
        }
        if self.reconnect_max_ms < self.reconnect_initial_ms {
            return Err("reconnect_max_ms must be >= reconnect_initial_ms".to_string());
        }
        if self.reconnect_multiplier < 1.0 {
            return Err("reconnect_multiplier must be >= 1.0".to_string());
        }

        Ok(())
    }

    /// Build the events URL with query parameters
    pub fn events_url_with_params(&self) -> String {
        let mut url = self.events_url.clone();
        let mut params = vec![];

        if self.all {
            params.push("all=true".to_string());
        } else {
            for l in &self.ledgers {
                params.push(format!("ledger={}", url_encode(l)));
            }
            for gs in &self.graph_sources {
                params.push(format!("graph-source={}", url_encode(gs)));
            }
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        url
    }

    /// Load the Bearer token, resolving @filepath if needed
    pub fn load_token(&self) -> Result<Option<String>, std::io::Error> {
        match &self.events_token {
            Some(token) if token.starts_with('@') => {
                let path = shellexpand(&token[1..]);
                let content = std::fs::read_to_string(path)?;
                Ok(Some(content.trim().to_string()))
            }
            Some(token) => Ok(Some(token.clone())),
            None => Ok(None),
        }
    }
}

/// URL-encode a string for use in query parameters
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            ':' => result.push(c), // Keep colons readable for aliases
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    result
}

/// Simple shell expansion for ~ in paths
fn shellexpand(path: &str) -> String {
    if path.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}{}", home.to_string_lossy(), &path[1..]);
        }
    }
    path.to_string()
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            events_url: "http://localhost:8090/fluree/events".to_string(),
            events_token: None,
            ledgers: Vec::new(),
            graph_sources: Vec::new(),
            all: false,
            read_mode: ReadMode::SharedStorage,
            storage_path: None,
            reconnect_initial_ms: 1000,
            reconnect_max_ms: 30000,
            reconnect_multiplier: 2.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_shared_storage_requires_path() {
        let config = PeerConfig {
            read_mode: ReadMode::SharedStorage,
            storage_path: None,
            all: true,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("storage-path"));
    }

    #[test]
    fn test_validate_requires_subscriptions() {
        let config = PeerConfig {
            all: false,
            ledgers: vec![],
            graph_sources: vec![],
            storage_path: Some(PathBuf::from("/tmp")),
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("--all"));
    }

    #[test]
    fn test_validate_valid_config() {
        let config = PeerConfig {
            all: true,
            storage_path: Some(PathBuf::from("/tmp")),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_events_url_with_params_all() {
        let config = PeerConfig {
            events_url: "http://localhost:8090/fluree/events".to_string(),
            all: true,
            ..Default::default()
        };
        assert_eq!(
            config.events_url_with_params(),
            "http://localhost:8090/fluree/events?all=true"
        );
    }

    #[test]
    fn test_events_url_with_params_specific() {
        let config = PeerConfig {
            events_url: "http://localhost:8090/fluree/events".to_string(),
            all: false,
            ledgers: vec!["books:main".to_string(), "users:main".to_string()],
            graph_sources: vec!["search:main".to_string()],
            ..Default::default()
        };
        assert_eq!(
            config.events_url_with_params(),
            "http://localhost:8090/fluree/events?ledger=books:main&ledger=users:main&graph-source=search:main"
        );
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("books:main"), "books:main");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("test&value=1"), "test%26value%3D1");
    }

    #[test]
    fn test_reconnect_validation() {
        let mut config = PeerConfig {
            all: true,
            storage_path: Some(PathBuf::from("/tmp")),
            reconnect_initial_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        config.reconnect_initial_ms = 1000;
        config.reconnect_max_ms = 500;
        assert!(config.validate().is_err());

        config.reconnect_max_ms = 30000;
        config.reconnect_multiplier = 0.5;
        assert!(config.validate().is_err());
    }
}
