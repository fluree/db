//! CLI tool for issuing events endpoint tokens
//!
//! Generate JWS tokens for authenticating to the `/fluree/events` SSE endpoint.
//!
//! # Usage
//!
//! ```bash
//! # Generate a token granting access to all events
//! fluree-events-token --private-key @~/.fluree/server.key --all --subject admin@example.com
//!
//! # Generate a token for specific ledgers
//! fluree-events-token --private-key 0x<hex> --ledger books:main --ledger users:prod
//!
//! # Generate with custom expiry and curl output
//! fluree-events-token --private-key @key.pem --all --expires-in 7d --output curl
//! ```

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use clap::{Parser, ValueEnum};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_credential::did_from_pubkey;
use serde_json::json;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

/// Generate JWS tokens for the Fluree events endpoint
#[derive(Parser, Debug)]
#[command(name = "fluree-events-token")]
#[command(about = "Generate Bearer tokens for /fluree/events authentication")]
struct Args {
    /// Ed25519 private key (hex with 0x prefix, base58btc, or @filepath)
    #[arg(long, required = true)]
    private_key: String,

    /// Audience claim (aud) - must match server's --events-auth-audience
    #[arg(long)]
    audience: Option<String>,

    /// Token lifetime (e.g., "1h", "30m", "7d", "1w") [default: 1h]
    #[arg(long, default_value = "1h")]
    expires_in: String,

    /// Subject claim (sub) - identity of the token holder
    /// Required when connecting to servers with --events-auth-mode=required
    #[arg(long)]
    subject: Option<String>,

    /// Fluree identity claim (fluree.identity) - takes precedence over sub for policy
    #[arg(long)]
    identity: Option<String>,

    /// Grant access to all ledgers and graph sources (fluree.events.all=true)
    #[arg(long)]
    all: bool,

    /// Grant access to specific ledger (repeatable)
    #[arg(long = "ledger")]
    ledgers: Vec<String>,

    /// Grant access to specific graph source (repeatable)
    #[arg(long = "graph-source")]
    graph_sources: Vec<String>,

    /// Output format
    #[arg(long, default_value = "token", value_enum)]
    output: OutputFormat,

    /// Print decoded claims to stderr (for verification)
    #[arg(long)]
    print_claims: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    /// Just the JWS token string
    Token,
    /// JSON object with token and decoded claims
    Json,
    /// Ready-to-use curl command
    Curl,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load private key
    let signing_key = load_private_key(&args.private_key)?;

    // Derive public key and DID (iss will be the DID)
    let pubkey = signing_key.verifying_key().to_bytes();
    let issuer_did = did_from_pubkey(&pubkey);

    // Parse expiration duration
    let exp_secs = parse_duration(&args.expires_in)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exp = now + exp_secs;
    let iat = now;

    // Warn if subject not provided
    if args.subject.is_none() {
        eprintln!("Warning: --subject not provided. Token may be rejected by servers with --events-auth-mode=required");
    }

    // Warn if no permissions granted
    if !args.all && args.ledgers.is_empty() && args.graph_sources.is_empty() {
        return Err("No permissions granted. Use --all, --ledger, or --graph-source".into());
    }

    // Build claims payload
    let mut claims = json!({
        "iss": issuer_did,
        "exp": exp,
        "iat": iat,
    });

    if let Some(ref sub) = args.subject {
        claims["sub"] = json!(sub);
    }

    if let Some(ref aud) = args.audience {
        claims["aud"] = json!(aud);
    }

    if let Some(ref identity) = args.identity {
        claims["fluree.identity"] = json!(identity);
    }

    if args.all {
        claims["fluree.events.all"] = json!(true);
    }

    if !args.ledgers.is_empty() {
        claims["fluree.events.ledgers"] = json!(args.ledgers);
    }

    if !args.graph_sources.is_empty() {
        claims["fluree.events.graph_sources"] = json!(args.graph_sources);
    }

    // Create JWS
    let token = create_jws(&claims, &signing_key)?;

    // Print claims to stderr if requested
    if args.print_claims {
        eprintln!("--- Token Claims ---");
        eprintln!("{}", serde_json::to_string_pretty(&claims)?);
        eprintln!("--- End Claims ---\n");
    }

    // Output based on format
    match args.output {
        OutputFormat::Token => {
            println!("{token}");
        }
        OutputFormat::Json => {
            let output = json!({
                "token": token,
                "claims": claims,
                "issuer": issuer_did,
                "expires_at": exp,
                "issued_at": iat,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        OutputFormat::Curl => {
            let mut ledger_params = String::new();
            if args.all {
                ledger_params.push_str("all=true");
            } else {
                for l in &args.ledgers {
                    if !ledger_params.is_empty() {
                        ledger_params.push('&');
                    }
                    ledger_params.push_str(&format!("ledger={}", url_encode(l)));
                }
                for v in &args.graph_sources {
                    if !ledger_params.is_empty() {
                        ledger_params.push('&');
                    }
                    ledger_params.push_str(&format!("graph-source={}", url_encode(v)));
                }
            }
            println!(
                r#"curl -N -H "Authorization: Bearer {token}" "http://localhost:8090/fluree/events?{ledger_params}""#
            );
        }
    }

    Ok(())
}

/// Load Ed25519 private key from various formats
fn load_private_key(input: &str) -> Result<SigningKey, Box<dyn std::error::Error>> {
    // Handle @filepath
    let key_str = if let Some(path) = input.strip_prefix('@') {
        let expanded = shellexpand::tilde(path);
        fs::read_to_string(expanded.as_ref())
            .map_err(|e| format!("Failed to read key file '{path}': {e}"))?
            .trim()
            .to_string()
    } else {
        input.to_string()
    };

    // Try hex format (0x prefix or raw hex)
    let hex_str = key_str.strip_prefix("0x").unwrap_or(&key_str);
    if hex_str.len() == 64 && hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(hex_str)?;
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        return Ok(SigningKey::from_bytes(&key));
    }

    // Try base58btc (starts with z or just raw base58)
    let b58_str = key_str.strip_prefix('z').unwrap_or(&key_str);
    if let Ok(bytes) = bs58::decode(b58_str).into_vec() {
        if bytes.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return Ok(SigningKey::from_bytes(&key));
        }
        // Check for multicodec prefix (0x8026 for Ed25519 private key)
        if bytes.len() == 34 && bytes[0] == 0x80 && bytes[1] == 0x26 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes[2..]);
            return Ok(SigningKey::from_bytes(&key));
        }
    }

    Err("Invalid private key format. Expected:\n\
         - Hex: 0x<64 hex chars> or <64 hex chars>\n\
         - Base58: z<base58> or <base58> (32 bytes)\n\
         - File: @/path/to/keyfile"
        .to_string()
        .into())
}

/// Parse duration string (e.g., "1h", "30m", "7d")
fn parse_duration(s: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let s = s.trim().to_lowercase();

    // Try to parse as just seconds
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(secs);
    }

    // Parse with unit suffix
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600u64)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86400u64)
    } else if let Some(n) = s.strip_suffix('w') {
        (n, 604_800_u64)
    } else {
        return Err(format!("Invalid duration '{s}'. Use format like 30s, 5m, 1h, 7d, 1w").into());
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("Invalid duration number in '{s}'"))?;

    Ok(num * multiplier)
}

/// Create JWS with embedded JWK
fn create_jws(
    claims: &serde_json::Value,
    signing_key: &SigningKey,
) -> Result<String, Box<dyn std::error::Error>> {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

    // Create header with embedded JWK
    let header = json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

    // Sign header.payload
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Ok(format!("{header_b64}.{payload_b64}.{sig_b64}"))
}

/// URL-encode a string for use in query parameters
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            // Unreserved characters (RFC 3986)
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            // Colon is commonly used in aliases (namespace:name), keep it readable
            ':' => result.push(c),
            // Encode everything else
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("1h").unwrap(), 3600);
        assert_eq!(parse_duration("7d").unwrap(), 604_800);
        assert_eq!(parse_duration("1w").unwrap(), 604_800);
        assert_eq!(parse_duration("3600").unwrap(), 3600);
    }

    #[test]
    fn test_load_private_key_hex() {
        let hex_key = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let key = load_private_key(hex_key).unwrap();
        assert_eq!(key.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn test_create_jws_roundtrip() {
        let claims = json!({
            "iss": "did:key:z6MkTest",
            "exp": 9_999_999_999_u64,
            "sub": "test@example.com"
        });
        let key = SigningKey::from_bytes(&[0u8; 32]);
        let jws = create_jws(&claims, &key).unwrap();

        // Verify it has 3 parts
        assert_eq!(jws.split('.').count(), 3);

        // Verify with the credential library
        let verified = fluree_db_credential::verify_jws(&jws).unwrap();
        assert!(verified.payload.contains("test@example.com"));
    }
}
