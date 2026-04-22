//! Token management commands: create, keygen, inspect

use crate::cli::{InspectOutputFormat, KeyFormat, TokenAction, TokenOutputFormat};
use crate::error::{CliError, CliResult};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use colored::Colorize;
use comfy_table::{Cell, Table};
use fluree_db_credential::{did_from_pubkey, verify_jws, SigningKey};
use serde_json::json;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Token claims for JWT creation.
struct TokenClaims<'a> {
    /// Subject claim (sub).
    subject: Option<&'a str>,
    /// Audience claim (aud).
    audiences: &'a [String],
    /// Fluree identity claim.
    identity: Option<&'a str>,
}

/// Permission settings for the token.
struct TokenPermissions<'a> {
    /// Grant all permissions.
    all: bool,
    /// Ledgers for events access.
    events_ledgers: &'a [String],
    /// Ledgers for storage access.
    storage_ledgers: &'a [String],
    /// Data API read-all.
    read_all: bool,
    /// Ledgers for data API read access.
    read_ledgers: &'a [String],
    /// Data API write-all.
    write_all: bool,
    /// Ledgers for data API write access.
    write_ledgers: &'a [String],
    /// Graph sources for access.
    graph_sources: &'a [String],
}

pub fn run(action: TokenAction) -> CliResult<()> {
    match action {
        TokenAction::Create(args) => {
            let claims = TokenClaims {
                subject: args.subject.as_deref(),
                audiences: &args.audiences,
                identity: args.identity.as_deref(),
            };
            let permissions = TokenPermissions {
                all: args.all,
                events_ledgers: &args.events_ledgers,
                storage_ledgers: &args.storage_ledgers,
                read_all: args.read_all,
                read_ledgers: &args.read_ledgers,
                write_all: args.write_all,
                write_ledgers: &args.write_ledgers,
                graph_sources: &args.graph_sources,
            };
            run_create(
                &args.private_key,
                &args.expires_in,
                claims,
                permissions,
                args.output,
                args.print_claims,
            )
        }
        TokenAction::Keygen { format, output } => run_keygen(format, output),
        TokenAction::Inspect {
            token,
            no_verify,
            output,
        } => run_inspect(&token, !no_verify, output),
    }
}

fn run_create(
    private_key: &str,
    expires_in: &str,
    token_claims: TokenClaims<'_>,
    permissions: TokenPermissions<'_>,
    output: TokenOutputFormat,
    print_claims: bool,
) -> CliResult<()> {
    // Validate permissions
    if !permissions.all
        && permissions.events_ledgers.is_empty()
        && permissions.storage_ledgers.is_empty()
        && !permissions.read_all
        && permissions.read_ledgers.is_empty()
        && !permissions.write_all
        && permissions.write_ledgers.is_empty()
        && permissions.graph_sources.is_empty()
    {
        return Err(CliError::Usage(
            "no permissions granted; use --all, --events-ledger, --storage-ledger, --read-ledger/--read-all, --write-ledger/--write-all, or --graph-source".into(),
        ));
    }

    // Load private key
    let signing_key = load_private_key(private_key)?;

    // Derive public key and DID
    let pubkey = signing_key.verifying_key().to_bytes();
    let issuer_did = did_from_pubkey(&pubkey);

    // Calculate timestamps
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exp_secs = parse_duration(expires_in)?;
    let exp = now + exp_secs;
    let iat = now;

    // Warn if subject not provided
    if token_claims.subject.is_none() {
        eprintln!(
            "{} --subject not provided; token may be rejected by servers with --events-auth-mode=required",
            "warning:".yellow().bold()
        );
    }

    // Build claims payload
    let mut claims = json!({
        "iss": issuer_did,
        "exp": exp,
        "iat": iat,
    });

    if let Some(sub) = token_claims.subject {
        claims["sub"] = json!(sub);
    }

    // Handle audience (single value or array)
    match token_claims.audiences.len() {
        0 => {}
        1 => claims["aud"] = json!(&token_claims.audiences[0]),
        _ => claims["aud"] = json!(token_claims.audiences),
    }

    if let Some(id) = token_claims.identity {
        claims["fluree.identity"] = json!(id);
    }

    // Events permissions
    if permissions.all {
        claims["fluree.events.all"] = json!(true);
        claims["fluree.storage.all"] = json!(true);
        claims["fluree.ledger.read.all"] = json!(true);
        claims["fluree.ledger.write.all"] = json!(true);
    }

    if !permissions.events_ledgers.is_empty() {
        claims["fluree.events.ledgers"] = json!(permissions.events_ledgers);
    }

    if !permissions.storage_ledgers.is_empty() {
        claims["fluree.storage.ledgers"] = json!(permissions.storage_ledgers);
    }

    // Data API permissions
    if permissions.read_all {
        claims["fluree.ledger.read.all"] = json!(true);
    }
    if !permissions.read_ledgers.is_empty() {
        claims["fluree.ledger.read.ledgers"] = json!(permissions.read_ledgers);
    }
    if permissions.write_all {
        claims["fluree.ledger.write.all"] = json!(true);
    }
    if !permissions.write_ledgers.is_empty() {
        claims["fluree.ledger.write.ledgers"] = json!(permissions.write_ledgers);
    }

    if !permissions.graph_sources.is_empty() {
        claims["fluree.events.graph_sources"] = json!(permissions.graph_sources);
    }

    // Create JWS
    let token = create_jws(&claims, &signing_key)?;

    // Print claims to stderr if requested
    if print_claims {
        eprintln!("--- Token Claims ---");
        eprintln!("{}", serde_json::to_string_pretty(&claims)?);
        eprintln!("--- End Claims ---\n");
    }

    // Output based on format
    match output {
        TokenOutputFormat::Token => {
            println!("{token}");
        }
        TokenOutputFormat::Json => {
            let output = json!({
                "token": token,
                "claims": claims,
                "issuer": issuer_did,
                "expires_at": exp,
                "issued_at": iat,
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        TokenOutputFormat::Curl => {
            let params = build_curl_params(
                permissions.all,
                permissions.events_ledgers,
                permissions.graph_sources,
            );
            println!(
                r#"curl -N -H "Authorization: Bearer {token}" "http://localhost:8090/fluree/events?{params}""#
            );
        }
    }

    Ok(())
}

fn run_keygen(format: KeyFormat, output_path: Option<PathBuf>) -> CliResult<()> {
    use rand::rngs::OsRng;
    use rand::RngCore;

    // Generate 32 random bytes
    let mut secret_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut secret_bytes);

    // Create signing key
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let pubkey = signing_key.verifying_key().to_bytes();

    // Derive DID
    let did = did_from_pubkey(&pubkey);

    match format {
        KeyFormat::Hex => {
            let priv_hex = format!("0x{}", hex::encode(secret_bytes));
            let pub_hex = format!("0x{}", hex::encode(pubkey));

            if let Some(path) = output_path {
                fs::write(&path, &priv_hex)?;
                eprintln!("Private key written to: {}", path.display());
                eprintln!("Public key: {pub_hex}");
                eprintln!("DID:        {did}");
            } else {
                println!("Private key: {priv_hex}");
                println!("Public key:  {pub_hex}");
                println!("DID:         {did}");
            }
        }
        KeyFormat::Base58 => {
            // Base58btc with multicodec prefix (0x8026 for Ed25519 private key)
            let mut priv_bytes = vec![0x80, 0x26];
            priv_bytes.extend_from_slice(&secret_bytes);
            let priv_b58 = format!("z{}", bs58::encode(&priv_bytes).into_string());

            // Public key with Ed25519 multicodec prefix (0xed01)
            let mut pub_bytes = vec![0xed, 0x01];
            pub_bytes.extend_from_slice(&pubkey);
            let pub_b58 = format!("z{}", bs58::encode(&pub_bytes).into_string());

            if let Some(path) = output_path {
                fs::write(&path, &priv_b58)?;
                eprintln!("Private key written to: {}", path.display());
                eprintln!("Public key: {pub_b58}");
                eprintln!("DID:        {did}");
            } else {
                println!("Private key: {priv_b58}");
                println!("Public key:  {pub_b58}");
                println!("DID:         {did}");
            }
        }
        KeyFormat::Json => {
            let priv_hex = format!("0x{}", hex::encode(secret_bytes));

            // Base58 versions
            let mut priv_bytes_mc = vec![0x80, 0x26];
            priv_bytes_mc.extend_from_slice(&secret_bytes);
            let priv_b58 = format!("z{}", bs58::encode(&priv_bytes_mc).into_string());

            let mut pub_bytes_mc = vec![0xed, 0x01];
            pub_bytes_mc.extend_from_slice(&pubkey);
            let pub_b58 = format!("z{}", bs58::encode(&pub_bytes_mc).into_string());

            let output_json = json!({
                "privateKey": {
                    "hex": priv_hex,
                    "base58": priv_b58,
                },
                "publicKey": {
                    "hex": format!("0x{}", hex::encode(pubkey)),
                    "base58": pub_b58,
                },
                "did": did,
            });

            if let Some(path) = output_path {
                // Write only private key hex to file
                fs::write(&path, &priv_hex)?;
                eprintln!("Private key written to: {}", path.display());
                eprintln!("DID: {did}");
            } else {
                println!("{}", serde_json::to_string_pretty(&output_json)?);
            }
        }
    }

    Ok(())
}

fn run_inspect(token_input: &str, verify: bool, output: InspectOutputFormat) -> CliResult<()> {
    // Load token from file if @filepath or @- for stdin
    let token = load_token(token_input)?;

    // Split JWS to decode parts
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(CliError::Input(format!(
            "invalid JWS format: expected 3 parts (header.payload.signature), got {}",
            parts.len()
        )));
    }

    // Decode header and payload
    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|e| CliError::Input(format!("invalid header encoding: {e}")))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| CliError::Input(format!("invalid payload encoding: {e}")))?;

    let header: serde_json::Value = serde_json::from_slice(&header_bytes)?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)?;

    // Verify signature if requested
    let (verified, did, verify_error) = if verify {
        match verify_jws(&token) {
            Ok(result) => (true, Some(result.did), None),
            Err(e) => (false, None, Some(e.to_string())),
        }
    } else {
        (false, None, None)
    };

    // Check expiration
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expired = payload
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .map(|exp| exp < now)
        .unwrap_or(false);

    // Format output
    match output {
        InspectOutputFormat::Pretty => {
            print_pretty_inspect(
                &header,
                &payload,
                verify,
                verified,
                did.as_deref(),
                verify_error.as_deref(),
                expired,
            );
        }
        InspectOutputFormat::Json => {
            let result = json!({
                "header": header,
                "payload": payload,
                "signature": {
                    "verified": if verify { Some(verified) } else { None },
                    "did": did,
                    "error": verify_error,
                },
                "status": {
                    "expired": expired,
                }
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        InspectOutputFormat::Table => {
            print_table_inspect(&payload, verify, verified, did.as_deref(), expired);
        }
    }

    // Exit with error if verification was requested and failed
    if verify && !verified {
        return Err(CliError::Credential(
            fluree_db_credential::CredentialError::InvalidSignature("verification failed".into()),
        ));
    }

    Ok(())
}

// --- Helper functions ---

/// Load Ed25519 private key from various formats
fn load_private_key(input: &str) -> CliResult<SigningKey> {
    // Handle @- (stdin)
    let key_str = if input == "@-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .map_err(|e| CliError::Input(format!("failed to read key from stdin: {e}")))?;
        buffer.trim().to_string()
    } else if let Some(path) = input.strip_prefix('@') {
        // Handle @filepath
        let expanded = shellexpand::tilde(path);
        fs::read_to_string(expanded.as_ref())
            .map_err(|e| CliError::Input(format!("failed to read key file '{path}': {e}")))?
            .trim()
            .to_string()
    } else {
        input.to_string()
    };

    // Try hex format (0x prefix or raw hex)
    let hex_str = key_str.strip_prefix("0x").unwrap_or(&key_str);
    if hex_str.len() == 64 && hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes =
            hex::decode(hex_str).map_err(|e| CliError::Input(format!("invalid hex key: {e}")))?;
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

    Err(CliError::Input(
        "invalid private key format; expected:\n  \
         - Hex: 0x<64 hex chars> or <64 hex chars>\n  \
         - Base58: z<base58> or <base58> (32 bytes)\n  \
         - File: @/path/to/keyfile or @- for stdin"
            .into(),
    ))
}

/// Load token from string, @filepath, or @- (stdin)
fn load_token(input: &str) -> CliResult<String> {
    if input == "@-" {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .map_err(|e| CliError::Input(format!("failed to read token from stdin: {e}")))?;
        Ok(buffer.trim().to_string())
    } else if let Some(path) = input.strip_prefix('@') {
        let expanded = shellexpand::tilde(path);
        fs::read_to_string(expanded.as_ref())
            .map_err(|e| CliError::Input(format!("failed to read token file '{path}': {e}")))?
            .trim()
            .to_string()
            .pipe(Ok)
    } else {
        Ok(input.to_string())
    }
}

/// Parse duration string (e.g., "1h", "30m", "7d")
fn parse_duration(s: &str) -> CliResult<u64> {
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
        return Err(CliError::Usage(format!(
            "invalid duration '{s}'; use format like 30s, 5m, 1h, 7d, 1w"
        )));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| CliError::Usage(format!("invalid duration number in '{s}'")))?;

    Ok(num * multiplier)
}

/// Create JWS with embedded JWK
fn create_jws(claims: &serde_json::Value, signing_key: &SigningKey) -> CliResult<String> {
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
    let signature = fluree_db_credential::sign_ed25519(signing_key, signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature);

    Ok(format!("{header_b64}.{payload_b64}.{sig_b64}"))
}

/// Build URL params for curl output
fn build_curl_params(all: bool, ledgers: &[String], graph_sources: &[String]) -> String {
    let mut params = String::new();
    if all {
        params.push_str("all=true");
    } else {
        for l in ledgers {
            if !params.is_empty() {
                params.push('&');
            }
            params.push_str(&format!("ledger={}", url_encode(l)));
        }
        for gs in graph_sources {
            if !params.is_empty() {
                params.push('&');
            }
            params.push_str(&format!("graph-source={}", url_encode(gs)));
        }
    }
    params
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

/// Format timestamp for display
fn format_timestamp(ts: u64) -> String {
    // Simple formatting without chrono - just show relative time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let diff = ts.abs_diff(now);
    let relative = if ts > now {
        format_duration_human(diff, "from now")
    } else {
        format_duration_human(diff, "ago")
    };

    format!("{ts} ({relative})")
}

fn format_duration_human(secs: u64, suffix: &str) -> String {
    if secs < 60 {
        format!("{secs} seconds {suffix}")
    } else if secs < 3600 {
        format!("{} minutes {}", secs / 60, suffix)
    } else if secs < 86400 {
        format!("{} hours {}", secs / 3600, suffix)
    } else {
        format!("{} days {}", secs / 86400, suffix)
    }
}

fn print_pretty_inspect(
    header: &serde_json::Value,
    payload: &serde_json::Value,
    verify_requested: bool,
    verified: bool,
    did: Option<&str>,
    verify_error: Option<&str>,
    expired: bool,
) {
    println!("{}", "=== Header ===".bold());
    println!(
        "Algorithm: {}",
        header.get("alg").unwrap_or(&json!("unknown"))
    );

    println!("\n{}", "=== Claims ===".bold());

    // Standard claims first
    if let Some(iss) = payload.get("iss") {
        println!("Issuer (iss):   {}", iss.as_str().unwrap_or(""));
    }
    if let Some(sub) = payload.get("sub") {
        println!("Subject (sub):  {}", sub.as_str().unwrap_or(""));
    }
    if let Some(aud) = payload.get("aud") {
        println!("Audience (aud): {aud}");
    }
    if let Some(exp) = payload.get("exp").and_then(serde_json::Value::as_u64) {
        let status = if expired {
            " [EXPIRED]".red().to_string()
        } else {
            " [valid]".green().to_string()
        };
        println!("Expires (exp):  {}{}", format_timestamp(exp), status);
    }
    if let Some(iat) = payload.get("iat").and_then(serde_json::Value::as_u64) {
        println!("Issued (iat):   {}", format_timestamp(iat));
    }
    if let Some(nbf) = payload.get("nbf").and_then(serde_json::Value::as_u64) {
        println!("Not before:     {}", format_timestamp(nbf));
    }

    // Fluree-specific claims
    println!("\n{}", "=== Permissions ===".bold());

    if let Some(id) = payload.get("fluree.identity") {
        println!("Identity:       {}", id.as_str().unwrap_or(""));
    }

    // Events permissions
    if payload.get("fluree.events.all") == Some(&json!(true)) {
        println!("Events:         {} (all)", "✓".green());
    } else if let Some(ledgers) = payload.get("fluree.events.ledgers") {
        println!("Events ledgers: {ledgers}");
    }

    if let Some(gs) = payload.get("fluree.events.graph_sources") {
        println!("Graph sources:  {gs}");
    }

    // Storage permissions
    if payload.get("fluree.storage.all") == Some(&json!(true)) {
        println!("Storage:        {} (all)", "✓".green());
    } else if let Some(ledgers) = payload.get("fluree.storage.ledgers") {
        println!("Storage ledgers: {ledgers}");
    }

    println!("\n{}", "=== Verification ===".bold());
    if verify_requested {
        if verified {
            println!("Signature: {} valid", "✓".green());
            if let Some(d) = did {
                println!("Signer:    {d}");
            }
        } else {
            println!("Signature: {} invalid", "✗".red());
            if let Some(e) = verify_error {
                println!("Error:     {e}");
            }
        }
    } else {
        println!("Signature: (not verified)");
    }

    println!(
        "Expired:   {}",
        if expired {
            format!("{} yes", "✗".red())
        } else {
            format!("{} no", "✓".green())
        }
    );
}

fn print_table_inspect(
    payload: &serde_json::Value,
    verify_requested: bool,
    verified: bool,
    did: Option<&str>,
    expired: bool,
) {
    let mut table = Table::new();
    table.set_header(vec!["Claim", "Value"]);

    if let Some(obj) = payload.as_object() {
        for (k, v) in obj {
            let display_value = match k.as_str() {
                "exp" | "iat" | "nbf" => {
                    if let Some(ts) = v.as_u64() {
                        format_timestamp(ts)
                    } else {
                        v.to_string()
                    }
                }
                _ => {
                    if v.is_string() {
                        v.as_str().unwrap_or("").to_string()
                    } else {
                        v.to_string()
                    }
                }
            };
            table.add_row(vec![Cell::new(k), Cell::new(display_value)]);
        }
    }

    // Add status rows
    table.add_row(vec![Cell::new("---"), Cell::new("---")]);
    if verify_requested {
        table.add_row(vec![
            Cell::new("Signature"),
            Cell::new(if verified { "valid" } else { "INVALID" }),
        ]);
        if let Some(d) = did {
            table.add_row(vec![Cell::new("Signer DID"), Cell::new(d)]);
        }
    }
    table.add_row(vec![
        Cell::new("Expired"),
        Cell::new(if expired { "YES" } else { "no" }),
    ]);

    println!("{table}");
}

/// Pipe trait for method chaining
trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

impl<T> Pipe for T {}

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
    fn test_parse_duration_invalid() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("1x").is_err());
    }

    #[test]
    fn test_load_private_key_hex() {
        let hex_key = "0x0000000000000000000000000000000000000000000000000000000000000000";
        let key = load_private_key(hex_key).unwrap();
        assert_eq!(key.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn test_load_private_key_hex_no_prefix() {
        let hex_key = "0000000000000000000000000000000000000000000000000000000000000000";
        let key = load_private_key(hex_key).unwrap();
        assert_eq!(key.to_bytes(), [0u8; 32]);
    }

    #[test]
    fn test_create_and_verify_jws() {
        let key = SigningKey::from_bytes(&[1u8; 32]);
        let claims = json!({"sub": "test", "exp": 9_999_999_999_u64});
        let jws = create_jws(&claims, &key).unwrap();

        let verified = verify_jws(&jws).unwrap();
        assert!(verified.did.starts_with("did:key:z"));
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello:world"), "hello:world");
        assert_eq!(url_encode("hello world"), "hello%20world");
    }
}
