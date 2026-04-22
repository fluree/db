use regex::Regex;
use std::sync::OnceLock;

/// Detects secrets (API keys, passwords, connection strings, private keys)
/// in text content before it's stored as a memory.
pub struct SecretDetector;

/// A detected secret with its location and type.
#[derive(Debug, Clone)]
pub struct SecretMatch {
    /// The type of secret detected.
    pub kind: &'static str,
    /// Start byte offset in the original text.
    pub start: usize,
    /// End byte offset in the original text.
    pub end: usize,
}

/// Compiled regex patterns for secret detection.
struct Patterns {
    patterns: Vec<(&'static str, Regex)>,
}

fn compiled_patterns() -> &'static Patterns {
    static PATTERNS: OnceLock<Patterns> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        let patterns = vec![
            // AWS keys
            (
                "AWS Access Key",
                Regex::new(r"(?i)AKIA[0-9A-Z]{16}").unwrap(),
            ),
            // Generic API key patterns
            (
                "API Key",
                Regex::new(r#"(?i)(api[_-]?key|apikey)\s*[=:]\s*['"]?[a-zA-Z0-9_\-]{20,}"#)
                    .unwrap(),
            ),
            // OpenAI keys
            ("OpenAI Key", Regex::new(r"sk-[a-zA-Z0-9]{20,}").unwrap()),
            // Anthropic keys
            (
                "Anthropic Key",
                Regex::new(r"sk-ant-[a-zA-Z0-9_\-]{20,}").unwrap(),
            ),
            // Fluree proxy keys
            (
                "Fluree API Key",
                Regex::new(r"flk_[a-zA-Z0-9_\-]{10,}").unwrap(),
            ),
            // GitHub tokens
            (
                "GitHub Token",
                Regex::new(r"(ghp|gho|ghu|ghs|ghr)_[a-zA-Z0-9]{36,}").unwrap(),
            ),
            // Generic secret/password patterns
            (
                "Password",
                Regex::new(r#"(?i)(password|passwd|pwd)\s*[=:]\s*['"]?[^\s'"]{8,}"#).unwrap(),
            ),
            // Connection strings with credentials
            (
                "Connection String",
                Regex::new(r"(?i)(postgres|mysql|mongodb|redis|amqp)://[^@\s]+@").unwrap(),
            ),
            // Private keys (PEM format)
            (
                "Private Key",
                Regex::new(r"-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----").unwrap(),
            ),
            // Bearer tokens
            (
                "Bearer Token",
                Regex::new(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{20,}").unwrap(),
            ),
            // JWT tokens (three base64 segments separated by dots)
            (
                "JWT Token",
                Regex::new(r"eyJ[a-zA-Z0-9_-]{10,}\.eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}")
                    .unwrap(),
            ),
        ];
        Patterns { patterns }
    })
}

impl SecretDetector {
    /// Detect all secrets in the given text.
    pub fn detect(text: &str) -> Vec<SecretMatch> {
        let patterns = compiled_patterns();
        let mut matches = Vec::new();

        for (kind, re) in &patterns.patterns {
            for m in re.find_iter(text) {
                matches.push(SecretMatch {
                    kind,
                    start: m.start(),
                    end: m.end(),
                });
            }
        }

        // Sort by position
        matches.sort_by_key(|m| m.start);
        matches
    }

    /// Check if text contains any secrets.
    pub fn has_secrets(text: &str) -> bool {
        let patterns = compiled_patterns();
        patterns.patterns.iter().any(|(_, re)| re.is_match(text))
    }

    /// Redact secrets in text, replacing them with `[REDACTED]`.
    pub fn redact(text: &str) -> String {
        let matches = Self::detect(text);
        if matches.is_empty() {
            return text.to_string();
        }

        let mut result = String::with_capacity(text.len());
        let mut last_end = 0;

        for m in &matches {
            if m.start > last_end {
                result.push_str(&text[last_end..m.start]);
            }
            result.push_str("[REDACTED]");
            last_end = m.end;
        }

        if last_end < text.len() {
            result.push_str(&text[last_end..]);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_aws_key() {
        let text = "Use AKIAIOSFODNN7EXAMPLE for access";
        let matches = SecretDetector::detect(text);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].kind, "AWS Access Key");
    }

    #[test]
    fn detect_openai_key() {
        let text = "export OPENAI_API_KEY=sk-proj1234567890abcdefghij";
        assert!(SecretDetector::has_secrets(text));
    }

    #[test]
    fn detect_password() {
        let text = "password=mysecretpassword123";
        assert!(SecretDetector::has_secrets(text));
    }

    #[test]
    fn detect_private_key() {
        let text = "-----BEGIN RSA PRIVATE KEY-----\nMIIE...";
        assert!(SecretDetector::has_secrets(text));
    }

    #[test]
    fn no_false_positives_on_normal_text() {
        let text = "We use nextest for running tests and cargo clippy for linting.";
        assert!(!SecretDetector::has_secrets(text));
    }

    #[test]
    fn redact_replaces_secrets() {
        let text = "key is AKIAIOSFODNN7EXAMPLE ok";
        let redacted = SecretDetector::redact(text);
        assert_eq!(redacted, "key is [REDACTED] ok");
        assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn detect_fluree_api_key() {
        let text = "API key: flk_a1b2c3d4e5f6_Yx7kM9pQ2rS5tU8vW0zA";
        assert!(SecretDetector::has_secrets(text));
    }

    #[test]
    fn detect_connection_string() {
        let text = "DATABASE_URL=postgres://user:pass@host:5432/db";
        assert!(SecretDetector::has_secrets(text));
    }
}
