//! Which storage a location names, and which identity may read it.
//!
//! A storage identity serves a fixed set of schemes: AWS reads `s3://` /
//! `s3a://` (and `gs://` only through a GCS interop endpoint), a Google identity
//! reads `gs://`, the local filesystem reads `file://` and absolute paths. A path
//! outside that set is refused ([`IcebergError::SchemeNotServed`]), never
//! re-routed: an S3 client would otherwise read `gs://b/k` as `s3://b/k`, on
//! AWS, with the host's identity.

#[cfg(feature = "aws")]
use crate::error::{IcebergError, Result};

/// The host Google serves its S3-compatible XML API on.
const GCS_INTEROP_HOST: &str = "storage.googleapis.com";

/// The storage family a location's scheme names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationScheme {
    /// `s3://` or `s3a://`.
    S3,
    /// `gs://`.
    Gs,
    /// `file://` or a bare absolute path.
    Local,
    /// Anything else (no reader serves it).
    Other,
}

impl LocationScheme {
    /// The scheme `location` is written in.
    pub fn of(location: &str) -> Self {
        if location.starts_with("s3://") || location.starts_with("s3a://") {
            LocationScheme::S3
        } else if location.starts_with("gs://") {
            LocationScheme::Gs
        } else if crate::local_guard::is_local_location(location) {
            LocationScheme::Local
        } else {
            LocationScheme::Other
        }
    }
}

/// The scheme of `path` as written (`s3`, `gs`, `file`, …), for messages.
#[cfg(feature = "aws")]
pub(crate) fn scheme_label(path: &str) -> String {
    match path.split_once("://") {
        Some((scheme, _)) => scheme.to_string(),
        None if path.starts_with('/') => "file".to_string(),
        None => "(none)".to_string(),
    }
}

/// The refusal for `path`, which `identity` (serving `served`) does not read.
#[cfg(feature = "aws")]
pub(crate) fn scheme_not_served(path: &str, identity: &str, served: &str) -> IcebergError {
    IcebergError::SchemeNotServed {
        path: path.to_string(),
        scheme: scheme_label(path),
        identity: identity.to_string(),
        served: served.to_string(),
    }
}

/// Split `scheme://bucket/key` into `(bucket, key)` after `scheme://`, refusing
/// an empty bucket or a missing key.
#[cfg(feature = "aws")]
pub(crate) fn split_bucket_key<'a>(path: &'a str, rest: &'a str) -> Result<(&'a str, &'a str)> {
    let (bucket, key) = rest.split_once('/').ok_or_else(|| {
        IcebergError::storage(format!("Invalid object URI (no key path): {path}"))
    })?;
    if bucket.is_empty() {
        return Err(IcebergError::storage(format!(
            "Invalid object URI (empty bucket): {path}"
        )));
    }
    Ok((bucket, key))
}

/// Whether `endpoint` is Google's S3-interoperability endpoint, the one an
/// S3 client may read `gs://` objects through.
pub fn is_gcs_interop_endpoint(endpoint: &str) -> bool {
    reqwest::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == GCS_INTEROP_HOST)
}

/// Whether an AWS (S3) client pointed at `endpoint` serves `scheme`.
pub fn s3_serves(scheme: LocationScheme, endpoint: Option<&str>) -> bool {
    match scheme {
        LocationScheme::S3 => true,
        LocationScheme::Gs => endpoint.is_some_and(is_gcs_interop_endpoint),
        LocationScheme::Local | LocationScheme::Other => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemes_are_classified_by_prefix() {
        assert_eq!(LocationScheme::of("s3://b/k"), LocationScheme::S3);
        assert_eq!(LocationScheme::of("s3a://b/k"), LocationScheme::S3);
        assert_eq!(LocationScheme::of("gs://b/k"), LocationScheme::Gs);
        assert_eq!(LocationScheme::of("file:///t"), LocationScheme::Local);
        assert_eq!(LocationScheme::of("/t"), LocationScheme::Local);
        assert_eq!(LocationScheme::of("abfss://c@a/k"), LocationScheme::Other);
    }

    #[test]
    fn only_googles_interop_host_lets_s3_read_gs() {
        assert!(s3_serves(
            LocationScheme::Gs,
            Some("https://storage.googleapis.com")
        ));
        assert!(s3_serves(
            LocationScheme::Gs,
            Some("https://STORAGE.googleapis.com/")
        ));
        for endpoint in [
            None,
            Some("http://localhost:9000"),
            Some("https://storage.googleapis.com.attacker.example"),
            Some("https://s3.amazonaws.com"),
        ] {
            assert!(!s3_serves(LocationScheme::Gs, endpoint), "{endpoint:?}");
        }
        assert!(s3_serves(LocationScheme::S3, None));
        assert!(!s3_serves(LocationScheme::Local, None));
    }

    #[test]
    #[cfg(feature = "aws")]
    fn the_refusal_names_the_path_scheme_and_identity() {
        let err = scheme_not_served("gs://b/k", "ambient AWS", "s3://, s3a://").to_string();
        assert!(err.contains("gs://b/k"), "{err}");
        assert!(err.contains("ambient AWS"), "{err}");
        assert!(err.contains("gs:// paths"), "{err}");
    }
}
