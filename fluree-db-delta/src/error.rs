//! Error type for the Delta reader.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, DeltaError>;

#[derive(Debug, Error)]
pub enum DeltaError {
    /// The graph-source configuration is unusable as written.
    #[error("invalid Delta configuration: {0}")]
    Config(String),

    /// A pinned version the table's log does not retain (or never had).
    #[error("Delta table '{table}' has no readable version {version}")]
    VersionNotFound { table: String, version: u64 },

    /// An instant before the oldest version the log can still reconstruct.
    #[error("Delta table '{table}' has no version at or before {requested_ms} ms")]
    NoVersionAtTime {
        table: String,
        requested_ms: i64,
        /// Commit time of the oldest reconstructable version, when known.
        oldest_ms: Option<i64>,
    },

    /// A projected column the selected version's schema does not have.
    #[error("Delta table '{table}' has no column '{column}' at version {version}")]
    ColumnNotFound {
        table: String,
        column: String,
        version: u64,
    },

    /// A column type with no `fluree-db-tabular` representation. Raised before
    /// any rows are read, so an unsupported column never reads as nulls.
    #[error("unsupported Delta column type: {0}")]
    UnsupportedType(String),

    /// Kernel's logical batch disagreed with the schema it planned.
    #[error("Delta batch does not match its planned schema: {0}")]
    SchemaMismatch(String),

    /// Anything Kernel reports: protocol features it cannot read, log replay
    /// failures, and data files a historical version references but storage no
    /// longer holds.
    #[error("Delta table '{table}': {source}")]
    Kernel {
        table: String,
        #[source]
        source: Box<delta_kernel::Error>,
    },

    /// A catalog could not place a table or would not issue credentials for it.
    /// `denied` when the catalog answered 401/403: this principal may not read it.
    #[error("Unity Catalog, table '{table}': {message}")]
    Catalog {
        table: String,
        message: String,
        denied: bool,
    },

    /// A catalog would not list what it holds.
    #[error("Unity Catalog, {scope}: {message}")]
    CatalogListing { scope: String, message: String },

    #[error("Delta reader internal error: {0}")]
    Internal(String),
}

impl DeltaError {
    pub(crate) fn kernel(table: &str, source: delta_kernel::Error) -> Self {
        if let Some(refusal) = catalog_refusal(&source) {
            return refusal;
        }
        Self::Kernel {
            table: table.to_string(),
            source: Box::new(source),
        }
    }

    /// True when a data or log file the selected version needs is gone from
    /// storage — the `VACUUM`ed-history case, as opposed to an unsupported
    /// feature or a transport failure.
    pub fn is_missing_file(&self) -> bool {
        match self {
            Self::Kernel { source, .. } => kernel_is_not_found(source),
            _ => false,
        }
    }

    /// Whether a failure to replay an older version means the log no longer
    /// reaches it, rather than that storage could not be read (a 403, a 503,
    /// an expired credential) or that the version needs a feature Kernel does
    /// not implement. Kernel reports the log-cleaned case as a plain message,
    /// so this rules the other causes out instead of matching it.
    pub(crate) fn is_unreachable_version(&self) -> bool {
        match self {
            Self::Kernel { source, .. } => !kernel_is_access_or_unsupported(source),
            _ => false,
        }
    }
}

fn kernel_is_access_or_unsupported(error: &delta_kernel::Error) -> bool {
    use delta_kernel::Error as K;
    match error {
        K::Backtraced { source, .. } => kernel_is_access_or_unsupported(source),
        K::Unsupported(_) | K::Reqwest(_) => true,
        K::ObjectStore(e) => !matches!(e, delta_kernel::object_store::Error::NotFound { .. }),
        K::IOError(e) => e.kind() != std::io::ErrorKind::NotFound,
        _ => false,
    }
}

fn kernel_is_not_found(error: &delta_kernel::Error) -> bool {
    use delta_kernel::Error as K;
    match error {
        K::FileNotFound(_) => true,
        K::IOError(e) => e.kind() == std::io::ErrorKind::NotFound,
        K::ObjectStore(delta_kernel::object_store::Error::NotFound { .. }) => true,
        K::Backtraced { source, .. } => kernel_is_not_found(source),
        other => std::error::Error::source(other).is_some_and(source_is_not_found),
    }
}

fn source_is_not_found(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(kernel) = error.downcast_ref::<delta_kernel::Error>() {
        return kernel_is_not_found(kernel);
    }
    if let Some(delta_kernel::object_store::Error::NotFound { .. }) =
        error.downcast_ref::<delta_kernel::object_store::Error>()
    {
        return true;
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if io.kind() == std::io::ErrorKind::NotFound {
            return true;
        }
    }
    error.source().is_some_and(source_is_not_found)
}

/// A catalog's refusal reaches Kernel through the object store that asked it
/// for credentials, and says more than the layers it came through. Kernel
/// holds the store's error without naming it as its source, so it is matched.
fn catalog_refusal(error: &delta_kernel::Error) -> Option<DeltaError> {
    let store = match error {
        delta_kernel::Error::Backtraced { source, .. } => return catalog_refusal(source),
        delta_kernel::Error::ObjectStore(store) => store,
        _ => return None,
    };
    let mut cause = std::error::Error::source(store);
    while let Some(error) = cause {
        if let Some(DeltaError::Catalog {
            table,
            message,
            denied,
        }) = error.downcast_ref::<DeltaError>()
        {
            return Some(DeltaError::Catalog {
                table: table.clone(),
                message: message.clone(),
                denied: *denied,
            });
        }
        cause = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replay_failure(source: delta_kernel::Error) -> DeltaError {
        DeltaError::kernel("t", source)
    }

    #[test]
    fn only_a_log_that_cannot_reach_the_version_is_a_missing_version() {
        use delta_kernel::object_store::Error as Store;
        use delta_kernel::Error as K;

        let unreachable = [
            K::Generic("No files in log segment".to_string()),
            K::FileNotFound("_delta_log/00000000000000000001.json".to_string()),
            K::ObjectStore(Store::NotFound {
                path: "_delta_log/00000000000000000001.json".to_string(),
                source: "gone".into(),
            }),
        ];
        for source in unreachable {
            let error = replay_failure(source);
            assert!(error.is_unreachable_version(), "{error}");
        }

        let not_a_retention_problem = [
            K::ObjectStore(Store::Generic {
                store: "MicrosoftAzure",
                source: "503 Service Unavailable".into(),
            }),
            K::ObjectStore(Store::Unauthenticated {
                path: "_delta_log/00000000000000000001.json".to_string(),
                source: "token expired".into(),
            }),
            K::IOError(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            K::Unsupported("Feature 'x' is not supported".to_string()),
        ];
        for source in not_a_retention_problem {
            let error = replay_failure(source);
            assert!(!error.is_unreachable_version(), "{error}");
        }
    }
}
