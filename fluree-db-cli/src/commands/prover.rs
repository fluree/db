//! `fluree prover`: the prover permanent erasure needs, a separate binary
//! released with each version (`docs/cli/prover.md`).

use std::io::Read;
use std::path::{Path, PathBuf};

use colored::Colorize;
use fluree_db_api::retention::erasure::{check_prover, find_prover, PROVER_BINARY};
use sha2::{Digest, Sha256};

use crate::cli::ProverAction;
use crate::error::{CliError, CliResult};

/// Where releases are.
const RELEASES: &str = "https://github.com/fluree/db/releases/download";

/// The URL of a directory holding a release's prover assets, in place of
/// the release on GitHub: a mirror.
const DOWNLOAD_ENV: &str = "FLUREE_PROVER_DOWNLOAD_URL";

pub async fn run(action: ProverAction) -> CliResult<()> {
    match action {
        ProverAction::Install {
            version,
            dir,
            force,
        } => {
            let version = version.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
            let tag = match version.starts_with('v') {
                true => version,
                false => format!("v{version}"),
            };
            let base = std::env::var(DOWNLOAD_ENV).unwrap_or_else(|_| format!("{RELEASES}/{tag}"));
            let dir = match dir {
                Some(dir) => dir,
                None => beside_this_binary()?,
            };
            let installed = install(&base, target()?, &dir, force).await?;
            println!(
                "{} Installed {PROVER_BINARY} {tag} at {}",
                "✓".green(),
                installed.display()
            );
            Ok(())
        }
        ProverAction::Status => {
            match find_prover(None) {
                None => println!(
                    "No {PROVER_BINARY} beside this binary or on the PATH: permanent erasure is \
                     unavailable. Install it with `fluree prover install`."
                ),
                Some(prover) => match check_prover(&prover) {
                    Ok(()) => println!(
                        "{} {}: proves an image this release accepts",
                        "✓".green(),
                        prover.display()
                    ),
                    Err(why) => println!("{} {why}", "✗".red()),
                },
            }
            Ok(())
        }
    }
}

/// The prover build for this platform.
fn target() -> CliResult<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        (os, arch) => Err(CliError::Config(format!(
            "no prover is built for {os} on {arch}: permanent erasure needs Linux (x86-64 or \
             arm64) or macOS on Apple silicon"
        ))),
    }
}

fn beside_this_binary() -> CliResult<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
        .ok_or_else(|| CliError::Config("cannot tell where this binary is: pass --dir".into()))
}

/// Download `target`'s prover from `base` (`fluree-prover-<target>.zst` and
/// its `.sha256`), check it, and install it in `dir`. Nothing is installed
/// unless its checksum matches and it proves an image this release accepts.
/// Returns where it is.
async fn install(base: &str, target: &str, dir: &Path, force: bool) -> CliResult<PathBuf> {
    let dest = dir.join(format!("{PROVER_BINARY}{}", std::env::consts::EXE_SUFFIX));
    if dest.exists() && !force {
        return match check_prover(&dest) {
            Ok(()) => Ok(dest),
            Err(why) => Err(CliError::Config(format!(
                "{why}; replace it with `fluree prover install --force`"
            ))),
        };
    }
    let failed = |e: String| CliError::Config(format!("prover install failed: {e}"));
    let asset = format!(
        "{}/{PROVER_BINARY}-{target}.zst",
        base.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let fetch = |url: String| {
        let client = client.clone();
        async move {
            let response = client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{url}: {e}"))?;
            if !response.status().is_success() {
                return Err(format!("{url}: {}", response.status()));
            }
            response.bytes().await.map_err(|e| format!("{url}: {e}"))
        }
    };
    let listed = fetch(format!("{asset}.sha256")).await.map_err(failed)?;
    let expected = String::from_utf8_lossy(&listed)
        .split_whitespace()
        .next()
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| failed(format!("{asset}.sha256 is empty")))?;
    let packed = fetch(asset.clone()).await.map_err(failed)?;
    if hex::encode(Sha256::digest(&packed)) != expected {
        return Err(failed(format!("{asset} does not match its checksum")));
    }
    let mut binary = Vec::new();
    zstd::stream::read::Decoder::new(&packed[..])
        .and_then(|mut decoder| decoder.read_to_end(&mut binary))
        .map_err(|e| failed(format!("unpacking {asset}: {e}")))?;

    std::fs::create_dir_all(dir).map_err(|e| failed(format!("{}: {e}", dir.display())))?;
    let staged = tempfile::Builder::new()
        .prefix(".fluree-prover-")
        .tempfile_in(dir)
        .map_err(|e| failed(format!("{}: {e}", dir.display())))?;
    std::fs::write(staged.path(), &binary).map_err(|e| failed(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(staged.path(), std::fs::Permissions::from_mode(0o755))
            .map_err(|e| failed(e.to_string()))?;
    }
    check_prover(staged.path()).map_err(failed)?;
    staged
        .persist(&dest)
        .map_err(|e| failed(format!("{}: {}", dest.display(), e.error)))?;
    Ok(dest)
}

#[cfg(all(test, unix))]
mod tests {
    use axum::routing::get;

    use super::*;

    /// A stand-in prover printing `id` for `--image-id`, packed as a
    /// release packs it (`zstd -19 --long=27`).
    fn packed(id: &str) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 19).unwrap();
        encoder.long_distance_matching(true).unwrap();
        encoder.window_log(27).unwrap();
        encoder
            .write_all(format!("#!/bin/sh\necho '{id}'\n").as_bytes())
            .unwrap();
        encoder.finish().unwrap()
    }

    fn pinned() -> String {
        fluree_db_api::retention::erasure::REDACT_IMAGE_IDS[0]
            .iter()
            .map(|w| format!("{w:#010x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Serve `asset` and `checksum` as a release's assets for `target`.
    async fn release(target: &str, asset: Vec<u8>, checksum: String) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route(
                &format!("/{PROVER_BINARY}-{target}.zst"),
                get(move || async move { asset }),
            )
            .route(
                &format!("/{PROVER_BINARY}-{target}.zst.sha256"),
                get(move || async move { checksum }),
            );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    /// A prover installs only when its checksum matches and it proves an
    /// image this release pins; an installed one is kept unless forced.
    #[tokio::test]
    async fn only_a_checked_prover_of_this_release_is_installed() {
        let Ok(target) = target() else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join(PROVER_BINARY);
        let sum = |bytes: &[u8]| {
            format!(
                "{}  {PROVER_BINARY}-{target}.zst",
                hex::encode(Sha256::digest(bytes))
            )
        };

        let other = packed("0x00000001");
        let base = release(target, other.clone(), sum(&other)).await;
        let refused = install(&base, target, dir.path(), false).await.unwrap_err();
        assert!(refused.to_string().contains("does not accept"), "{refused}");
        assert!(!dest.exists());

        let ours = packed(&pinned());
        let base = release(target, ours.clone(), sum(b"something else")).await;
        let refused = install(&base, target, dir.path(), false).await.unwrap_err();
        assert!(refused.to_string().contains("checksum"), "{refused}");
        assert!(!dest.exists());

        let base = release(target, ours.clone(), sum(&ours)).await;
        assert_eq!(
            install(&base, target, dir.path(), false).await.unwrap(),
            dest
        );
        check_prover(&dest).unwrap();
        let base = release(target, other.clone(), sum(&other)).await;
        install(&base, target, dir.path(), false).await.unwrap();
        check_prover(&dest).unwrap();
        assert!(install(&base, target, dir.path(), true).await.is_err());
        check_prover(&dest).unwrap();
    }
}
