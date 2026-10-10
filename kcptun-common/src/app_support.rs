//! Shared binary-support helpers used by both kcptun-client and
//! kcptun-server: log rotation/initialization, startup validation, the
//! key/session log preamble, and the diagnostic task spawns (SNMP CSV
//! logger, Ctrl-C stop hook).
//!
//! The two binaries used to carry byte-identical copies of these blocks;
//! they live here so a behaviour change cannot drift apart between them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use log::{error, info, warn};

/// Rewrite Go kcptun's short FEC aliases (`-ds` / `-ps`) to their long
/// forms so `clap` sees `--datashard` / `--parityshard` (matching the Go
/// client's flag handling).
pub fn normalize_go_alias(arg: std::ffi::OsString) -> std::ffi::OsString {
    match arg.to_str() {
        Some("-ds") => "--datashard".into(),
        Some("-ps") => "--parityshard".into(),
        _ => arg,
    }
}

/// Rotate `log_path` when it exceeds `max_size` bytes. Keeps up to 5
/// rotated copies (`path.1` … `path.5`).
pub fn rotate_log(log_path: &str, max_size: u64) {
    if let Ok(meta) = std::fs::metadata(log_path) {
        if meta.len() > max_size {
            for i in (1..5).rev() {
                let old = format!("{}.{}", log_path, i);
                let new = format!("{}.{}", log_path, i + 1);
                let _ = std::fs::rename(&old, &new);
            }
            let _ = std::fs::rename(log_path, format!("{}.1", log_path));
        }
    }
}

/// Initialize `env_logger`: append to `log_path` (rotated at 10 MiB) when
/// set, otherwise stderr. Default level is `info`, overridable via
/// `RUST_LOG` (matching Go kcptun's `-log` / log-level behaviour).
pub fn init_logging(log_path: Option<&str>) -> Result<()> {
    if let Some(log_path) = log_path.filter(|s| !s.is_empty()) {
        rotate_log(log_path, 10 * 1024 * 1024);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .target(env_logger::Target::Pipe(Box::new(file)))
            .init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format_timestamp_secs()
            .init();
        info!(
            "log level: {} (set RUST_LOG=debug for verbose output)",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        );
    }
    Ok(())
}

/// Validate QPP parameters against the derived key when `--QPP` is on.
///
/// Warnings are logged; a fatal configuration error is returned as
/// `Err("QPP: …")` (matching Go's `ValidateQPPParams`). Without the `qpp`
/// feature this is a no-op — the binaries then never set `enabled`.
pub fn validate_qpp_config(enabled: bool, count: u16, key: &[u8]) -> Result<()> {
    #[cfg(feature = "qpp")]
    if enabled {
        match crate::qpp_validate::validate_qpp_params(count, key) {
            Ok(warnings) => {
                for w in &warnings {
                    warn!("{}", w);
                }
            }
            Err(e) => {
                error!("QPP configuration error: {}", e);
                return Err(anyhow::anyhow!("QPP: {}", e));
            }
        }
    }
    #[cfg(not(feature = "qpp"))]
    let _ = (enabled, count, key);
    Ok(())
}

/// Log the key-derivation and session-tuning preamble both binaries print
/// once at startup (key fingerprint, ack-stall window, SMUX keepalive).
pub fn log_session_bootstrap(
    crypt: &str,
    key_len: usize,
    ack_stall_secs: u64,
    keepalive: u64,
    keepalive_timeout: u64,
) {
    info!(
        "key derived: crypt={}, key=[REDACTED len={}]",
        crypt, key_len
    );
    info!(
        "session watchdog: ack-stall window={}s ({}), fast path needs peer-restart evidence",
        ack_stall_secs,
        if ack_stall_secs == 0 {
            "disabled"
        } else {
            "closes after that much unacknowledged outbound data"
        }
    );
    info!(
        "smux keepalive: interval={}s timeout={}s ({})",
        keepalive,
        keepalive_timeout,
        if keepalive_timeout == 0 {
            "timeout check disabled"
        } else {
            "session closed after that much inbound silence"
        }
    );
}

/// Spawn the periodic SNMP CSV logger when `--snmplog` is set with a
/// positive `--snmpperiod`; warn (and skip) otherwise. `stop` shuts the
/// task down alongside the rest of the process.
#[cfg(feature = "tokio")]
pub fn spawn_snmp_logger(path: Option<&str>, period: Option<i64>, stop: Arc<AtomicBool>) {
    let Some(path) = path else {
        return;
    };
    let secs = period.unwrap_or(60).max(0) as u64;
    if secs > 0 && !path.is_empty() {
        let period = Duration::from_secs(secs);
        let path = path.to_string();
        knet::spawn_task(async move {
            crate::snmp_log::snmp_logger(path, period, stop).await;
        });
    } else {
        warn!("snmplog set but snmpperiod=0 or empty path — SNMP collection disabled");
    }
}

/// Spawn the Ctrl-C handler that flips the shared stop flag (runtime-
/// agnostic: `knet::ctrl_c` works on every backend).
#[cfg(feature = "tokio")]
pub fn spawn_ctrl_c(stop: Arc<AtomicBool>) {
    knet::spawn_task(async move {
        let _ = knet::ctrl_c().await;
        stop.store(true, Ordering::Relaxed);
    });
}
