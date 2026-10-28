//! The state directory, `$XDG_STATE_HOME/rtrnt`, where the resume files and
//! the session file live, and how files are written there.

use anyhow::{Context, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The state directory: `$XDG_STATE_HOME/rtrnt`, or `~/.local/state/rtrnt`
/// when that is unset. `None` if neither directory is known.
///
/// This is the XDG location on every platform, macOS included, like the
/// config file.
pub fn state_dir(xdg_state_home: Option<OsString>, home: Option<&Path>) -> Option<PathBuf> {
    // The XDG spec says to ignore a relative (or empty) value.
    let state_dir = match xdg_state_home.map(PathBuf::from) {
        Some(dir) if dir.is_absolute() => dir,
        _ => home?.join(".local").join("state"),
    };
    Some(state_dir.join("rtrnt"))
}

/// `state_dir` for this process's environment.
pub fn default_state_dir() -> Option<PathBuf> {
    state_dir(
        std::env::var_os("XDG_STATE_HOME"),
        std::env::home_dir().as_deref(),
    )
}

/// Writes `data` to a temporary file next to `path` and renames it over
/// `path`, so a reader never sees half a file.
pub fn write_atomically(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
    }
    let mut temp = path.as_os_str().to_owned();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    std::fs::write(&temp, data).with_context(|| format!("failed to write {}", temp.display()))?;
    std::fs::rename(&temp, path).with_context(|| format!("failed to replace {}", path.display()))
}
