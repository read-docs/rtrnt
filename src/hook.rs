//! The completion hook: a command run when a torrent finishes downloading.
//!
//! The command is an argument list taken from the config file or the
//! command line, and is executed directly, never through a shell. What the
//! torrent controls (its name, and so the path) reaches the command only in
//! environment variables, so it cannot change what runs:
//!
//! - `RTRNT_NAME`: the torrent's name.
//! - `RTRNT_PATH`: where its content is: the download directory joined with
//!   the name, which is the file of a single-file torrent and the directory
//!   of a multi-file one.
//! - `RTRNT_DIR`: the download directory.
//! - `RTRNT_INFO_HASH`: the info hash, in lowercase hex.

use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{info, warn};

/// A command ready to run for one torrent that has completed.
#[derive(Debug, Clone, PartialEq)]
pub struct Completion {
    argv: Vec<String>,
    env: Vec<(&'static str, OsString)>,
}

impl Completion {
    /// The hook `argv` (program first) for a torrent called `name`
    /// downloaded into `dir`, or `None` if `argv` holds no program.
    pub fn new(argv: &[String], name: &str, dir: &Path, info_hash: [u8; 20]) -> Option<Self> {
        if argv.first().is_none_or(String::is_empty) {
            return None;
        }
        let hash: String = info_hash.iter().map(|b| format!("{b:02x}")).collect();
        Some(Completion {
            argv: argv.to_vec(),
            env: vec![
                ("RTRNT_NAME", name.into()),
                ("RTRNT_PATH", dir.join(name).into_os_string()),
                ("RTRNT_DIR", dir.as_os_str().to_owned()),
                ("RTRNT_INFO_HASH", hash.into()),
            ],
        })
    }

    /// The process to run: the program and its arguments as configured, the
    /// torrent's details in the environment, and no input or output of its
    /// own (the terminal UI owns the terminal).
    fn command(&self) -> Command {
        let mut command = Command::new(&self.argv[0]);
        command
            .args(&self.argv[1..])
            .envs(self.env.iter().map(|(k, v)| (*k, v)))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    /// Runs the command in the background, on the current runtime. The task
    /// waits for the process, so it is reaped, and logs how it ended.
    pub fn fire(&self) {
        let command = self.command();
        let program = self.argv[0].clone();
        tokio::spawn(run(command, program));
    }
}

/// Runs `command` to the end and logs its exit status.
async fn run(mut command: Command, program: String) -> Option<std::process::ExitStatus> {
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            warn!(%program, "cannot run the completion hook: {e}");
            return None;
        }
    };
    match child.wait().await {
        Ok(status) if status.success() => {
            info!(%program, "completion hook finished");
            Some(status)
        }
        Ok(status) => {
            warn!(%program, %status, "completion hook failed");
            Some(status)
        }
        Err(e) => {
            warn!(%program, "cannot wait for the completion hook: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests;
