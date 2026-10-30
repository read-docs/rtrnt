//! Local JSON control protocol over a private Unix socket. One JSON request
//! and response per line; `version` must be 1. Commands are queued, with their
//! results visible in subsequent snapshots and notices.

use crate::manager::Command;
use crate::stats::SessionStats;
use crate::tui::log_buffer::LogBuffer;
use crate::tui::snapshot::Snapshot;
use crate::tui::{Feed, TICK};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};

const MAX_LINE: usize = 8 * 1024 * 1024;
const MAX_CLIENTS: usize = 32;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// A versioned request. Snapshot selection asks for the torrent's detail tabs.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub version: u32,
    #[serde(flatten)]
    pub action: Action,
}

/// Supported requests; command fields follow `manager::Command`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Action {
    Snapshot { selected: Option<[u8; 20]> },
    Command { command: Command },
}

/// Responses distinguish an accepted command from a snapshot or protocol error.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", content = "data", rename_all = "snake_case")]
pub enum Response {
    Snapshot(Box<Snapshot>),
    Accepted,
    Error(String),
}

/// Default socket location, in a private directory inside the state directory.
pub fn default_socket() -> Result<PathBuf> {
    Ok(crate::state::default_state_dir()
        .context("no state directory; give --rpc-socket explicitly")?
        .join("rpc/control.sock"))
}

/// Owns the listener task and socket pathname. Dropping it ends client tasks
/// and removes only the socket this server created, never a replacement.
pub struct Server {
    task: AbortHandle,
    path: PathBuf,
    identity: (u64, u64),
}

impl Server {
    /// Binds a new socket in a private directory. Existing paths are never
    /// unlinked: another daemon or a stale socket must be dealt with explicitly.
    pub fn bind(
        path: PathBuf,
        stats: Arc<SessionStats>,
        logs: LogBuffer,
        idle: watch::Receiver<bool>,
        commands: mpsc::UnboundedSender<Command>,
        stop: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .context("RPC socket needs a parent directory")?;
        if !parent.exists() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
        }
        let meta = std::fs::symlink_metadata(parent)?;
        if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
            bail!(
                "RPC socket directory {} must be private (mode 0700)",
                parent.display()
            );
        }
        let listener = UnixListener::bind(&path).with_context(|| {
            format!(
                "cannot bind RPC socket {}; existing sockets are not replaced",
                path.display()
            )
        })?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let meta = std::fs::symlink_metadata(&path)?;
        let identity = (meta.dev(), meta.ino());
        let stop: Arc<dyn Fn() + Send + Sync> = Arc::new(stop);
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        if clients.len() >= MAX_CLIENTS { continue; }
                        let stats = Arc::clone(&stats);
                        let logs = logs.clone();
                        let idle = idle.clone();
                        let commands = commands.clone();
                        let stop = Arc::clone(&stop);
                        clients.spawn(async move {
                            let _ = serve_client(socket, stats, logs, idle, commands, stop).await;
                        });
                    }
                    Some(_) = clients.join_next() => {}
                }
            }
        })
        .abort_handle();
        Ok(Self {
            task,
            path,
            identity,
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        if let Ok(meta) = std::fs::symlink_metadata(&self.path)
            && (meta.dev(), meta.ino()) == self.identity
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

async fn line(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            bail!("unterminated JSON line");
        }
        let n = buf
            .iter()
            .position(|&b| b == b'\n')
            .map_or(buf.len(), |i| i + 1);
        if bytes.len() + n > MAX_LINE {
            bail!("JSON line exceeds {MAX_LINE} bytes");
        }
        bytes.extend_from_slice(&buf[..n]);
        reader.consume(n);
        if bytes.last() == Some(&b'\n') {
            return Ok(Some(bytes));
        }
    }
}

async fn write_json(writer: &mut (impl AsyncWrite + Unpin), value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    if bytes.len() >= MAX_LINE {
        bail!("JSON response exceeds {MAX_LINE} bytes");
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn serve_client(
    socket: UnixStream,
    stats: Arc<SessionStats>,
    logs: LogBuffer,
    idle: watch::Receiver<bool>,
    commands: mpsc::UnboundedSender<Command>,
    stop: Arc<dyn Fn() + Send + Sync>,
) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    while let Some(bytes) = tokio::time::timeout(IO_TIMEOUT, line(&mut reader)).await?? {
        let mut stopping = false;
        let response = match serde_json::from_slice::<Request>(&bytes) {
            Err(e) => Response::Error(format!("invalid request: {e}")),
            Ok(request) if request.version != 1 => {
                Response::Error("unsupported version (expected 1)".into())
            }
            Ok(Request {
                action: Action::Snapshot { selected },
                ..
            }) => Response::Snapshot(Box::new(Snapshot::take(
                &stats,
                &logs,
                selected,
                *idle.borrow(),
            ))),
            Ok(Request {
                action:
                    Action::Command {
                        command: Command::Stop,
                    },
                ..
            }) => {
                stopping = true;
                Response::Accepted
            }
            Ok(Request {
                action: Action::Command { command },
                ..
            }) => {
                if commands.send(command).is_ok() {
                    Response::Accepted
                } else {
                    Response::Error("session has stopped".into())
                }
            }
        };
        tokio::time::timeout(IO_TIMEOUT, write_json(&mut writer, &response)).await??;
        if stopping {
            stop();
            break;
        }
    }
    Ok(())
}

/// Sends one request, useful to scripts and local clients.
pub async fn request(path: &Path, action: Action) -> Result<Response> {
    tokio::time::timeout(IO_TIMEOUT, async {
        let mut stream = BufReader::new(UnixStream::connect(path).await?);
        exchange(&mut stream, action).await
    })
    .await
    .context("RPC request timed out")?
}

async fn exchange(stream: &mut BufReader<UnixStream>, action: Action) -> Result<Response> {
    write_json(stream.get_mut(), &Request { version: 1, action }).await?;
    let bytes = line(stream).await?.context("daemon disconnected")?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// A TUI feed backed by the daemon. The I/O worker refreshes snapshots and
/// queues commands without blocking terminal rendering. `q` only detaches.
pub struct RemoteFeed {
    snapshot: Arc<Mutex<Snapshot>>,
    selected: watch::Sender<Option<[u8; 20]>>,
    commands: mpsc::Sender<Command>,
    task: AbortHandle,
}

impl Drop for RemoteFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RemoteFeed {
    /// Connects and validates the daemon before taking over the terminal.
    pub async fn connect(path: &Path) -> Result<(Self, watch::Receiver<bool>)> {
        let mut stream = BufReader::new(UnixStream::connect(path).await?);
        let first = tokio::time::timeout(
            IO_TIMEOUT,
            exchange(&mut stream, Action::Snapshot { selected: None }),
        )
        .await??;
        let Response::Snapshot(first) = first else {
            bail!("daemon rejected snapshot request: {first:?}")
        };
        let (idle, idle_rx) = watch::channel(first.finished);
        let snapshot = Arc::new(Mutex::new(*first));
        let (selected, selection) = watch::channel(None);
        let (commands, mut command_rx) = mpsc::channel(128);
        let shared = Arc::clone(&snapshot);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(TICK);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                let action = tokio::select! {
                    _ = tick.tick() => Action::Snapshot { selected: *selection.borrow() },
                    command = command_rx.recv() => match command {
                        Some(command) => Action::Command { command },
                        None => break,
                    }
                };
                match tokio::time::timeout(IO_TIMEOUT, exchange(&mut stream, action)).await {
                    Ok(Ok(Response::Snapshot(next))) => {
                        idle.send_replace(next.finished);
                        *shared.lock().unwrap_or_else(PoisonError::into_inner) = *next;
                    }
                    Ok(Ok(Response::Accepted)) => {}
                    error => {
                        let mut snapshot = shared.lock().unwrap_or_else(PoisonError::into_inner);
                        snapshot.notice = Some(format!("daemon connection ended: {error:?}"));
                        snapshot.finished = true;
                        idle.send_replace(true);
                        break;
                    }
                }
            }
        })
        .abort_handle();
        Ok((
            Self {
                snapshot,
                selected,
                commands,
                task,
            },
            idle_rx,
        ))
    }
}

impl Feed for RemoteFeed {
    fn snapshot(&mut self, selected: Option<[u8; 20]>, _: bool) -> Snapshot {
        self.selected.send_replace(selected);
        let mut snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if snapshot
            .detail
            .as_ref()
            .is_some_and(|detail| Some(detail.info_hash) != selected)
        {
            snapshot.detail = None;
        }
        snapshot
    }

    fn command(&mut self, command: Command) -> bool {
        if command == Command::Stop {
            return false;
        }
        if self.commands.try_send(command).is_err() {
            self.snapshot
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .notice = Some("daemon command queue is full or disconnected".into());
        }
        true
    }
}

#[cfg(test)]
mod tests;
