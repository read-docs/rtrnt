use super::*;
use crate::download::{DownloadConfig, SeedLimits};
use crate::stats::TorrentState;
use anyhow::anyhow;
use indicatif::{MultiProgress, ProgressDrawTarget};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

fn client() -> Arc<Client> {
    let config = DownloadConfig {
        output_dir: ".".into(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
        recheck: false,
    };
    Arc::new(Client::new(
        config,
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
    ))
}

/// How the fake torrent with info hash `[how; 20]` ends.
const DONE: u8 = 1;
const INTERRUPTED: u8 = 2;
const FAILED: u8 = 3;
/// Runs until it is stopped, then ends as interrupted.
const UNTIL_STOPPED: u8 = 4;
const PANICS: u8 = 5;

/// A magnet link for the fake torrent that ends as `how` says.
fn magnet(how: u8) -> Source {
    let hex: String = [how; 20].iter().map(|b| format!("{b:02x}")).collect();
    Source::parse(&format!("magnet:?xt=urn:btih:{hex}")).unwrap()
}

/// Ends each torrent as the first byte of its info hash says, without a
/// swarm.
fn fake_run(
    _: Arc<Client>,
    torrent: Torrent,
    stop: StopSignal,
) -> impl Future<Output = Result<()>> + Send + 'static {
    let how = torrent.info_hash()[0];
    async move {
        let interrupted = Interrupted {
            remaining: 1,
            pieces: 2,
        };
        match how {
            DONE => Ok(()),
            INTERRUPTED => Err(interrupted.into()),
            UNTIL_STOPPED => {
                stop.await;
                Err(interrupted.into())
            }
            PANICS => panic!("the torrent panics"),
            _ => Err(anyhow!("the torrent fails")),
        }
    }
}

/// A session stop that never comes.
fn never() -> impl Future<Output = ()> + Send + 'static {
    std::future::pending()
}

/// A command channel already closed, as main gives without the UI.
fn closed() -> mpsc::UnboundedReceiver<Command> {
    mpsc::unbounded_channel().1
}

/// The exit code of a session of `sources`, with no commands.
async fn exit_code(sources: Vec<Source>) -> Result<ExitCode> {
    let mut manager = Manager::with_runner(client(), never, fake_run);
    manager.start(sources).await;
    manager.run(closed()).await
}

#[tokio::test]
async fn every_torrent_is_started_and_its_outcome_kept() {
    let mut manager = Manager::with_runner(client(), never, fake_run);
    let idle = manager.idle();
    manager
        .start(vec![magnet(DONE), magnet(INTERRUPTED), magnet(FAILED)])
        .await;
    assert!(!*idle.borrow());
    let hashes: Vec<_> = manager.entries().iter().map(|e| e.info_hash).collect();
    assert_eq!(hashes, [[DONE; 20], [INTERRUPTED; 20], [FAILED; 20]]);
    assert!(
        manager
            .entries()
            .iter()
            .all(|entry| matches!(entry.state, State::Running { .. }))
    );

    manager.drive(closed()).await;
    assert!(*idle.borrow());
    let [done, interrupted, failed] = manager.entries() else {
        panic!("three entries");
    };
    assert!(matches!(done.state, State::Ended(Outcome::Done)));
    assert!(matches!(
        interrupted.state,
        State::Ended(Outcome::Interrupted)
    ));
    assert!(matches!(failed.state, State::Ended(Outcome::Failed(_))));
}

#[tokio::test]
async fn a_single_torrent_gives_its_exit_code_or_its_error() {
    assert_eq!(
        exit_code(vec![magnet(DONE)]).await.unwrap(),
        ExitCode::SUCCESS
    );
    assert_eq!(
        exit_code(vec![magnet(INTERRUPTED)]).await.unwrap(),
        ExitCode::from(EXIT_INTERRUPTED)
    );
    let error = exit_code(vec![magnet(FAILED)]).await.unwrap_err();
    assert_eq!(error.to_string(), "the torrent fails");
    assert!(exit_code(vec![magnet(PANICS)]).await.is_err());
    let missing = Source::File("no such dir/x.torrent".into());
    let error = exit_code(vec![missing]).await.unwrap_err();
    assert!(error.to_string().contains("failed to read"), "{error}");
}

#[tokio::test]
async fn with_several_any_failure_fails_and_otherwise_an_interruption_shows() {
    assert_eq!(
        exit_code(vec![magnet(DONE), magnet(DONE)]).await.unwrap(),
        ExitCode::SUCCESS
    );
    assert_eq!(
        exit_code(vec![magnet(DONE), magnet(INTERRUPTED)])
            .await
            .unwrap(),
        ExitCode::from(EXIT_INTERRUPTED)
    );
    assert_eq!(
        exit_code(vec![magnet(INTERRUPTED), magnet(FAILED)])
            .await
            .unwrap(),
        ExitCode::FAILURE
    );
    assert_eq!(
        exit_code(vec![magnet(DONE), magnet(PANICS)]).await.unwrap(),
        ExitCode::FAILURE
    );
    // A file that cannot be loaded has no entry, but still fails the run.
    let missing = Source::File("no such dir/x.torrent".into());
    let mut manager = Manager::with_runner(client(), never, fake_run);
    manager.start(vec![missing, magnet(DONE)]).await;
    assert_eq!(manager.entries().len(), 1);
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::FAILURE);
}

#[tokio::test]
async fn the_session_stop_stops_the_torrents() {
    let (stop, stopped) = watch::channel(false);
    let session_stop = move || {
        let mut stopped = stopped.clone();
        async move {
            let _ = stopped.wait_for(|&stop| stop).await;
        }
    };
    let mut manager = Manager::with_runner(client(), session_stop, fake_run);
    manager.start(vec![magnet(UNTIL_STOPPED)]).await;
    let run = tokio::spawn(manager.run(closed()));
    tokio::task::yield_now().await;
    assert!(!run.is_finished());
    stop.send_replace(true);
    assert_eq!(
        run.await.unwrap().unwrap(),
        ExitCode::from(EXIT_INTERRUPTED)
    );
}

#[tokio::test]
async fn a_torrent_stops_on_its_own_stop() {
    let mut manager = Manager::with_runner(client(), never, fake_run);
    manager.start(vec![magnet(UNTIL_STOPPED)]).await;
    let State::Running { stop } = &manager.entries()[0].state else {
        panic!("not running");
    };
    stop.send_replace(true);
    manager.drive(closed()).await;
    // Stopped on its own, it was paused.
    assert!(matches!(manager.entries()[0].state, State::Paused));
}

#[tokio::test]
async fn the_run_ends_once_the_commands_end() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    let mut idle = manager.idle();
    manager.start(vec![magnet(DONE)]).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    idle.wait_for(|&idle| idle).await.unwrap();
    // Nothing is running, but the UI may still send commands.
    tokio::task::yield_now().await;
    assert!(!run.is_finished());

    commands.send(Command::SetRateLimits(100, 0)).unwrap();
    commands.send(Command::Stop).unwrap();
    drop(commands);
    assert_eq!(run.await.unwrap().unwrap(), ExitCode::SUCCESS);
    assert_eq!(client.stats().rate_limits(), (100, 0));
    let (_, notice) = client.stats().last_notice().unwrap();
    assert_eq!(notice, "rate limits: download 100 KiB/s, upload none");
}

/// `fake_run`, counting the runs in `runs`.
fn counting(
    runs: &Arc<AtomicUsize>,
) -> impl Fn(Arc<Client>, Torrent, StopSignal) -> BoxFuture<'static, Result<()>> + use<> {
    let runs = Arc::clone(runs);
    move |client, torrent, stop| {
        runs.fetch_add(1, Ordering::Relaxed);
        Box::pin(fake_run(client, torrent, stop))
    }
}

#[tokio::test]
async fn a_paused_torrent_resumes_and_the_session_can_end_paused() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let idle = manager.idle();
    let hash = [UNTIL_STOPPED; 20];
    manager.start(vec![magnet(UNTIL_STOPPED)]).await;
    // What the real run would have published.
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());

    // It shows as pausing while its task stops.
    manager.command(Command::Pause(hash)).await;
    assert!(matches!(&manager.entries()[0].state, State::Running { stop } if *stop.borrow()));
    assert_eq!(stats.state(), TorrentState::Pausing);
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined);
    assert!(matches!(manager.entries()[0].state, State::Paused));
    assert_eq!(stats.state(), TorrentState::Paused);
    assert!(*idle.borrow());
    // Pausing again does nothing.
    manager.command(Command::Pause(hash)).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));

    manager.command(Command::Resume(hash)).await;
    assert!(matches!(&manager.entries()[0].state, State::Running { stop } if !*stop.borrow()));
    assert!(!*idle.borrow());
    assert_eq!(runs.load(Ordering::Relaxed), 2);
    // Resuming a running torrent does nothing.
    manager.command(Command::Resume(hash)).await;
    assert_eq!(runs.load(Ordering::Relaxed), 2);

    // Paused when the session ends: run again to resume.
    let (commands, commands_rx) = mpsc::unbounded_channel();
    commands.send(Command::Pause(hash)).unwrap();
    drop(commands);
    assert_eq!(
        manager.run(commands_rx).await.unwrap(),
        ExitCode::from(EXIT_INTERRUPTED)
    );
}

#[tokio::test]
async fn an_ended_torrent_can_be_run_again() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(client(), never, counting(&runs));
    manager.start(vec![magnet(DONE), magnet(FAILED)]).await;
    manager.drive(closed()).await;
    // Neither is running, so neither pauses.
    manager.command(Command::Pause([DONE; 20])).await;
    assert!(matches!(
        manager.entries()[0].state,
        State::Ended(Outcome::Done)
    ));

    manager.command(Command::Resume([DONE; 20])).await;
    manager.command(Command::Resume([FAILED; 20])).await;
    assert_eq!(runs.load(Ordering::Relaxed), 4);
    manager.drive(closed()).await;
    let [done, failed] = manager.entries() else {
        panic!("two entries");
    };
    assert!(matches!(done.state, State::Ended(Outcome::Done)));
    assert!(matches!(failed.state, State::Ended(Outcome::Failed(_))));
}
