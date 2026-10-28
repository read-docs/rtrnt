use super::*;
use crate::download::{DownloadConfig, SeedLimits};
use crate::stats::TorrentState;
use anyhow::anyhow;
use indicatif::{MultiProgress, ProgressDrawTarget};
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

fn client() -> Arc<Client> {
    client_in(".".as_ref())
}

fn client_in(output_dir: &std::path::Path) -> Arc<Client> {
    let config = DownloadConfig {
        output_dir: output_dir.to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
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
/// swarm, unless it is only to be checked.
fn fake_run(
    _: Arc<Client>,
    torrent: Torrent,
    options: StartOptions,
    stop: StopSignal,
) -> impl Future<Output = Result<()>> + Send + 'static {
    let how = torrent.info_hash()[0];
    async move {
        if options.recheck == Recheck::ThenPause {
            return Err(CheckedOnly.into());
        }
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
    manager.start(sources, StartOptions::default()).await;
    manager.run(closed()).await
}

#[tokio::test]
async fn every_torrent_is_started_and_its_outcome_kept() {
    let mut manager = Manager::with_runner(client(), never, fake_run);
    let idle = manager.idle();
    manager
        .start(
            vec![magnet(DONE), magnet(INTERRUPTED), magnet(FAILED)],
            StartOptions::default(),
        )
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
    manager
        .start(vec![missing, magnet(DONE)], StartOptions::default())
        .await;
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
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
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
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
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
    manager
        .start(vec![magnet(DONE)], StartOptions::default())
        .await;
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
) -> impl Fn(Arc<Client>, Torrent, StartOptions, StopSignal) -> BoxFuture<'static, Result<()>> + use<>
{
    let runs = Arc::clone(runs);
    move |client, torrent, options, stop| {
        runs.fetch_add(1, Ordering::Relaxed);
        Box::pin(fake_run(client, torrent, options, stop))
    }
}

/// `fake_run`, keeping the options of each run in `seen`.
fn recording(
    seen: &Arc<Mutex<Vec<StartOptions>>>,
) -> impl Fn(Arc<Client>, Torrent, StartOptions, StopSignal) -> BoxFuture<'static, Result<()>> + use<>
{
    let seen = Arc::clone(seen);
    move |client, torrent, options, stop| {
        seen.lock().unwrap().push(options.clone());
        Box::pin(fake_run(client, torrent, options, stop))
    }
}

const RECHECK: StartOptions = StartOptions {
    recheck: Recheck::ThenRun,
    selection: None,
    picks: None,
};

#[tokio::test]
async fn a_paused_torrent_resumes_and_the_session_can_end_paused() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let idle = manager.idle();
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    // What the real run would have published.
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());

    // It shows as pausing while its task stops.
    manager.command(Command::Pause(hash)).await;
    assert!(matches!(&manager.entries()[0].state, State::Running { stop } if *stop.borrow()));
    assert_eq!(stats.state(), TorrentState::Pausing);
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
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
    manager
        .start(vec![magnet(DONE), magnet(FAILED)], StartOptions::default())
        .await;
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

#[tokio::test]
async fn a_running_torrent_is_removed_once_its_task_has_ended() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    let idle = manager.idle();
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());

    // Asked to pause first, it is removed all the same.
    manager.command(Command::Pause(hash)).await;
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: false,
        })
        .await;
    assert!(
        matches!(&manager.entries()[0].state, State::Removing { stop, delete: false } if *stop.borrow())
    );
    assert_eq!(stats.state(), TorrentState::Removing);
    // Nothing else is done to it meanwhile.
    manager.command(Command::Pause(hash)).await;
    manager.command(Command::Resume(hash)).await;
    assert!(matches!(manager.entries()[0].state, State::Removing { .. }));
    assert_eq!(stats.state(), TorrentState::Removing);

    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(manager.entries().is_empty());
    assert!(client.stats().torrents().is_empty());
    assert!(*idle.borrow());
    let (_, notice) = client.stats().last_notice().unwrap();
    assert_eq!(notice, "removed t");
    // Stopped by the removal, it does not make the run interrupted.
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::SUCCESS);
}

#[tokio::test]
async fn a_paused_or_ended_torrent_is_removed_at_once() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    let (paused, failed) = ([UNTIL_STOPPED; 20], [FAILED; 20]);
    manager
        .start(
            vec![magnet(UNTIL_STOPPED), magnet(FAILED), magnet(DONE)],
            StartOptions::default(),
        )
        .await;
    // What the real runs would have published.
    client.stats().add_torrent(paused, "paused", ".".as_ref());
    let failed_stats = client.stats().add_torrent(failed, "failed", ".".as_ref());
    failed_stats.set_state(TorrentState::Failed("x".into()));
    manager.command(Command::Pause(paused)).await;
    manager.drive(closed()).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));

    manager
        .command(Command::Remove {
            info_hash: paused,
            delete: false,
        })
        .await;
    // A magnet link without metadata has no files to delete.
    manager
        .command(Command::Remove {
            info_hash: failed,
            delete: true,
        })
        .await;
    let hashes: Vec<_> = manager.entries().iter().map(|e| e.info_hash).collect();
    assert_eq!(hashes, [[DONE; 20]]);
    assert!(client.stats().torrents().is_empty());
    let (_, notice) = client.stats().last_notice().unwrap();
    assert_eq!(notice, "removed failed and deleted its files");
    // Removing what is not there does nothing.
    manager
        .command(Command::Remove {
            info_hash: failed,
            delete: false,
        })
        .await;
    assert_eq!(manager.entries().len(), 1);
    // The failure went with its torrent, and so did the pause.
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::SUCCESS);
}

#[tokio::test]
async fn an_added_magnet_link_starts_with_the_others() {
    let mut manager = Manager::with_runner(client(), never, fake_run);
    let idle = manager.idle();
    // A session can start with none, for the UI to add them.
    manager.expect_more();
    manager.start(Vec::new(), StartOptions::default()).await;
    assert!(*idle.borrow());

    manager.command(Command::Add(magnet(UNTIL_STOPPED))).await;
    manager.command(Command::Add(magnet(FAILED))).await;
    assert!(!*idle.borrow());
    let hashes: Vec<_> = manager.entries().iter().map(|e| e.info_hash).collect();
    assert_eq!(hashes, [[UNTIL_STOPPED; 20], [FAILED; 20]]);
    assert!(matches!(manager.entries()[0].state, State::Running { .. }));
    // With more than one, a failure is not the run's error but a failure
    // among several.
    manager.command(Command::Pause([UNTIL_STOPPED; 20])).await;
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::FAILURE);
}

#[tokio::test]
async fn a_torrent_the_session_has_is_not_added_again() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let (running, done) = ([UNTIL_STOPPED; 20], [DONE; 20]);
    manager
        .start(
            vec![magnet(UNTIL_STOPPED), magnet(DONE)],
            StartOptions::default(),
        )
        .await;
    // What the real runs would have published.
    client.stats().add_torrent(running, "running", ".".as_ref());
    client.stats().add_torrent(done, "done", ".".as_ref());
    let notice = || client.stats().last_notice().unwrap().1;

    manager.command(Command::Add(magnet(UNTIL_STOPPED))).await;
    assert_eq!(notice(), "running: already running");
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(matches!(
        manager.entries()[1].state,
        State::Ended(Outcome::Done)
    ));
    manager.command(Command::Add(magnet(DONE))).await;
    assert_eq!(notice(), "done: already added, press p to run it again");

    manager.command(Command::Pause(running)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));
    manager.command(Command::Add(magnet(UNTIL_STOPPED))).await;
    assert_eq!(notice(), "running: already added, press p to resume");

    // Nothing was started again, and no row was made.
    assert_eq!(runs.load(Ordering::Relaxed), 2);
    assert_eq!(manager.entries().len(), 2);
    assert_eq!(client.stats().torrents().len(), 2);
    assert!(matches!(manager.entries()[0].state, State::Paused));
}

#[tokio::test]
async fn a_torrent_file_that_does_not_load_is_not_added() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    manager.expect_more();
    let missing = Source::File("no such dir/x.torrent".into());
    manager.command(Command::Add(missing)).await;
    assert!(manager.entries().is_empty());
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(
        notice.starts_with("cannot add x.torrent: failed to read"),
        "{notice}"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.torrent");
    std::fs::write(&path, b"not bencode").unwrap();
    manager.command(Command::Add(Source::File(path))).await;
    assert!(manager.entries().is_empty());
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.starts_with("cannot add bad.torrent: "), "{notice}");
    // It does not count toward the exit code either.
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::SUCCESS);
}

#[tokio::test]
async fn a_session_with_no_torrents_waits_for_the_commands() {
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(client(), never, counting(&runs));
    manager.expect_more();
    manager.start(Vec::new(), StartOptions::default()).await;
    let mut idle = manager.idle();
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    tokio::task::yield_now().await;
    assert!(!run.is_finished());

    // Its torrent may end before the change is seen.
    commands.send(Command::Add(magnet(DONE))).unwrap();
    idle.changed().await.unwrap();
    idle.wait_for(|&idle| idle).await.unwrap();
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert!(!run.is_finished());
    drop(commands);
    assert_eq!(run.await.unwrap().unwrap(), ExitCode::SUCCESS);
}

#[tokio::test]
async fn a_restarted_torrent_runs_again_with_the_new_options() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, recording(&seen));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());

    manager.restart(hash, RECHECK);
    assert!(
        matches!(&manager.entries()[0].state, State::Restarting { stop, then } if *stop.borrow() && *then == RECHECK)
    );
    assert_eq!(stats.state(), TorrentState::QueuedForChecking);
    // Asking again, or to pause, does not stop it twice.
    manager.restart(hash, StartOptions::default());
    assert!(
        matches!(&manager.entries()[0].state, State::Restarting { then, .. } if *then == RECHECK)
    );

    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(matches!(&manager.entries()[0].state, State::Running { stop } if !*stop.borrow()));
    assert_eq!(*seen.lock().unwrap(), [StartOptions::default(), RECHECK]);
    // A torrent that is not running is not restarted.
    manager.restart([DONE; 20], RECHECK);
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_torrent_restarting_can_be_paused_or_removed_meanwhile() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;

    manager.restart(hash, RECHECK);
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));
    assert_eq!(runs.load(Ordering::Relaxed), 1);

    manager.command(Command::Resume(hash)).await;
    manager.restart(hash, RECHECK);
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: false,
        })
        .await;
    assert!(matches!(
        manager.entries()[0].state,
        State::Removing { delete: false, .. }
    ));
    // Restarting a torrent being removed is ignored.
    manager.restart(hash, RECHECK);
    assert!(matches!(manager.entries()[0].state, State::Removing { .. }));
    manager.drive(closed()).await;
    assert!(manager.entries().is_empty());
    assert_eq!(runs.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn adding_a_torrent_restarting_says_so() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    manager.restart([UNTIL_STOPPED; 20], RECHECK);
    // A magnet link is always loaded, so this is the session's own torrent.
    manager.command(Command::Add(magnet(UNTIL_STOPPED))).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("already running, restarting"), "{notice}");
    assert_eq!(manager.entries().len(), 1);
}

#[tokio::test]
async fn a_torrent_only_checked_ends_paused() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    let only_check = StartOptions {
        recheck: Recheck::ThenPause,
        ..StartOptions::default()
    };
    manager.start(vec![magnet(UNTIL_STOPPED)], only_check).await;
    manager.drive(closed()).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));
    // It is paused, so the session says to run again, as for any pause.
    assert_eq!(
        manager.run(closed()).await.unwrap(),
        ExitCode::from(EXIT_INTERRUPTED)
    );
}

#[tokio::test]
async fn a_recheck_restarts_a_running_torrent_and_shows_it_at_once() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, recording(&seen));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());

    manager.command(Command::Recheck(hash)).await;
    assert!(
        matches!(&manager.entries()[0].state, State::Restarting { then, .. } if then.recheck == Recheck::ThenRun)
    );
    assert_eq!(stats.state(), TorrentState::QueuedForChecking);
    // Again, while it restarts: a notice, no second stop.
    manager.command(Command::Recheck(hash)).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("already restarting"), "{notice}");

    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert_eq!(*seen.lock().unwrap(), [StartOptions::default(), RECHECK]);
}

#[tokio::test]
async fn a_torrent_being_removed_is_not_rechecked() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: false,
        })
        .await;
    manager.command(Command::Recheck(hash)).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(
        notice.ends_with("being removed, nothing to check"),
        "{notice}"
    );
    assert!(matches!(manager.entries()[0].state, State::Removing { .. }));
    manager.drive(closed()).await;
    assert_eq!(runs.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn a_magnet_link_without_metadata_is_not_rechecked() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());
    stats.set_state(TorrentState::FetchingMetadata {
        pieces: 0,
        total: 0,
        peers: 0,
    });
    manager.command(Command::Recheck(hash)).await;
    assert!(matches!(manager.entries()[0].state, State::Running { .. }));

    // Paused, with no saved torrent to load.
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    manager.command(Command::Recheck(hash)).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("nothing to check yet"), "{notice}");
    assert!(matches!(manager.entries()[0].state, State::Paused));
    assert_eq!(runs.load(Ordering::Relaxed), 1);
}

/// A runner that holds every torrent until it is stopped.
async fn until_stopped(
    _: Arc<Client>,
    _: Torrent,
    _: StartOptions,
    stop: StopSignal,
) -> Result<()> {
    stop.await;
    Err(Interrupted {
        remaining: 1,
        pieces: 2,
    }
    .into())
}

/// Writes a valid one-piece `.torrent` called `name` into `dir`, and
/// returns its path and info hash.
fn torrent_file(dir: &std::path::Path, name: &str) -> (std::path::PathBuf, [u8; 20]) {
    let torrent = crate::torrent::TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: name.to_string(),
            piece_length: 16 * 1024,
            pieces: serde_bytes::ByteBuf::from(vec![0; 20]),
            private: None,
            length: Some(16 * 1024),
            files: None,
        },
    };
    let path = dir.join(format!("{name}.torrent"));
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
    let metainfo = crate::torrent::parse_metainfo(&std::fs::read(&path).unwrap()).unwrap();
    (path, metainfo.info_hash)
}

fn saved_session(store: &SessionStore) -> SavedSession {
    store.load().unwrap()
}

/// The (info hash, paused) of each torrent in the session file.
fn saved_state(store: &SessionStore) -> Vec<([u8; 20], bool)> {
    saved_session(store)
        .torrents
        .iter()
        .map(|saved| (saved.info_hash, saved.paused))
        .collect()
}

#[tokio::test]
async fn the_session_is_saved_after_each_change() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    let mut manager = Manager::with_runner(client(), never, fake_run).with_session(store.clone());
    manager.expect_more();
    let (running, done) = ([UNTIL_STOPPED; 20], [DONE; 20]);
    manager
        .start(
            vec![magnet(UNTIL_STOPPED), magnet(DONE)],
            StartOptions::default(),
        )
        .await;
    assert_eq!(saved_state(&store), [(running, false), (done, false)]);
    assert_eq!(saved_session(&store).rate_limits, None);

    // Pausing shows at once, and stays once the task has ended.
    manager.command(Command::Pause(running)).await;
    assert_eq!(saved_state(&store), [(running, true), (done, false)]);
    while !manager.tasks.is_empty() {
        let joined = manager.tasks.join_next_with_id().await.unwrap();
        manager.ended(joined).await;
    }
    assert_eq!(saved_state(&store), [(running, true), (done, false)]);

    manager.command(Command::Resume(running)).await;
    assert_eq!(saved_state(&store), [(running, false), (done, false)]);

    manager.command(Command::SetRateLimits(100, 0)).await;
    assert_eq!(
        saved_session(&store).rate_limits,
        Some(SavedLimits {
            download: 100,
            upload: 0
        })
    );

    manager.command(Command::Add(magnet(FAILED))).await;
    assert_eq!(saved_state(&store).len(), 3);
    manager
        .command(Command::Remove {
            info_hash: done,
            delete: false,
        })
        .await;
    assert_eq!(
        saved_state(&store),
        [(running, false), ([FAILED; 20], false)]
    );
}

#[tokio::test]
async fn a_session_that_quits_saves_its_torrents_as_running() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    // The session stops at once, which ends the torrent as interrupted.
    let mut manager =
        Manager::with_runner(client(), || async {}, fake_run).with_session(store.clone());
    manager.expect_more();
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    manager.run(closed()).await.unwrap();
    assert_eq!(saved_state(&store), [([UNTIL_STOPPED; 20], false)]);
}

#[tokio::test]
async fn a_torrent_file_is_copied_and_the_copy_goes_with_the_torrent() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let (path, hash) = torrent_file(dir.path(), "t");
    let runs = Arc::new(AtomicUsize::new(0));
    let counting = {
        let runs = Arc::clone(&runs);
        move |client, torrent, options, stop| {
            runs.fetch_add(1, Ordering::Relaxed);
            Box::pin(until_stopped(client, torrent, options, stop))
                as BoxFuture<'static, Result<()>>
        }
    };
    let mut manager = Manager::with_runner(client(), never, counting).with_session(store.clone());
    manager.expect_more();
    manager.start(Vec::new(), StartOptions::default()).await;
    manager
        .command(Command::Add(Source::File(path.clone())))
        .await;

    let copy = store.torrent_path(hash);
    assert!(copy.exists());
    assert_eq!(manager.entries()[0].source, Source::File(copy.clone()));
    assert_eq!(
        saved_session(&store).torrents[0].source,
        Source::File(copy.clone())
    );

    // The original can go: pausing and resuming load the copy.
    std::fs::remove_file(&path).unwrap();
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    manager.command(Command::Resume(hash)).await;
    assert_eq!(runs.load(Ordering::Relaxed), 2);
    assert!(matches!(manager.entries()[0].state, State::Running { .. }));

    // Removing it removes the copy, and the session file forgets it.
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: false,
        })
        .await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(manager.entries().is_empty());
    assert!(!copy.exists());
    assert!(saved_session(&store).torrents.is_empty());
}

#[tokio::test]
async fn deleting_a_torrent_with_its_files_deletes_the_copy_too() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let (path, hash) = torrent_file(dir.path(), "t");
    let mut manager =
        Manager::with_runner(client_in(&dir.path().join("out")), never, until_stopped)
            .with_session(store.clone());
    manager.expect_more();
    manager.command(Command::Add(Source::File(path))).await;
    // Paused, so it goes at once.
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: true,
        })
        .await;
    assert!(manager.entries().is_empty());
    assert!(!store.torrent_path(hash).exists());
}

#[tokio::test]
async fn a_torrent_added_from_the_store_keeps_its_copy() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let (path, hash) = torrent_file(dir.path(), "t");
    // As the watch directory does: copy, then add the copy.
    let copy = store.store_torrent(&path, hash).unwrap();
    let mut manager =
        Manager::with_runner(client(), never, until_stopped).with_session(store.clone());
    manager.expect_more();
    manager
        .command(Command::Add(Source::File(copy.clone())))
        .await;
    assert_eq!(manager.entries()[0].source, Source::File(copy));
}

#[tokio::test]
async fn a_saved_session_is_restored_in_order_and_paused_torrents_stay_paused() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs))
        .with_session(store.clone());
    manager.expect_more();
    let saved = |how: u8, paused: bool| SavedTorrent {
        info_hash: [how; 20],
        source: magnet(how),
        paused,
    };
    let limits = SavedLimits {
        download: 7,
        upload: 8,
    };
    manager
        .restore(SavedSession {
            torrents: vec![
                saved(UNTIL_STOPPED, true),
                saved(DONE, false),
                // Listed twice, it is restored once.
                saved(UNTIL_STOPPED, false),
            ],
            rate_limits: Some(limits),
        })
        .await;
    manager
        .start(vec![magnet(FAILED)], StartOptions::default())
        .await;

    let hashes: Vec<_> = manager.entries().iter().map(|e| e.info_hash).collect();
    assert_eq!(hashes, [[UNTIL_STOPPED; 20], [DONE; 20], [FAILED; 20]]);
    assert!(matches!(manager.entries()[0].state, State::Paused));
    // Only the torrents not paused ran, and the paused one has a row.
    assert_eq!(runs.load(Ordering::Relaxed), 2);
    let rows = client.stats().torrents();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].info_hash(), [UNTIL_STOPPED; 20]);
    assert_eq!(rows[0].state(), TorrentState::Paused);
    // What the session had is saved again, limits included, until the UI
    // changes them.
    assert_eq!(saved_session(&store).rate_limits, Some(limits));
    assert_eq!(
        saved_state(&store),
        [
            ([UNTIL_STOPPED; 20], true),
            ([DONE; 20], false),
            ([FAILED; 20], false)
        ]
    );

    // A resume runs it, taking the row's place.
    manager.command(Command::Resume([UNTIL_STOPPED; 20])).await;
    assert_eq!(runs.load(Ordering::Relaxed), 3);
    assert_eq!(client.stats().torrents().len(), 1);
}

#[tokio::test]
async fn a_saved_torrent_that_no_longer_loads_is_reported_and_dropped() {
    let client = client();
    let mut manager = Manager::with_runner(Arc::clone(&client), never, fake_run);
    manager.expect_more();
    manager
        .restore(SavedSession {
            torrents: vec![SavedTorrent {
                info_hash: [1; 20],
                source: Source::File("/no/such/file.torrent".into()),
                paused: true,
            }],
            rate_limits: None,
        })
        .await;
    assert!(manager.entries().is_empty());
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(
        notice.starts_with("cannot restore file.torrent: "),
        "{notice}"
    );
    assert_eq!(manager.run(closed()).await.unwrap(), ExitCode::FAILURE);
}

/// A torrent of two files, "a" and "b", of one block each.
fn two_files() -> crate::torrent::TorrentFile {
    crate::torrent::TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: "two".to_string(),
            piece_length: 16 * 1024,
            pieces: serde_bytes::ByteBuf::from(vec![0; 40]),
            private: None,
            length: None,
            files: Some(vec![
                crate::torrent::FileInfo {
                    path: vec!["a".into()],
                    length: 16 * 1024,
                },
                crate::torrent::FileInfo {
                    path: vec!["b".into()],
                    length: 16 * 1024,
                },
            ]),
        },
    }
}

fn skip_second() -> FileSelection {
    FileSelection::new(vec![Priority::Normal, Priority::Skip])
}

#[tokio::test]
async fn choosing_files_restarts_a_running_torrent_with_them() {
    let client = client();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, recording(&seen));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "t", ".".as_ref());
    let set = |priorities: Vec<Priority>| Command::SetPriorities {
        info_hash: hash,
        priorities,
    };

    // Its files are not known yet.
    manager.command(set(skip_second().for_files(2))).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("no files to choose yet"), "{notice}");
    assert!(matches!(manager.entries()[0].state, State::Running { .. }));

    stats.set_torrent(&two_files());
    // A list for another number of files.
    manager.command(set(vec![Priority::Skip])).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("wrong number of files"), "{notice}");
    assert!(matches!(manager.entries()[0].state, State::Running { .. }));

    manager.command(set(skip_second().for_files(2))).await;
    let wanted = StartOptions {
        selection: Some(skip_second()),
        ..StartOptions::default()
    };
    assert!(
        matches!(&manager.entries()[0].state, State::Restarting { stop, then } if *stop.borrow() && *then == wanted)
    );
    // Changing its mind meanwhile replaces the choice.
    let high = FileSelection::new(vec![Priority::High, Priority::Normal]);
    manager.command(set(high.for_files(2))).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    let last = seen.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.selection, Some(high));
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_torrent_being_removed_or_without_files_keeps_no_choice() {
    let client = client();
    let runs = Arc::new(AtomicUsize::new(0));
    let mut manager = Manager::with_runner(Arc::clone(&client), never, counting(&runs));
    let hash = [UNTIL_STOPPED; 20];
    manager
        .start(vec![magnet(UNTIL_STOPPED)], StartOptions::default())
        .await;
    let set = Command::SetPriorities {
        info_hash: hash,
        priorities: vec![Priority::Skip],
    };
    // Paused, with no saved torrent to take the files from.
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    manager.command(set.clone()).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.ends_with("no files to choose yet"), "{notice}");

    manager.command(Command::Resume(hash)).await;
    manager
        .command(Command::Remove {
            info_hash: hash,
            delete: false,
        })
        .await;
    manager.command(set).await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(
        notice.ends_with("being removed, no files to choose"),
        "{notice}"
    );
    assert!(matches!(manager.entries()[0].state, State::Removing { .. }));
    manager.drive(closed()).await;
    assert_eq!(runs.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn a_paused_torrent_keeps_the_choice_of_files_in_its_resume_file() {
    let (out, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (path, hash) = {
        let torrent = two_files();
        let path = out.path().join("two.torrent");
        std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
        let metainfo = crate::torrent::parse_metainfo(&std::fs::read(&path).unwrap()).unwrap();
        (path, metainfo.info_hash)
    };
    let client = Arc::new(Client::new(
        DownloadConfig {
            output_dir: out.path().to_path_buf(),
            max_peers: 10,
            max_connections: 100,
            listen_port: 0,
            seed: SeedLimits::default(),
            dht: false,
            max_download_rate: 0,
            max_upload_rate: 0,
            resume_dir: Some(state.path().to_path_buf()),
        },
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
    ));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = {
        let seen = Arc::clone(&seen);
        move |client, torrent, options: StartOptions, stop| {
            seen.lock().unwrap().push(options.clone());
            Box::pin(until_stopped(client, torrent, options, stop))
                as BoxFuture<'static, Result<()>>
        }
    };
    let mut manager = Manager::with_runner(Arc::clone(&client), never, runner);
    manager
        .start(vec![Source::File(path)], StartOptions::default())
        .await;
    let stats = client.stats().add_torrent(hash, "two", out.path());
    stats.set_torrent(&two_files());
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    assert!(matches!(manager.entries()[0].state, State::Paused));

    manager
        .command(Command::SetPriorities {
            info_hash: hash,
            priorities: skip_second().for_files(2),
        })
        .await;
    let (_, notice) = client.stats().last_notice().unwrap();
    assert!(notice.contains("file choice saved"), "{notice}");
    assert!(matches!(manager.entries()[0].state, State::Paused));
    let target = resume::Target::new(client.config(), hash).unwrap();
    assert_eq!(resume::load_selection(&target, 2), Some(skip_second()));
    // The row shows it, and only the first file is wanted.
    assert_eq!(
        stats.priorities(),
        Some(vec![Priority::Normal, Priority::Skip])
    );
    assert_eq!(stats.wanted(), Some(16 * 1024));

    // Resumed with no options of its own, it goes by the resume file.
    manager.command(Command::Resume(hash)).await;
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert_eq!(seen.lock().unwrap()[1], StartOptions::default());
    // And a second change keeps the rest of the file.
    manager.command(Command::Pause(hash)).await;
    let joined = manager.tasks.join_next_with_id().await.unwrap();
    manager.ended(joined).await;
    manager
        .command(Command::SetPriorities {
            info_hash: hash,
            priorities: vec![Priority::High; 2],
        })
        .await;
    assert_eq!(
        resume::load_selection(&target, 2),
        Some(FileSelection::new(vec![Priority::High; 2]))
    );
}
