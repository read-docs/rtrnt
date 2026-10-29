use super::*;
use std::path::PathBuf;

fn argv(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

fn env_of(completion: &Completion) -> std::collections::HashMap<&'static str, String> {
    completion
        .env
        .iter()
        .map(|(k, v)| (*k, v.to_string_lossy().into_owned()))
        .collect()
}

#[test]
fn the_environment_holds_the_torrent_s_details() {
    let completion = Completion::new(
        &argv(&["notify"]),
        "Some Name",
        Path::new("/data"),
        [0xab; 20],
    )
    .unwrap();
    let env = env_of(&completion);
    assert_eq!(env["RTRNT_NAME"], "Some Name");
    assert_eq!(env["RTRNT_PATH"], "/data/Some Name");
    assert_eq!(env["RTRNT_DIR"], "/data");
    assert_eq!(env["RTRNT_INFO_HASH"], "ab".repeat(20));
}

#[test]
fn no_program_means_no_hook() {
    let dir = PathBuf::from("/data");
    assert_eq!(Completion::new(&[], "x", &dir, [0; 20]), None);
    assert_eq!(Completion::new(&argv(&[""]), "x", &dir, [0; 20]), None);
}

#[test]
fn the_torrent_name_never_reaches_the_command_line() {
    let name = "$(touch pwned); `id` ; rm -rf /";
    let completion =
        Completion::new(&argv(&["hook", "--flag"]), name, Path::new("/d"), [1; 20]).unwrap();
    let command = completion.command();
    let command = command.as_std();
    assert_eq!(command.get_program(), "hook");
    let args: Vec<_> = command.get_args().collect();
    assert_eq!(args, ["--flag"]);
    let name_env = command
        .get_envs()
        .find(|(k, _)| *k == "RTRNT_NAME")
        .and_then(|(_, v)| v);
    assert_eq!(name_env, Some(std::ffi::OsStr::new(name)));
}

#[tokio::test]
async fn the_command_runs_with_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let script = r#"printf '%s|%s|%s' "$RTRNT_NAME" "$RTRNT_PATH" "$RTRNT_INFO_HASH" > "$1""#;
    let completion = Completion::new(
        &argv(&["sh", "-c", script, "sh", out.to_str().unwrap()]),
        "name with $dollar",
        Path::new("/dl"),
        [0x0f; 20],
    )
    .unwrap();
    let status = run(completion.command(), "sh".into()).await.unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        format!(
            "name with $dollar|/dl/name with $dollar|{}",
            "0f".repeat(20)
        )
    );
}

#[tokio::test]
async fn a_failing_or_missing_command_is_reported_not_fatal() {
    let failing = Completion::new(&argv(&["sh", "-c", "exit 3"]), "n", Path::new("/"), [0; 20]);
    let status = run(failing.unwrap().command(), "sh".into()).await.unwrap();
    assert_eq!(status.code(), Some(3));
    let missing = Completion::new(&argv(&["/no/such/program"]), "n", Path::new("/"), [0; 20]);
    assert!(run(missing.unwrap().command(), "x".into()).await.is_none());
}

#[tokio::test]
async fn fire_runs_it_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let completion = Completion::new(
        &argv(&[
            "sh",
            "-c",
            r#"echo $RTRNT_NAME > "$1""#,
            "sh",
            out.to_str().unwrap(),
        ]),
        "bg",
        Path::new("/"),
        [0; 20],
    )
    .unwrap();
    completion.fire();
    for _ in 0..100 {
        if std::fs::read_to_string(&out).is_ok_and(|s| s.ends_with('\n')) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "bg\n");
}
