//! The subcommands that do not download: `info` and `create`.

use crate::cli::{CreateArgs, Subcommands};
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rtrnt::create::{self, Created};
use rtrnt::magnet::hex;
use rtrnt::{info, tui::view::human_bytes};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Runs `command` and prints what it has to say.
pub(crate) async fn run(command: Subcommands) -> Result<()> {
    match command {
        Subcommands::Info { file } => {
            let data = tokio::fs::read(&file)
                .await
                .with_context(|| format!("failed to read {}", file.display()))?;
            print!("{}", info::describe(&data)?);
            Ok(())
        }
        Subcommands::Create(args) => create(args).await,
    }
}

/// Makes the torrent, showing a progress bar while it hashes, writes it,
/// and prints its info hash and magnet link.
async fn create(args: CreateArgs) -> Result<()> {
    let options = create::Options {
        path: args.path,
        trackers: args.trackers,
        web_seeds: args.web_seeds,
        piece_length: args.piece_length,
        private: args.private,
        comment: args.comment,
        source: args.source,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok()),
    };
    let bar = ProgressBar::new(0);
    if let Ok(style) =
        ProgressStyle::with_template("hashing {bar:40.cyan/blue} {bytes}/{total_bytes} eta {eta}")
    {
        bar.set_style(style);
    }
    let hashing = bar.clone();
    let created = tokio::task::spawn_blocking(move || {
        create::build(&options, &|total| bar.set_length(total), &|len| {
            bar.inc(len)
        })
    })
    .await?;
    hashing.finish_and_clear();
    let created = created?;

    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(format!("{}.torrent", created.name)));
    tokio::fs::write(&output, &created.data)
        .await
        .with_context(|| format!("failed to write {}", output.display()))?;
    for path in &created.skipped {
        eprintln!(
            "skipped {} (a symbolic link or other special file)",
            path.display()
        );
    }
    print!("{}", summary(&created, &output));
    Ok(())
}

/// What `create` prints at the end.
fn summary(created: &Created, output: &std::path::Path) -> String {
    format!(
        "Wrote {}\nName:         {}\nSize:         {} ({} bytes)\nPieces:       {} of {}\nInfo hash:    {}\nMagnet:       {}\n",
        output.display(),
        created.name,
        human_bytes(created.size),
        created.size,
        created.pieces,
        human_bytes(created.piece_length),
        hex(&created.info_hash),
        created.magnet,
    )
}
