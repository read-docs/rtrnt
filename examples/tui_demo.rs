//! The terminal UI with a made-up session that moves, for working on the
//! UI without a swarm: `cargo run --example tui_demo`. `q` stops the
//! made-up torrents, and `q` again leaves.

use rtrnt::tui::{self, demo::DemoFeed};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (_finished, finished) = tokio::sync::watch::channel(false);
    tui::run(DemoFeed::new(), finished, std::env::current_dir()?).await?;
    Ok(())
}
