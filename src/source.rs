//! What a torrent is downloaded from: a `.torrent` file or a magnet link.

use crate::magnet::{self, MagnetLink};
use anyhow::Result;
use std::path::PathBuf;

/// What to download.
#[derive(Clone)]
pub enum Source {
    File(PathBuf),
    Magnet(Box<MagnetLink>),
}

impl Source {
    /// Parses a torrent argument: a magnet link if it starts with `magnet:`,
    /// otherwise the path of a .torrent file.
    pub fn parse(arg: &str) -> Result<Self> {
        if magnet::is_magnet(arg) {
            Ok(Source::Magnet(Box::new(MagnetLink::parse(arg)?)))
        } else {
            Ok(Source::File(arg.into()))
        }
    }

    /// A short name for the torrent, for logs and headings: the file's
    /// name, or the magnet link's `dn` or else its info hash.
    pub fn label(&self) -> String {
        match self {
            Source::File(path) => path.file_name().map_or_else(
                || path.display().to_string(),
                |name| name.to_string_lossy().into_owned(),
            ),
            Source::Magnet(link) => link
                .name
                .clone()
                .unwrap_or_else(|| link.info_hash.iter().map(|b| format!("{b:02x}")).collect()),
        }
    }
}
