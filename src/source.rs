//! What a torrent is downloaded from: a `.torrent` file or a magnet link.

use crate::magnet::{self, MagnetLink};
use anyhow::Result;
use std::path::{Path, PathBuf};

/// What to download.
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// `parse`, with a relative path taken as under `dir`.
    pub fn parse_in(arg: &str, dir: &Path) -> Result<Self> {
        Ok(match Self::parse(arg)? {
            Source::File(path) => Source::File(dir.join(path)),
            magnet => magnet,
        })
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

impl serde::Serialize for Source {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Source::File(path) => serializer.serialize_str(&path.to_string_lossy()),
            Source::Magnet(link) => serializer.serialize_str(&link.to_uri()),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Source {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}
