//! Tags: free-form labels on a torrent, kept in the session file, that the
//! terminal UI filters the table by (see `tui::filter`) and that can give
//! their torrents settings, from `[tags.<name>]` tables of the config file.
//!
//! A tag is a short lowercase word (spaces inside are fine); a torrent has
//! any number, in the order they were typed. A torrent's own settings win
//! over those of its tags (see `torrent_limits`); between two tags that set
//! the same thing, the one earlier in the torrent's list wins.

use crate::config::deserialize_ratio;
use crate::torrent_limits::TorrentSettings;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The settings a tag gives, by tag name: what `[tags.<name>]` holds.
pub type TagSettings = BTreeMap<String, TorrentSettings>;

/// One `[tags.<name>]` table of the config file: the limits and seed
/// settings of the torrents with the tag, each applying unless the torrent
/// sets its own.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct TagConfig {
    /// Most block data to download per second, in KiB, across the
    /// torrent's peers (0: no limit of the tag's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download: Option<u64>,
    /// The same for upload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload: Option<u64>,
    /// Seed until we uploaded this many times the torrent's size, in place
    /// of the session's `seed-ratio` (0: no ratio limit).
    #[serde(
        default,
        deserialize_with = "deserialize_ratio",
        skip_serializing_if = "Option::is_none"
    )]
    pub seed_ratio: Option<f64>,
    /// Seed this many minutes, in place of the session's `seed-time` (0: no
    /// time limit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_time: Option<u64>,
}

impl TagConfig {
    /// The settings this table gives.
    pub fn settings(&self) -> TorrentSettings {
        TorrentSettings {
            download: self.download.unwrap_or(0),
            upload: self.upload.unwrap_or(0),
            seed_ratio_milli: self
                .seed_ratio
                .map(|ratio| (ratio * 1000.0).round().min(f64::from(u32::MAX)) as u32),
            seed_time: self.seed_time,
        }
    }
}

/// Every `[tags.<name>]` table of the config file, by name.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TagTable(pub BTreeMap<String, TagConfig>);

impl TagTable {
    /// Checks the names of the tags: each must be as `normalize_tag` leaves
    /// it, so that the tag a user types finds its table.
    pub fn validate(&self) -> Result<()> {
        for name in self.0.keys() {
            if normalize_tag(name).as_deref() != Some(name) {
                bail!(
                    "invalid tag name {name:?} in the config file: use lowercase words \
                     without commas"
                );
            }
        }
        Ok(())
    }

    /// The settings of each tag.
    pub fn settings(&self) -> TagSettings {
        self.0
            .iter()
            .map(|(name, config)| (name.clone(), config.settings()))
            .collect()
    }

    /// The table as TOML, an inline table of tables.
    pub fn to_toml(&self) -> String {
        match toml::Value::try_from(self) {
            Ok(value) => value.to_string(),
            Err(_) => "{}".to_string(),
        }
    }
}

/// `text` as a tag: trimmed and lowercased; `None` if that leaves nothing,
/// or it holds a comma (which separates tags) or a control character.
pub fn normalize_tag(text: &str) -> Option<String> {
    let tag = text.trim().to_lowercase();
    let valid = !tag.is_empty() && !tag.chars().any(|c| c == ',' || c.is_control());
    valid.then_some(tag)
}

/// The tags in `text`, comma separated, as `normalize_tag` leaves them,
/// without repeats and in the order typed. Empty parts are skipped.
pub fn parse_tags(text: &str) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    for tag in text.split(',').filter_map(normalize_tag) {
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    }
    tags
}

/// The tags as the prompt and the Info tab show them.
pub fn join_tags(tags: &[String]) -> String {
    tags.join(", ")
}

/// What `tags` give a torrent, from `table`: for each setting, the first of
/// its tags that sets it.
pub fn tag_settings(tags: &[String], table: &TagSettings) -> TorrentSettings {
    tags.iter()
        .filter_map(|tag| table.get(tag))
        .fold(TorrentSettings::default(), |given, next| given.over(*next))
}

#[cfg(test)]
mod tests;
