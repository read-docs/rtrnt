//! One `[[rss]]` entry of the config file: a feed to poll and what to take
//! from it.
//!
//! Everything that can be checked is checked when the file is read, so a
//! bad URL, a regular expression that does not compile or an interval that
//! is too short stops the start with a message naming the problem rather
//! than failing quietly in the background.

use regex::{Regex, RegexBuilder, RegexSet, RegexSetBuilder};
use serde::Deserialize;
use std::time::Duration;
use url::Url;

/// Minutes between polls of a feed unless it says otherwise.
pub const DEFAULT_INTERVAL_MINUTES: u64 = 15;
/// The shortest `interval`, in minutes, so a feed's server is not hammered.
pub const MIN_INTERVAL_MINUTES: u64 = 5;

/// What decides whether a feed's item is taken: its title must match any of
/// the `match` patterns and not the `exclude` one. Matching ignores case
/// (`(?-i)` in a pattern turns that off for it) and may find the pattern
/// anywhere in the title.
#[derive(Debug, Clone)]
pub struct Filter {
    include: RegexSet,
    exclude: Option<Regex>,
}

impl Filter {
    /// A filter taking titles that match any of `patterns` and not
    /// `exclude`. At least one pattern is needed, or nothing would ever be
    /// taken.
    pub fn new(patterns: &[String], exclude: Option<&str>) -> Result<Self, String> {
        if patterns.is_empty() {
            return Err("`match` needs at least one regular expression".to_string());
        }
        let include = RegexSetBuilder::new(patterns)
            .case_insensitive(true)
            .build()
            .map_err(|e| format!("invalid `match`: {e}"))?;
        let exclude = exclude
            .map(|pattern| {
                RegexBuilder::new(pattern)
                    .case_insensitive(true)
                    .build()
                    .map_err(|e| format!("invalid `exclude`: {e}"))
            })
            .transpose()?;
        Ok(Self { include, exclude })
    }

    /// Whether an item called `title` is to be taken.
    pub fn accepts(&self, title: &str) -> bool {
        self.include.is_match(title) && !self.exclude.as_ref().is_some_and(|r| r.is_match(title))
    }

    /// The `match` patterns as written.
    pub fn patterns(&self) -> &[String] {
        self.include.patterns()
    }

    /// The `exclude` pattern as written.
    pub fn exclude(&self) -> Option<&str> {
        self.exclude.as_ref().map(Regex::as_str)
    }
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        self.patterns() == other.patterns() && self.exclude() == other.exclude()
    }
}

/// A feed to follow.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(try_from = "Raw")]
pub struct Subscription {
    /// Where the feed is, `http` or `https`.
    pub url: Url,
    /// How long to wait between polls (a whole number of minutes, at least
    /// `MIN_INTERVAL_MINUTES`).
    pub interval: Duration,
    /// Which items to take.
    pub filter: Filter,
    /// Whether the torrents are added paused.
    pub paused: bool,
}

/// An entry as the file spells it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    url: String,
    /// In minutes.
    interval: Option<u64>,
    #[serde(rename = "match")]
    patterns: Vec<String>,
    exclude: Option<String>,
    #[serde(default)]
    paused: bool,
}

impl TryFrom<Raw> for Subscription {
    type Error = String;

    fn try_from(raw: Raw) -> Result<Self, String> {
        let url = Url::parse(&raw.url).map_err(|e| format!("invalid `url` {:?}: {e}", raw.url))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(format!("`url` {:?} is not an http or https URL", raw.url));
        }
        let minutes = raw.interval.unwrap_or(DEFAULT_INTERVAL_MINUTES);
        if minutes < MIN_INTERVAL_MINUTES {
            return Err(format!(
                "`interval` is in minutes and must be at least {MIN_INTERVAL_MINUTES}, not {minutes}"
            ));
        }
        Ok(Self {
            url,
            interval: Duration::from_secs(minutes.saturating_mul(60)),
            filter: Filter::new(&raw.patterns, raw.exclude.as_deref())?,
            paused: raw.paused,
        })
    }
}

/// The feeds as a TOML array of inline tables, as the config file could
/// spell them, for `--print-config`.
pub fn to_toml(feeds: &[Subscription]) -> String {
    let quote = |text: &str| toml::Value::from(text).to_string();
    let feeds: Vec<String> = feeds
        .iter()
        .map(|feed| {
            let patterns: Vec<String> = feed.filter.patterns().iter().map(|p| quote(p)).collect();
            let mut fields = vec![
                format!("url = {}", quote(feed.url.as_str())),
                format!("interval = {}", feed.interval.as_secs() / 60),
                format!("match = [{}]", patterns.join(", ")),
            ];
            if let Some(exclude) = feed.filter.exclude() {
                fields.push(format!("exclude = {}", quote(exclude)));
            }
            if feed.paused {
                fields.push("paused = true".to_string());
            }
            format!("{{ {} }}", fields.join(", "))
        })
        .collect();
    format!("[{}]", feeds.join(", "))
}

#[cfg(test)]
mod tests;
