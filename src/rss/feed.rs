//! Reading a feed document, RSS 2.0 or Atom, into its items.
//!
//! The parser is `roxmltree`: a small read-only tree with no dependencies of
//! its own and no unsafe code, which is enough for documents of a few
//! megabytes that are read whole anyway. It does not read DTDs, so a feed
//! cannot make it expand entities. A document that is not valid UTF-8 is
//! read with the bad bytes replaced, which only touches titles.
//!
//! Each item gives its title, an id to remember it by and where its torrent
//! is. The id is the `guid` (RSS) or `id` (Atom), else the item's link. The
//! torrent is the first of these that is a magnet link or an `http(s)` URL:
//! the enclosure (RSS `enclosure`, Atom `link rel="enclosure"`), an Atom link
//! of type `application/x-bittorrent`, a `magnetURI` element (as in the
//! `torrent` extension of some trackers' feeds), and last the item's link.
//! A relative URL is resolved against the feed's own.

use crate::magnet;
use anyhow::{Result, bail};
use roxmltree::{Document, Node, ParsingOptions};
use url::Url;

/// The most nodes a document may have, which bounds the memory it takes.
const NODES_LIMIT: u32 = 500_000;
/// The Atom namespace.
const ATOM: &str = "http://www.w3.org/2005/Atom";

/// Where an item's torrent is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A magnet link, as written.
    Magnet(String),
    /// A `.torrent` file to download.
    Torrent(Url),
}

/// One item of a feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedItem {
    /// What the item is called, which the feed's patterns are matched
    /// against; empty if it has none.
    pub title: String,
    /// What tells the item from the others across polls; `None` if it has
    /// nothing to go by, in which case it cannot be remembered and is
    /// skipped.
    pub id: Option<String>,
    /// Where its torrent is, if it says.
    pub target: Option<Target>,
}

/// Reads the items of the feed `data`. Relative links are under `base`,
/// the feed's URL, if given.
pub fn parse_feed(data: &[u8], base: Option<&Url>) -> Result<Vec<FeedItem>> {
    let text = String::from_utf8_lossy(data);
    // A byte order mark or a blank line before the declaration would be an
    // XML error, and servers do send them.
    let text = text.trim_start_matches('\u{feff}').trim_start();
    let options = ParsingOptions {
        allow_dtd: false,
        nodes_limit: NODES_LIMIT,
        ..ParsingOptions::default()
    };
    let document = Document::parse_with_options(text, options)
        .map_err(|e| anyhow::anyhow!("not an XML document: {e}"))?;
    let root = document.root_element();
    let name = root.tag_name().name();
    match (root.tag_name().namespace(), name) {
        (Some(ATOM), "feed") => Ok(root
            .children()
            .filter(|node| is_element(*node, "entry"))
            .map(|entry| atom_entry(entry, base))
            .collect()),
        (_, "rss") => Ok(root
            .descendants()
            .filter(|node| is_element(*node, "item"))
            .map(|item| rss_item(item, base))
            .collect()),
        _ => bail!("not an RSS or Atom feed: the document element is <{name}>"),
    }
}

/// Whether `node` is an element called `name`.
fn is_element(node: Node, name: &str) -> bool {
    node.is_element() && node.tag_name().name() == name
}

/// The children of `node` called `name` in the same namespace as `node`, so
/// that `<media:title>` is not taken for `<title>`.
fn children<'a, 'input>(
    node: Node<'a, 'input>,
    name: &'a str,
) -> impl Iterator<Item = Node<'a, 'input>> {
    let namespace = node.tag_name().namespace();
    node.children()
        .filter(move |child| is_element(*child, name) && child.tag_name().namespace() == namespace)
}

/// The text of the first child called `name`, trimmed, unless empty.
fn text_of(node: Node, name: &str) -> Option<String> {
    let child = children(node, name).next()?;
    let text: String = child
        .descendants()
        .filter(Node::is_text)
        .filter_map(|text| text.text())
        .collect();
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn rss_item(item: Node, base: Option<&Url>) -> FeedItem {
    let link = text_of(item, "link");
    let mut candidates: Vec<String> = children(item, "enclosure")
        .filter_map(|enclosure| enclosure.attribute("url"))
        .map(str::to_string)
        .collect();
    candidates.extend(
        item.descendants()
            .filter(|node| {
                node.is_element() && node.tag_name().name().eq_ignore_ascii_case("magnetURI")
            })
            .filter_map(|node| node.text())
            .map(str::to_string),
    );
    candidates.extend(link.clone());
    finish(item, text_of(item, "guid"), link, &candidates, base)
}

fn atom_entry(entry: Node, base: Option<&Url>) -> FeedItem {
    let links: Vec<(&str, &str, &str)> = children(entry, "link")
        .filter_map(|link| {
            Some((
                link.attribute("rel").unwrap_or("alternate"),
                link.attribute("type").unwrap_or(""),
                link.attribute("href")?,
            ))
        })
        .collect();
    let of = |wanted: &dyn Fn(&(&str, &str, &str)) -> bool| {
        links
            .iter()
            .filter(|link| wanted(link))
            .map(|(_, _, href)| (*href).to_string())
            .collect::<Vec<_>>()
    };
    let alternate = of(&|(rel, _, _)| *rel == "alternate");
    let mut candidates = of(&|(rel, _, _)| *rel == "enclosure");
    candidates.extend(of(&|(_, kind, _)| {
        kind.eq_ignore_ascii_case("application/x-bittorrent")
    }));
    candidates.extend(of(&|(_, _, href)| magnet::is_magnet(href.trim())));
    candidates.extend(alternate.iter().cloned());
    finish(
        entry,
        text_of(entry, "id"),
        alternate.first().cloned(),
        &candidates,
        base,
    )
}

/// The item from what was found: its id is `id`, else `link`, else the
/// first candidate.
fn finish(
    node: Node,
    id: Option<String>,
    link: Option<String>,
    candidates: &[String],
    base: Option<&Url>,
) -> FeedItem {
    let id = id
        .or(link)
        .or_else(|| candidates.first().map(|c| c.trim().to_string()))
        .filter(|id| !id.is_empty());
    FeedItem {
        title: text_of(node, "title").unwrap_or_default(),
        id,
        target: candidates.iter().find_map(|c| target(c, base)),
    }
}

/// `text` as a place to get a torrent from, if it is one.
fn target(text: &str, base: Option<&Url>) -> Option<Target> {
    let text = text.trim();
    if magnet::is_magnet(text) {
        return Some(Target::Magnet(text.to_string()));
    }
    let url = match Url::parse(text) {
        Ok(url) => url,
        Err(url::ParseError::RelativeUrlWithoutBase) => base?.join(text).ok()?,
        Err(_) => return None,
    };
    matches!(url.scheme(), "http" | "https").then_some(Target::Torrent(url))
}

#[cfg(test)]
mod tests;
