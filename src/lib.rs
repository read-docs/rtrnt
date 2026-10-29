//! rtrnt: a terminal BitTorrent client.

pub mod altspeed;
pub mod bencode;
pub mod blocklist;
pub mod client;
pub mod config;
pub mod dht;
pub mod download;
pub mod hook;
pub mod listener;
pub mod magnet;
pub mod manager;
pub mod peer;
mod periodic;
pub mod rate_limit;
pub mod resume;
pub mod selection;
pub mod session;
pub mod source;
pub mod state;
pub mod stats;
pub mod storage;
pub mod torrent;
pub mod tracker;
pub mod tui;
pub mod watch;
