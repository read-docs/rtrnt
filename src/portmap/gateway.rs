//! Finding the router: the default gateway of this machine.
//!
//! Linux says it in `/proc/net/route`; elsewhere (macOS, the BSDs) the
//! `route` command does. Both are parsed by pure functions, so the parsing
//! is tested on fixtures.

use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

/// How long to wait for `route`.
const ROUTE_TIMEOUT: Duration = Duration::from_secs(2);

/// The IPv4 address of the default gateway, or `None` if there is none or
/// it cannot be told.
pub async fn default_gateway() -> Option<Ipv4Addr> {
    if let Ok(text) = tokio::fs::read_to_string("/proc/net/route").await {
        return parse_proc_net_route(&text);
    }
    let output = Command::new("route")
        .args(["-n", "get", "default"])
        .kill_on_drop(true)
        .output();
    let output = timeout(ROUTE_TIMEOUT, output).await.ok()?.ok()?;
    parse_route_get(&String::from_utf8_lossy(&output.stdout))
}

/// The gateway of the default route in the text of `/proc/net/route`.
///
/// Its columns are `Iface Destination Gateway Flags ...`, the addresses
/// being hex of the native-endian `u32`; the default route has destination
/// 0 and the flags say it is up (1) and goes through a gateway (2).
pub fn parse_proc_net_route(text: &str) -> Option<Ipv4Addr> {
    text.lines().skip(1).find_map(|line| {
        let mut columns = line.split_whitespace().skip(1);
        let destination = u32::from_str_radix(columns.next()?, 16).ok()?;
        let gateway = u32::from_str_radix(columns.next()?, 16).ok()?;
        let flags = u32::from_str_radix(columns.next()?, 16).ok()?;
        let usable = destination == 0 && flags & 0b11 == 0b11 && gateway != 0;
        usable.then(|| Ipv4Addr::from(gateway.to_ne_bytes()))
    })
}

/// The gateway in the output of `route -n get default`, a line such as
/// `    gateway: 192.168.1.1`. An IPv6 gateway, or none, gives `None`.
pub fn parse_route_get(text: &str) -> Option<Ipv4Addr> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim() != "gateway" {
            return None;
        }
        value.trim().parse().ok()
    })
}

#[cfg(test)]
mod tests;
