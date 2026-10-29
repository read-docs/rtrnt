//! NAT-PMP (RFC 6886): port mapping by UDP packets to the gateway on port
//! 5351. Version 0 of the protocol; a mapping is asked for, and removed,
//! with a request of one fixed size.

use super::{Granted, Protocol, connect, transact};
use anyhow::{Result, bail};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;

/// The port the gateway listens on, for NAT-PMP and PCP alike.
pub const PORT: u16 = 5351;

const VERSION: u8 = 0;
const OP_EXTERNAL_ADDRESS: u8 = 0;
/// Added to the opcode of a request in the answer to it.
const RESPONSE: u8 = 128;

/// The request for the gateway's external address.
pub fn external_address_request() -> [u8; 2] {
    [VERSION, OP_EXTERNAL_ADDRESS]
}

/// The opcode that maps `protocol`.
fn map_opcode(protocol: Protocol) -> u8 {
    match protocol {
        Protocol::Udp => 1,
        Protocol::Tcp => 2,
    }
}

/// The request to map `internal_port` of `protocol` for `lifetime`,
/// preferably to `suggested_port` on the outside. A lifetime of 0 and a
/// suggested port of 0 remove the mapping.
pub fn map_request(
    protocol: Protocol,
    internal_port: u16,
    suggested_port: u16,
    lifetime: Duration,
) -> [u8; 12] {
    let mut packet = [0; 12];
    packet[0] = VERSION;
    packet[1] = map_opcode(protocol);
    // Bytes 2 and 3 are reserved.
    packet[4..6].copy_from_slice(&internal_port.to_be_bytes());
    packet[6..8].copy_from_slice(&suggested_port.to_be_bytes());
    packet[8..12].copy_from_slice(&secs(lifetime).to_be_bytes());
    packet
}

/// `lifetime` in whole seconds, saturating.
fn secs(lifetime: Duration) -> u32 {
    u32::try_from(lifetime.as_secs()).unwrap_or(u32::MAX)
}

/// Why the gateway said no, from its result code.
fn result_text(code: u16) -> &'static str {
    match code {
        1 => "unsupported version",
        2 => "not authorized or refused",
        3 => "network failure",
        4 => "out of resources",
        5 => "unsupported opcode",
        _ => "unknown error",
    }
}

/// Checks the first 8 bytes of an answer to a request with `opcode`:
/// version, opcode, result code, and epoch. `None` if it is not an answer
/// to that request.
fn check_header(packet: &[u8], opcode: u8) -> Option<Result<()>> {
    if packet.len() < 8 || packet[0] != VERSION || packet[1] != opcode | RESPONSE {
        return None;
    }
    let code = u16::from_be_bytes([packet[2], packet[3]]);
    Some(match code {
        0 => Ok(()),
        _ => Err(anyhow::anyhow!("{} (code {code})", result_text(code))),
    })
}

/// The external address in the answer to `external_address_request`;
/// `None` if the packet is not one.
pub fn parse_external_address(packet: &[u8]) -> Option<Result<Ipv4Addr>> {
    let header = check_header(packet, OP_EXTERNAL_ADDRESS)?;
    Some(header.and_then(|()| match packet {
        [_, _, _, _, _, _, _, _, a, b, c, d, ..] => Ok(Ipv4Addr::new(*a, *b, *c, *d)),
        _ => bail!("answer too short"),
    }))
}

/// What the gateway granted, from its answer to a map request for
/// `protocol` and `internal_port`; `None` if the packet is not the answer
/// to that.
pub fn parse_map_response(
    packet: &[u8],
    protocol: Protocol,
    internal_port: u16,
) -> Option<Result<Granted>> {
    let header = check_header(packet, map_opcode(protocol))?;
    if header.is_ok() {
        if packet.len() < 16 {
            return Some(Err(anyhow::anyhow!("answer too short")));
        }
        if u16::from_be_bytes([packet[8], packet[9]]) != internal_port {
            return None;
        }
    }
    Some(header.map(|()| Granted {
        external_port: u16::from_be_bytes([packet[10], packet[11]]),
        lifetime: Duration::from_secs(u32::from_be_bytes([
            packet[12], packet[13], packet[14], packet[15],
        ]) as u64),
        external_ip: None,
    }))
}

/// A NAT-PMP gateway.
pub struct Pmp {
    gateway: SocketAddr,
}

impl Pmp {
    /// The gateway at `address`, port 5351.
    pub fn new(address: Ipv4Addr) -> Self {
        Self::at(SocketAddr::from((address, PORT)))
    }

    /// The gateway at `gateway`.
    pub fn at(gateway: SocketAddr) -> Self {
        Self { gateway }
    }

    async fn socket(&self) -> Result<UdpSocket> {
        connect(self.gateway).await
    }

    /// The address the gateway has on the outside.
    pub async fn external_address(&self) -> Result<Ipv4Addr> {
        let socket = self.socket().await?;
        transact(&socket, &external_address_request(), parse_external_address).await
    }

    /// Maps `internal_port` of `protocol` to the same port outside, for
    /// `lifetime`.
    pub async fn map(
        &self,
        protocol: Protocol,
        internal_port: u16,
        lifetime: Duration,
    ) -> Result<Granted> {
        let socket = self.socket().await?;
        let request = map_request(protocol, internal_port, internal_port, lifetime);
        transact(&socket, &request, |packet| {
            parse_map_response(packet, protocol, internal_port)
        })
        .await
    }

    /// Removes the mapping of `internal_port`.
    pub async fn unmap(&self, protocol: Protocol, internal_port: u16) -> Result<()> {
        let socket = self.socket().await?;
        let request = map_request(protocol, internal_port, 0, Duration::ZERO);
        transact(&socket, &request, |packet| {
            parse_map_response(packet, protocol, internal_port).map(|r| r.map(|_| ()))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
