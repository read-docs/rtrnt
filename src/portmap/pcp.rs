//! PCP (RFC 6887): the successor of NAT-PMP, on the same port. Only the
//! `MAP` opcode, over IPv4, is used. A request is 60 bytes: a 24-byte
//! header and a 36-byte `MAP` payload.

use super::{Granted, Protocol, connect, transact};
use anyhow::Result;
use rand::RngCore;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

const VERSION: u8 = 2;
const OP_MAP: u8 = 1;
/// The top bit of the opcode byte, set in answers.
const RESPONSE: u8 = 0x80;
/// Length of the header, then of the whole request and answer.
const HEADER: usize = 24;
const LEN: usize = 60;

/// The IP protocol number of `protocol`.
fn protocol_number(protocol: Protocol) -> u8 {
    match protocol {
        Protocol::Tcp => 6,
        Protocol::Udp => 17,
    }
}

/// An IPv4 address as PCP writes it, as a 16-byte IPv4-mapped IPv6 one.
fn mapped(address: Ipv4Addr) -> [u8; 16] {
    address.to_ipv6_mapped().octets()
}

/// The `MAP` request for `internal_port` of `protocol`, sent from
/// `client`, with `nonce` to tell the answer from others and to remove the
/// mapping later with. A `lifetime` of 0 removes the mapping.
pub fn map_request(
    client: Ipv4Addr,
    nonce: [u8; 12],
    protocol: Protocol,
    internal_port: u16,
    suggested_port: u16,
    lifetime: Duration,
) -> [u8; LEN] {
    let mut packet = [0; LEN];
    packet[0] = VERSION;
    packet[1] = OP_MAP;
    let lifetime = u32::try_from(lifetime.as_secs()).unwrap_or(u32::MAX);
    packet[4..8].copy_from_slice(&lifetime.to_be_bytes());
    packet[8..24].copy_from_slice(&mapped(client));
    packet[24..36].copy_from_slice(&nonce);
    packet[36] = protocol_number(protocol);
    // Three reserved bytes.
    packet[40..42].copy_from_slice(&internal_port.to_be_bytes());
    packet[42..44].copy_from_slice(&suggested_port.to_be_bytes());
    // The suggested external address stays all zeros: any.
    packet
}

/// Why the server said no, from its result code.
fn result_text(code: u8) -> &'static str {
    match code {
        1 => "unsupported version",
        2 => "not authorized or refused",
        3 => "malformed request",
        4 => "unsupported opcode",
        5 => "unsupported option",
        6 => "malformed option",
        7 => "network failure",
        8 => "out of resources",
        9 => "unsupported protocol",
        10 => "quota exceeded",
        11 => "cannot provide the external port",
        12 => "address mismatch (the router sees us behind another NAT)",
        13 => "too many remote peers",
        _ => "unknown error",
    }
}

/// The IPv4 address in the 16 bytes PCP uses for addresses, if it is one.
fn ipv4_of(bytes: [u8; 16]) -> Option<Ipv4Addr> {
    match IpAddr::V6(Ipv6Addr::from(bytes)).to_canonical() {
        IpAddr::V4(address) if !address.is_unspecified() => Some(address),
        _ => None,
    }
}

/// What the server granted, from its answer to the `MAP` request with
/// `nonce`; `None` if the packet is not an answer to that. Errors in the
/// answer (a result code other than 0) are errors of the result.
pub fn parse_map_response(packet: &[u8], nonce: [u8; 12]) -> Option<Result<Granted>> {
    if packet.len() < HEADER || packet[1] != RESPONSE | OP_MAP {
        return None;
    }
    // A server that does not speak version 2 answers in its own version.
    if packet[0] != VERSION {
        return Some(Err(anyhow::anyhow!("version {} only", packet[0])));
    }
    let code = packet[3];
    if code != 0 {
        return Some(Err(anyhow::anyhow!("{} (code {code})", result_text(code))));
    }
    if packet.len() < LEN || packet[24..36] != nonce {
        return None;
    }
    let lifetime = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    let external_ip: [u8; 16] = packet[44..60].try_into().ok()?;
    Some(Ok(Granted {
        external_port: u16::from_be_bytes([packet[42], packet[43]]),
        lifetime: Duration::from_secs(u64::from(lifetime)),
        external_ip: ipv4_of(external_ip),
    }))
}

/// A PCP server.
pub struct Pcp {
    server: SocketAddr,
    /// Mixed into the nonce of each mapping, so ours differ from other
    /// clients' and from earlier runs'.
    salt: [u8; 12],
}

impl Pcp {
    /// The server at `address`, port 5351.
    pub fn new(address: Ipv4Addr) -> Self {
        Self::at(SocketAddr::from((address, super::pmp::PORT)))
    }

    /// The server at `server`.
    pub fn at(server: SocketAddr) -> Self {
        let mut salt = [0; 12];
        rand::rng().fill_bytes(&mut salt);
        Self { server, salt }
    }

    /// The nonce of the mapping of `internal_port`: the same each time, so
    /// a renewal and a removal name the mapping the first request made.
    fn nonce(&self, protocol: Protocol, internal_port: u16) -> [u8; 12] {
        let mut nonce = self.salt;
        nonce[0] ^= protocol_number(protocol);
        nonce[1] ^= internal_port.to_be_bytes()[0];
        nonce[2] ^= internal_port.to_be_bytes()[1];
        nonce
    }

    async fn request(
        &self,
        protocol: Protocol,
        internal_port: u16,
        suggested_port: u16,
        lifetime: Duration,
    ) -> Result<Granted> {
        let socket = connect(self.server).await?;
        let client = match socket.local_addr()?.ip() {
            IpAddr::V4(address) => address,
            IpAddr::V6(_) => anyhow::bail!("not an IPv4 route"),
        };
        let nonce = self.nonce(protocol, internal_port);
        let request = map_request(
            client,
            nonce,
            protocol,
            internal_port,
            suggested_port,
            lifetime,
        );
        transact(&socket, &request, |packet| {
            parse_map_response(packet, nonce)
        })
        .await
    }

    /// Maps `internal_port` of `protocol` to the same port outside, for
    /// `lifetime`. The answer has the external address too.
    pub async fn map(
        &self,
        protocol: Protocol,
        internal_port: u16,
        lifetime: Duration,
    ) -> Result<Granted> {
        self.request(protocol, internal_port, internal_port, lifetime)
            .await
    }

    /// Removes the mapping of `internal_port`.
    pub async fn unmap(&self, protocol: Protocol, internal_port: u16) -> Result<()> {
        self.request(protocol, internal_port, 0, Duration::ZERO)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests;
