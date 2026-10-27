use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

/// How the two sides of a connection stand, packed in a byte.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerFlags(u8);

impl PeerFlags {
    const AM_CHOKING: u8 = 1;
    const PEER_CHOKING: u8 = 1 << 1;
    const AM_INTERESTED: u8 = 1 << 2;
    const PEER_INTERESTED: u8 = 1 << 3;
    const FAST: u8 = 1 << 4;
    const PEX: u8 = 1 << 5;

    pub fn new(
        am_choking: bool,
        peer_choking: bool,
        am_interested: bool,
        peer_interested: bool,
        fast: bool,
        pex: bool,
    ) -> Self {
        let bit = |set: bool, bit: u8| if set { bit } else { 0 };
        Self(
            bit(am_choking, Self::AM_CHOKING)
                | bit(peer_choking, Self::PEER_CHOKING)
                | bit(am_interested, Self::AM_INTERESTED)
                | bit(peer_interested, Self::PEER_INTERESTED)
                | bit(fast, Self::FAST)
                | bit(pex, Self::PEX),
        )
    }

    /// Every connection starts choked both ways, with neither side
    /// interested.
    pub fn initial() -> Self {
        Self::new(true, true, false, false, false, false)
    }

    pub fn am_choking(self) -> bool {
        self.0 & Self::AM_CHOKING != 0
    }

    pub fn peer_choking(self) -> bool {
        self.0 & Self::PEER_CHOKING != 0
    }

    pub fn am_interested(self) -> bool {
        self.0 & Self::AM_INTERESTED != 0
    }

    pub fn peer_interested(self) -> bool {
        self.0 & Self::PEER_INTERESTED != 0
    }

    pub fn fast(self) -> bool {
        self.0 & Self::FAST != 0
    }

    pub fn pex(self) -> bool {
        self.0 & Self::PEX != 0
    }
}

/// One connected peer, kept up to date by its worker.
pub struct PeerStats {
    pub addr: SocketAddr,
    /// Whether the peer connected to us.
    pub inbound: bool,
    /// `v` from its extension handshake.
    client: OnceLock<String>,
    downloaded: AtomicU64,
    uploaded: AtomicU64,
    /// Pieces the peer has.
    pieces: AtomicU32,
    flags: AtomicU8,
}

impl PeerStats {
    pub fn new(addr: SocketAddr, inbound: bool) -> Self {
        Self {
            addr,
            inbound,
            client: OnceLock::new(),
            downloaded: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            pieces: AtomicU32::new(0),
            flags: AtomicU8::new(PeerFlags::initial().0),
        }
    }

    pub fn client(&self) -> Option<&str> {
        self.client.get().map(String::as_str)
    }

    pub(crate) fn has_client(&self) -> bool {
        self.client.get().is_some()
    }

    pub(crate) fn set_client(&self, client: &str) {
        let _ = self.client.set(client.to_string());
    }

    /// Block data received from the peer.
    pub fn downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::Relaxed)
    }

    /// Block data sent to the peer.
    pub fn uploaded(&self) -> u64 {
        self.uploaded.load(Ordering::Relaxed)
    }

    pub(crate) fn add_downloaded(&self, bytes: u64) {
        self.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_uploaded(&self, bytes: u64) {
        self.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Pieces the peer has.
    pub fn pieces(&self) -> u32 {
        self.pieces.load(Ordering::Relaxed)
    }

    pub(crate) fn set_pieces(&self, pieces: usize) {
        self.pieces
            .store(u32::try_from(pieces).unwrap_or(u32::MAX), Ordering::Relaxed);
    }

    pub fn flags(&self) -> PeerFlags {
        PeerFlags(self.flags.load(Ordering::Relaxed))
    }

    pub(crate) fn set_flags(&self, flags: PeerFlags) {
        self.flags.store(flags.0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_flags_pack_and_unpack() {
        for bits in 0..64u8 {
            let set = |i: u8| bits & (1 << i) != 0;
            let flags = PeerFlags::new(set(0), set(1), set(2), set(3), set(4), set(5));
            assert_eq!(
                [
                    flags.am_choking(),
                    flags.peer_choking(),
                    flags.am_interested(),
                    flags.peer_interested(),
                    flags.fast(),
                    flags.pex(),
                ],
                [set(0), set(1), set(2), set(3), set(4), set(5)]
            );
        }
        let peer = PeerStats::new("127.0.0.1:1".parse().unwrap(), false);
        assert_eq!(peer.flags(), PeerFlags::initial());
        let flags = PeerFlags::new(false, true, true, false, true, false);
        peer.set_flags(flags);
        assert_eq!(peer.flags(), flags);
    }
}
