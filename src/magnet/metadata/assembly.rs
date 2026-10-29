use super::{MAX_METADATA_SIZE, PIECE_LEN};
use crate::download::Misbehaved;
use anyhow::{Result, bail};
use sha1::Digest;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;

/// A piece of metadata and the peer that sent it.
struct Received {
    data: Vec<u8>,
    from: SocketAddr,
}

/// What became of a metadata piece handed to `Assembly::add`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Added {
    /// Kept; more pieces are needed.
    Stored,
    /// Not asked for (any more), or a duplicate; dropped.
    Ignored,
    /// That was the last piece, and the metadata matches the info hash.
    Complete,
    /// That was the last piece, but the metadata does not match the info
    /// hash, so every piece was thrown away. `banned` is the peer that sent
    /// them all, if a single one did.
    Failed { banned: Option<SocketAddr> },
}

/// The metadata being put together from pieces sent by several peers, and
/// which peer has been asked for which piece.
///
/// Pieces from different peers are combined, so a copy that fails the hash
/// check cannot always be blamed on one peer. If it came from a single peer,
/// that peer is banned; if not, the pieces are thrown away and the fetch
/// goes on one peer at a time (`exclusive`): one peer is asked for every
/// piece, the others wait until it leaves or fails, and a bad copy is then
/// always one peer's.
pub(crate) struct Assembly {
    info_hash: [u8; 20],
    /// Bumped whenever the pieces are thrown away, after a failed hash
    /// check or because every peer left; peers must then `join` again.
    generation: u64,
    /// The metadata's length, as the first peer to `join` says.
    size: Option<usize>,
    pieces: Vec<Option<Received>>,
    /// Peers that have joined since the pieces were last thrown away.
    joined: HashSet<SocketAddr>,
    /// Pieces each peer has been asked for and not sent yet.
    requested: HashMap<SocketAddr, BTreeSet<u32>>,
    /// Whether every piece must come from the same peer.
    exclusive: bool,
    /// In exclusive mode, the peer being asked.
    owner: Option<SocketAddr>,
    /// Peers that sent a whole copy that failed the hash check.
    banned: HashSet<SocketAddr>,
    /// The metadata, once complete and verified.
    metadata: Option<Vec<u8>>,
}

impl Assembly {
    pub(crate) fn new(info_hash: [u8; 20]) -> Self {
        Self {
            info_hash,
            generation: 0,
            size: None,
            pieces: Vec::new(),
            joined: HashSet::new(),
            requested: HashMap::new(),
            exclusive: false,
            owner: None,
            banned: HashSet::new(),
            metadata: None,
        }
    }

    /// Changes whenever the pieces are thrown away and peers must `join`
    /// again.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The metadata, once complete and verified.
    pub(crate) fn metadata(&self) -> Option<&[u8]> {
        self.metadata.as_deref()
    }

    /// Metadata pieces in, and the number there are (0 until a peer says).
    pub(crate) fn progress(&self) -> (usize, usize) {
        let received = self.pieces.iter().filter(|piece| piece.is_some()).count();
        (received, self.pieces.len())
    }

    /// Whether `peer` sent a copy of the metadata that failed the hash
    /// check.
    pub(crate) fn is_banned(&self, peer: SocketAddr) -> bool {
        self.banned.contains(&peer)
    }

    /// Takes on `peer`, whose extension handshake says the metadata is
    /// `size` bytes long. The first peer to join sets the size; a peer that
    /// disagrees, or names a size over `MAX_METADATA_SIZE`, is not used.
    pub(crate) fn join(&mut self, peer: SocketAddr, size: u32) -> Result<()> {
        let size = size as usize;
        if size == 0 || size > MAX_METADATA_SIZE {
            bail!("peer says the metadata is {size} bytes, over the {MAX_METADATA_SIZE} we accept");
        }
        match self.size {
            Some(known) if known != size => {
                bail!("peer says the metadata is {size} bytes, others say {known}")
            }
            Some(_) => {}
            None => {
                self.size = Some(size);
                self.pieces = std::iter::repeat_with(|| None)
                    .take(size.div_ceil(PIECE_LEN))
                    .collect();
            }
        }
        self.joined.insert(peer);
        Ok(())
    }

    /// Picks the next piece to ask `peer` for, if any: one nobody has sent
    /// and the peer was not asked for yet, asked of the fewest other peers.
    /// Once every missing piece is being asked for, they are asked of more
    /// peers too, and the first copy to arrive wins.
    pub(crate) fn claim(&mut self, peer: SocketAddr) -> Option<u32> {
        if self.metadata.is_some() || self.size.is_none() {
            return None;
        }
        if self.exclusive {
            match self.owner {
                Some(owner) if owner != peer => return None,
                Some(_) => {}
                None => self.owner = Some(peer),
            }
        }
        let mine = self.requested.get(&peer);
        let asked = |piece: u32| {
            self.requested
                .values()
                .filter(|requested| requested.contains(&piece))
                .count()
        };
        let piece = (0..self.pieces.len() as u32)
            .filter(|&piece| self.pieces[piece as usize].is_none())
            .filter(|piece| !mine.is_some_and(|mine| mine.contains(piece)))
            .min_by_key(|&piece| asked(piece))?;
        self.requested.entry(peer).or_default().insert(piece);
        Some(piece)
    }

    /// Pieces `peer` has been asked for and not sent yet.
    pub(crate) fn outstanding(&self, peer: SocketAddr) -> usize {
        self.requested.get(&peer).map_or(0, BTreeSet::len)
    }

    /// Takes a piece `peer` sent. Pieces it was not asked for are ignored,
    /// but one of the wrong length or with the wrong `total_size` is a
    /// `Misbehaved` error. With the last piece, the metadata is checked
    /// against the info hash.
    pub(crate) fn add(
        &mut self,
        peer: SocketAddr,
        piece: u32,
        total_size: Option<u64>,
        data: Vec<u8>,
    ) -> Result<Added> {
        let Some(size) = self.size else {
            return Ok(Added::Ignored);
        };
        let asked = self
            .requested
            .get_mut(&peer)
            .is_some_and(|requested| requested.remove(&piece));
        if !asked {
            return Ok(Added::Ignored);
        }
        if total_size.is_some_and(|total_size| total_size != size as u64) {
            bail!(Misbehaved(
                "peer sent a metadata piece with the wrong total size"
            ));
        }
        let start = piece as usize * PIECE_LEN;
        if data.len() != PIECE_LEN.min(size - start) {
            bail!(Misbehaved("peer sent a metadata piece of the wrong length"));
        }
        let slot = &mut self.pieces[piece as usize];
        if slot.is_some() {
            return Ok(Added::Ignored);
        }
        *slot = Some(Received { data, from: peer });
        if self.pieces.iter().any(Option::is_none) {
            return Ok(Added::Stored);
        }

        let received = || self.pieces.iter().flatten();
        let metadata: Vec<u8> = received()
            .flat_map(|piece| piece.data.iter().copied())
            .collect();
        if <[u8; 20]>::from(sha1::Sha1::digest(&metadata)) == self.info_hash {
            self.metadata = Some(metadata);
            self.requested.clear();
            return Ok(Added::Complete);
        }
        let senders: BTreeSet<SocketAddr> = received().map(|piece| piece.from).collect();
        let banned = match senders.first() {
            Some(&sender) if senders.len() == 1 => {
                self.banned.insert(sender);
                Some(sender)
            }
            _ => {
                self.exclusive = true;
                None
            }
        };
        self.restart();
        Ok(Added::Failed { banned })
    }

    /// Throws away the size and the pieces, so that peers must `join`
    /// again.
    fn restart(&mut self) {
        self.generation += 1;
        self.size = None;
        self.pieces.clear();
        self.joined.clear();
        self.requested.clear();
        self.owner = None;
    }

    /// Forgets the pieces `peer` was asked for, once it is gone. In
    /// exclusive mode, if it was the peer being asked, the pieces it sent
    /// are thrown away too, and the next peer starts over. When the last
    /// peer to have joined leaves with nothing verified, the size it gave
    /// is forgotten as well, as it may have been wrong.
    pub(crate) fn leave(&mut self, peer: SocketAddr) {
        self.requested.remove(&peer);
        if self.metadata.is_some() {
            return;
        }
        if self.joined.remove(&peer) && self.joined.is_empty() {
            self.restart();
        } else if self.owner == Some(peer) {
            self.owner = None;
            self.pieces.iter_mut().for_each(|piece| *piece = None);
        }
    }
}
