//! Message Stream Encryption (MSE), also called Protocol Encryption (PE):
//! the handshake Vuze and libtorrent defined, and most clients speak, that
//! makes a BitTorrent connection look like random bytes.
//!
//! The side that connects is A, the other B. They agree on a secret S by
//! Diffie-Hellman (see `dh`), and SKEY, the torrent's info hash, which B
//! must have, is never sent in the clear:
//!
//! 1. A → B: Ya, PadA
//! 2. B → A: Yb, PadB
//! 3. A → B: HASH('req1', S), HASH('req2', SKEY) xor HASH('req3', S),
//!    ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA)), ENCRYPT(IA)
//! 4. B → A: ENCRYPT(VC, crypto_select, len(PadD), PadD), ENCRYPT2(payload)
//! 5. A → B: ENCRYPT2(payload)
//!
//! HASH is SHA-1, the pads are 0 to 512 random bytes, VC is 8 zero bytes
//! that let A find where PadB ends, and B finds where PadA ends by the
//! first hash. ENCRYPT is RC4 keyed with HASH('keyA', S, SKEY) from A and
//! HASH('keyB', S, SKEY) from B, the first 1024 bytes of each keystream
//! thrown away. ENCRYPT2 is that RC4 again or nothing, as B selects from
//! what A provides. IA, the initial payload, carries A's BitTorrent
//! handshake, so it costs no extra round trip.

use super::stream::PeerStream;
use anyhow::{Context, Result, bail};
use rand::Rng;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

mod dh;
mod rc4;

pub use rc4::Rc4;

use dh::{KEY_LEN, KeyPair};

/// The verification constant.
const VC: [u8; 8] = [0; 8];
/// `crypto_provide` and `crypto_select` bits.
const CRYPTO_PLAINTEXT: u32 = 0x01;
const CRYPTO_RC4: u32 = 0x02;
/// Longest pad either side may send.
const MAX_PAD: usize = 512;
/// Keystream bytes thrown away before use.
const DISCARD: usize = 1024;

/// Whether connections to peers are encrypted, the `encryption` setting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Encryption {
    /// Encrypt where the peer can: connect with MSE and fall back to plain
    /// BitTorrent if the peer does not answer it, and accept both.
    #[default]
    Prefer,
    /// Only encrypted connections, both ways.
    Require,
    /// Only plain BitTorrent, both ways.
    Off,
}

impl std::fmt::Display for Encryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::Off => "off",
        })
    }
}

/// How a connection we open greets the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    /// A plain BitTorrent handshake.
    Plaintext,
    /// An MSE handshake, then RC4.
    Encrypted,
}

impl Encryption {
    /// The handshakes to try, in order, on a connection we open, each on a
    /// fresh connection once the one before failed. With `prefer`,
    /// encryption comes first, as in libtorrent and Transmission, unless
    /// `plaintext_first`: the peer was last reached in plaintext after
    /// encryption failed.
    pub fn attempts(self, plaintext_first: bool) -> &'static [Wire] {
        match (self, plaintext_first) {
            (Self::Off, _) => &[Wire::Plaintext],
            (Self::Require, _) => &[Wire::Encrypted],
            (Self::Prefer, false) => &[Wire::Encrypted, Wire::Plaintext],
            (Self::Prefer, true) => &[Wire::Plaintext, Wire::Encrypted],
        }
    }
}

fn hash(parts: &[&[u8]]) -> [u8; 20] {
    let mut sha = Sha1::new();
    for part in parts {
        sha.update(part);
    }
    sha.finalize().into()
}

/// HASH('req2', SKEY), which step 3 hides the info hash behind.
fn torrent_hash(info_hash: &[u8; 20]) -> [u8; 20] {
    hash(&[b"req2", info_hash])
}

/// A random pad of 0 to `MAX_PAD` bytes.
fn random_pad() -> Vec<u8> {
    let mut rng = rand::rng();
    let mut pad = vec![0u8; rng.random_range(0..=MAX_PAD)];
    rng.fill(&mut pad[..]);
    pad
}

/// The keystreams for both directions: A's to B, then B's to A.
fn keystreams(secret: &[u8; KEY_LEN], info_hash: &[u8; 20]) -> (Rc4, Rc4) {
    (
        Rc4::skipping(&hash(&[b"keyA", secret, info_hash]), DISCARD),
        Rc4::skipping(&hash(&[b"keyB", secret, info_hash]), DISCARD),
    )
}

/// Reads from `stream` until `buf` holds at least `len` bytes. Takes what
/// the reads give, which may be more.
async fn fill<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut Vec<u8>, len: usize) -> Result<()> {
    while buf.len() < len {
        buf.reserve(len - buf.len());
        if stream.read_buf(buf).await? == 0 {
            bail!("peer closed the connection during the encryption handshake");
        }
    }
    Ok(())
}

/// Reads from `stream` until `pattern` turns up in `buf` after `from` and
/// at most `MAX_PAD` bytes past it, and returns where the pattern ends.
async fn find<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    from: usize,
    pattern: &[u8],
) -> Result<usize> {
    let limit = from + MAX_PAD + pattern.len();
    loop {
        let end = buf.len().min(limit);
        if let Some(at) = buf
            .get(from..end)
            .and_then(|window| window.windows(pattern.len()).position(|w| w == pattern))
        {
            return Ok(from + at + pattern.len());
        }
        if buf.len() >= limit {
            bail!("no encryption handshake in what the peer sent");
        }
        fill(stream, buf, buf.len() + 1).await?;
    }
}

/// Reads `len` bytes at `*pos` in `buf`, decrypted with `cipher`, and moves
/// `*pos` past them.
async fn take<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    pos: &mut usize,
    len: usize,
    cipher: &mut Rc4,
) -> Result<Vec<u8>> {
    fill(stream, buf, *pos + len).await?;
    let mut bytes = buf[*pos..*pos + len].to_vec();
    cipher.apply(&mut bytes);
    *pos += len;
    Ok(bytes)
}

fn be_u16(bytes: &[u8]) -> usize {
    usize::from(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

/// Opens an encrypted stream over `stream`, a connection we made to a peer
/// of the torrent `info_hash`, sending `initial` (our BitTorrent handshake)
/// with the MSE handshake. We provide RC4 only, as Transmission does when it
/// prefers encryption: plaintext after an MSE handshake would hide the
/// handshake and nothing else.
pub async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    info_hash: &[u8; 20],
    initial: &[u8],
) -> Result<PeerStream<S>> {
    initiate_providing(stream, info_hash, initial, CRYPTO_RC4).await
}

/// `initiate`, providing the methods in `provide` (`CRYPTO_*` bits).
async fn initiate_providing<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    info_hash: &[u8; 20],
    initial: &[u8],
    provide: u32,
) -> Result<PeerStream<S>> {
    let initial_len = u16::try_from(initial.len()).context("initial payload too long")?;
    let keys = KeyPair::generate();
    let mut hello = keys.public.to_vec();
    hello.extend_from_slice(&random_pad());
    stream.write_all(&hello).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    fill(&mut stream, &mut buf, KEY_LEN).await?;
    let mut their_key = [0u8; KEY_LEN];
    their_key.copy_from_slice(&buf[..KEY_LEN]);
    let secret = keys.shared_secret(&their_key);
    let (mut encrypt, mut decrypt) = keystreams(&secret, info_hash);

    let mut step3 = hash(&[b"req1", &secret]).to_vec();
    let req3 = hash(&[b"req3", &secret]);
    step3.extend(torrent_hash(info_hash).iter().zip(req3).map(|(a, b)| a ^ b));
    let start = step3.len();
    step3.extend_from_slice(&VC);
    step3.extend_from_slice(&provide.to_be_bytes());
    // No PadC.
    step3.extend_from_slice(&0u16.to_be_bytes());
    step3.extend_from_slice(&initial_len.to_be_bytes());
    step3.extend_from_slice(initial);
    encrypt.apply(&mut step3[start..]);
    stream.write_all(&step3).await?;
    stream.flush().await?;

    // B's VC, encrypted, marks where PadB ends.
    let mut vc = VC;
    decrypt.apply(&mut vc);
    let mut pos = find(&mut stream, &mut buf, KEY_LEN, &vc).await?;
    let head = take(&mut stream, &mut buf, &mut pos, 6, &mut decrypt).await?;
    let select = be_u32(&head);
    let pad_len = be_u16(&head[4..]);
    if pad_len > MAX_PAD {
        bail!("peer sent a pad of {pad_len} bytes");
    }
    take(&mut stream, &mut buf, &mut pos, pad_len, &mut decrypt).await?;
    let mut unread = buf.split_off(pos);
    match select {
        CRYPTO_RC4 if provide & CRYPTO_RC4 != 0 => {
            decrypt.apply(&mut unread);
            Ok(PeerStream::encrypted(stream, unread, decrypt, encrypt))
        }
        CRYPTO_PLAINTEXT if provide & CRYPTO_PLAINTEXT != 0 => {
            Ok(PeerStream::with_unread(stream, unread))
        }
        _ => bail!("peer selected encryption method {select:#x}, which we did not provide"),
    }
}

/// Answers the MSE handshake of a peer that connected to us over `stream`,
/// of which `received` holds the first bytes, already read. The peer's
/// torrent must be one of `torrents`, asked for once the peer has named it.
/// RC4 is selected if the peer provides it, else plaintext unless
/// `encryption` is `Require`.
///
/// Returns the stream, which reads the peer's initial payload first, and
/// the info hash of the torrent the peer asked for.
pub async fn respond<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    received: Vec<u8>,
    encryption: Encryption,
    torrents: impl FnOnce() -> Vec<[u8; 20]>,
) -> Result<(PeerStream<S>, [u8; 20])> {
    let mut buf = received;
    fill(&mut stream, &mut buf, KEY_LEN).await?;
    let mut their_key = [0u8; KEY_LEN];
    their_key.copy_from_slice(&buf[..KEY_LEN]);
    let keys = KeyPair::generate();
    let mut hello = keys.public.to_vec();
    hello.extend_from_slice(&random_pad());
    stream.write_all(&hello).await?;
    stream.flush().await?;
    let secret = keys.shared_secret(&their_key);

    let mut pos = find(&mut stream, &mut buf, KEY_LEN, &hash(&[b"req1", &secret])).await?;
    fill(&mut stream, &mut buf, pos + 20).await?;
    let req3 = hash(&[b"req3", &secret]);
    let asked: Vec<u8> = buf[pos..pos + 20]
        .iter()
        .zip(req3)
        .map(|(a, b)| a ^ b)
        .collect();
    pos += 20;
    let Some(info_hash) = torrents()
        .into_iter()
        .find(|info_hash| torrent_hash(info_hash)[..] == asked[..])
    else {
        bail!("peer asked for a torrent we do not have");
    };
    let (mut decrypt, mut encrypt) = keystreams(&secret, &info_hash);

    let head = take(&mut stream, &mut buf, &mut pos, 14, &mut decrypt).await?;
    if head[..8] != VC {
        bail!("wrong verification constant in the encryption handshake");
    }
    let provide = be_u32(&head[8..]);
    let pad_len = be_u16(&head[12..]);
    if pad_len > MAX_PAD {
        bail!("peer sent a pad of {pad_len} bytes");
    }
    let pad_and_len = take(&mut stream, &mut buf, &mut pos, pad_len + 2, &mut decrypt).await?;
    let initial_len = be_u16(&pad_and_len[pad_len..]);
    let mut unread = take(&mut stream, &mut buf, &mut pos, initial_len, &mut decrypt).await?;

    let select = if provide & CRYPTO_RC4 != 0 {
        CRYPTO_RC4
    } else if provide & CRYPTO_PLAINTEXT != 0 && encryption != Encryption::Require {
        CRYPTO_PLAINTEXT
    } else {
        bail!("peer provides no encryption method we accept ({provide:#x})");
    };
    let mut reply = VC.to_vec();
    reply.extend_from_slice(&select.to_be_bytes());
    // No PadD.
    reply.extend_from_slice(&0u16.to_be_bytes());
    encrypt.apply(&mut reply);
    stream.write_all(&reply).await?;
    stream.flush().await?;

    let mut rest = buf.split_off(pos);
    if select == CRYPTO_RC4 {
        decrypt.apply(&mut rest);
        unread.extend_from_slice(&rest);
        Ok((
            PeerStream::encrypted(stream, unread, decrypt, encrypt),
            info_hash,
        ))
    } else {
        unread.extend_from_slice(&rest);
        Ok((PeerStream::with_unread(stream, unread), info_hash))
    }
}

#[cfg(test)]
mod tests;
