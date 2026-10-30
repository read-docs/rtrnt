use super::*;
use tokio::io::{DuplexStream, duplex};

const INFO_HASH: [u8; 20] = [7; 20];
const OTHER: [u8; 20] = [8; 20];

/// Runs our initiator, providing `provide`, against our receiver with
/// `encryption`, the receiver knowing `torrents`.
async fn handshake(
    provide: u32,
    encryption: Encryption,
    torrents: Vec<[u8; 20]>,
) -> (
    Result<PeerStream<DuplexStream>>,
    Result<(PeerStream<DuplexStream>, [u8; 20])>,
) {
    let (a, b) = duplex(4096);
    let initiator = initiate_providing(a, &INFO_HASH, b"initial payload", provide);
    let receiver = async {
        // As the listener does: the first bytes are read to tell MSE from
        // plain BitTorrent.
        let mut b = b;
        let mut first = vec![0u8; 20];
        b.read_exact(&mut first).await.unwrap();
        respond(b, first, encryption, || torrents).await
    };
    tokio::join!(initiator, receiver)
}

/// Sends `message` from `from` and reads it at `to`.
async fn send_through(
    from: &mut PeerStream<DuplexStream>,
    to: &mut PeerStream<DuplexStream>,
    message: &[u8],
) {
    let mut received = vec![0u8; message.len()];
    // At once, since the pipe is smaller than some messages.
    let (sent, read) = tokio::join!(
        async {
            from.write_all(message).await?;
            from.flush().await
        },
        to.read_exact(&mut received)
    );
    sent.unwrap();
    read.unwrap();
    assert_eq!(received, message);
}

#[tokio::test]
async fn our_initiator_and_receiver_agree_on_rc4() {
    // Random pads of every length: run it a few times.
    for _ in 0..16 {
        let (a, b) = handshake(CRYPTO_RC4, Encryption::Prefer, vec![OTHER, INFO_HASH]).await;
        let mut a = a.unwrap();
        let (mut b, info_hash) = b.unwrap();
        assert_eq!(info_hash, INFO_HASH);
        assert!(a.is_encrypted() && b.is_encrypted());
        // The initial payload comes first.
        let mut initial = [0u8; 15];
        b.read_exact(&mut initial).await.unwrap();
        assert_eq!(&initial, b"initial payload");
        send_through(&mut b, &mut a, b"from B to A").await;
        send_through(&mut a, &mut b, &[0xab; 10_000]).await;
        send_through(&mut b, &mut a, &[0xcd; 10_000]).await;
    }
}

#[tokio::test]
async fn the_receiver_picks_rc4_when_both_are_provided() {
    let (a, b) = handshake(
        CRYPTO_RC4 | CRYPTO_PLAINTEXT,
        Encryption::Prefer,
        vec![INFO_HASH],
    )
    .await;
    assert!(a.unwrap().is_encrypted());
    assert!(b.unwrap().0.is_encrypted());
}

#[tokio::test]
async fn plaintext_after_the_handshake_is_selected_only_if_allowed() {
    let (a, b) = handshake(CRYPTO_PLAINTEXT, Encryption::Prefer, vec![INFO_HASH]).await;
    let mut a = a.unwrap();
    let (mut b, _) = b.unwrap();
    assert!(!a.is_encrypted() && !b.is_encrypted());
    // The initial payload was encrypted all the same.
    let mut initial = [0u8; 15];
    b.read_exact(&mut initial).await.unwrap();
    assert_eq!(&initial, b"initial payload");
    send_through(&mut a, &mut b, b"plain from A").await;
    send_through(&mut b, &mut a, b"plain from B").await;

    let (a, b) = handshake(CRYPTO_PLAINTEXT, Encryption::Require, vec![INFO_HASH]).await;
    assert!(b.is_err());
    assert!(a.is_err());
}

#[tokio::test]
async fn a_torrent_we_do_not_have_is_refused() {
    let (a, b) = handshake(CRYPTO_RC4, Encryption::Prefer, vec![OTHER]).await;
    let error = b.unwrap_err().to_string();
    assert!(error.contains("torrent we do not have"), "{error}");
    assert!(a.is_err());
}

#[tokio::test]
async fn a_plain_bittorrent_peer_fails_the_initiator() {
    let (a, mut b) = duplex(4096);
    let peer = async move {
        // A peer that expects a plain handshake gives up on what it reads.
        let mut handshake = [0u8; 68];
        b.read_exact(&mut handshake).await.unwrap();
        drop(b);
    };
    let (result, ()) = tokio::join!(initiate(a, &INFO_HASH, b"hello"), peer);
    assert!(result.is_err());
}

#[tokio::test]
async fn the_receiver_gives_up_on_bytes_without_a_handshake() {
    let (mut a, b) = duplex(4096);
    let garbage = vec![0x5a; KEY_LEN + MAX_PAD + 20];
    let sender = async move {
        a.write_all(&garbage).await.unwrap();
        // Keep the connection open, so the receiver stops for want of the
        // hash rather than for want of bytes.
        let mut sink = Vec::new();
        let _ = a.read_to_end(&mut sink).await;
    };
    let receiver = async move {
        let result = respond(b, Vec::new(), Encryption::Prefer, || vec![INFO_HASH]).await;
        result.map(|_| ())
    };
    let (result, ()) = tokio::join!(receiver, sender);
    let error = result.unwrap_err().to_string();
    assert!(error.contains("no encryption handshake"), "{error}");
}

#[test]
fn prefer_tries_encryption_first_unless_told_otherwise() {
    use Wire::{Encrypted, Plaintext};
    assert_eq!(Encryption::Prefer.attempts(false), [Encrypted, Plaintext]);
    assert_eq!(Encryption::Prefer.attempts(true), [Plaintext, Encrypted]);
    assert_eq!(Encryption::Require.attempts(true), [Encrypted]);
    assert_eq!(Encryption::Off.attempts(false), [Plaintext]);
}
