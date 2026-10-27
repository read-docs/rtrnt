use super::assembly::{Added, Assembly};
use super::*;
use crate::download::Misbehaved;
use sha1::Digest;
use std::net::SocketAddr;

fn peer(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// Metadata of `len` bytes, and its hash.
fn metadata(len: usize) -> (Vec<u8>, [u8; 20]) {
    let data: Vec<u8> = (0..len).map(|i| (i * 7 % 256) as u8).collect();
    let hash = sha1::Sha1::digest(&data).into();
    (data, hash)
}

fn piece_of(data: &[u8], piece: u32) -> Vec<u8> {
    let start = piece as usize * PIECE_LEN;
    data[start..data.len().min(start + PIECE_LEN)].to_vec()
}

#[test]
fn messages_roundtrip() {
    let messages = [
        Message::Request { piece: 0 },
        Message::Request { piece: u32::MAX },
        Message::Reject { piece: 3 },
        Message::Data {
            piece: 1,
            total_size: Some(31_337),
            data: b"d1:ae\x00\xff".to_vec(),
        },
        Message::Data {
            piece: 0,
            total_size: None,
            data: Vec::new(),
        },
    ];
    for message in messages {
        let payload = message.encode();
        assert_eq!(Message::decode(&payload).unwrap(), Some(message));
    }
    assert_eq!(
        Message::Request { piece: 2 }.encode(),
        b"d8:msg_typei0e5:piecei2ee"
    );
    assert_eq!(
        Message::Data {
            piece: 0,
            total_size: Some(3),
            data: b"abc".to_vec()
        }
        .encode(),
        b"d8:msg_typei1e5:piecei0e10:total_sizei3eeabc"
    );
}

#[test]
fn decoding_is_lenient_but_wants_a_dictionary() {
    // Unknown types and missing or bad pieces are ignored.
    let ignored: &[&[u8]] = &[
        b"d8:msg_typei3e5:piecei0ee",
        b"d8:msg_typei0ee",
        b"d5:piecei0ee",
        b"d8:msg_typei0e5:piecei-1ee",
        b"d8:msg_typei0e5:piecei4294967296ee",
        b"d8:msg_type1:05:piecei0ee",
    ];
    for payload in ignored {
        assert_eq!(Message::decode(payload).unwrap(), None, "{payload:?}");
    }
    // Extra keys, and a bad `total_size`, are ignored too.
    let message = Message::decode(b"d8:msg_typei1e5:piecei2e10:total_size1:x1:zi0eedata");
    assert_eq!(
        message.unwrap(),
        Some(Message::Data {
            piece: 2,
            total_size: None,
            data: b"data".to_vec()
        })
    );
    let errors: &[&[u8]] = &[b"", b"i1e", b"le", b"d8:msg_type", b"\xff"];
    for payload in errors {
        assert!(Message::decode(payload).is_err(), "accepted {payload:?}");
    }
}

#[test]
fn answers_requests_from_our_metadata() {
    let (data, _) = metadata(PIECE_LEN * 2 + 100);
    assert_eq!(
        answer(Some(&data), 0),
        Message::Data {
            piece: 0,
            total_size: Some(data.len() as u64),
            data: data[..PIECE_LEN].to_vec()
        }
    );
    let Message::Data { data: last, .. } = answer(Some(&data), 2) else {
        panic!("no data for the last piece");
    };
    assert_eq!(last, data[PIECE_LEN * 2..]);
    assert_eq!(answer(Some(&data), 3), Message::Reject { piece: 3 });
    assert_eq!(
        answer(Some(&data), u32::MAX),
        Message::Reject { piece: u32::MAX }
    );
    assert_eq!(answer(None, 0), Message::Reject { piece: 0 });
}

#[test]
fn assembles_pieces_from_several_peers() {
    let (data, hash) = metadata(PIECE_LEN * 2 + 1);
    let size = data.len() as u32;
    let mut assembly = Assembly::new(hash);
    assert_eq!(assembly.claim(peer(1)), None, "size unknown");
    assembly.join(size).unwrap();
    assembly.join(size).unwrap();
    assert!(assembly.join(size + 1).is_err());

    // Each peer gets the pieces fewest others were asked for.
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assert_eq!(assembly.claim(peer(2)), Some(1));
    assert_eq!(assembly.claim(peer(2)), Some(2));
    assert_eq!(assembly.claim(peer(1)), Some(1));
    assert_eq!(assembly.claim(peer(1)), Some(2));
    assert_eq!(assembly.claim(peer(1)), None);
    assert_eq!(assembly.outstanding(peer(1)), 3);

    let add = |assembly: &mut Assembly, from, piece| {
        assembly.add(peer(from), piece, Some(size.into()), piece_of(&data, piece))
    };
    assert_eq!(add(&mut assembly, 1, 0).unwrap(), Added::Stored);
    assert_eq!(
        add(&mut assembly, 3, 1).unwrap(),
        Added::Ignored,
        "not asked"
    );
    assert_eq!(add(&mut assembly, 1, 1).unwrap(), Added::Stored);
    assert_eq!(
        add(&mut assembly, 2, 1).unwrap(),
        Added::Ignored,
        "duplicate"
    );
    assert_eq!(assembly.claim(peer(3)), Some(2));
    assert_eq!(add(&mut assembly, 2, 2).unwrap(), Added::Complete);
    assert_eq!(assembly.metadata(), Some(&data[..]));
    assert_eq!(assembly.claim(peer(3)), None);
}

#[test]
fn rejects_bad_sizes_and_lengths() {
    let (data, hash) = metadata(PIECE_LEN + 10);
    let size = data.len() as u32;
    let mut assembly = Assembly::new(hash);
    assert!(assembly.join(0).is_err());
    assert!(assembly.join(MAX_METADATA_SIZE as u32 + 1).is_err());
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assert_eq!(assembly.claim(peer(1)), Some(1));
    let err = assembly
        .add(peer(1), 0, Some(size as u64 + 1), piece_of(&data, 0))
        .unwrap_err();
    assert!(err.is::<Misbehaved>());
    let err = assembly
        .add(peer(1), 1, Some(size.into()), piece_of(&data, 0))
        .unwrap_err();
    assert!(err.is::<Misbehaved>());
}

#[test]
fn bans_the_peer_behind_a_bad_copy() {
    let (data, hash) = metadata(PIECE_LEN * 2);
    let size = data.len() as u32;
    let mut assembly = Assembly::new(hash);
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assert_eq!(assembly.claim(peer(1)), Some(1));
    let mut bad = piece_of(&data, 1);
    bad[0] ^= 1;
    let generation = assembly.generation();
    assert_eq!(
        assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap(),
        Added::Stored
    );
    assert_eq!(
        assembly.add(peer(1), 1, None, bad).unwrap(),
        Added::Failed {
            banned: Some(peer(1))
        }
    );
    assert!(assembly.is_banned(peer(1)));
    assert_ne!(assembly.generation(), generation);
    // Peers join again, and pieces from before are void.
    assert_eq!(assembly.claim(peer(2)), None);
    assert_eq!(
        assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap(),
        Added::Ignored
    );
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(2)), Some(0));
}

#[test]
fn goes_one_peer_at_a_time_after_a_mixed_copy_fails() {
    let (data, hash) = metadata(PIECE_LEN * 2);
    let size = data.len() as u32;
    let mut bad = piece_of(&data, 1);
    bad[0] ^= 1;
    let mut assembly = Assembly::new(hash);
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assert_eq!(assembly.claim(peer(2)), Some(1));
    assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap();
    // Peer 2 lied, but which of the two did cannot be told.
    assert_eq!(
        assembly.add(peer(2), 1, None, bad.clone()).unwrap(),
        Added::Failed { banned: None }
    );
    assert!(!assembly.is_banned(peer(1)) && !assembly.is_banned(peer(2)));

    // From now on, a whole copy comes from one peer.
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(2)), Some(0));
    assert_eq!(assembly.claim(peer(1)), None);
    assert_eq!(assembly.claim(peer(2)), Some(1));
    assembly.add(peer(2), 0, None, piece_of(&data, 0)).unwrap();
    assert_eq!(
        assembly.add(peer(2), 1, None, bad).unwrap(),
        Added::Failed {
            banned: Some(peer(2))
        }
    );

    // Peer 1 takes over; when it leaves halfway, peer 3 starts over.
    assembly.join(size).unwrap();
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap();
    assembly.leave(peer(1));
    assert_eq!(assembly.claim(peer(3)), Some(0));
    assert_eq!(assembly.claim(peer(3)), Some(1));
    assembly.add(peer(3), 1, None, piece_of(&data, 1)).unwrap();
    assert_eq!(
        assembly.add(peer(3), 0, None, piece_of(&data, 0)).unwrap(),
        Added::Complete
    );
    assert_eq!(assembly.metadata(), Some(&data[..]));
}

#[test]
fn a_leaving_peer_frees_its_pieces() {
    let (data, hash) = metadata(PIECE_LEN);
    let mut assembly = Assembly::new(hash);
    assembly.join(data.len() as u32).unwrap();
    assert_eq!(assembly.claim(peer(1)), Some(0));
    assembly.leave(peer(1));
    assert_eq!(assembly.outstanding(peer(1)), 0);
    // Not asked for any more, so a late reply is ignored.
    assert_eq!(
        assembly.add(peer(1), 0, None, data.clone()).unwrap(),
        Added::Ignored
    );
    assert_eq!(assembly.claim(peer(2)), Some(0));
    assert_eq!(
        assembly.add(peer(2), 0, None, data).unwrap(),
        Added::Complete
    );
}
