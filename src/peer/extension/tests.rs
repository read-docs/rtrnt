use super::*;

#[test]
fn our_handshake_roundtrips() {
    let ip: IpAddr = "10.1.2.3".parse().unwrap();
    let advertised = Advertised {
        listen_port: Some(6881),
        ..Advertised::default()
    };
    let ours = ExtendedHandshake::ours(advertised, ip);
    assert_eq!(ours.id(pex::NAME), Some(pex::ID));
    assert_eq!(ours.id(UT_METADATA), Some(UT_METADATA_ID));
    assert_eq!(ours.client.as_deref(), Some(CLIENT));
    assert!(CLIENT.starts_with("rtrnt "));

    let payload = ours.encode().unwrap();
    assert_eq!(
        payload,
        format!(
            "d1:md11:ut_metadatai2e6:ut_pexi1ee1:pi6881e4:reqqi500e1:v{}:{}6:yourip4:\x0a\x01\x02\x03e",
            CLIENT.len(),
            CLIENT
        )
        .as_bytes()
    );
    assert_eq!(ExtendedHandshake::decode(&payload).unwrap(), ours);

    // With the metadata to send, its size goes out too.
    let advertised = Advertised {
        metadata_size: Some(5205),
        ..Advertised::default()
    };
    let ours = ExtendedHandshake::ours(advertised, ip);
    let payload = ours.encode().unwrap();
    let expected = b"13:metadata_sizei5205e";
    assert!(payload.windows(expected.len()).any(|w| w == expected));
    assert_eq!(ExtendedHandshake::decode(&payload).unwrap(), ours);
}

#[test]
fn roundtrips_every_field() {
    let handshake = ExtendedHandshake {
        extensions: [("ut_metadata".to_string(), 3), ("ut_pex".to_string(), 0)].into(),
        client: Some("µTorrent 3.5".into()),
        listen_port: None,
        request_queue: Some(1),
        your_ip: Some("2001:db8::1".parse().unwrap()),
        metadata_size: Some(31_337),
    };
    let decoded = ExtendedHandshake::decode(&handshake.encode().unwrap()).unwrap();
    assert_eq!(decoded, handshake);
    assert_eq!(decoded.id("ut_metadata"), Some(3));
    assert_eq!(decoded.id("ut_pex"), None, "id 0 means disabled");
    assert_eq!(decoded.id("lt_donthave"), None);
}

#[test]
fn private_torrents_do_not_offer_peer_exchange() {
    let ip: IpAddr = "10.1.2.3".parse().unwrap();
    let private = Advertised {
        private: true,
        ..Advertised::default()
    };
    let ours = ExtendedHandshake::ours(private, ip);
    assert_eq!(ours.id(pex::NAME), None);
    // Magnet links for private torrents are rare, but the metadata may
    // still be fetched from us.
    assert_eq!(ours.id(UT_METADATA), Some(UT_METADATA_ID));
    let public = ExtendedHandshake::ours(Advertised::default(), ip);
    assert_eq!(
        ours.extensions.len(),
        public.extensions.len() - PUBLIC_ONLY.len()
    );
}

#[test]
fn ipv4_mapped_addresses_are_sent_as_ipv4() {
    let ip: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
    let payload = ExtendedHandshake::ours(Advertised::default(), ip)
        .encode()
        .unwrap();
    let decoded = ExtendedHandshake::decode(&payload).unwrap();
    assert_eq!(decoded.your_ip, Some("10.1.2.3".parse().unwrap()));
    assert_eq!(decoded.listen_port, None);
}

#[test]
fn decodes_a_typical_handshake_ignoring_unknown_keys() {
    let payload = b"d12:complete_agoi1441e1:md11:lt_donthavei7e\
        10:share_modei8e11:ut_metadatai2e6:ut_pexi1ee13:metadata_sizei5205e\
        1:pi51413e4:reqqi512e1:v16:Transmission 4.06:yourip4:\x7f\x00\x00\x01e";
    let handshake = ExtendedHandshake::decode(payload).unwrap();
    assert_eq!(handshake.id("ut_metadata"), Some(2));
    assert_eq!(handshake.id("ut_pex"), Some(1));
    assert_eq!(handshake.extensions.len(), 4);
    assert_eq!(handshake.client.as_deref(), Some("Transmission 4.0"));
    assert_eq!(handshake.listen_port, Some(51413));
    assert_eq!(handshake.request_queue, Some(512));
    assert_eq!(handshake.your_ip, Some("127.0.0.1".parse().unwrap()));
    assert_eq!(handshake.metadata_size, Some(5205));
}

#[test]
fn missing_and_ill_typed_keys_are_ignored() {
    assert_eq!(
        ExtendedHandshake::decode(b"de").unwrap(),
        ExtendedHandshake::default()
    );
    let payload = b"d1:mli1ee13:metadata_size1:51:p4:68814:reqqi-1e1:vi7e6:yourip3:abce";
    assert_eq!(
        ExtendedHandshake::decode(payload).unwrap(),
        ExtendedHandshake::default()
    );
    // Out of range values, and extension entries that make no sense.
    let payload = b"d1:md1:ai256e1:b1:x1:ci-1e1:di4ee13:metadata_sizei0e1:pi65536e4:reqqi0ee";
    let handshake = ExtendedHandshake::decode(payload).unwrap();
    assert_eq!(handshake.extensions, [("d".to_string(), 4)].into());
    assert_eq!(handshake.metadata_size, None);
    let handshake = ExtendedHandshake::decode(b"d13:metadata_sizei4294967296ee").unwrap();
    assert_eq!(handshake.metadata_size, None);
    assert_eq!(handshake.listen_port, None);
    assert_eq!(handshake.request_queue, None);
    // A client name that is not UTF-8 is kept, lossily.
    let handshake = ExtendedHandshake::decode(b"d1:v2:a\xffe").unwrap();
    assert_eq!(handshake.client.as_deref(), Some("a\u{fffd}"));
}

#[test]
fn rejects_payloads_that_are_not_a_dictionary() {
    let cases: &[&[u8]] = &[
        b"",
        b"i5e",
        b"le",
        b"4:spam",
        b"d1:m",
        b"d1:mde",
        b"di1ei2ee",
        b"\xff",
    ];
    for case in cases {
        assert!(
            ExtendedHandshake::decode(case).is_err(),
            "accepted {case:?}"
        );
    }
    let mut deep = b"d1:x".to_vec();
    deep.extend(std::iter::repeat_n(b'l', 100_000));
    assert!(ExtendedHandshake::decode(&deep).is_err());
}

#[test]
fn trailing_bytes_are_ignored() {
    let handshake = ExtendedHandshake::decode(b"d4:reqqi8eeextra").unwrap();
    assert_eq!(handshake.request_queue, Some(8));
}

#[test]
fn later_handshakes_update_earlier_ones() {
    let mut handshake =
        ExtendedHandshake::decode(b"d1:md11:ut_metadatai2e6:ut_pexi1ee4:reqqi64e1:v1:ae").unwrap();
    handshake.update(
        ExtendedHandshake::decode(b"d1:md6:ut_pexi0ee13:metadata_sizei99e4:reqqi32ee").unwrap(),
    );
    assert_eq!(handshake.id("ut_metadata"), Some(2));
    assert_eq!(handshake.metadata_size, Some(99));
    assert_eq!(handshake.id("ut_pex"), None);
    assert_eq!(handshake.request_queue, Some(32));
    assert_eq!(handshake.client.as_deref(), Some("a"));
}
