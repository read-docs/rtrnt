use super::*;
use crate::torrent::{FileInfo, Info};
use serde_bytes::ByteBuf;

fn torrent(name: &str, files: &[(&[&str], u64)], piece_length: u64) -> TorrentFile {
    let total: u64 = files.iter().map(|(_, length)| length).sum();
    let pieces = total.div_ceil(piece_length) as usize;
    let multi = files.len() != 1 || files[0].0.len() > 1;
    TorrentFile {
        announce: None,
        announce_list: None,
        url_list: Vec::new(),
        httpseeds: Vec::new(),
        info: Info {
            name: name.into(),
            piece_length,
            pieces: ByteBuf::from(vec![0u8; pieces * 20]),
            private: None,
            length: (!multi).then_some(total),
            files: multi.then(|| {
                files
                    .iter()
                    .map(|(path, length)| FileInfo {
                        path: path.iter().map(|part| part.to_string()).collect(),
                        length: *length,
                    })
                    .collect()
            }),
        },
    }
}

fn range(url: &str, start: u64, len: u64, file_len: u64) -> FileRange {
    FileRange {
        url: url.into(),
        start,
        len,
        file_len,
    }
}

#[test]
fn percent_encoding_keeps_only_unreserved_characters() {
    assert_eq!(percent_encode(b"a-b_c.d~E9"), "a-b_c.d~E9");
    assert_eq!(percent_encode(b"a b/c?#%"), "a%20b%2Fc%3F%23%25");
    assert_eq!(percent_encode("żółw".as_bytes()), "%C5%BC%C3%B3%C5%82w");
}

#[test]
fn a_single_file_torrent_is_fetched_from_the_url_itself() {
    let t = torrent("f.iso", &[(&["f.iso"], 100)], 32);
    assert_eq!(
        file_ranges("http://h/other.iso", &t, 1).unwrap(),
        [range("http://h/other.iso", 32, 32, 100)]
    );
    // The last piece is short.
    assert_eq!(
        file_ranges("http://h/other.iso", &t, 3).unwrap(),
        [range("http://h/other.iso", 96, 4, 100)]
    );
    assert!(file_ranges("http://h/x", &t, 4).is_err());
}

#[test]
fn a_url_ending_in_a_slash_gets_the_name_appended() {
    let t = torrent("my file.iso", &[(&["my file.iso"], 10)], 32);
    assert_eq!(
        file_ranges("http://h/dir/", &t, 0).unwrap(),
        [range("http://h/dir/my%20file.iso", 0, 10, 10)]
    );
}

#[test]
fn a_piece_spanning_files_makes_a_request_for_each() {
    let t = torrent(
        "pack",
        &[(&["a.txt"], 10), (&["sub", "b c.txt"], 25), (&["d"], 5)],
        16,
    );
    // Bytes 0..16: a.txt and the start of b.
    assert_eq!(
        file_ranges("http://h/files", &t, 0).unwrap(),
        [
            range("http://h/files/pack/a.txt", 0, 10, 10),
            range("http://h/files/pack/sub/b%20c.txt", 0, 6, 25),
        ]
    );
    // Bytes 16..32: inside b.
    assert_eq!(
        file_ranges("http://h/files/", &t, 1).unwrap(),
        [range("http://h/files/pack/sub/b%20c.txt", 6, 16, 25)]
    );
    // Bytes 32..40: the end of b and all of d.
    assert_eq!(
        file_ranges("http://h/files/", &t, 2).unwrap(),
        [
            range("http://h/files/pack/sub/b%20c.txt", 22, 3, 25),
            range("http://h/files/pack/d", 0, 5, 5),
        ]
    );
    assert!(file_ranges("http://h/files", &t, 3).is_err());
}

#[test]
fn a_piece_within_one_file_makes_one_request() {
    let t = torrent("p", &[(&["a"], 40), (&["b"], 40)], 16);
    assert_eq!(
        file_ranges("http://h/", &t, 3).unwrap(),
        [range("http://h/p/b", 8, 16, 40)]
    );
}

#[test]
fn empty_files_get_no_request() {
    let t = torrent("p", &[(&["a"], 8), (&["empty"], 0), (&["b"], 8)], 16);
    let ranges = file_ranges("http://h/", &t, 0).unwrap();
    let urls: Vec<&str> = ranges.iter().map(|r| r.url.as_str()).collect();
    assert_eq!(urls, ["http://h/p/a", "http://h/p/b"]);
}

#[test]
fn http_seed_urls_carry_the_info_hash_and_piece() {
    let hash = [0xab; 20];
    let expected = format!("info_hash={}&piece=7", "%AB".repeat(20));
    assert_eq!(
        http_seed_url("http://h/seed.php", &hash, 7),
        format!("http://h/seed.php?{expected}")
    );
    assert_eq!(
        http_seed_url("http://h/seed.php?k=1", &hash, 7),
        format!("http://h/seed.php?k=1&{expected}")
    );
}
