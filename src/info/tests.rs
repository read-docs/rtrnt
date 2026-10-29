use super::*;
use crate::create::{self, Options};

fn torrent_with_everything(dir: &std::path::Path) -> create::Created {
    let data = dir.join("pack");
    std::fs::create_dir_all(data.join("docs")).unwrap();
    std::fs::write(data.join("a.txt"), vec![1u8; 3000]).unwrap();
    std::fs::write(data.join("docs").join("b.txt"), vec![2u8; 2_000_000]).unwrap();
    let options = Options {
        path: data,
        trackers: vec![
            "udp://t1.example:80".into(),
            "http://t2.example/announce".into(),
        ],
        web_seeds: vec!["https://seed.example/pack/".into()],
        piece_length: Some(1 << 20),
        comment: Some("a comment".into()),
        source: Some("SRC".into()),
        created_at: Some(1_700_000_000),
        ..Options::default()
    };
    create::build(&options, &|_| {}, &|_| {}).unwrap()
}

#[test]
fn describes_a_multi_file_torrent() {
    let dir = tempfile::tempdir().unwrap();
    let created = torrent_with_everything(dir.path());
    let hash = hex(&created.info_hash);
    let expected = format!(
        "\
Name:         pack
Info hash:    {hash}
Size:         1.9 MiB (2003000 bytes)
Piece length: 1.0 MiB (1048576 bytes)
Pieces:       2
Private:      no
Source:       SRC
Created:      2023-11-14 22:13:20 UTC
Created by:   {created_by}
Comment:      a comment
Trackers:
  tier 1: udp://t1.example:80
  tier 2: http://t2.example/announce
Web seeds:
  https://seed.example/pack/
Files (2):
  2.9 KiB  a.txt
  1.9 MiB  docs/b.txt
Magnet:       {magnet}
",
        created_by = crate::peer::extension::CLIENT,
        magnet = created.magnet,
    );
    assert_eq!(describe(&created.data).unwrap(), expected);
}

#[test]
fn the_magnet_link_has_name_trackers_and_web_seeds() {
    let dir = tempfile::tempdir().unwrap();
    let created = torrent_with_everything(dir.path());
    let hash = hex(&created.info_hash);
    assert_eq!(
        created.magnet,
        format!(
            "magnet:?xt=urn:btih:{hash}&dn=pack&tr=udp%3A%2F%2Ft1.example%3A80\
             &tr=http%3A%2F%2Ft2.example%2Fannounce&ws=https%3A%2F%2Fseed.example%2Fpack%2F"
        )
    );
    // Our own parser reads it back.
    let link = MagnetLink::parse(&created.magnet).unwrap();
    assert_eq!(link.info_hash, created.info_hash);
    assert_eq!(link.trackers.len(), 2);
}

#[test]
fn a_bare_single_file_torrent_has_only_the_basics() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("x.iso");
    std::fs::write(&file, vec![0u8; 100]).unwrap();
    let options = Options {
        path: file,
        private: true,
        ..Options::default()
    };
    let created = create::build(&options, &|_| {}, &|_| {}).unwrap();
    let text = describe(&created.data).unwrap();
    assert!(text.contains("Private:      yes\n"), "{text}");
    assert!(text.contains("Files (1):\n  100 B  x.iso\n"), "{text}");
    for absent in ["Trackers:", "Web seeds:", "Created:", "Comment:", "Source:"] {
        assert!(!text.contains(absent), "{text}");
    }
}

#[test]
fn reads_a_single_url_list_and_ignores_odd_extras() {
    let dir = tempfile::tempdir().unwrap();
    let created = torrent_with_everything(dir.path());
    // `url-list` as one string, as many torrents have it.
    let mut data = created.data.clone();
    let list = b"8:url-listl26:https://seed.example/pack/e";
    let at = data.windows(list.len()).position(|w| w == list).unwrap();
    data.splice(
        at..at + list.len(),
        b"8:url-list26:https://seed.example/pack/".iter().copied(),
    );
    let extras = Extras::parse(&data);
    assert_eq!(extras.web_seeds, ["https://seed.example/pack/"]);
    assert_eq!(Extras::parse(b"garbage"), Extras::default());
}

#[test]
fn refuses_what_is_not_a_torrent() {
    assert!(describe(b"not a torrent").is_err());
}
