use super::*;
use crate::torrent::{FileInfo, Info};
use serde_bytes::ByteBuf;
use std::path::PathBuf;

use Priority::{High, Normal, Skip};

/// Files of the given lengths with pieces of `piece_length` bytes.
fn torrent(lengths: &[u64], piece_length: u64) -> TorrentFile {
    let total: u64 = lengths.iter().sum();
    TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: "root".into(),
            piece_length,
            pieces: ByteBuf::from(vec![0; total.div_ceil(piece_length) as usize * 20]),
            private: None,
            length: None,
            files: Some(
                lengths
                    .iter()
                    .enumerate()
                    .map(|(i, &length)| FileInfo {
                        path: vec![format!("f{i}")],
                        length,
                    })
                    .collect(),
            ),
        },
    }
}

fn spans(lengths: &[u64]) -> Vec<FileSpan> {
    let mut offset = 0;
    lengths
        .iter()
        .enumerate()
        .map(|(i, &length)| {
            let span = FileSpan {
                path: PathBuf::from(format!("f{i}")),
                offset,
                length,
                found: None,
            };
            offset += length;
            span
        })
        .collect()
}

fn priorities(lengths: &[u64], piece_length: u64, selection: &[Priority]) -> Vec<Option<Priority>> {
    piece_priorities(
        &torrent(lengths, piece_length),
        &spans(lengths),
        &FileSelection::new(selection.to_vec()),
    )
}

#[test]
fn priorities_are_ordered_skip_normal_high() {
    assert!(Skip < Normal && Normal < High);
    assert_eq!(Priority::default(), Normal);
}

#[test]
fn every_piece_is_normal_by_default() {
    assert_eq!(
        priorities(&[3, 6], 4, &[]),
        [Some(Normal), Some(Normal), Some(Normal)]
    );
}

#[test]
fn a_piece_takes_the_highest_priority_of_its_files() {
    // Pieces: [a a a b] [b b b b] [b c c].
    let got = priorities(&[3, 6, 2], 4, &[Normal, High, Normal]);
    assert_eq!(got, [Some(High), Some(High), Some(High)]);
    let got = priorities(&[3, 6, 2], 4, &[High, Normal, Skip]);
    assert_eq!(got, [Some(High), Some(Normal), Some(Normal)]);
    let got = priorities(&[3, 6, 2], 4, &[Normal, Normal, High]);
    assert_eq!(got, [Some(Normal), Some(Normal), Some(High)]);
}

#[test]
fn a_piece_is_unwanted_only_if_every_file_in_it_is_skipped() {
    // Pieces: [a a a b] [b b b b] [c c c]. The middle file is skipped,
    // but piece 0 shares it with a wanted neighbour.
    let got = priorities(&[3, 5, 3], 4, &[Normal, Skip, Normal]);
    assert_eq!(got, [Some(Normal), None, Some(Normal)]);
    let got = priorities(&[3, 5, 3], 4, &[Skip, Skip, Skip]);
    assert_eq!(got, [None, None, None]);
}

#[test]
fn empty_files_do_not_count() {
    // The empty high file lies between the others and touches no piece.
    let got = priorities(&[4, 0, 4], 4, &[Skip, High, Normal]);
    assert_eq!(got, [None, Some(Normal)]);
}

#[test]
fn wanted_bytes_count_only_wanted_pieces() {
    let torrent = torrent(&[3, 5, 3], 4);
    let spans = spans(&[3, 5, 3]);
    let selection = FileSelection::new(vec![Normal, Skip, Normal]);
    let priorities = piece_priorities(&torrent, &spans, &selection);
    // Pieces 0 and 2: 4 bytes and 3.
    assert_eq!(wanted_len(&torrent, &priorities).unwrap(), 7);
    let have = [true, true, false];
    assert_eq!(wanted_done(&torrent, &priorities, &have).unwrap(), 4);
}

#[test]
fn selection_helpers() {
    let selection = FileSelection::new(vec![Normal, Skip]);
    assert_eq!(selection.priority(1), Skip);
    assert_eq!(selection.priority(7), Normal);
    assert_eq!(selection.for_files(3), [Normal, Skip, Normal]);
    assert!(!selection.is_all_normal());
    assert!(FileSelection::all_normal(3).is_all_normal());
    assert!(FileSelection::default().is_empty());
    for priority in [Skip, Normal, High] {
        assert_eq!(Priority::from_wire(priority.to_wire()), Some(priority));
    }
    assert_eq!(Priority::from_wire(3), None);
}

#[test]
fn priorities_move_one_step_at_a_time() {
    assert_eq!(Skip.raised(), Normal);
    assert_eq!(Normal.raised(), High);
    assert_eq!(High.raised(), High);
    assert_eq!(High.lowered(), Normal);
    assert_eq!(Normal.lowered(), Skip);
    assert_eq!(Skip.lowered(), Skip);
    assert_eq!(
        [Skip, Normal, High].map(Priority::toggled),
        [Normal, Skip, Skip]
    );
}

#[test]
fn file_picks_parse_numbers_and_ranges_from_one() {
    let picks = FilePicks::parse("1,3-5").unwrap();
    assert_eq!(picks.last(), 5);
    assert_eq!(
        picks.selection(6).priorities(),
        [Normal, Skip, Normal, Normal, Normal, Skip]
    );
    // Past the last file picks nothing there.
    assert_eq!(picks.selection(3).priorities(), [Normal, Skip, Normal]);
    assert!(picks.selection(0).is_empty());
    assert_eq!(
        FilePicks::parse(" 2 , 2-2,1-2")
            .unwrap()
            .selection(2)
            .priorities(),
        [Normal, Normal]
    );
    for bad in ["", "0", "1,", "a", "3-1", "1-", "-2", "1-2-3", "1;2"] {
        assert!(FilePicks::parse(bad).is_err(), "{bad:?}");
    }
    assert!(FilePicks::parse("9").unwrap().selection(3).is_all_skipped());
    assert!(!FileSelection::default().is_all_skipped());
}

#[test]
fn wanted_bytes_follow_a_selection() {
    let torrent = torrent(&[3, 5, 3], 4);
    let selection = FileSelection::new(vec![Normal, Skip, Normal]);
    assert_eq!(wanted_of(&torrent, &selection).unwrap(), 7);
    assert_eq!(wanted_of(&torrent, &FileSelection::default()).unwrap(), 11);
}
