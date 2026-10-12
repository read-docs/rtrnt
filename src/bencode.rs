//! Minimal bencode scanner used to locate raw byte spans inside a document.
//!
//! `serde_bencode` decodes into structs, which drops unknown keys. The info
//! hash must be computed over the exact bytes of the `info` dictionary, so we
//! walk the raw input to find them.

use anyhow::{Result, anyhow, bail};
use std::ops::Range;

/// Nesting limit, so a malicious file cannot overflow the stack.
const MAX_DEPTH: usize = 64;

/// Returns the byte range of the value stored under the top-level `info` key.
pub fn info_span(data: &[u8]) -> Result<Range<usize>> {
    if data.first() != Some(&b'd') {
        bail!("torrent is not a bencoded dictionary");
    }
    let mut pos = 1;
    loop {
        match data.get(pos) {
            Some(b'e') => bail!("torrent has no info dictionary"),
            None => bail!("unexpected end of torrent data"),
            Some(_) => {}
        }
        let key = parse_string(data, &mut pos)?;
        let start = pos;
        skip_value(data, &mut pos, 0)?;
        if key == b"info" {
            return Ok(start..pos);
        }
    }
}

/// Length of the bencoded value at the start of `data`, which may be
/// followed by other bytes. Also bounds the nesting depth, so the value is
/// safe to hand to `serde_bencode`.
pub fn value_len(data: &[u8]) -> Result<usize> {
    let mut pos = 0;
    skip_value(data, &mut pos, 0)?;
    Ok(pos)
}

/// Parses `<len>:<bytes>` at `pos` and returns the bytes.
fn parse_string<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let colon = data[*pos..]
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| anyhow!("unterminated string length at byte {}", *pos))?;
    let digits = &data[*pos..*pos + colon];
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        bail!("invalid string length at byte {}", *pos);
    }
    let len: usize = std::str::from_utf8(digits)?.parse()?;
    let start = *pos + colon + 1;
    let end = start
        .checked_add(len)
        .filter(|&end| end <= data.len())
        .ok_or_else(|| anyhow!("string at byte {} runs past end of data", *pos))?;
    *pos = end;
    Ok(&data[start..end])
}

fn skip_value(data: &[u8], pos: &mut usize, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        bail!("bencode nesting too deep");
    }
    match data.get(*pos) {
        Some(b'i') => {
            let end = data[*pos..]
                .iter()
                .position(|&b| b == b'e')
                .ok_or_else(|| anyhow!("unterminated integer at byte {}", *pos))?;
            *pos += end + 1;
        }
        Some(b'l') => {
            *pos += 1;
            while data.get(*pos) != Some(&b'e') {
                skip_value(data, pos, depth + 1)?;
            }
            *pos += 1;
        }
        Some(b'd') => {
            *pos += 1;
            while data.get(*pos) != Some(&b'e') {
                parse_string(data, pos)?;
                skip_value(data, pos, depth + 1)?;
            }
            *pos += 1;
        }
        Some(b'0'..=b'9') => {
            parse_string(data, pos)?;
        }
        Some(&b) => bail!("unexpected byte {:#04x} at {}", b, *pos),
        None => bail!("unexpected end of data"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_info_with_unknown_keys() {
        let data = b"d8:announce3:url4:infod4:name1:a7:privatei1ee5:otherli1ei2eee";
        let span = info_span(data).unwrap();
        assert_eq!(&data[span], b"d4:name1:a7:privatei1ee");
    }

    #[test]
    fn skips_nested_values_before_info() {
        let data = b"d1:ald1:xl1:y1:zeee4:infod1:ki0eee";
        let span = info_span(data).unwrap();
        assert_eq!(&data[span], b"d1:ki0ee");
    }

    #[test]
    fn rejects_malformed_input() {
        let cases: &[&[u8]] = &[
            b"",
            b"l4:infoe",
            b"d4:info",
            b"d4:infod",
            b"d4:infoi12",
            b"d4:info99:short",
            b"d3:abcde",
            b"d4:infox",
            b"d4:infod1:ae",
            b"d18446744073709551616:x",
        ];
        for case in cases {
            assert!(info_span(case).is_err(), "accepted {:?}", case);
        }
    }

    #[test]
    fn rejects_deep_nesting() {
        let mut data = b"d4:info".to_vec();
        data.extend(std::iter::repeat_n(b'l', 1000));
        assert!(info_span(&data).is_err());
        assert!(value_len(&data).is_err());
    }

    #[test]
    fn measures_a_value_followed_by_other_bytes() {
        assert_eq!(value_len(b"d1:ali1eee").unwrap(), 10);
        assert_eq!(value_len(b"d1:ali1eeeextra").unwrap(), 10);
        assert_eq!(value_len(b"i42e").unwrap(), 4);
        assert_eq!(value_len(b"3:abcd").unwrap(), 5);
        for case in [&b""[..], b"d1:a", b"x", b"5:abc"] {
            assert!(value_len(case).is_err(), "accepted {case:?}");
        }
    }
}
