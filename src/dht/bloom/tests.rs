use super::*;
use std::net::{Ipv4Addr, Ipv6Addr};

/// The filter of BEP 33's test vector, as the spec prints it.
const VECTOR: &str = "
F6C3F5EA A07FFD91 BDE89F77 7F26FB2B FF37BDB8 FB2BBAA2 FD3DDDE7 BACFFF75 EE7CCBAE
FE5EEDB1 FBFAFF67 F6ABFF5E 43DDBCA3 FD9B9FFD F4FFD3E9 DFF12D1B DF59DB53 DBE9FA5B
7FF3B8FD FCDE1AFB 8BEDD7BE 2F3EE71E BBBFE93B CDEEFE14 8246C2BC 5DBFF7E7 EFDCF24F
D8DC7ADF FD8FFFDF DDFFF7A4 BBEEDF5C B95CE81F C7FCFF1F F4FFFFDF E5F7FDCB B7FD79B3
FA1FC77B FE07FFF9 05B7B7FF C7FEFEFF E0B8370B B0CD3F5B 7F2BD93F EB4386CF DD6F7FD5
BFAF2E9E BFFFFEEC D67ADBF7 C67F17EF D5D75EBA 6FFEBA7F FF47A91E B1BFBB53 E8ABFB57
62ABE8FF 237279BF EFBFEEF5 FFC5FEBF DFE5ADFF ADFEE1FB 737FFFFB FD9F6AEF FEEE76B6
FD8F72EF";

fn vector() -> Vec<u8> {
    let hex: String = VECTOR.split_whitespace().collect();
    (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

/// 192.0.2.0 to 192.0.2.255 and 2001:db8:: to 2001:db8::3e7.
fn example() -> Bloom {
    let mut bloom = Bloom::default();
    for host in 0..=255 {
        bloom.insert(IpAddr::V4(Ipv4Addr::new(192, 0, 2, host)));
    }
    let base = u128::from(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0));
    for n in 0..=0x3e7 {
        bloom.insert(IpAddr::V6(Ipv6Addr::from(base + n)));
    }
    bloom
}

#[test]
fn matches_the_bep_33_test_vector() {
    assert_eq!(vector().len(), BYTES);
    assert_eq!(example().as_bytes().as_slice(), vector());
}

#[test]
fn estimates_the_bep_33_example() {
    let bloom = Bloom::from_bytes(&vector()).unwrap();
    assert!((bloom.estimate() - 1224.9308).abs() < 0.0001, "{bloom:?}");
    assert_eq!(bloom.count(), 1225);
}

#[test]
fn contains_what_was_inserted() {
    let bloom = example();
    for host in 0..=255 {
        assert!(bloom.contains(IpAddr::V4(Ipv4Addr::new(192, 0, 2, host))));
    }
    // A few of 100 unrelated addresses may hit by chance, not most.
    let hits = (0..100u8)
        .filter(|&n| bloom.contains(IpAddr::V4(Ipv4Addr::new(10, 1, 1, n))))
        .count();
    assert!(hits < 100, "{hits}");
    // An IPv4 address mapped into IPv6 is the same address.
    let mapped = Ipv4Addr::new(192, 0, 2, 7).to_ipv6_mapped();
    assert!(bloom.contains(IpAddr::V6(mapped)));
}

#[test]
fn empty_and_tiny_filters() {
    let mut bloom = Bloom::default();
    assert_eq!(bloom.count(), 0);
    bloom.insert(Ipv4Addr::new(1, 2, 3, 4).into());
    assert_eq!(bloom.count(), 1);
    // The same address twice is still one.
    bloom.insert(Ipv4Addr::new(1, 2, 3, 4).into());
    assert_eq!(bloom.count(), 1);
}

#[test]
fn a_full_filter_does_not_overflow() {
    let full = Bloom(Box::new([0xff; BYTES]));
    let estimate = full.estimate();
    assert!(estimate.is_finite() && estimate > 7000.0, "{estimate}");
    assert!(full.count() > 7000);
}

#[test]
fn merging_counts_the_union() {
    let (mut a, mut b) = (Bloom::default(), Bloom::default());
    for n in 0..100u8 {
        a.insert(Ipv4Addr::new(10, 0, 0, n).into());
        b.insert(Ipv4Addr::new(10, 0, 0, n + 50).into());
    }
    a.merge(&b);
    // 150 distinct addresses.
    assert!((140..=160).contains(&a.count()), "{}", a.count());
}

#[test]
fn only_exact_sizes_are_filters() {
    assert!(Bloom::from_bytes(&[0; BYTES - 1]).is_none());
    assert!(Bloom::from_bytes(&[0; BYTES + 1]).is_none());
    assert!(Bloom::from_bytes(&[]).is_none());
}
