//! Diffie-Hellman over the 768-bit prime MSE uses, with generator 2.
//!
//! Numbers are 12 little-endian 64-bit limbs, multiplied in Montgomery
//! form. Nothing here runs in constant time: MSE keeps traffic from being
//! recognised, it does not keep it secret from anyone who tries, and a
//! 768-bit group would not either way.

use rand::Rng;

/// Bytes in a public key or shared secret, big-endian.
pub const KEY_LEN: usize = 96;
/// Bytes in a private key. MSE leaves the size open; 160 bits is what
/// clients use.
pub const PRIVATE_LEN: usize = 20;

const LIMBS: usize = 12;
type Limbs = [u64; LIMBS];

/// The prime P of the MSE spec, least significant limb first:
/// `0xFFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563`.
const P: Limbs = [
    0x0000_0000_0009_0563,
    0xf44c_42e9_a63a_3621,
    0xe485_b576_625e_7ec6,
    0x4fe1_356d_6d51_c245,
    0x302b_0a6d_f25f_1437,
    0xef95_19b3_cd3a_431b,
    0x514a_0879_8e34_04dd,
    0x020b_bea6_3b13_9b22,
    0x2902_4e08_8a67_cc74,
    0xc4c6_628b_80dc_1cd1,
    0xc90f_daa2_2168_c234,
    0xffff_ffff_ffff_ffff,
];
/// The generator G.
const G: u64 = 2;

/// `-P⁻¹ mod 2⁶⁴`, for Montgomery reduction. Newton's iteration doubles the
/// correct low bits each step, from 1 (P is odd) to 64 in six.
const P_INV: u64 = {
    let mut x: u64 = 1;
    let mut step = 0;
    while step < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(P[0].wrapping_mul(x)));
        step += 1;
    }
    x.wrapping_neg()
};

/// A private key and the public key that goes with it.
pub struct KeyPair {
    private: [u8; PRIVATE_LEN],
    /// G to the power of the private key, mod P: sent to the peer.
    pub public: [u8; KEY_LEN],
}

impl KeyPair {
    /// A fresh random key pair.
    pub fn generate() -> Self {
        let mut private = [0u8; PRIVATE_LEN];
        rand::rng().fill(&mut private);
        Self::from_private(private)
    }

    /// The key pair with the private key `private`.
    pub fn from_private(private: [u8; PRIVATE_LEN]) -> Self {
        let mut base = [0u64; LIMBS];
        base[0] = G;
        let public = to_bytes(&pow_mod(&base, &private));
        Self { private, public }
    }

    /// The secret shared with the peer whose public key is `peer`: its key
    /// to the power of our private key, mod P. Any 96 bytes are taken, even
    /// a key no honest peer would send.
    pub fn shared_secret(&self, peer: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
        to_bytes(&pow_mod(&from_bytes(peer), &self.private))
    }
}

/// `base` to the power of `exponent` (big-endian), mod P.
fn pow_mod(base: &Limbs, exponent: &[u8]) -> Limbs {
    let r2 = r_squared();
    let mut one = [0u64; LIMBS];
    one[0] = 1;
    // A base of P or more (it is below 2P, since P > 2⁷⁶⁷) is reduced first.
    let mut base = *base;
    if !less_than(&base, &P) {
        sub_assign(&mut base, &P);
    }
    let base = mont_mul(&base, &r2);
    let mut result = mont_mul(&one, &r2);
    for byte in exponent {
        for bit in (0..8).rev() {
            result = mont_mul(&result, &result);
            if byte >> bit & 1 == 1 {
                result = mont_mul(&result, &base);
            }
        }
    }
    mont_mul(&result, &one)
}

/// R² mod P, where R = 2⁷⁶⁸, which takes a number into Montgomery form.
/// From R mod P = R - P, doubled mod P another 768 times.
fn r_squared() -> Limbs {
    let mut r = [0u64; LIMBS];
    sub_assign(&mut r, &P);
    for _ in 0..LIMBS * 64 {
        let carry = r[LIMBS - 1] >> 63;
        for i in (1..LIMBS).rev() {
            r[i] = r[i] << 1 | r[i - 1] >> 63;
        }
        r[0] <<= 1;
        if carry == 1 || !less_than(&r, &P) {
            sub_assign(&mut r, &P);
        }
    }
    r
}

/// `a * b / R mod P`, for `a` and `b` below P (CIOS Montgomery
/// multiplication).
fn mont_mul(a: &Limbs, b: &Limbs) -> Limbs {
    let mut t = [0u64; LIMBS + 2];
    for &b_i in b {
        let mut carry = 0u64;
        for j in 0..LIMBS {
            let sum = u128::from(t[j]) + u128::from(a[j]) * u128::from(b_i) + u128::from(carry);
            t[j] = sum as u64;
            carry = (sum >> 64) as u64;
        }
        let sum = u128::from(t[LIMBS]) + u128::from(carry);
        t[LIMBS] = sum as u64;
        t[LIMBS + 1] = (sum >> 64) as u64;

        let m = t[0].wrapping_mul(P_INV);
        let sum = u128::from(t[0]) + u128::from(m) * u128::from(P[0]);
        let mut carry = (sum >> 64) as u64;
        for j in 1..LIMBS {
            let sum = u128::from(t[j]) + u128::from(m) * u128::from(P[j]) + u128::from(carry);
            t[j - 1] = sum as u64;
            carry = (sum >> 64) as u64;
        }
        let sum = u128::from(t[LIMBS]) + u128::from(carry);
        t[LIMBS - 1] = sum as u64;
        t[LIMBS] = t[LIMBS + 1] + (sum >> 64) as u64;
    }
    let mut result = [0u64; LIMBS];
    result.copy_from_slice(&t[..LIMBS]);
    if t[LIMBS] != 0 || !less_than(&result, &P) {
        sub_assign(&mut result, &P);
    }
    result
}

/// Whether `a < b`.
fn less_than(a: &Limbs, b: &Limbs) -> bool {
    a.iter().rev().cmp(b.iter().rev()).is_lt()
}

/// `a -= b`, wrapping below zero.
fn sub_assign(a: &mut Limbs, b: &Limbs) {
    let mut borrow = false;
    for (a, &b) in a.iter_mut().zip(b) {
        let (diff, under) = a.overflowing_sub(b);
        let (diff, under_again) = diff.overflowing_sub(u64::from(borrow));
        *a = diff;
        borrow = under || under_again;
    }
}

fn from_bytes(bytes: &[u8; KEY_LEN]) -> Limbs {
    let mut limbs = [0u64; LIMBS];
    for (limb, chunk) in limbs.iter_mut().zip(bytes.rchunks_exact(8)) {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        *limb = u64::from_be_bytes(word);
    }
    limbs
}

fn to_bytes(limbs: &Limbs) -> [u8; KEY_LEN] {
    let mut bytes = [0u8; KEY_LEN];
    for (chunk, limb) in bytes.rchunks_exact_mut(8).zip(limbs) {
        chunk.copy_from_slice(&limb.to_be_bytes());
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unhex<const N: usize>(text: &str) -> [u8; N] {
        let mut bytes = [0u8; N];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).unwrap();
        }
        bytes
    }

    // Worked out with Python's `pow`.
    const XA: &str = "0102030405060708090a0b0c0d0e0f1011121314";
    const YA: &str = "96e112dab29e8c5272accb9b17b26887ce54a144a4e3b697c7d159b7a817e556b0918db2b4c658e02a87f7e5fb14b18a553e084cbf3dad2d30f16596ccb982d406258c61b30c5c1dae2ddc60bdbd48d79896312aad63238c39e1a633821eb693";
    const XB: &str = "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3";
    const YB: &str = "84b23a1e8480b595426889ed448dfc92dc2c293006e0cf39657f70c3eca33cb50bd62ca343a558ca8489018d6986a1d347686b3343453259367421bd5cf35c60d834afa278f71223a4c79cdb9a1bc918e09099b4f5aabd652c226edbb75f88f7";
    const S: &str = "1aea23a0431eeac96cfe444068c2674f97b4ac97054382d31445f162f8b1e576cf94207839779de0a37f42501cf321226af0d346b0b7cdb8ff4a0123b228b38dc7e696ead6b6f17264c28d21f1dc74536a94993f7943401a06e4b2f99febf21a";
    /// (2⁷⁶⁸ - 1)^XA mod P: a base above P.
    const ALL_ONES_TO_XA: &str = "82c3267215ffe2bb1124e8b7660a70d0f7054e08ecf932d8a911fda3a2119ec818f216308807fb70d7442f161c57034ca27018e90b28e79b611a9fd9bbe5c62690d1da9006e8e79ba1b8ba2141742824a2af367fa3f3d37fecf996ba27c65e41";

    #[test]
    fn the_prime_round_trips_through_bytes() {
        let bytes = to_bytes(&P);
        assert_eq!(&bytes[..8], &[0xff; 8]);
        assert_eq!(&bytes[88..], &[0, 0, 0, 0, 0, 0x09, 0x05, 0x63]);
        assert_eq!(from_bytes(&bytes), P);
        assert_eq!(P[0].wrapping_mul(P_INV), u64::MAX, "P * P_INV = -1");
    }

    #[test]
    fn public_keys_and_the_shared_secret_match_known_values() {
        let a = KeyPair::from_private(unhex(XA));
        let b = KeyPair::from_private(unhex(XB));
        assert_eq!(a.public, unhex::<96>(YA));
        assert_eq!(b.public, unhex::<96>(YB));
        assert_eq!(a.shared_secret(&b.public), unhex::<96>(S));
        assert_eq!(b.shared_secret(&a.public), unhex::<96>(S));
    }

    #[test]
    fn a_key_above_the_prime_is_reduced() {
        let a = KeyPair::from_private(unhex(XA));
        assert_eq!(a.shared_secret(&[0xff; 96]), unhex::<96>(ALL_ONES_TO_XA));
        // Degenerate keys give degenerate secrets, without panicking.
        assert_eq!(a.shared_secret(&[0; 96]), [0; 96]);
        let mut one = [0; 96];
        one[95] = 1;
        assert_eq!(a.shared_secret(&one), one);
        assert_eq!(a.shared_secret(&to_bytes(&P)), [0; 96]);
    }

    #[test]
    fn random_key_pairs_agree() {
        for _ in 0..4 {
            let a = KeyPair::generate();
            let b = KeyPair::generate();
            assert_eq!(a.shared_secret(&b.public), b.shared_secret(&a.public));
            assert_ne!(a.public, b.public);
        }
    }
}
