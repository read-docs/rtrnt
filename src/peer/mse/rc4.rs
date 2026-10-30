//! The RC4 stream cipher, which MSE encrypts with. It is a few lines, so it
//! is written out here rather than taken from a crate.

/// An RC4 keystream. Encrypting and decrypting are the same operation.
#[derive(Clone)]
pub struct Rc4 {
    state: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    /// The keystream for `key`, which must not be empty (MSE keys are 20
    /// bytes); an empty key is taken as a single zero byte.
    pub fn new(key: &[u8]) -> Self {
        let key = if key.is_empty() { &[0u8][..] } else { key };
        let mut state = [0u8; 256];
        for (value, slot) in (0..=255u8).zip(state.iter_mut()) {
            *slot = value;
        }
        let mut j = 0u8;
        for (i, &key_byte) in (0..256).zip(key.iter().cycle()) {
            j = j.wrapping_add(state[i]).wrapping_add(key_byte);
            state.swap(i, usize::from(j));
        }
        Self { state, i: 0, j: 0 }
    }

    /// The keystream for `key` with its first `skip` bytes thrown away, as
    /// MSE does with the first 1024, which leak the key.
    pub fn skipping(key: &[u8], skip: usize) -> Self {
        let mut rc4 = Self::new(key);
        let mut discard = [0u8; 256];
        let mut left = skip;
        while left > 0 {
            let n = left.min(discard.len());
            rc4.apply(&mut discard[..n]);
            left -= n;
        }
        rc4
    }

    /// XORs `data` with the next `data.len()` bytes of the keystream.
    pub fn apply(&mut self, data: &mut [u8]) {
        for byte in data {
            self.i = self.i.wrapping_add(1);
            let si = self.state[usize::from(self.i)];
            self.j = self.j.wrapping_add(si);
            self.state.swap(usize::from(self.i), usize::from(self.j));
            let k = self.state[usize::from(self.i)].wrapping_add(self.state[usize::from(self.j)]);
            *byte ^= self.state[usize::from(k)];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn encrypt(key: &[u8], plaintext: &[u8]) -> String {
        let mut data = plaintext.to_vec();
        Rc4::new(key).apply(&mut data);
        hex(&data)
    }

    #[test]
    fn matches_the_classic_vectors() {
        assert_eq!(encrypt(b"Key", b"Plaintext"), "bbf316e8d940af0ad3");
        assert_eq!(encrypt(b"Wiki", b"pedia"), "1021bf0420");
        assert_eq!(
            encrypt(b"Secret", b"Attack at dawn"),
            "45a01f645fc35b383552544b9bf5"
        );
    }

    /// RFC 6229, the 40-bit key 0x0102030405, at offsets 0 and 1024: the
    /// latter is where MSE's keystream starts.
    #[test]
    fn matches_rfc_6229_at_offsets_0_and_1024() {
        let key = [1, 2, 3, 4, 5];
        let mut block = [0u8; 16];
        Rc4::new(&key).apply(&mut block);
        assert_eq!(hex(&block), "b2396305f03dc027ccc3524a0a1118a8");
        let mut block = [0u8; 16];
        Rc4::skipping(&key, 1024).apply(&mut block);
        assert_eq!(hex(&block), "30abbcc7c20b01609f23ee2d5f6bb7df");
    }

    #[test]
    fn decrypting_undoes_encrypting_in_any_pieces() {
        let plaintext: Vec<u8> = (0..1000u32).map(|i| (i * 7) as u8).collect();
        let mut data = plaintext.clone();
        Rc4::skipping(b"some key", 1024).apply(&mut data);
        assert_ne!(data, plaintext);
        let mut decrypt = Rc4::skipping(b"some key", 1024);
        for chunk in data.chunks_mut(37) {
            decrypt.apply(chunk);
        }
        assert_eq!(data, plaintext);
    }
}
