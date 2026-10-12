//! `get_peers` tokens (BEP 5): proof that a node asking to `announce_peer`
//! recently asked us for peers from the same IP address, so nobody can
//! announce other people's addresses.
//!
//! A token is a hash of the address and a secret. The secret changes every
//! 5 minutes, and tokens made with the previous one are still accepted, so a
//! token stays valid for 5 to 10 minutes.

use sha1::Digest;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::time::Instant;

pub const ROTATE_EVERY: Duration = Duration::from_secs(5 * 60);
const TOKEN_LEN: usize = 8;

pub struct Tokens {
    current: [u8; 16],
    previous: [u8; 16],
    rotated: Instant,
}

impl Tokens {
    pub fn new(now: Instant) -> Self {
        Self {
            current: rand::random(),
            previous: rand::random(),
            rotated: now,
        }
    }

    /// Changes the secret if it is due. After a long quiet spell both
    /// secrets are replaced, so old tokens never come back to life.
    fn rotate(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.rotated);
        if elapsed < ROTATE_EVERY {
            return;
        }
        self.previous = if elapsed < 2 * ROTATE_EVERY {
            self.current
        } else {
            rand::random()
        };
        self.current = rand::random();
        self.rotated = now;
    }

    /// The token for a node at `ip`.
    pub fn issue(&mut self, ip: Ipv4Addr, now: Instant) -> Vec<u8> {
        self.rotate(now);
        token(&self.current, ip).to_vec()
    }

    /// Whether the `presented` token was issued to `ip` with the current or
    /// the previous secret.
    pub fn check(&mut self, presented: &[u8], ip: Ipv4Addr, now: Instant) -> bool {
        self.rotate(now);
        presented == token(&self.current, ip) || presented == token(&self.previous, ip)
    }
}

fn token(secret: &[u8; 16], ip: Ipv4Addr) -> [u8; TOKEN_LEN] {
    let mut hasher = sha1::Sha1::new();
    hasher.update(secret);
    hasher.update(ip.octets());
    let hash: [u8; 20] = hasher.finalize().into();
    let mut token = [0u8; TOKEN_LEN];
    token.copy_from_slice(&hash[..TOKEN_LEN]);
    token
}

#[cfg(test)]
mod tests {
    use super::*;

    const IP: Ipv4Addr = Ipv4Addr::new(1, 2, 3, 4);

    #[test]
    fn tokens_are_bound_to_the_address() {
        let now = Instant::now();
        let mut tokens = Tokens::new(now);
        let token = tokens.issue(IP, now);
        assert_eq!(token.len(), TOKEN_LEN);
        assert!(tokens.check(&token, IP, now));
        assert!(!tokens.check(&token, Ipv4Addr::new(1, 2, 3, 5), now));
        assert!(!tokens.check(b"", IP, now));
        assert!(!tokens.check(&token[1..], IP, now));
    }

    #[test]
    fn tokens_outlive_one_rotation_but_not_two() {
        let start = Instant::now();
        let mut tokens = Tokens::new(start);
        let token = tokens.issue(IP, start);
        let one = start + ROTATE_EVERY;
        assert!(tokens.check(&token, IP, one));
        // A token issued now is new.
        let newer = tokens.issue(IP, one);
        assert_ne!(newer, token);
        let two = one + ROTATE_EVERY;
        assert!(!tokens.check(&token, IP, two));
        assert!(tokens.check(&newer, IP, two));
    }

    #[test]
    fn a_long_gap_expires_every_token() {
        let start = Instant::now();
        let mut tokens = Tokens::new(start);
        let token = tokens.issue(IP, start);
        assert!(!tokens.check(&token, IP, start + 3 * ROTATE_EVERY));
    }
}
