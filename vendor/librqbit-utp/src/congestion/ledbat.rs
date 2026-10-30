//! Delay-based congestion control for BEP 29 / RFC 6817.
//!
//! A ten-minute history of per-minute minimum one-way delays estimates
//! propagation delay. The minimum of the last three samples filters noise.
//! Timestamp offsets and wraparound cancel in signed serial differences.

use super::CongestionController;
use crate::rtte::RttEstimator;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const TARGET_US: f64 = 100_000.0;
const MINUTE: Duration = Duration::from_secs(60);
const HISTORY: usize = 10;
const MAX_WINDOW: f64 = 16.0 * 1024.0 * 1024.0;

#[derive(Debug)]
pub struct Ledbat {
    cwnd: f64,
    ssthresh: usize,
    mss: usize,
    remote: usize,
    base: VecDeque<(Instant, u32)>,
    recent: VecDeque<u32>,
    queue_us: Option<u32>,
}

/// Signed serial ordering handles the wrap in the 32-bit timestamp clock.
fn earlier(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}
fn minimum(samples: impl IntoIterator<Item = u32>) -> Option<u32> {
    samples
        .into_iter()
        .reduce(|a, b| if earlier(a, b) { a } else { b })
}

impl Ledbat {
    pub fn new(_now: Instant, mss: usize) -> Self {
        let mss = mss.max(1);
        Self {
            cwnd: (2 * mss) as f64,
            ssthresh: usize::MAX,
            mss,
            remote: 0,
            base: VecDeque::new(),
            recent: VecDeque::new(),
            queue_us: None,
        }
    }
    fn minimum_window(&self) -> f64 {
        (2 * self.mss) as f64
    }
}

impl CongestionController for Ledbat {
    fn window(&self) -> usize {
        (self.cwnd as usize).min(self.remote)
    }
    fn sshthresh(&self) -> usize {
        self.ssthresh
    }
    fn smss(&self) -> usize {
        self.mss
    }
    fn set_mss(&mut self, mss: usize) {
        self.mss = mss.max(1);
        self.cwnd = self.cwnd.max(self.minimum_window());
    }
    fn set_remote_window(&mut self, win: usize) {
        self.remote = win;
    }
    fn on_delay(&mut self, now: Instant, delay: u32) {
        // Zero means the peer has not measured delay yet (BEP 29).
        if delay == 0 {
            return;
        }
        if self
            .base
            .back()
            .is_some_and(|(at, _)| now.duration_since(*at) >= MINUTE * HISTORY as u32)
        {
            self.base.clear();
            self.recent.clear();
        }
        while self
            .base
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= MINUTE * HISTORY as u32)
        {
            self.base.pop_front();
        }
        if self
            .base
            .back()
            .is_none_or(|(at, _)| now.duration_since(*at) >= MINUTE)
        {
            self.base.push_back((now, delay));
            while self.base.len() > HISTORY {
                self.base.pop_front();
            }
        } else if let Some((_, sample)) = self.base.back_mut() {
            if earlier(delay, *sample) {
                *sample = delay;
            }
        }
        self.recent.push_back(delay);
        if self.recent.len() > 3 {
            self.recent.pop_front();
        }
        let base = minimum(self.base.iter().map(|(_, sample)| *sample)).unwrap();
        let current = minimum(self.recent.iter().copied()).unwrap();
        self.queue_us = Some((current.wrapping_sub(base) as i32).max(0) as u32);
    }
    fn on_ack(&mut self, _now: Instant, len: usize, _rtt: &RttEstimator) {
        let Some(queue) = self.queue_us else {
            return;
        };
        if len == 0 {
            return;
        }
        let offset = ((TARGET_US - f64::from(queue)) / TARGET_US).clamp(-1.0, 1.0);
        // One MSS at most per window acknowledged: no more aggressive
        // than TCP, and independent of the receiver's ACK frequency.
        let acknowledged = (len as f64 / self.cwnd).min(1.0);
        let change = self.mss as f64 * acknowledged * offset;
        // Do not grow when the receiver's window, rather than ours, is full.
        if change <= 0.0 || self.cwnd < self.remote as f64 {
            self.cwnd = (self.cwnd + change).clamp(self.minimum_window(), MAX_WINDOW);
        }
    }
    fn on_enter_recovery(&mut self, _now: Instant) {
        self.cwnd = (self.cwnd * 0.5).max(self.minimum_window());
        self.ssthresh = self.cwnd as usize;
    }
    fn on_retransmission_timeout(&mut self, _now: Instant) {
        self.ssthresh = (self.cwnd * 0.5).max(self.minimum_window()) as usize;
        self.cwnd = self.minimum_window();
    }
    fn on_recovered(&mut self, new_cwnd_bytes: usize, new_ssthresh: usize) {
        self.cwnd = self
            .cwnd
            .min(new_cwnd_bytes as f64)
            .max(self.minimum_window());
        self.ssthresh = new_ssthresh.max(self.minimum_window() as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn controller() -> (Instant, Ledbat) {
        let now = Instant::now();
        let mut c = Ledbat::new(now, 1000);
        c.set_remote_window(1_000_000);
        (now, c)
    }
    #[test]
    fn low_delay_grows_at_most_one_mss_per_window() {
        let (now, mut c) = controller();
        c.on_delay(now, 10_000);
        let before = c.window();
        c.on_ack(now, before, &RttEstimator::default());
        assert_eq!(c.window(), before + 1000);
    }
    #[test]
    fn yields_when_queue_exceeds_target() {
        let (now, mut c) = controller();
        c.on_delay(now, 10_000);
        for _ in 0..20 {
            c.on_ack(now, 1000, &RttEstimator::default());
        }
        let before = c.window();
        for _ in 0..3 {
            c.on_delay(now, 210_000);
        }
        c.on_ack(now, 1000, &RttEstimator::default());
        assert!(c.window() < before);
        assert!(c.window() >= 2000);
    }
    #[test]
    fn timestamp_wrap_and_clock_offsets_cancel() {
        let (now, mut c) = controller();
        c.on_delay(now, u32::MAX - 20_000);
        for _ in 0..3 {
            c.on_delay(now, 29_999);
        }
        assert_eq!(c.queue_us, Some(50_000));
        let (_, mut other) = controller();
        other.on_delay(now, 10_000);
        for _ in 0..3 {
            other.on_delay(now, 60_000);
        }
        assert_eq!(c.queue_us, other.queue_us);
    }
    #[test]
    fn loss_halves_window_and_timeouts_reset_it() {
        let (now, mut c) = controller();
        c.on_delay(now, 10_000);
        for _ in 0..50 {
            c.on_ack(now, 1000, &RttEstimator::default());
        }
        let before = c.window();
        c.on_enter_recovery(now);
        assert_eq!(c.window(), (before / 2).max(2000));
        c.on_retransmission_timeout(now);
        assert_eq!(c.window(), 2000);
        c.set_remote_window(500);
        assert_eq!(c.window(), 500);
    }
    #[test]
    fn old_propagation_samples_expire_and_zero_is_ignored() {
        let (now, mut c) = controller();
        c.on_delay(now, 0);
        assert_eq!(c.queue_us, None);
        c.on_delay(now, 10_000);
        c.on_delay(now + MINUTE * 11, 100_000);
        assert_eq!(c.queue_us, Some(0));
        assert_eq!(c.base.len(), 1);
    }
}
