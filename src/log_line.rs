use indicatif::MultiProgress;
use std::io::Write;

/// One log event, written to stderr once complete, with the progress bars
/// hidden meanwhile and drawn again below it.
pub(crate) struct LogLine {
    bars: MultiProgress,
    line: Vec<u8>,
}

impl LogLine {
    pub(crate) fn new(bars: MultiProgress) -> Self {
        Self {
            bars,
            line: Vec::new(),
        }
    }
}

impl Write for LogLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.line.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for LogLine {
    fn drop(&mut self) {
        if !self.line.is_empty() {
            // Nowhere to report a failure to write a log line.
            let _ = self
                .bars
                .suspend(|| std::io::stderr().write_all(&self.line));
        }
    }
}
