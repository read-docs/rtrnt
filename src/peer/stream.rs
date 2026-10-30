//! The byte stream under a peer connection: plain, or encrypted with RC4
//! once an MSE handshake (see `mse`) has agreed on keys. Either way it reads
//! and writes plaintext, so the wire protocol above it does not change.
//!
//! It wraps any `AsyncRead + AsyncWrite` stream, not just TCP, so other
//! transports can carry it.

use super::mse::Rc4;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// What a peer connection runs over.
pub type Transport = TcpStream;

/// Most encrypted bytes kept waiting to be written before a write waits.
const MAX_UNSENT: usize = 64 * 1024;

/// A peer's byte stream, plain or RC4-encrypted, over `S`.
///
/// Encrypted writes are buffered like a `BufWriter`'s, so the writer must
/// flush once it has written a message.
#[derive(Debug)]
pub struct PeerStream<S> {
    inner: S,
    /// Plaintext read from `inner` during the handshake that the reader has
    /// not taken yet, from `unread_pos` on.
    unread: Vec<u8>,
    unread_pos: usize,
    /// The keystreams, if encrypted.
    cipher: Option<Box<Ciphers>>,
    /// Encrypted bytes not yet written to `inner`, from `unsent_pos` on.
    unsent: Vec<u8>,
    unsent_pos: usize,
}

/// The two keystreams of an encrypted stream.
struct Ciphers {
    read: Rc4,
    write: Rc4,
}

impl std::fmt::Debug for Ciphers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Ciphers")
    }
}

impl<S> PeerStream<S> {
    /// A plain stream over `inner`.
    pub fn plain(inner: S) -> Self {
        Self::with_unread(inner, Vec::new())
    }

    /// A plain stream over `inner` whose first bytes, already read from it,
    /// are `unread`.
    pub fn with_unread(inner: S, unread: Vec<u8>) -> Self {
        Self {
            inner,
            unread,
            unread_pos: 0,
            cipher: None,
            unsent: Vec::new(),
            unsent_pos: 0,
        }
    }

    /// A stream over `inner` that decrypts what it reads with `read` and
    /// encrypts what it writes with `write`, and whose first bytes, already
    /// read from `inner` and decrypted, are `unread`.
    pub(super) fn encrypted(inner: S, unread: Vec<u8>, read: Rc4, write: Rc4) -> Self {
        Self {
            cipher: Some(Box::new(Ciphers { read, write })),
            ..Self::with_unread(inner, unread)
        }
    }

    /// Whether what goes over the stream is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.cipher.is_some()
    }

    /// The stream underneath.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S: AsyncWrite + Unpin> PeerStream<S> {
    /// Writes the encrypted bytes waiting in `unsent` to `inner`.
    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.unsent_pos < self.unsent.len() {
            let n =
                ready!(Pin::new(&mut self.inner).poll_write(cx, &self.unsent[self.unsent_pos..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.unsent_pos += n;
        }
        self.unsent.clear();
        self.unsent_pos = 0;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.unread_pos < this.unread.len() {
            let n = buf.remaining().min(this.unread.len() - this.unread_pos);
            buf.put_slice(&this.unread[this.unread_pos..this.unread_pos + n]);
            this.unread_pos += n;
            if this.unread_pos == this.unread.len() {
                this.unread = Vec::new();
                this.unread_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        let start = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if let Some(cipher) = &mut this.cipher {
            cipher.read.apply(&mut buf.filled_mut()[start..]);
        }
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PeerStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.cipher.is_none() {
            return Pin::new(&mut this.inner).poll_write(cx, data);
        }
        if this.unsent.len() - this.unsent_pos >= MAX_UNSENT {
            ready!(this.poll_send(cx))?;
        }
        let room = MAX_UNSENT - (this.unsent.len() - this.unsent_pos);
        let n = data.len().min(room);
        // Keep the allocation bounded even if the transport repeatedly
        // writes only part of the buffered ciphertext.
        if this.unsent_pos > 0 {
            this.unsent.drain(..this.unsent_pos);
            this.unsent_pos = 0;
        }
        let start = this.unsent.len();
        this.unsent.extend_from_slice(&data[..n]);
        if let Some(cipher) = &mut this.cipher {
            cipher.write.apply(&mut this.unsent[start..]);
        }
        // Sent now if the socket takes it; else on the next write or flush.
        if let Poll::Ready(Err(e)) = this.poll_send(cx) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    fn pair() -> (Rc4, Rc4) {
        (Rc4::skipping(b"a", 1024), Rc4::skipping(b"b", 1024))
    }

    #[tokio::test]
    async fn encrypted_streams_talk_through_a_small_pipe() {
        // A pipe smaller than the message, so writes wait on the reader.
        let (a, b) = duplex(100);
        let (a_to_b, b_to_a) = pair();
        let (a_to_b_too, b_to_a_too) = pair();
        let mut a = PeerStream::encrypted(a, Vec::new(), b_to_a, a_to_b);
        let mut b = PeerStream::encrypted(b, b"early ".to_vec(), a_to_b_too, b_to_a_too);
        let message: Vec<u8> = (0..256_000u32).map(|i| i as u8).collect();
        let sent = message.clone();
        let writer = tokio::spawn(async move {
            a.write_all(&sent).await.unwrap();
            a.flush().await.unwrap();
            a
        });
        let mut received = vec![0u8; 6 + message.len()];
        b.read_exact(&mut received).await.unwrap();
        assert_eq!(&received[..6], b"early ");
        assert_eq!(&received[6..], &message[..]);
        let a = writer.await.unwrap();
        assert!(a.is_encrypted());
    }

    /// Makes progress without ever draining an entire encrypted buffer.
    #[derive(Default)]
    struct PartialWriter(bool);

    impl AsyncWrite for PartialWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            data: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0 = !self.0;
            if self.0 {
                Poll::Ready(Ok(data.len().min(1)))
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn partial_writes_keep_the_ciphertext_buffer_bounded() {
        let (read, write) = pair();
        let mut stream = PeerStream::encrypted(PartialWriter::default(), Vec::new(), read, write);
        for _ in 0..300 {
            let _ = stream.write(&[0xab; 1024]).await.unwrap();
            assert!(stream.unsent.len() <= MAX_UNSENT);
        }
    }

    #[tokio::test]
    async fn what_goes_over_the_wire_is_encrypted() {
        let (a, mut wire) = duplex(1024);
        let (_, write) = pair();
        let mut a = PeerStream::encrypted(a, Vec::new(), Rc4::new(b"unused"), write);
        a.write_all(b"BitTorrent protocol").await.unwrap();
        a.flush().await.unwrap();
        let mut raw = [0u8; 19];
        wire.read_exact(&mut raw).await.unwrap();
        assert_ne!(&raw, b"BitTorrent protocol");
        let (_, mut decrypt) = pair();
        decrypt.apply(&mut raw);
        assert_eq!(&raw, b"BitTorrent protocol");
    }

    #[tokio::test]
    async fn a_plain_stream_passes_bytes_through_after_the_unread_ones() {
        let (a, mut wire) = duplex(1024);
        let mut a = PeerStream::with_unread(a, b"abc".to_vec());
        wire.write_all(b"def").await.unwrap();
        let mut read = [0u8; 6];
        a.read_exact(&mut read).await.unwrap();
        assert_eq!(&read, b"abcdef");
        a.write_all(b"xyz").await.unwrap();
        let mut raw = [0u8; 3];
        wire.read_exact(&mut raw).await.unwrap();
        assert_eq!(&raw, b"xyz");
        assert!(!a.is_encrypted());
    }
}
