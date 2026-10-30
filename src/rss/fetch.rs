//! Getting a feed or a `.torrent` over HTTP, with a cap on the size.
//!
//! The poller reaches the network through the `Fetch` trait, so that its
//! tests can give it canned answers; `HttpFetcher` is the real one.

use reqwest::StatusCode;
use std::fmt;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;
use url::Url;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A whole request, body included, is given up on after this long.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Shared by every feed and torrent download, so connections to a server
/// are reused.
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .user_agent(concat!("rtrnt/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("the HTTP client builds with its default settings")
});

/// Why a document could not be got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchError {
    /// What went wrong.
    pub message: String,
    /// Whether asking again later may work: the server or the network
    /// failed, rather than the answer being refused or too big.
    pub retry: bool,
}

impl FetchError {
    pub fn new(message: impl Into<String>, retry: bool) -> Self {
        Self {
            message: message.into(),
            retry,
        }
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FetchError {}

/// Gets documents by URL.
pub trait Fetch {
    /// The body of `url`, which must not be more than `limit` bytes.
    fn get(
        &self,
        url: &Url,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<u8>, FetchError>> + Send;
}

/// Fetches over HTTP and HTTPS.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpFetcher;

/// A failure to reach the server or read what it sent: worth retrying.
fn transport(error: reqwest::Error) -> FetchError {
    // The URL, which may hold a key, is left out of the message.
    FetchError::new(error.without_url().to_string(), true)
}

impl Fetch for HttpFetcher {
    async fn get(&self, url: &Url, limit: usize) -> Result<Vec<u8>, FetchError> {
        let mut response = CLIENT.get(url.clone()).send().await.map_err(transport)?;
        let status = response.status();
        if !status.is_success() {
            let retry = status.is_server_error()
                || status == StatusCode::TOO_MANY_REQUESTS
                || status == StatusCode::REQUEST_TIMEOUT;
            return Err(FetchError::new(
                format!("the server answered {status}"),
                retry,
            ));
        }
        let too_big = || FetchError::new(format!("more than {limit} bytes"), false);
        if response
            .content_length()
            .is_some_and(|len| usize::try_from(len).map_or(true, |len| len > limit))
        {
            return Err(too_big());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if body.len() + chunk.len() > limit {
                return Err(too_big());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}
