//! The process-wide network policy, installed before any network clients start.
//! Explicit binding failures never fall back to an unrestricted socket.

use anyhow::{Context, Result, bail, ensure};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};

/// Binding and optional SOCKS5 routing shared by this process.
#[derive(Debug, Default)]
pub struct Policy {
    address: Option<IpAddr>,
    interface: Option<String>,
    proxy: Option<SocketAddr>,
}

static POLICY: OnceLock<Policy> = OnceLock::new();
static DIRECT: Policy = Policy {
    address: None,
    interface: None,
    proxy: None,
};

/// Installs the policy once, before any HTTP clients or sockets are created.
pub fn install(policy: Policy) -> Result<()> {
    POLICY
        .set(policy)
        .map_err(|_| anyhow::anyhow!("network policy already installed"))
}

/// The installed policy, or unrestricted defaults for library callers.
pub fn global() -> &'static Policy {
    POLICY.get().unwrap_or(&DIRECT)
}

impl Policy {
    /// Validates binding options and a numeric SOCKS5 proxy endpoint.
    pub fn new(
        address: Option<IpAddr>,
        interface: Option<String>,
        proxy: Option<&str>,
    ) -> Result<Self> {
        if let Some(name) = &interface {
            interface_index(name)?;
        }
        let proxy = proxy
            .map(|value| -> Result<SocketAddr> {
                let url = url::Url::parse(value).context("invalid SOCKS5 proxy URL")?;
                ensure!(
                    matches!(url.scheme(), "socks5" | "socks5h"),
                    "proxy must use socks5h://IP:PORT"
                );
                ensure!(
                    url.username().is_empty() && url.password().is_none(),
                    "SOCKS5 authentication is not supported"
                );
                ensure!(
                    url.path().is_empty() && url.query().is_none() && url.fragment().is_none(),
                    "proxy URL must contain only IP and port"
                );
                let host = url.host().context("proxy IP address missing")?;
                let ip = match host {
                    url::Host::Ipv4(ip) => ip.into(),
                    url::Host::Ipv6(ip) => ip.into(),
                    url::Host::Domain(host) => host.parse::<IpAddr>().context(
                        "proxy endpoint must be a numeric IP address to avoid local DNS",
                    )?,
                };
                let port = url.port().context("proxy port missing")?;
                ensure!(port != 0, "proxy port must not be zero");
                Ok(SocketAddr::new(ip, port))
            })
            .transpose()?;
        if let (Some(source), Some(proxy)) = (address, proxy) {
            ensure!(
                source.is_ipv4() == proxy.is_ipv4(),
                "bind address and proxy must use the same address family"
            );
        }
        Ok(Self {
            address,
            interface,
            proxy,
        })
    }

    /// The explicitly selected local source address, if any.
    pub fn source_address(&self) -> Option<IpAddr> {
        self.address
    }

    /// Whether peer and HTTP traffic must pass through SOCKS5.
    pub fn proxied(&self) -> bool {
        self.proxy.is_some()
    }

    /// Whether discovery protocols without binding support must be disabled.
    pub fn restricted(&self) -> bool {
        self.proxied() || self.address.is_some() || self.interface.is_some()
    }

    /// Local address for a socket; refuses an incompatible address family.
    pub fn bind_addr(&self, ipv6: bool, port: u16) -> Result<SocketAddr> {
        let ip = self.address.unwrap_or(if ipv6 {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        });
        ensure!(
            ip.is_ipv6() == ipv6,
            "destination address family differs from bind-address"
        );
        Ok(SocketAddr::new(ip, port))
    }

    /// Restricts a socket to the selected interface, before connecting or binding.
    pub fn bind_socket(&self, socket: &SockRef<'_>, ipv6: bool) -> Result<()> {
        if let Some(name) = &self.interface {
            let index = interface_index(name)?;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                let _ = (index, ipv6);
                socket.bind_device(Some(name.as_bytes()))?;
            }
            #[cfg(target_os = "macos")]
            if ipv6 {
                socket.bind_device_by_index_v6(Some(index))?;
            } else {
                socket.bind_device_by_index_v4(Some(index))?;
            }
            #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
            {
                let _ = (socket, ipv6, index);
                bail!("interface binding is unsupported on this platform");
            }
        }
        Ok(())
    }

    /// Creates a bound UDP socket; unavailable through a TCP-only proxy.
    pub fn udp_socket(&self, ipv6: bool, port: u16) -> Result<UdpSocket> {
        ensure!(!self.proxied(), "UDP is disabled by the SOCKS5 proxy");
        let socket = Socket::new(
            if ipv6 { Domain::IPV6 } else { Domain::IPV4 },
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        if ipv6 {
            socket.set_only_v6(true)?;
        }
        self.bind_socket(&SockRef::from(&socket), ipv6)?;
        socket.bind(&self.bind_addr(ipv6, port)?.into())?;
        socket.set_nonblocking(true)?;
        Ok(UdpSocket::from_std(socket.into())?)
    }

    async fn direct_connect(&self, addr: SocketAddr) -> Result<TcpStream> {
        let socket = if addr.is_ipv6() {
            TcpSocket::new_v6()?
        } else {
            TcpSocket::new_v4()?
        };
        self.bind_socket(&SockRef::from(&socket), addr.is_ipv6())?;
        socket.bind(self.bind_addr(addr.is_ipv6(), 0)?)?;
        Ok(socket.connect(addr).await?)
    }

    /// Connects to a numeric peer, through the proxy when configured.
    pub async fn connect(&self, addr: SocketAddr) -> Result<TcpStream> {
        let Some(proxy) = self.proxy else {
            return self.direct_connect(addr).await;
        };
        let mut stream = self.direct_connect(proxy).await?;
        stream.write_all(&[5, 1, 0]).await?;
        let mut greeting = [0; 2];
        stream.read_exact(&mut greeting).await?;
        ensure!(
            greeting == [5, 0],
            "SOCKS5 proxy refused unauthenticated connections"
        );
        let mut request = vec![5, 1, 0];
        match addr.ip() {
            IpAddr::V4(ip) => {
                request.push(1);
                request.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                request.push(4);
                request.extend_from_slice(&ip.octets());
            }
        }
        request.extend_from_slice(&addr.port().to_be_bytes());
        stream.write_all(&request).await?;
        let mut header = [0; 4];
        stream.read_exact(&mut header).await?;
        ensure!(
            header[0] == 5 && header[1] == 0 && header[2] == 0,
            "SOCKS5 CONNECT failed (reply {})",
            header[1]
        );
        let len = match header[3] {
            1 => 4,
            4 => 16,
            3 => usize::from(stream.read_u8().await?),
            _ => bail!("invalid SOCKS5 reply address"),
        };
        let mut bound = vec![0; len + 2];
        stream.read_exact(&mut bound).await?;
        Ok(stream)
    }

    /// HTTP client builder with the same binding and remote-DNS proxy policy.
    pub fn http_builder(&self) -> reqwest::ClientBuilder {
        let mut builder = reqwest::Client::builder();
        if self.restricted() {
            builder = builder.no_proxy();
        }
        if let Some(ip) = self.address {
            builder = builder.local_address(ip);
        }
        #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
        if let Some(name) = &self.interface {
            builder = builder.interface(name);
        }
        if let Some(proxy) = self.proxy {
            builder = builder.proxy(
                reqwest::Proxy::all(format!("socks5h://{proxy}"))
                    .expect("validated numeric SOCKS5 endpoint"),
            );
        }
        builder
    }
}

fn interface_index(name: &str) -> Result<std::num::NonZeroU32> {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
    {
        let name = std::ffi::CString::new(name).context("interface name contains a zero byte")?;
        // The C string remains alive for this call; if_nametoindex takes no ownership.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        std::num::NonZeroU32::new(index).context("network interface does not exist")
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    {
        let _ = name;
        bail!("interface binding is unsupported on this platform")
    }
}

#[cfg(test)]
mod tests;
