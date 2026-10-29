//! UPnP Internet Gateway Device: SSDP discovery over multicast UDP, the
//! device description as XML over HTTP, and SOAP calls to the router's
//! `WANIPConnection` or `WANPPPConnection` service.
//!
//! The XML is read by a small tag scanner (`tag_values`), which is enough
//! for the few fixed elements needed here.

use super::{Granted, Protocol, local_ip_towards};
use anyhow::{Context, Result, anyhow, bail};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout_at};
use url::Url;

/// Where SSDP searches go.
const SSDP_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(239, 255, 255, 250)), 1900);
/// How long routers get to answer the search.
const SEARCH_WINDOW: Duration = Duration::from_secs(3);
/// How long each HTTP request may take.
const HTTP_TIMEOUT: Duration = Duration::from_secs(4);
/// What the router calls our mappings.
const DESCRIPTION: &str = "rtrnt";
/// The search targets: the gateway device of both versions.
const SEARCH_TARGETS: [&str; 2] = [
    "urn:schemas-upnp-org:device:InternetGatewayDevice:1",
    "urn:schemas-upnp-org:device:InternetGatewayDevice:2",
];
/// The services that map ports, best first.
const SERVICES: [&str; 2] = ["WANIPConnection", "WANPPPConnection"];

/// `AddPortMapping` error: the router only gives permanent leases.
const ERR_ONLY_PERMANENT_LEASES: u32 = 725;

/// The SSDP search for `target`, which routers answer by unicast.
pub fn search_request(target: &str) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: {target}\r\n\r\n"
    )
}

/// The device description URL in an SSDP answer (`LOCATION`), or `None` if
/// the datagram is not a successful answer or has none.
pub fn parse_ssdp_response(text: &str) -> Option<Url> {
    let mut lines = text.lines();
    let status = lines.next()?;
    let mut parts = status.split_whitespace();
    if !parts.next()?.starts_with("HTTP/1.") || parts.next()? != "200" {
        return None;
    }
    lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("location"))
        .and_then(|(_, value)| Url::parse(value.trim()).ok())
}

/// Whether a router would be at `url`: HTTP to a private or link-local
/// IPv4 address. What answers an SSDP search could be anything on the
/// network, and is not sent anywhere else.
fn is_local_http(url: &Url) -> bool {
    let local = match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_private() || address.is_link_local(),
        _ => false,
    };
    url.scheme() == "http" && local
}

/// The text of each `<tag>...</tag>` element in `xml`, in order, with the
/// tag's attributes ignored. Nested elements of the same name are not
/// told apart, which device descriptions do not have.
pub fn tag_values<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        // `<service` is not `<serviceList`.
        let Some(next) = after.chars().next() else {
            break;
        };
        if !(next == '>' || next == '/' || next.is_whitespace()) {
            rest = after;
            continue;
        }
        let Some(end_of_tag) = after.find('>') else {
            break;
        };
        if after[..end_of_tag].ends_with('/') {
            values.push("");
            rest = &after[end_of_tag + 1..];
            continue;
        }
        let body = &after[end_of_tag + 1..];
        let Some(end) = body.find(&close) else {
            break;
        };
        values.push(body[..end].trim());
        rest = &body[end + close.len()..];
    }
    values
}

/// The text of the first `<tag>` in `xml`, with the XML escapes undone.
fn first_tag(xml: &str, tag: &str) -> Option<String> {
    tag_values(xml, tag).first().map(|value| unescape(value))
}

/// `text` with `&amp;`, `&lt;`, `&gt;`, `&quot;` and `&apos;` replaced.
fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// `text` safe to put inside an element.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// A router's mapping service: what to POST to, and its type, which names
/// the SOAP actions.
#[derive(Debug, PartialEq, Eq)]
pub struct Service {
    pub control_url: Url,
    pub service_type: String,
}

/// The service that maps ports in the device description `xml`, fetched
/// from `location`. Its control URL may be relative to `<URLBase>`, or to
/// `location` without one. `None` if there is no such service.
pub fn find_service(xml: &str, location: &Url) -> Option<Service> {
    let base = first_tag(xml, "URLBase")
        .and_then(|base| Url::parse(&base).ok())
        .unwrap_or_else(|| location.clone());
    let services: Vec<(String, String)> = tag_values(xml, "service")
        .into_iter()
        .filter_map(|block| {
            Some((
                first_tag(block, "serviceType")?,
                first_tag(block, "controlURL")?,
            ))
        })
        .collect();
    SERVICES.iter().find_map(|wanted| {
        services.iter().find_map(|(service_type, control)| {
            let is_wanted = service_type.contains(&format!(":{wanted}:"));
            let control_url = base.join(control).ok()?;
            is_wanted.then(|| Service {
                control_url,
                service_type: service_type.clone(),
            })
        })
    })
}

/// The SOAP envelope calling `action` of `service_type` with `arguments`.
pub fn soap_envelope(service_type: &str, action: &str, arguments: &[(&str, String)]) -> String {
    let arguments: String = arguments
        .iter()
        .map(|(name, value)| format!("<{name}>{}</{name}>", escape(value)))
        .collect();
    format!(
        "<?xml version=\"1.0\"?>\r\n\
         <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
         s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\
         <s:Body><u:{action} xmlns:u=\"{service_type}\">{arguments}</u:{action}></s:Body>\
         </s:Envelope>"
    )
}

/// The arguments of `AddPortMapping`, mapping `port` of `protocol` on
/// `client` to the same port outside, for `lease_secs` (0: no end).
pub fn add_mapping_arguments(
    protocol: Protocol,
    port: u16,
    client: Ipv4Addr,
    lease_secs: u64,
) -> Vec<(&'static str, String)> {
    vec![
        ("NewRemoteHost", String::new()),
        ("NewExternalPort", port.to_string()),
        ("NewProtocol", protocol.name().to_string()),
        ("NewInternalPort", port.to_string()),
        ("NewInternalClient", client.to_string()),
        ("NewEnabled", "1".to_string()),
        ("NewPortMappingDescription", DESCRIPTION.to_string()),
        ("NewLeaseDuration", lease_secs.to_string()),
    ]
}

/// The arguments of `DeletePortMapping` for `port` of `protocol`.
pub fn delete_mapping_arguments(protocol: Protocol, port: u16) -> Vec<(&'static str, String)> {
    vec![
        ("NewRemoteHost", String::new()),
        ("NewExternalPort", port.to_string()),
        ("NewProtocol", protocol.name().to_string()),
    ]
}

/// The external address in a `GetExternalIPAddress` answer. Empty or
/// `0.0.0.0` (a router not yet connected) is none.
pub fn parse_external_ip(body: &str) -> Option<Ipv4Addr> {
    let address: Ipv4Addr = first_tag(body, "NewExternalIPAddress")?
        .trim()
        .parse()
        .ok()?;
    (!address.is_unspecified()).then_some(address)
}

/// The UPnP error code and description in a SOAP fault, if the body is one.
pub fn parse_soap_fault(body: &str) -> Option<(u32, String)> {
    let code = first_tag(body, "errorCode")?.trim().parse().ok()?;
    let description = first_tag(body, "errorDescription").unwrap_or_default();
    Some((code, description))
}

/// A router found by UPnP.
pub struct Upnp {
    service: Service,
    http: reqwest::Client,
    /// Our address on the network the router is on.
    local: Ipv4Addr,
}

impl Upnp {
    /// Looks for a router for a few seconds: asks the network by SSDP
    /// (and `gateway` directly, which some networks need) and reads the
    /// description of each answer until one has a mapping service.
    pub async fn discover(gateway: Option<Ipv4Addr>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        for target in SEARCH_TARGETS {
            let request = search_request(target);
            let _ = socket.send_to(request.as_bytes(), SSDP_ADDRESS).await;
            if let Some(gateway) = gateway {
                let _ = socket.send_to(request.as_bytes(), (gateway, 1900)).await;
            }
        }
        let deadline = Instant::now() + SEARCH_WINDOW;
        let mut tried = Vec::new();
        let mut last_error = anyhow!("no router answered");
        let mut buf = vec![0; 2048];
        while let Ok(received) = timeout_at(deadline, socket.recv_from(&mut buf)).await {
            let Ok((len, _)) = received else { break };
            let text = String::from_utf8_lossy(&buf[..len]);
            let Some(location) = parse_ssdp_response(&text) else {
                continue;
            };
            if tried.contains(&location) {
                continue;
            }
            match Self::from_location(&http, &location).await {
                Ok(upnp) => return Ok(upnp),
                Err(e) => last_error = e.context(format!("router at {location}")),
            }
            tried.push(location);
        }
        Err(last_error)
    }

    /// The router whose description is at `location`.
    async fn from_location(http: &reqwest::Client, location: &Url) -> Result<Self> {
        if !is_local_http(location) {
            bail!("not a local HTTP address");
        }
        let xml = http
            .get(location.clone())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let service =
            find_service(&xml, location).context("no WANIPConnection or WANPPPConnection")?;
        // The control URL may not send us elsewhere.
        if !is_local_http(&service.control_url) || service.control_url.host() != location.host() {
            bail!("control URL {} is not on the router", service.control_url);
        }
        let port = service.control_url.port_or_known_default().unwrap_or(80);
        let host = service.control_url.host_str().context("no host")?;
        let target: SocketAddr = (host.parse::<Ipv4Addr>()?, port).into();
        let local = local_ip_towards(target).await?;
        Ok(Self {
            service,
            http: http.clone(),
            local,
        })
    }

    /// Calls the SOAP `action` and returns the body of the answer. A fault
    /// is an error, with the UPnP code in its text.
    async fn call(&self, action: &str, arguments: &[(&str, String)]) -> Result<String> {
        let service_type = &self.service.service_type;
        let response = self
            .http
            .post(self.service.control_url.clone())
            .header("Content-Type", "text/xml; charset=\"utf-8\"")
            .header("SOAPAction", format!("\"{service_type}#{action}\""))
            .body(soap_envelope(service_type, action, arguments))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if status.is_success() {
            return Ok(body);
        }
        match parse_soap_fault(&body) {
            Some((code, description)) => bail!("{action}: error {code} {description}"),
            None => bail!("{action}: HTTP {status}"),
        }
    }

    /// The address the router has on the outside.
    pub async fn external_ip(&self) -> Result<Ipv4Addr> {
        let body = self.call("GetExternalIPAddress", &[]).await?;
        parse_external_ip(&body).context("the router has no external address yet")
    }

    /// Maps `port` of `protocol` to the same port outside, for `lifetime`.
    /// A router that only gives permanent mappings gets one of those.
    pub async fn map(&self, protocol: Protocol, port: u16, lifetime: Duration) -> Result<Granted> {
        let lease = |secs| add_mapping_arguments(protocol, port, self.local, secs);
        let mut granted = lifetime;
        let mut result = self
            .call("AddPortMapping", &lease(lifetime.as_secs()))
            .await;
        if let Err(e) = &result
            && e.to_string()
                .contains(&format!("error {ERR_ONLY_PERMANENT_LEASES}"))
        {
            granted = Duration::ZERO;
            result = self.call("AddPortMapping", &lease(0)).await;
        }
        result?;
        Ok(Granted {
            external_port: port,
            lifetime: granted,
            external_ip: None,
        })
    }

    /// Removes the mapping of external `port`.
    pub async fn unmap(&self, protocol: Protocol, port: u16) -> Result<()> {
        self.call(
            "DeletePortMapping",
            &delete_mapping_arguments(protocol, port),
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests;
