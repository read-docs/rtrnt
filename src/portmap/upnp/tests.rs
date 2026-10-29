use super::*;

const SSDP_ANSWER: &str = "HTTP/1.1 200 OK\r\n\
CACHE-CONTROL: max-age=120\r\n\
ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\
USN: uuid:abc::urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\
EXT:\r\n\
Server: Linux/4.9 UPnP/1.1 MiniUPnPd/2.1\r\n\
Location: http://192.168.1.1:5000/rootDesc.xml\r\n\
\r\n";

const DESCRIPTION_XML: &str = r#"<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType>
        <controlURL>/ctl/L3F</controlURL>
      </service>
    </serviceList>
    <deviceList>
      <device>
        <deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
        <deviceList>
          <device>
            <deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType>
            <serviceList>
              <service>
                <serviceType>urn:schemas-upnp-org:service:WANPPPConnection:1</serviceType>
                <controlURL>/ctl/PPP</controlURL>
              </service>
              <service>
                <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
                <SCPDURL>/WANIPCn.xml</SCPDURL>
                <controlURL>/ctl/IPConn?a=1&amp;b=2</controlURL>
              </service>
            </serviceList>
          </device>
        </deviceList>
      </device>
    </deviceList>
  </device>
</root>"#;

fn location() -> Url {
    Url::parse("http://192.168.1.1:5000/rootDesc.xml").unwrap()
}

#[test]
fn the_search_asks_for_the_gateway_device() {
    let request = search_request(SEARCH_TARGETS[0]);
    assert!(request.starts_with("M-SEARCH * HTTP/1.1\r\n"));
    assert!(request.contains("HOST: 239.255.255.250:1900\r\n"));
    assert!(request.contains("MAN: \"ssdp:discover\"\r\n"));
    assert!(request.contains("ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n"));
    assert!(request.ends_with("\r\n\r\n"));
}

#[test]
fn reads_the_location_of_an_answer() {
    assert_eq!(parse_ssdp_response(SSDP_ANSWER), Some(location()));
}

#[test]
fn other_datagrams_have_no_location() {
    assert_eq!(parse_ssdp_response(""), None);
    assert_eq!(
        parse_ssdp_response("HTTP/1.1 500 Oops\r\nLOCATION: http://1.2.3.4/x\r\n\r\n"),
        None
    );
    // Someone else's search, which we hear on the multicast group.
    let search =
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nLOCATION: http://1.2.3.4/\r\n\r\n";
    assert_eq!(parse_ssdp_response(search), None);
    assert_eq!(
        parse_ssdp_response("HTTP/1.1 200 OK\r\nST: x\r\n\r\n"),
        None
    );
    assert_eq!(
        parse_ssdp_response("HTTP/1.1 200 OK\r\nLOCATION: not a url\r\n\r\n"),
        None
    );
}

#[test]
fn only_local_http_is_fetched() {
    let local = |url: &str| is_local_http(&Url::parse(url).unwrap());
    assert!(local("http://192.168.1.1:5000/x"));
    assert!(local("http://10.0.0.1/x"));
    assert!(local("http://169.254.1.1/x"));
    assert!(!local("http://93.184.216.34/x"));
    assert!(!local("https://192.168.1.1/x"));
    assert!(!local("http://router.example/x"));
    assert!(!local("http://127.0.0.1/x"));
}

#[test]
fn scans_tags() {
    let xml = "<a><b>one</b><bc>no</bc><b x=\"1\"> two </b><b/><b>three</b></a>";
    assert_eq!(tag_values(xml, "b"), ["one", "two", "", "three"]);
    assert_eq!(tag_values(xml, "bc"), ["no"]);
    assert!(tag_values(xml, "z").is_empty());
    // An unclosed element ends the scan.
    assert_eq!(tag_values("<b>one</b><b>two", "b"), ["one"]);
}

#[test]
fn finds_the_ip_connection_service() {
    let service = find_service(DESCRIPTION_XML, &location()).unwrap();
    assert_eq!(
        service.service_type,
        "urn:schemas-upnp-org:service:WANIPConnection:1"
    );
    // Relative to the description, with the escape undone.
    assert_eq!(
        service.control_url.as_str(),
        "http://192.168.1.1:5000/ctl/IPConn?a=1&b=2"
    );
}

#[test]
fn falls_back_to_the_ppp_service() {
    let xml = DESCRIPTION_XML.replace("WANIPConnection", "Other");
    let service = find_service(&xml, &location()).unwrap();
    assert!(service.service_type.contains("WANPPPConnection"));
    assert_eq!(service.control_url.path(), "/ctl/PPP");
}

#[test]
fn the_url_base_wins_over_the_location() {
    let xml = DESCRIPTION_XML.replace(
        "<specVersion>",
        "<URLBase>http://192.168.1.1:49152/</URLBase><specVersion>",
    );
    let service = find_service(&xml, &location()).unwrap();
    assert_eq!(service.control_url.port(), Some(49152));
}

#[test]
fn no_mapping_service_gives_none() {
    let xml = "<root><service><serviceType>urn:x:service:Layer3Forwarding:1</serviceType>\
               <controlURL>/x</controlURL></service></root>";
    assert_eq!(find_service(xml, &location()), None);
    assert_eq!(find_service("", &location()), None);
}

#[test]
fn builds_the_add_mapping_call() {
    let service = "urn:schemas-upnp-org:service:WANIPConnection:1";
    let arguments =
        add_mapping_arguments(Protocol::Tcp, 6881, Ipv4Addr::new(192, 168, 1, 50), 3600);
    let body = soap_envelope(service, "AddPortMapping", &arguments);
    assert!(body.starts_with("<?xml version=\"1.0\"?>"));
    assert!(body.contains(&format!("<u:AddPortMapping xmlns:u=\"{service}\">")));
    assert!(body.contains("<NewRemoteHost></NewRemoteHost>"));
    assert!(body.contains("<NewExternalPort>6881</NewExternalPort>"));
    assert!(body.contains("<NewProtocol>TCP</NewProtocol>"));
    assert!(body.contains("<NewInternalPort>6881</NewInternalPort>"));
    assert!(body.contains("<NewInternalClient>192.168.1.50</NewInternalClient>"));
    assert!(body.contains("<NewEnabled>1</NewEnabled>"));
    assert!(body.contains("<NewLeaseDuration>3600</NewLeaseDuration>"));
    assert!(body.ends_with("</u:AddPortMapping></s:Body></s:Envelope>"));
}

#[test]
fn builds_the_delete_call_and_escapes_values() {
    let arguments = delete_mapping_arguments(Protocol::Udp, 6881);
    let body = soap_envelope(
        "urn:x:service:WANIPConnection:1",
        "DeletePortMapping",
        &arguments,
    );
    assert!(body.contains("<NewProtocol>UDP</NewProtocol>"));
    assert!(!body.contains("NewInternalClient"));
    let body = soap_envelope("t", "A", &[("X", "a<b&c".to_string())]);
    assert!(body.contains("<X>a&lt;b&amp;c</X>"));
    // No arguments, as for GetExternalIPAddress.
    assert!(soap_envelope("t", "A", &[]).contains("<u:A xmlns:u=\"t\"></u:A>"));
}

#[test]
fn parses_the_external_ip() {
    let body = "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">\
        <s:Body><u:GetExternalIPAddressResponse xmlns:u=\"urn:x\">\
        <NewExternalIPAddress>203.0.113.7</NewExternalIPAddress>\
        </u:GetExternalIPAddressResponse></s:Body></s:Envelope>";
    assert_eq!(parse_external_ip(body), Some(Ipv4Addr::new(203, 0, 113, 7)));
    let unconnected = body.replace("203.0.113.7", "0.0.0.0");
    assert_eq!(parse_external_ip(&unconnected), None);
    assert_eq!(parse_external_ip(&body.replace("203.0.113.7", "")), None);
    assert_eq!(parse_external_ip("<html></html>"), None);
}

#[test]
fn parses_a_soap_fault() {
    let body = "<s:Envelope><s:Body><s:Fault><faultcode>s:Client</faultcode>\
        <faultstring>UPnPError</faultstring><detail>\
        <UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\">\
        <errorCode>725</errorCode>\
        <errorDescription>OnlyPermanentLeasesSupported</errorDescription>\
        </UPnPError></detail></s:Fault></s:Body></s:Envelope>";
    assert_eq!(
        parse_soap_fault(body),
        Some((725, "OnlyPermanentLeasesSupported".to_string()))
    );
    assert_eq!(parse_soap_fault("<html>Bad gateway</html>"), None);
}
