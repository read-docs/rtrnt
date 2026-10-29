use super::*;

#[test]
fn reads_the_default_route_of_proc_net_route() {
    let text = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n\
                eth0\t00000000\t0101A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0\n\
                eth0\t0001A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0\n";
    let gateway = parse_proc_net_route(text).unwrap();
    assert_eq!(gateway.octets(), 0x0101A8C0u32.to_ne_bytes());
}

#[test]
fn proc_net_route_without_a_usable_default_gives_none() {
    let header = "Iface\tDestination\tGateway \tFlags\n";
    // The only route is the local network, and the default one is down.
    let text = format!("{header}eth0\t0001A8C0\t00000000\t0001\neth0\t00000000\t0101A8C0\t0002\n");
    assert_eq!(parse_proc_net_route(&text), None);
    assert_eq!(parse_proc_net_route(""), None);
    assert_eq!(parse_proc_net_route("garbage\nmore garbage\n"), None);
}

#[test]
fn reads_the_gateway_of_route_get() {
    let text = "   route to: default\n\
                destination: default\n\
                       mask: default\n\
                    gateway: 192.168.1.1\n\
                  interface: en0\n";
    assert_eq!(parse_route_get(text), Some(Ipv4Addr::new(192, 168, 1, 1)));
}

#[test]
fn route_get_with_no_ipv4_gateway_gives_none() {
    assert_eq!(parse_route_get("    gateway: fe80::1%en0\n"), None);
    assert_eq!(parse_route_get("   interface: utun3\n"), None);
    assert_eq!(parse_route_get(""), None);
}
