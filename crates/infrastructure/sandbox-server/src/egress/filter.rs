//! Destination address and host policy for the egress proxy.
//!
//! The proxy resolves the destination itself and connects to exactly the
//! address it checked, so DNS rebinding cannot swap the target after the
//! check. Every resolved address must pass; one forbidden address refuses
//! the whole destination.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipnet::IpNet;

/// Why a destination was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    ResolutionFailed,
    AuditLimit,
    Unspecified,
    Loopback,
    Private,
    SharedAddressSpace,
    LinkLocal,
    CloudMetadata,
    Multicast,
    Broadcast,
    Reserved,
    Documentation,
    UniqueLocal,
    EmbeddedIpv4,
    HostDenyList,
    CidrDenyList,
    PortNotAllowed,
}

impl DenyReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResolutionFailed => "resolve_failed",
            Self::AuditLimit => "audit_limit",
            Self::Unspecified => "unspecified",
            Self::Loopback => "loopback",
            Self::Private => "private",
            Self::SharedAddressSpace => "shared_address_space",
            Self::LinkLocal => "link_local",
            Self::CloudMetadata => "cloud_metadata",
            Self::Multicast => "multicast",
            Self::Broadcast => "broadcast",
            Self::Reserved => "reserved",
            Self::Documentation => "documentation",
            Self::UniqueLocal => "unique_local",
            Self::EmbeddedIpv4 => "embedded_ipv4",
            Self::HostDenyList => "host_deny_list",
            Self::CidrDenyList => "cidr_deny_list",
            Self::PortNotAllowed => "port_not_allowed",
        }
    }

    /// Hard denials cannot be lifted by an allow-list exception.
    #[must_use]
    pub const fn is_hard(self) -> bool {
        matches!(
            self,
            Self::ResolutionFailed
                | Self::AuditLimit
                | Self::Loopback
                | Self::Private
                | Self::SharedAddressSpace
                | Self::UniqueLocal
                | Self::EmbeddedIpv4
                | Self::Unspecified
                | Self::LinkLocal
                | Self::CloudMetadata
                | Self::Multicast
                | Self::Broadcast
                | Self::HostDenyList
                | Self::CidrDenyList
                | Self::PortNotAllowed
        )
    }
}

/// Built-in classification of one address, independent of host config.
#[must_use]
pub fn classify(address: IpAddr) -> Option<DenyReason> {
    match address {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

fn classify_v4(address: Ipv4Addr) -> Option<DenyReason> {
    let [a, b, c, _] = address.octets();
    if address == Ipv4Addr::new(169, 254, 169, 254) || address == Ipv4Addr::new(100, 100, 100, 200)
    {
        return Some(DenyReason::CloudMetadata);
    }
    if address.is_unspecified() || a == 0 {
        return Some(DenyReason::Unspecified);
    }
    if address.is_loopback() {
        return Some(DenyReason::Loopback);
    }
    if address.is_private() {
        return Some(DenyReason::Private);
    }
    if a == 100 && (64..128).contains(&b) {
        return Some(DenyReason::SharedAddressSpace);
    }
    if address.is_link_local() {
        return Some(DenyReason::LinkLocal);
    }
    if address.is_broadcast() {
        return Some(DenyReason::Broadcast);
    }
    if address.is_multicast() {
        return Some(DenyReason::Multicast);
    }
    if address.is_documentation() {
        return Some(DenyReason::Documentation);
    }
    // 192.0.0.0/24 IETF protocol assignments, 198.18.0.0/15 benchmarking,
    // 240.0.0.0/4 reserved (incl. broadcast handled above).
    if (a == 192 && b == 0 && c == 0) || (a == 198 && (b == 18 || b == 19)) || a >= 240 {
        return Some(DenyReason::Reserved);
    }
    None
}

fn classify_v6(address: Ipv6Addr) -> Option<DenyReason> {
    if let Some(v4) = address.to_ipv4_mapped() {
        return classify_v4(v4);
    }
    let segments = address.segments();
    if address.is_unspecified() {
        return Some(DenyReason::Unspecified);
    }
    if address.is_loopback() {
        return Some(DenyReason::Loopback);
    }
    if address == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254) {
        return Some(DenyReason::CloudMetadata);
    }
    if address.is_multicast() {
        return Some(DenyReason::Multicast);
    }
    // fe80::/10 link-local; fec0::/10 deprecated site-local.
    if (segments[0] & 0xffc0) == 0xfe80 {
        return Some(DenyReason::LinkLocal);
    }
    if (segments[0] & 0xffc0) == 0xfec0 {
        return Some(DenyReason::Private);
    }
    if (segments[0] & 0xfe00) == 0xfc00 {
        return Some(DenyReason::UniqueLocal);
    }
    // ::/96 IPv4-compatible (deprecated) and other reserved low space.
    if segments[..6].iter().all(|segment| *segment == 0) {
        return Some(DenyReason::Reserved);
    }
    // 2001:db8::/32 documentation.
    if segments[0] == 0x2001 && segments[1] == 0x0db8 {
        return Some(DenyReason::Documentation);
    }
    // 2001::/32 Teredo tunnels carry an obfuscated IPv4 peer.
    if segments[0] == 0x2001 && segments[1] == 0 {
        return Some(DenyReason::EmbeddedIpv4);
    }
    // 64:ff9b::/96 and 64:ff9b:1::/48 NAT64: judge the embedded IPv4.
    if segments[0] == 0x0064 && segments[1] == 0xff9b {
        let embedded = Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            (segments[6] & 0xff) as u8,
            (segments[7] >> 8) as u8,
            (segments[7] & 0xff) as u8,
        );
        return classify_v4(embedded).map(|_| DenyReason::EmbeddedIpv4);
    }
    // 2002::/16 6to4: embedded IPv4 in bits 16..48.
    if segments[0] == 0x2002 {
        let embedded = Ipv4Addr::new(
            (segments[1] >> 8) as u8,
            (segments[1] & 0xff) as u8,
            (segments[2] >> 8) as u8,
            (segments[2] & 0xff) as u8,
        );
        return classify_v4(embedded).map(|_| DenyReason::EmbeddedIpv4);
    }
    // 100::/64 discard-only.
    if segments[0] == 0x0100 && segments[1..4].iter().all(|segment| *segment == 0) {
        return Some(DenyReason::Reserved);
    }
    None
}

/// Host-configured egress policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EgressPolicy {
    pub deny_cidrs: Vec<IpNet>,
    /// Lower-case host names; an entry also matches its subdomains.
    pub deny_hosts: Vec<String>,
    /// Exceptions to the overridable built-in denials.
    pub allow_cidrs: Vec<IpNet>,
    /// `None` allows every port.
    pub allowed_ports: Option<std::collections::BTreeSet<u16>>,
}

impl EgressPolicy {
    /// Checks the destination host name and port before resolution.
    pub fn check_host(&self, host: &str, port: u16) -> Result<(), DenyReason> {
        if let Some(ports) = &self.allowed_ports
            && !ports.contains(&port)
        {
            return Err(DenyReason::PortNotAllowed);
        }
        let host = normalize_host(host);
        if self.deny_hosts.iter().any(|denied| {
            let denied = normalize_host(denied);
            host == denied || host.ends_with(&format!(".{denied}"))
        }) {
            return Err(DenyReason::HostDenyList);
        }
        Ok(())
    }

    /// Checks one resolved (or literal) destination address.
    pub fn check_address(&self, address: IpAddr) -> Result<(), DenyReason> {
        let address = canonical(address);
        if self.deny_cidrs.iter().any(|net| net.contains(&address)) {
            return Err(DenyReason::CidrDenyList);
        }
        match classify(address) {
            None => Ok(()),
            Some(reason) if reason.is_hard() => Err(reason),
            Some(reason) => {
                if self.allow_cidrs.iter().any(|net| net.contains(&address)) {
                    Ok(())
                } else {
                    Err(reason)
                }
            }
        }
    }

    /// Every address must pass; the first refusal wins.
    pub fn check_all(&self, addresses: &[IpAddr]) -> Result<(), DenyReason> {
        if addresses.is_empty() {
            return Err(DenyReason::Unspecified);
        }
        addresses
            .iter()
            .try_for_each(|address| self.check_address(*address))
    }
}

fn canonical(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 @ IpAddr::V4(_) => v4,
    }
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("ip")
    }

    fn deny(value: &str) -> Option<DenyReason> {
        EgressPolicy::default().check_address(ip(value)).err()
    }

    #[test]
    fn public_addresses_pass() {
        for address in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.255.255",
            "172.32.0.1",
            "192.169.0.1",
            "2606:4700:4700::1111",
            "2a00:1450:4001:80b::200e",
            "64:ff9b::808:808",
            "2002:0808:0808::1",
        ] {
            assert_eq!(deny(address), None, "{address}");
        }
    }

    #[test]
    fn loopback_and_unspecified_are_refused() {
        assert_eq!(deny("127.0.0.1"), Some(DenyReason::Loopback));
        assert_eq!(deny("127.255.255.254"), Some(DenyReason::Loopback));
        assert_eq!(deny("::1"), Some(DenyReason::Loopback));
        assert_eq!(deny("0.0.0.0"), Some(DenyReason::Unspecified));
        assert_eq!(deny("0.1.2.3"), Some(DenyReason::Unspecified));
        assert_eq!(deny("::"), Some(DenyReason::Unspecified));
    }

    #[test]
    fn rfc1918_ranges_are_refused_at_their_edges() {
        for address in [
            "10.0.0.0",
            "10.255.255.255",
            "172.16.0.0",
            "172.31.255.255",
            "192.168.0.0",
            "192.168.255.255",
        ] {
            assert_eq!(deny(address), Some(DenyReason::Private), "{address}");
        }
    }

    #[test]
    fn cgnat_shared_space_is_refused() {
        assert_eq!(deny("100.64.0.0"), Some(DenyReason::SharedAddressSpace));
        assert_eq!(
            deny("100.127.255.255"),
            Some(DenyReason::SharedAddressSpace)
        );
    }

    #[test]
    fn link_local_and_metadata_are_refused() {
        assert_eq!(deny("169.254.0.1"), Some(DenyReason::LinkLocal));
        assert_eq!(deny("169.254.169.254"), Some(DenyReason::CloudMetadata));
        assert_eq!(deny("100.100.100.200"), Some(DenyReason::CloudMetadata));
        assert_eq!(deny("fd00:ec2::254"), Some(DenyReason::CloudMetadata));
        assert_eq!(deny("fe80::1"), Some(DenyReason::LinkLocal));
        assert_eq!(deny("febf::1"), Some(DenyReason::LinkLocal));
    }

    #[test]
    fn ipv6_private_style_ranges_are_refused() {
        assert_eq!(deny("fc00::1"), Some(DenyReason::UniqueLocal));
        assert_eq!(deny("fdff:ffff::1"), Some(DenyReason::UniqueLocal));
        assert_eq!(deny("fec0::1"), Some(DenyReason::Private));
        assert_eq!(deny("ff02::1"), Some(DenyReason::Multicast));
        assert_eq!(deny("2001:db8::1"), Some(DenyReason::Documentation));
        assert_eq!(deny("::7f00:1"), Some(DenyReason::Reserved));
        assert_eq!(deny("100::1"), Some(DenyReason::Reserved));
    }

    #[test]
    fn ipv4_mapped_and_embedded_ipv6_cannot_smuggle_private_targets() {
        assert_eq!(deny("::ffff:127.0.0.1"), Some(DenyReason::Loopback));
        assert_eq!(deny("::ffff:10.1.2.3"), Some(DenyReason::Private));
        assert_eq!(
            deny("::ffff:169.254.169.254"),
            Some(DenyReason::CloudMetadata)
        );
        assert_eq!(deny("64:ff9b::a9fe:a9fe"), Some(DenyReason::EmbeddedIpv4));
        assert_eq!(deny("64:ff9b::7f00:1"), Some(DenyReason::EmbeddedIpv4));
        assert_eq!(deny("2002:c0a8:0101::1"), Some(DenyReason::EmbeddedIpv4));
        assert_eq!(deny("2002:7f00:0001::1"), Some(DenyReason::EmbeddedIpv4));
        assert_eq!(deny("2001:0:4136:e378::1"), Some(DenyReason::EmbeddedIpv4));
    }

    #[test]
    fn multicast_broadcast_and_reserved_v4_are_refused() {
        assert_eq!(deny("224.0.0.1"), Some(DenyReason::Multicast));
        assert_eq!(deny("239.255.255.255"), Some(DenyReason::Multicast));
        assert_eq!(deny("255.255.255.255"), Some(DenyReason::Broadcast));
        assert_eq!(deny("240.0.0.1"), Some(DenyReason::Reserved));
        assert_eq!(deny("192.0.0.8"), Some(DenyReason::Reserved));
        assert_eq!(deny("198.18.0.1"), Some(DenyReason::Reserved));
        assert_eq!(deny("198.19.255.255"), Some(DenyReason::Reserved));
        assert_eq!(deny("192.0.2.1"), Some(DenyReason::Documentation));
        assert_eq!(deny("198.51.100.1"), Some(DenyReason::Documentation));
        assert_eq!(deny("203.0.113.1"), Some(DenyReason::Documentation));
    }

    #[test]
    fn host_deny_list_applies_to_exact_and_subdomains() {
        let policy = EgressPolicy {
            deny_hosts: vec!["Example.COM.".to_owned()],
            ..Default::default()
        };
        assert_eq!(
            policy.check_host("example.com", 443),
            Err(DenyReason::HostDenyList)
        );
        assert_eq!(
            policy.check_host("API.example.com.", 443),
            Err(DenyReason::HostDenyList)
        );
        assert_eq!(policy.check_host("notexample.com", 443), Ok(()));
    }

    #[test]
    fn cidr_deny_list_overrides_everything() {
        let policy = EgressPolicy {
            deny_cidrs: vec!["8.8.8.0/24".parse().expect("net")],
            allow_cidrs: vec!["8.8.8.0/24".parse().expect("net")],
            ..Default::default()
        };
        assert_eq!(
            policy.check_address(ip("8.8.8.8")),
            Err(DenyReason::CidrDenyList)
        );
        assert_eq!(
            policy.check_address(ip("::ffff:8.8.8.8")),
            Err(DenyReason::CidrDenyList)
        );
        assert_eq!(policy.check_address(ip("8.8.4.4")), Ok(()));
    }

    #[test]
    fn allow_list_cannot_lift_private_or_loopback_denials() {
        let policy = EgressPolicy {
            allow_cidrs: vec![
                "10.20.0.0/16".parse().expect("net"),
                "169.254.0.0/16".parse().expect("net"),
                "127.0.0.1/32".parse().expect("net"),
            ],
            ..Default::default()
        };
        assert_eq!(
            policy.check_address(ip("10.20.1.1")),
            Err(DenyReason::Private)
        );
        assert_eq!(
            policy.check_address(ip("127.0.0.1")),
            Err(DenyReason::Loopback)
        );
        assert_eq!(
            policy.check_address(ip("10.21.1.1")),
            Err(DenyReason::Private)
        );
        assert_eq!(
            policy.check_address(ip("169.254.169.254")),
            Err(DenyReason::CloudMetadata)
        );
        assert_eq!(
            policy.check_address(ip("169.254.1.1")),
            Err(DenyReason::LinkLocal)
        );
    }

    #[test]
    fn port_allow_list_is_enforced() {
        let policy = EgressPolicy {
            allowed_ports: Some(BTreeSet::from([443])),
            ..Default::default()
        };
        assert_eq!(policy.check_host("example.org", 443), Ok(()));
        assert_eq!(
            policy.check_host("example.org", 22),
            Err(DenyReason::PortNotAllowed)
        );
    }

    #[test]
    fn one_bad_resolved_address_refuses_the_destination() {
        let policy = EgressPolicy::default();
        assert_eq!(policy.check_all(&[ip("1.1.1.1"), ip("8.8.8.8")]), Ok(()));
        assert_eq!(
            policy.check_all(&[ip("1.1.1.1"), ip("10.0.0.1")]),
            Err(DenyReason::Private)
        );
        assert_eq!(policy.check_all(&[]), Err(DenyReason::Unspecified));
    }
}
