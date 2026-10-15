//! Fixed host-enforced policies. No caller-supplied allow rules or port mappings.

use std::{collections::BTreeSet, net::Ipv4Addr};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Network {
    Offline,
    PublicInternet,
}

/// Canonicalize a CIDR; reject shorthand and accidental default-route exclusions.
pub fn cidr(value: &str) -> Result<String> {
    let (address, prefix) = value.split_once('/').context("expected IPv4 CIDR")?;
    let address: Ipv4Addr = address.parse().context("invalid IPv4 address")?;
    let prefix: u32 = prefix.parse().context("invalid IPv4 prefix")?;
    if prefix > 32 {
        bail!("IPv4 prefix exceeds 32");
    }
    let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
    Ok(format!(
        "{}/{}",
        Ipv4Addr::from(u32::from(address) & mask),
        prefix
    ))
}

pub fn block_rules(mode: Network, local: &[String], extra: &[String]) -> Result<String> {
    let mut rules = BTreeSet::from(["in 0.0.0.0/0".to_string()]);
    match mode {
        Network::Offline => {
            rules.insert("out 0.0.0.0/0".into());
        }
        Network::PublicInternet => {
            // Softnet's implicit global-address fallback admits outbound public IPv4.
            // Explicitly remove its gateway exception. A directional rule enables
            // return-flow tracking; the inbound deny does not block those replies.
            rules.insert("out @host".into());
            for subnet in local.iter().chain(extra) {
                rules.insert(format!("out {}", cidr(subnet)?));
            }
        }
    }
    Ok(rules.into_iter().collect::<Vec<_>>().join(","))
}

/// IPv4 networks attached to this host, including VPN interfaces. This is read again
/// while a public VM runs; a change stops the VM before it is considered healthy.
pub fn local_subnets() -> Result<Vec<String>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    struct Addresses(*mut libc::ifaddrs);
    impl Drop for Addresses {
        fn drop(&mut self) {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _addresses = Addresses(head);
    let mut found = BTreeSet::new();
    let mut current = head;
    while !current.is_null() {
        let interface = unsafe { &*current };
        if !interface.ifa_addr.is_null()
            && !interface.ifa_netmask.is_null()
            && unsafe { (*interface.ifa_addr).sa_family as i32 } == libc::AF_INET
        {
            let address = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in) };
            let mask = unsafe { &*(interface.ifa_netmask as *const libc::sockaddr_in) };
            let address = u32::from_be(address.sin_addr.s_addr);
            let mask = u32::from_be(mask.sin_addr.s_addr);
            let prefix = mask.leading_ones();
            if mask != u32::MAX.checked_shl(32 - prefix).unwrap_or(0) {
                bail!("non-contiguous interface netmask");
            }
            let ip = Ipv4Addr::from(address);
            // These are always denied by Softnet. Ignoring their changing virtual
            // interfaces avoids treating the VM's own new bridge as a route change.
            if !ip.is_private() && !ip.is_loopback() && !ip.is_link_local() && !ip.is_unspecified()
            {
                found.insert(format!("{}/{}", Ipv4Addr::from(address & mask), prefix));
            }
        }
        current = interface.ifa_next;
    }
    Ok(found.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_never_add_an_allow_or_expose_rule() {
        assert_eq!(
            block_rules(Network::Offline, &[], &[]).unwrap(),
            "in 0.0.0.0/0,out 0.0.0.0/0"
        );
        assert_eq!(
            block_rules(Network::PublicInternet, &["8.8.8.17/24".into()], &[]).unwrap(),
            "in 0.0.0.0/0,out 8.8.8.0/24,out @host"
        );
        for bad in [
            "@host",
            "1.2.3.4/33",
            "1.2.3.4/24,--allow=0.0.0.0/0",
            "../a",
        ] {
            assert!(cidr(bad).is_err());
        }
    }
}
