//! Address epee serialization
//!
//! Addresses needs to be serialized into a specific format before being sent to other peers.
//! This module is handling this particular construction.
//!

//---------------------------------------------------------------------------------------------------- Imports

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::Buf;
use thiserror::Error;

use cuprate_epee_encoding::{epee_object, EpeeObjectBuilder};
use cuprate_types::AddressType;

use crate::NetworkAddress;

use super::{I2pAddr, OnionAddr};

//---------------------------------------------------------------------------------------------------- Network address construction

#[derive(Default)]
/// A serialized network address being communicated to or from a peer.
pub struct TaggedNetworkAddress {
    /// Type of the network address (used later for conversion)
    ty: Option<AddressType>,
    /// All possible fields for a network address
    addr: Option<AllFieldsNetworkAddress>,
}

epee_object!(
    TaggedNetworkAddress,
    ty("type"): Option<AddressType>,
    addr: Option<AllFieldsNetworkAddress>,
);

impl EpeeObjectBuilder<NetworkAddress> for TaggedNetworkAddress {
    fn add_field<B: Buf>(&mut self, name: &str, b: &mut B) -> cuprate_epee_encoding::Result<bool> {
        match name {
            "type" => {
                if self
                    .ty
                    .replace(cuprate_epee_encoding::read_epee_value(
                        b,
                        Default::default(),
                    )?)
                    .is_some()
                {
                    return Err(cuprate_epee_encoding::Error::Format(
                        "Duplicate field in data.",
                    ));
                }
                Ok(true)
            }
            "addr" => {
                if std::mem::replace(
                    &mut self.addr,
                    cuprate_epee_encoding::read_epee_value(b, Default::default())?,
                )
                .is_some()
                {
                    return Err(cuprate_epee_encoding::Error::Format(
                        "Duplicate field in data.",
                    ));
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn finish(self) -> cuprate_epee_encoding::Result<NetworkAddress> {
        let ty = self.ty;
        let host = self.addr.as_ref().and_then(|a| a.host.clone());

        self.try_into().map_err(|_| {
            cuprate_epee_encoding::Error::Value(format!(
                "Invalid network address (type: {ty:?}, host: {host:?})"
            ))
        })
    }
}

#[derive(Error, Debug)]
#[error("Invalid network address")]
pub struct InvalidNetworkAddress;

impl TryFrom<TaggedNetworkAddress> for NetworkAddress {
    type Error = InvalidNetworkAddress;

    fn try_from(value: TaggedNetworkAddress) -> Result<Self, Self::Error> {
        value
            .addr
            .ok_or(InvalidNetworkAddress)?
            .try_into_network_address(value.ty.ok_or(InvalidNetworkAddress)?)
            .ok_or(InvalidNetworkAddress)
    }
}

impl From<NetworkAddress> for TaggedNetworkAddress {
    fn from(value: NetworkAddress) -> Self {
        match value {
            NetworkAddress::Clear(addr) => match addr {
                SocketAddr::V4(addr) => Self {
                    ty: Some(AddressType::Ipv4),
                    addr: Some(AllFieldsNetworkAddress {
                        m_ip: Some(u32::from_le_bytes(addr.ip().octets())),
                        m_port: Some(addr.port()),
                        addr: None,
                        host: None,
                        port: None,
                    }),
                },
                SocketAddr::V6(addr) => Self {
                    ty: Some(AddressType::Ipv6),
                    addr: Some(AllFieldsNetworkAddress {
                        addr: Some(addr.ip().octets()),
                        m_port: Some(addr.port()),
                        m_ip: None,
                        host: None,
                        port: None,
                    }),
                },
            },
            NetworkAddress::Tor(onion_addr) => Self {
                ty: Some(AddressType::Tor),
                addr: Some(AllFieldsNetworkAddress {
                    m_ip: None,
                    m_port: None,
                    addr: None,
                    host: Some(onion_addr.addr_string()),
                    port: Some(onion_addr.port()),
                }),
            },
            NetworkAddress::I2p(i2p_addr) => Self {
                ty: Some(AddressType::I2p),
                addr: Some(AllFieldsNetworkAddress {
                    m_ip: None,
                    m_port: None,
                    addr: None,
                    host: Some(i2p_addr.addr_string()),
                    port: Some(i2p_addr.port()),
                }),
            },
        }
    }
}

#[derive(Default)]
/// There are no ordering guarantees in epee format and as such all potential fields can be collected during deserialization.
/// The [`AllFieldsNetworkAddress`] is containing, as its name suggest, all optional field describing an address , as if it
/// could be of any type.
struct AllFieldsNetworkAddress {
    /// IPv4 address
    m_ip: Option<u32>,
    /// IP port field
    m_port: Option<u16>,

    /// IPv6 address
    addr: Option<[u8; 16]>,

    /// Alternative network domain name (<domain>.onion or <domain>.i2p)
    host: Option<String>,
    /// Alternative network virtual port
    port: Option<u16>,
}

epee_object!(
    AllFieldsNetworkAddress,
    m_ip: Option<u32>,
    m_port: Option<u16>,
    addr: Option<[u8; 16]>,
    host: Option<String>,
    port: Option<u16>,
);

impl AllFieldsNetworkAddress {
    fn try_into_network_address(self, ty: AddressType) -> Option<NetworkAddress> {
        Some(match ty {
            AddressType::Ipv4 => NetworkAddress::from(SocketAddrV4::new(
                Ipv4Addr::from(self.m_ip?.to_le_bytes()),
                self.m_port?,
            )),
            AddressType::Ipv6 => NetworkAddress::from(SocketAddrV6::new(
                Ipv6Addr::from(self.addr?),
                self.m_port?,
                0,
                0,
            )),
            AddressType::Tor => {
                NetworkAddress::from(OnionAddr::new(self.host?.as_str(), self.port?).ok()?)
            }
            AddressType::I2p => {
                NetworkAddress::from(I2pAddr::new(self.host?.as_str(), self.port?).ok()?)
            }
            AddressType::Invalid => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TaggedNetworkAddress;
    use crate::{I2pAddr, NetworkAddress};

    // Fixed portable-storage bytes, following monerod's i2p_address::store:
    // address type 3; host string; compatibility port encoded as uint16(1).
    // This is intentionally independent of TaggedNetworkAddress's serializer.
    const MONERO_I2P_ADDRESS: &[u8] = b"\x01\x11\x01\x01\x01\x01\x02\x01\x01\
        \x08\x04type\x08\x03\x04addr\x0c\x08\x04host\x0a\xf0\
        aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p\
        \x04port\x07\x01\x00";

    #[test]
    fn i2p_address_uses_monero_i2p_wire_variant() {
        let address: I2pAddr = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:18083"
            .parse()
            .unwrap();
        let encoded = TaggedNetworkAddress::from(NetworkAddress::from(address));
        let decoded = NetworkAddress::try_from(encoded).unwrap();

        assert_eq!(decoded, NetworkAddress::I2p(address));
    }

    #[test]
    fn i2p_address_matches_monero_portable_storage_fixture() {
        let mut bytes = MONERO_I2P_ADDRESS;
        let address: NetworkAddress = cuprate_epee_encoding::from_bytes(&mut bytes).unwrap();
        let NetworkAddress::I2p(address) = address else {
            panic!("expected the Monero I2P address type");
        };
        assert_eq!(address.port(), 1);
        assert_eq!(
            address.addr_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
        );
        let encoded = cuprate_epee_encoding::to_bytes(NetworkAddress::I2p(address)).unwrap();
        assert_eq!(encoded.as_ref(), MONERO_I2P_ADDRESS);
    }

    #[test]
    fn legacy_i2p_wire_ports_are_normalized_not_used_as_tcp_ports() {
        let mut legacy = MONERO_I2P_ADDRESS.to_vec();
        let length = legacy.len();
        legacy[length - 2..].copy_from_slice(&18084_u16.to_le_bytes());
        let decoded: NetworkAddress =
            cuprate_epee_encoding::from_bytes(&mut legacy.as_slice()).unwrap();
        assert_eq!(decoded.port(), 1);
        assert_eq!(
            cuprate_epee_encoding::to_bytes(decoded).unwrap().as_ref(),
            MONERO_I2P_ADDRESS
        );
    }
}
