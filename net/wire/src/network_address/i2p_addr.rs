//! I2P base32 destination addresses.

use std::{fmt::Display, str::FromStr};

use borsh::{io, BorshDeserialize, BorshSerialize};
use thiserror::Error;

use super::{NetworkAddress, NetworkAddressIncorrectZone};

/// A Monero I2P destination. Unlike Tor addresses, its identity has no port.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct I2pAddr {
    /// The 52-character base32 destination, without the `.b32.i2p` suffix.
    domain: [u8; 52],
}

#[derive(Debug, Error)]
pub enum I2pAddrParsingError {
    #[error("I2P base32 address must be 60 characters, got {0}")]
    InvalidLength(usize),
    #[error("Address is not an I2P base32 destination")]
    InvalidTld,
    #[error("I2P base32 address contains a non-base32 character")]
    NonBase32Char,
    #[error("Invalid I2P destination port")]
    InvalidPort,
}

impl I2pAddr {
    /// Monero uses port 1 for SOCKS and wire compatibility, independently of the
    /// local TCP port to which an i2pd server tunnel forwards connections.
    pub const COMPATIBILITY_PORT: u16 = 1;

    /// Parse a complete `<52-character-base32>.b32.i2p` destination.
    /// The legacy wire port is ignored, matching monerod's I2P address handling.
    pub fn new(address: &str, _port: u16) -> Result<Self, I2pAddrParsingError> {
        Self::check_addr(address).map(|domain| Self { domain })
    }

    /// Validate and normalize an I2P base32 destination.
    pub fn check_addr(address: &str) -> Result<[u8; 52], I2pAddrParsingError> {
        if address.len() != 60 {
            return Err(I2pAddrParsingError::InvalidLength(address.len()));
        }

        let (domain, tld) = address
            .split_at_checked(52)
            .ok_or(I2pAddrParsingError::InvalidLength(address.len()))?;
        if tld != ".b32.i2p" {
            return Err(I2pAddrParsingError::InvalidTld);
        }

        if !domain
            .as_bytes()
            .iter()
            .copied()
            .all(|c| c.is_ascii_alphabetic() || (b'2'..=b'7').contains(&c))
        {
            return Err(I2pAddrParsingError::NonBase32Char);
        }

        let mut domain: [u8; 52] = domain
            .as_bytes()
            .try_into()
            .expect("the address length was checked above");
        domain.make_ascii_lowercase();
        Ok(domain)
    }

    /// Return the complete base32 destination without a port.
    pub fn addr_string(&self) -> String {
        let mut address = String::from_utf8(self.domain.to_vec())
            .expect("validated I2P base32 destinations are ASCII");
        address.push_str(".b32.i2p");
        address
    }

    #[inline]
    pub const fn port(&self) -> u16 {
        Self::COMPATIBILITY_PORT
    }

    #[inline]
    pub const fn domain(&self) -> [u8; 52] {
        self.domain
    }
}

impl Display for I2pAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.addr_string())
    }
}

impl FromStr for I2pAddr {
    type Err = I2pAddrParsingError;

    fn from_str(address: &str) -> Result<Self, Self::Err> {
        let (host, port) = match address.split_once(':') {
            Some((host, port)) => (
                host,
                port.parse::<u16>()
                    .map_err(|_| I2pAddrParsingError::InvalidPort)?,
            ),
            None => (address, Self::COMPATIBILITY_PORT),
        };
        Self::new(host, port)
    }
}

// Retain the prototype's 54-byte address-book representation, but normalize its
// legacy port on read/write so restored peers cannot acquire duplicate identities.
impl BorshSerialize for I2pAddr {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        BorshSerialize::serialize(&self.domain, writer)?;
        BorshSerialize::serialize(&Self::COMPATIBILITY_PORT, writer)
    }
}

impl BorshDeserialize for I2pAddr {
    fn deserialize_reader<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let domain = <[u8; 52]>::deserialize_reader(reader)?;
        let port = u16::deserialize_reader(reader)?;
        let domain = std::str::from_utf8(&domain)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        Self::new(&format!("{domain}.b32.i2p"), port)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
    }
}

impl TryFrom<NetworkAddress> for I2pAddr {
    type Error = NetworkAddressIncorrectZone;

    fn try_from(value: NetworkAddress) -> Result<Self, Self::Error> {
        match value {
            NetworkAddress::I2p(addr) => Ok(addr),
            NetworkAddress::Clear(_) | NetworkAddress::Tor(_) => Err(NetworkAddressIncorrectZone),
        }
    }
}

impl From<I2pAddr> for NetworkAddress {
    fn from(value: I2pAddr) -> Self {
        Self::I2p(value)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for I2pAddr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for I2pAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let address = <String as serde::Deserialize>::deserialize(deserializer)?;
        address.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::I2pAddr;

    const I2P_HOST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p";

    #[test]
    fn parses_and_normalizes_address() {
        let address: I2pAddr = format!("{I2P_HOST}:18083").parse().unwrap();
        assert_eq!(address.addr_string(), I2P_HOST);
        assert_eq!(address.port(), 1);
        assert_eq!(address.to_string(), I2P_HOST);
        assert_eq!(address, I2P_HOST.parse().unwrap());
        assert_eq!(address, format!("{I2P_HOST}:1").parse().unwrap());
        assert_eq!(address, format!("{I2P_HOST}:0").parse().unwrap());
        assert_eq!(
            address,
            I2P_HOST
                .to_uppercase()
                .replace(".B32.I2P", ".b32.i2p")
                .parse()
                .unwrap()
        );
    }

    #[test]
    fn legacy_ports_do_not_create_distinct_peers_or_persist_again() {
        let mut legacy = vec![b'a'; 52];
        legacy.extend_from_slice(&18084_u16.to_le_bytes());
        let address: I2pAddr = borsh::from_slice(&legacy).unwrap();
        let canonical: I2pAddr = I2P_HOST.parse().unwrap();
        assert_eq!(address, canonical);
        assert_eq!(
            std::collections::HashSet::from([address, canonical]).len(),
            1
        );
        let encoded = borsh::to_vec(&address).unwrap();
        assert_eq!(encoded.len(), 54, "keep the address-book layout compatible");
        assert_eq!(&encoded[52..], &1_u16.to_le_bytes());
        legacy[0] = b'!';
        assert!(borsh::from_slice::<I2pAddr>(&legacy).is_err());
    }

    #[test]
    fn rejects_non_base32_addresses() {
        assert!(I2pAddr::new("not-an-i2p-address", 18083).is_err());
        assert!(I2pAddr::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.onion",
            18083
        )
        .is_err());
        assert!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:nope"
                .parse::<I2pAddr>()
                .is_err()
        );
    }
}
