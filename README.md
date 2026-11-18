<div align="center">
	<img src="misc/logo/wordmark/CuprateWordmark.svg" width="50%"/>

An alternative Monero node implementation.

_(work-in-progress)_

[![Matrix](https://img.shields.io/badge/Matrix-Cuprate-white?logo=matrix&labelColor=grey&logoColor=white)](https://matrix.to/#/#cuprate:monero.social) [![CI](https://github.com/Cuprate/cuprate/actions/workflows/ci.yml/badge.svg)](https://github.com/Cuprate/cuprate/actions/workflows/ci.yml)

</div>

## Contents

- [About](#about)
- [Changes in this fork](#changes-in-this-fork)
- [Books](#books)
- [Build](#build)
- [Crates](#crates)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

## About

Cuprate is an effort to create an alternative [Monero](https://getmonero.org) node implementation
in [Rust](https://rust-lang.org).

It is able to independently validate Monero consensus rules, providing a layer of security and redundancy for the
Monero network.

See <https://user.cuprate.org> for more details.

## Changes in this fork

The experimental changes described below are maintained on the
[`codex/upstream-i2p-update` branch](https://github.com/cacaosteve/cuprate/tree/codex/upstream-i2p-update).
The fork's default `main` branch does not contain them. This is an independent
fork of Cuprate; the features are not claims about upstream Cuprate.

- **I2P transaction relay:** An experimental I2P P2P zone uses an external
  `i2pd` SOCKS proxy for outbound peers and can accept inbound peers through a
  separately configured I2P tunnel. Block synchronization still uses the
  configured non-I2P network. I2P P2P is separate from an I2P wallet-RPC tunnel.
- **Local transaction routing:** `local_tx_relay_mode` defaults to `clearnet`;
  `i2p_then_clearnet` tries I2P first and permits fallback, while `i2p_only`
  retries over I2P without clearnet fallback. The selected policy is persisted
  for queued local transactions. It does not change the policy of transactions
  received from other peers.
- **Transaction-pool safeguards:** Admission accounts for private transactions
  in the pool size limit and reports success only after admission. Requests with
  `do_not_relay=true` are rejected because that option is not supported.
- **Operator diagnostics and fixes:** Peer details and per-network transaction
  counters are available through administrative RPC, with private details kept
  off the restricted public RPC. The fork also contains peer-shutdown fixes and
  a scoped Monero arithmetic dependency patch; see the [vendor notes](https://github.com/cacaosteve/cuprate/blob/codex/upstream-i2p-update/vendor/README.md).

I2P support is experimental. A successful peer write or handshake does not
prove that a transaction propagated or provide an anonymity guarantee. The
[fork's I2P guide](https://github.com/cacaosteve/cuprate/blob/codex/upstream-i2p-update/books/user/src/networking/i2p.md)
explains configuration, persistence, fallback behavior, and test limitations.

## Books

_Cuprate is currently a work-in-progress; documentation will be changing/unfinished._

Cuprate maintains various documentation books:

| Book                                                            | Description                                                |
|-----------------------------------------------------------------|------------------------------------------------------------|
| [Monero's protocol book](https://monero-book.cuprate.org)       | Documents the Monero protocol                              |
| [Cuprate's user book](https://user.cuprate.org)                 | Practical user-guide for using `cuprated`                  |

## Build

To build Cuprate from source code, see <https://user.cuprate.org/getting-started/source.html>.

## Crates
For a detailed list of all crates, see: <https://architecture.cuprate.org/appendix/crates.html>.

For crate (library) documentation, see: <https://doc.cuprate.org>. This site holds documentation for Cuprate's crates and all dependencies. All Cuprate crates start with `cuprate_`, for example: [`cuprate_database`](https://doc.cuprate.org/cuprate_database).

## Contributing

See [`CONTRIBUTING.md`](/CONTRIBUTING.md).

## Security

Cuprate has a responsible vulnerability disclosure policy, see [`SECURITY.md`](/SECURITY.md).

## License

The `binaries/` directory is licensed under AGPL-3.0, everything else is licensed under MIT.

See [`LICENSE`](/LICENSE) for more details.
