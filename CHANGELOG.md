# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- CI/CD pipeline with GitHub Actions (test, clippy, fmt, miri, benchmark build)
- Full-path inproc throughput benchmark using ZContext/ZSocket API
- Full-path inproc latency benchmark using ZContext/ZSocket API

### Fixed
- README performance comparison now accurately reflects measurement scope (full socket path vs raw ypipe)
- inproc_thr and inproc_lat examples updated to measure complete PUSH→PULL and REQ→REP paths

### Changed
- Updated test count in README to current state (209 passed, 205 ignored)

## [0.1.0] - 2026-06-12

### Added
- Initial Rust workspace: zmq-core, zmq-transport, zmq-runtime, zmq-context, zmq-ffi
- Sans-I/O ZMTP protocol core with zero I/O dependencies
- 19 socket types (PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB, STREAM, SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, DGRAM, PEER, CHANNEL)
- 10 lock-free data structures (ypipe, yqueue, ypipe_conflate, fair_queue, load_balancer, distribution, trie, radix_tree, array, dbuffer)
- ZMTP codec (greeting, framing, command, encoder, decoder)
- NULL and PLAIN security mechanisms
- CURVE security mechanism (feature-gated)
- inproc transport layer
- 209 passing tests (119 unit + 90 integration)
- C++ baseline performance data collection
- radix_tree performance at 185% of C++ baseline

### Security
- unsafe code ratio: ~0.8% (only in lock-free data structures)
- All unsafe blocks documented with `// SAFETY:` comments

[Unreleased]: https://github.com/user/libzmq_rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/user/libzmq_rust/releases/tag/v0.1.0
