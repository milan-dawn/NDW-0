# NDW Messaging — Overview

Messaging-related components for NDW.

## Subdirectories

- `go/` — Go workspace with `ndw` package and example apps (pubsub, test_utils, testharness). See `go/README.md`.
- `RunNATS/` — Scripts to start a local `nats-server` using provided configs. See `RunNATS/run_nats.sh`.
- `NATS_CLI/` — Convenience shell scripts to create streams/consumers and push/pull messages (JetStream).
- `NATSTesting/` — C tests for NATS/JetStream with Makefiles.
- `core/` — C headers and libraries required by cgo when building the Go examples.

If you are using Go, begin with `go/ndw/README.md`.