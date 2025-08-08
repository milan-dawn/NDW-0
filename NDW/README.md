# NDW — Repository Overview

This repository contains components and examples for the NDW messaging system.

## Structure

- `Messaging/`
  - `go/` — Go workspace with the `ndw` package and example apps. See `Messaging/go/README.md` and `Messaging/go/ndw/README.md` for API docs and examples.
  - `RunNATS/` — Scripts and configs to run a local NATS server for testing.
  - `NATS_CLI/` — Shell scripts to manage NATS JetStream streams and consumers (create/list/send/pop examples).
  - `NATSTesting/` — C-based tests and utilities for NATS/JetStream.
  - `core/` — Core C headers, build artifacts, and includes used by cgo.

For Go APIs, start with `Messaging/go/ndw/README.md`.