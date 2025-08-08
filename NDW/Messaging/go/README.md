# NDW Messaging — Go Workspace

This directory contains Go components and examples for the NDW messaging system.

## Packages and Apps

- `ndw/` — Core Go package exposing the NDW public APIs. See API docs at `ndw/README.md`.
- `pubsub/` — Minimal example app showing end-to-end publish/subscribe using the `ndw` package.
- `test_utils/` — Small utility app demonstrating parsing, logging, and callback registration.
- `testharness/` — Comprehensive test harness wiring multiple functions and counters using `ndw` APIs.
- `working_nats_pubsub/` and `WORKING_nats_pubsub.go` — Additional working examples exploring lower-level cgo flow.

## Build

The Makefile builds all apps and expects NDW headers and libraries installed.

Environment variables (example):

- `NDW_MSG_ROOT=/path/to/NDW/Messaging` (so headers at `$NDW_MSG_ROOT/core/include` and library at `$NDW_MSG_ROOT/core/build`)

Build all:

```sh
make -C NDW/Messaging/go all
```

Build one app:

```sh
make -C NDW/Messaging/go pubsub
```

## Run Examples

- `pubsub`:
  - Arguments parsed via `ParseMessageArgs` flags: `-m`, `-b`, `-w`, `-t`.
  - Example:

```sh
NDW_MSG_ROOT=/path/to/NDW/Messaging \
NDW_QUEUE_SIZE=1000000 \
NUM_MSGS=100 \
MSG_BYTES=1024 \
./pubsub/pubsub -m 100 -b 1024 -w 6 -t "DomainA^NatsConn1^TopicA|DomainA^NatsConn1^TopicB"
```

- `test_utils`:

```sh
./test_utils/test_utils -m 10 -b 512 -w 6 -t "DomainA^Conn1^TopicA"
```

Refer to `ndw/README.md` for detailed API usage and more examples.