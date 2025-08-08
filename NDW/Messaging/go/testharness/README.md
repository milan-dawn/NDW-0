# testharness — NDW Go Example

Comprehensive test harness wiring publish/subscribe, polling, and commit flows over `ndw`.

## Build

```sh
make -C NDW/Messaging/go testharness
# or
cd NDW/Messaging/go/testharness && go build -o testharness
```

## Run

The harness uses internal mappings of subscribe/poll/commit functions (e.g., `NATS_Subscribe_Async`, `NATS_Poll`, `NATS_Commit`) built on `ndw` APIs. Provide the usual flags for message count/size and topics if applicable to your flow. See source for advanced options.

## Notable bindings (wrappers over ndw)

- Subscribe: `NATS_Subscribe_Async`, `NATS_Subscribe_Sync`, `JS_subscribe_Async`, `JS_Subscribe_Sync`, `AsyncQ_Subscribe`
- Poll: `NATS_Poll`, `JS_Poll`, `AsyncQ_Poll`
- Commit: `NATS_Commit`, `JS_Commit`, `AsyncQ_Commit`

These are helpers inside the example; for reusable APIs prefer the `ndw` package documented in `go/ndw/README.md`.