# RunNATS — Local NATS Server

Scripts and configs to start a local `nats-server` for development and testing.

## Prerequisites

- `nats-server` installed (e.g., via package manager or binaries from the NATS project)

## Start

```sh
cd NDW/Messaging/RunNATS
./run_nats.sh
```

This uses `nats-server.conf` in the same directory. Adjust as needed.

## Files

- `run_nats.sh` — starts `nats-server` with the provided config
- `nats-server.conf` — example configuration
- `clean.conf` — helper for clean runs (if needed)