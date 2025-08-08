# pubsub — NDW Go Example

Minimal end-to-end publish/subscribe example using the `ndw` package.

## Build

Prereqs: NDW headers and libs must be available (see `go/README.md`).

```sh
make -C NDW/Messaging/go pubsub
# or
cd NDW/Messaging/go/pubsub && go build -o pubsub
```

## Run

Flags parsed via `ndw.ParseMessageArgs`:
- `-m` message count
- `-b` message size (bytes)
- `-w` wait time (seconds)
- `-t` topics (`Domain^Connection^Topic`), pipe-separated for multiple

Example:

```sh
./pubsub -m 100 -b 1024 -w 6 -t "DomainA^NatsConn1^TopicA|DomainA^NatsConn1^TopicB"
```

The app registers an async handler, subscribes to all topics, publishes messages, and waits for receives.