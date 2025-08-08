## NDW Go Messaging API

### Overview
NDW provides a Go API for high-throughput pub/sub messaging over NATS (with examples for JetStream). The `ndw` package wraps a native messaging core via cgo and exposes a simple, thread-safe interface for initializing the runtime, connecting to configured topics, publishing messages, and receiving them asynchronously or synchronously.

- **Module**: `ndw`
- **Primary package**: `ndw/ndw`
- **Go version**: 1.18+

### Quickstart

```go
package main

import (
  "fmt"
  "os"
  ndw "ndw/ndw"
)

func onMessage(topic *ndw.NDW_TopicData, msg *ndw.NDW_ReceivedMsg) {
  fmt.Printf("[%s] %s -> %d bytes\n", msg.Timestamp, topic.TopicName, msg.MsgSize)
  ndw.NDW_CommitLastMsg(topic)
}

func appMain() int {
  // Parse flags: -m <count> -b <bytes> -w <waitSec> -t <Domain^Connection^Topic|...>
  msgCount, msgSize, waitSec, topics, err := ndw.ParseMessageArgs(os.Args[1:])
  if err != nil { return 1 }

  _ = msgCount
  _ = msgSize
  _ = topics

  ndw.NDW_RegisterGoAsyncMsgCallback(onMessage)
  if ndw.NDW_Initialize() != 0 { return 1 }
  if ndw.NDW_ConnectAll() != 0 { return 1 }
  if ndw.NDW_SubscribeAsyncAll() != 0 { return 1 }

  // Publish one sample per topic
  for i := range ndw.TopicList {
    topic := &ndw.TopicList[i]
    jsonStr, _ := ndw.CreateTestJSONMessageOfSize(topic, "hello", 1, 256)
    ctx := ndw.CreateOutMsgCxt(topic, []byte(jsonStr), ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
    if ctx == nil { return 1 }
    if ndw.NDW_PublishMsg(topic) != 0 { return 1 }
  }

  ndw.NDW_SleepForMessages(2, waitSec)
  return 0
}

func main() { os.Exit(ndw.RunNDWApp(appMain)) }
```

Run example:

```bash
go run ./NDW/Messaging/go/pubsub # or your own main using the package
```

### Topic configuration
- Provide topics via `-t` flag as a pipe-separated list.
- Each topic path must be `Domain^Connection^TopicName`.

Example:

```bash
go run . -t "DomainA^NATSConn1^ACME.Orders|DomainA^NATSConn1^ACME.Invoices|DomainA^NATSConn2^ACME.Shipments" -m 10 -b 512 -w 6
```

The registry of available domains/connections/topics is defined in `NDW/Messaging/go/GO_NATS_Registry.JSON`.

### Publishing
- Build a payload (JSON, string, binary). Convenience helper: `CreateTestJSONMessageOfSize(topic, base, seq, targetBytes)`.
- Create an output context with `CreateOutMsgCxt` (header + encoding).
- Call `NDW_PublishMsg(topic)`.

Minimal loop:

```go
for i := 0; i < N; i++ {
  seq := ndw.NDW_IncrementTopicSequenceNumber(topic)
  jsonStr, _ := ndw.CreateTestJSONMessageOfSize(topic, "msg", seq, 1024)
  _ = ndw.CreateOutMsgCxt(topic, []byte(jsonStr), ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
  if ndw.NDW_PublishMsg(topic) != 0 { /* handle error */ }
}
```

### Subscribing
- Async: register a callback, subscribe, and commit acks in the handler.

```go
ndw.NDW_RegisterGoAsyncMsgCallback(func(t *ndw.NDW_TopicData, m *ndw.NDW_ReceivedMsg){
  // process
  ndw.NDW_CommitLastMsg(t)
})
_ = ndw.NDW_SubscribeAsyncAll()
```

- Sync: subscribe synchronously and poll.

```go
_ = ndw.NDW_SubscribeSynchronous(topic)
var dropped int64
for {
  got := ndw.NDW_SynchronousPollForMsg(topic, 1_000 /*us*/, &dropped)
  if got == 1 {
    msg := ndw.NDW_GetLastReceivedMsg(topic)
    // process msg
    ndw.NDW_CommitLastMsg(topic)
  }
}
```

A non-blocking async-queue poll is also available: `NDW_PollAsyncQeueue(topic, timeoutUs)`.

### Message model
`NDW_ReceivedMsg` contains:
- **TopicID**: int32
- **TopicName**: string
- **Sequence**: int64
- **MsgBody**: []byte
- **MsgSize**: int32
- **Header**: []byte (raw header)
- **HeaderSize**: int32
- **HeaderID**: uint8
- **HeaderSizeVal**: uint8
- **VendorID**: uint8
- **Timestamp**: string

Use `NDW_PrintReceivedMsg(msg)` for formatted output.

### Common workflow
1. `NDW_RegisterGoAsyncMsgCallback(cb)` (for async)
2. `NDW_Initialize()`
3. Populate topics via `ParseMessageArgs` or manually set `TopicList` and call `PopulateTopicData()` indirectly via `NDW_Initialize()`.
4. `NDW_ConnectAll()` or `NDW_Connect(domain, connection)`
5. `NDW_SubscribeAsyncAll()` or `NDW_SubscribeSynchronous(topic)`
6. Publish with `CreateOutMsgCxt` + `NDW_PublishMsg`
7. Acknowledge with `NDW_CommitLastMsg`
8. Sleep/wait with `NDW_SleepForMessages(unitSec, maxWaitSec)`
9. Shutdown is handled by `RunNDWApp` deferring `NDW_Exit()`

### API reference (selected)
- Initialization and lifecycle
  - `RunNDWApp(app func() int) int`: runs your logic on a locked OS thread and defers cleanup.
  - `NDW_Initialize() int32`: init native runtime and map configured topics.
  - `NDW_Exit()`: shuts down and prints totals (invoked by `RunNDWApp`).
  - `NDW_RegisterGoAsyncMsgCallback(cb)`
- Connections
  - `NDW_Connect(domain, connection) int32`
  - `NDW_ConnectAll() int32`
  - `NDW_IsConnected(domain, connection) bool`
  - `NDW_IsClosed(domain, connection) bool`
  - `NDW_IsDraining(domain, connection) bool`
- Subscriptions
  - `NDW_SubscribeAsync(topic *NDW_TopicData) int32`
  - `NDW_SubscribeAsyncAll() int32`
  - `NDW_SubscribeSynchronous(topic *NDW_TopicData) int32`
  - `NDW_Unsubscribe(cTopic *C.ndw_Topic_T) int32`
  - `NDW_UnsubscribeAll() int32`
  - `NDW_SynchronousPollForMsg(topic, timeoutUs, *dropped) int32`
  - `NDW_PollAsyncQeueue(topic, timeoutUs) int32`
  - `NDW_CommitLastMsg(topic) int32`
- Publishing
  - `CreateOutMsgCxt(topic, payload, headerID, encodingFormat) *C.ndw_OutMsgCxt_T`
  - `NDW_PublishMsg(topic) int32`
- Topic utilities
  - `CreateTopicList(n int)` / `InitializeTopicData(i, domain, connection, topic)`
  - `NDW_SetTopicSequenceNumber` / `NDW_GetTopicSequenceNumber` / `NDW_IncrementTopicSequenceNumber`
  - `NDW_GetPubKey` / `NDW_GetSubKey`
- Message helpers
  - `NDW_GetLastReceivedMsg(topic)`
  - `NDW_GetMsgCommitAckCounters(topic, *commit, *ack, *failedAck)`
  - `NDW_PrintReceivedMsg(msg)`
- Parsing, logging, timing
  - `ParseMessageArgs(args) (count, size, waitSec int, topics []NDW_TopicData, err error)`
  - `NDW_LOG`, `NDW_LOGX`, `NDW_LOGERR`
  - `NDW_StartTimeDuration`, `NDW_EndTimeDuration`, `NDW_PrintTimeDuration`

### Registries and examples
- NATS registry and topics: `NDW/Messaging/go/GO_NATS_Registry.JSON`
- App topic sets: `NDW/Messaging/go/APP_pubsub.JSON`, `APP_syncsub.JSON`, `APP_JS_PUSH.JSON`, `APP_JS_POLL.JSON`

JetStream examples (NEWS.*) are present but disabled in the registry by default; enable the corresponding connection/topics to use them.

### Notes
- This package uses cgo and requires the native NDW library/headers available at build time (`ndw_types.h`, etc.).
- All network connectivity details (URLs, vendor options) are defined in the registry JSON.