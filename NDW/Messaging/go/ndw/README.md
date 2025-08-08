# NDW Go Package — Public API Documentation

This package (`ndw`) provides Go bindings and utilities for the NDW messaging system via cgo. It exposes a high-level API to initialize the runtime, connect to domains/connections, subscribe (async and sync), publish messages, and retrieve metrics.

Note: Building and running requires the NDW C headers and libraries available to cgo at build and runtime.

## Quickstart

```go
package main

import (
    "os"
    "ndw/ndw"
)

func main() {
    os.Exit(ndw.RunNDWApp(func() int {
        // Parse arguments and populate TopicList
        msgCount, msgSize, waitSec, topics, err := ndw.ParseMessageArgs(os.Args[1:])
        if err != nil { return 1 }
        _ = msgCount; _ = msgSize; _ = waitSec; _ = topics

        // Register async callback before Initialize
        ndw.NDW_RegisterGoAsyncMsgCallback(func(t *ndw.NDW_TopicData, m *ndw.NDW_ReceivedMsg) {
            ndw.NDW_PrintReceivedMsg(m)
            ndw.NDW_CommitLastMsg(t)
        })

        if ndw.NDW_Initialize() != 0 { return 1 }
        if ndw.NDW_ConnectAll() != 0 { return 1 }
        if ndw.NDW_SubscribeAsyncAll() != 0 { return 1 }

        // Example publish
        for i := range ndw.TopicList {
            topic := &ndw.TopicList[i]
            payload := []byte(`{"hello":"world"}`)
            _ = ndw.CreateOutMsgCxt(topic, payload, ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
            ndw.NDW_PublishMsg(topic)
        }

        ndw.NDW_SleepForMessages(2, 6)
        return 0
    }))
}
```

---

## Constants

- `NDW_ENCODING_FORMAT_NONE`
- `NDW_ENCODING_FORMAT_STRING`
- `NDW_ENCODING_FORMAT_JSON`
- `NDW_ENCODING_FORMAT_BINARY`
- `NDW_ENCODING_FORMAT_XML`
- `NDW_MAX_ENCODING_FORMAT`
- `NDW_DEFAULT_MSG_HEADER`

## Global State

- `TopicList []NDW_TopicData`: Global topic configuration and runtime state. Many convenience APIs iterate this list.
- Atomic counters:
  - `IncrementTotalMsgsPublished() int64`, `GetTotalMsgsPublished() int64`
  - `IncrementTotalMsgsReceived() int64`, `GetTotalMsgsReceived() int64`

## Types

- `type NDW_TopicData struct`
  - `DomainName, ConnectionName, TopicName string`
  - `ToString string` — debug string populated during initialization
  - `TotalMsgsPublished, TotalMsgsReceived int64`
  - `DomainPtr *C.ndw_Domain_T`, `ConnectionPtr *C.ndw_Connection_T`, `TopicPtr *C.ndw_Topic_T` (set by initialization)

- `type NDW_ReceivedMsg struct`
  - `TopicPtr *C.ndw_Topic_T`
  - `TopicID int32`, `TopicName string`
  - `Sequence int64`
  - `MsgBody []byte`, `MsgSize int32`
  - `Header []byte`, `HeaderSize int32`
  - `HeaderID, HeaderSizeVal, VendorID uint8`
  - `Timestamp string`

- `type AppNDWAsyncMsgCallbackType func(topicData *NDW_TopicData, msg *NDW_ReceivedMsg)`

- `type NDW_TimeDuration struct` — for measuring durations
  - `NDW_StartTimeDuration() *NDW_TimeDuration`
  - `NDW_EndTimeDuration(td *NDW_TimeDuration)`
  - `NDW_PrintTimeDuration(td *NDW_TimeDuration)`

## Application Lifecycle

- `RunNDWApp(appLogic func() int) int`
  - Runs `appLogic` on a locked OS thread, sets up signal handling, and ensures `NDW_Exit()` is invoked.

- `NDW_Initialize() int32`
  - Initializes the NDW runtime and populates `TopicList` pointers. Must be called after setting up `TopicList` (e.g., via `ParseMessageArgs`) and before connect/subscribe.

- `NDW_Exit()`
  - Shuts down the NDW runtime and prints aggregate counters. Automatically invoked by `RunNDWApp` defer.

- `NDW_ThreadInit() int32`, `NDW_ThreadExit() int32`
  - Optional per-thread lifecycle if you manage threads yourself.

### Example entrypoint with lifecycle

```go
func main() {
    os.Exit(ndw.RunNDWApp(func() int {
        // set TopicList, register callback
        if ndw.NDW_Initialize() != 0 { return 1 }
        defer ndw.NDW_Exit()
        // ...
        return 0
    }))
}
```

## Connections

- `NDW_IsConnected(domain, connection string) bool`
- `NDW_IsClosed(domain, connection string) bool`
- `NDW_IsDraining(domain, connection string) bool`
- `NDW_Connect(domain, connection string) int32`
- `NDW_ConnectAll() int32` — connects all `TopicList` entries, skipping already-connected pairs

## Subscriptions

- `NDW_RegisterGoAsyncMsgCallback(cb AppNDWAsyncMsgCallbackType)` — must be set before `NDW_Initialize()` for async receive
- `NDW_SubscribeAsync(topic *NDW_TopicData) int32`
- `NDW_SubscribeAsyncAll() int32`
- `NDW_SubscribeSynchronous(topic *NDW_TopicData) int32`
- `NDW_Unsubscribe(topic *C.ndw_Topic_T) int32`
- `NDW_UnsubscribeAll() int32`

### Async subscription example

```go
ndw.NDW_RegisterGoAsyncMsgCallback(func(t *ndw.NDW_TopicData, m *ndw.NDW_ReceivedMsg) {
    ndw.NDW_PrintReceivedMsg(m)
    ndw.NDW_CommitLastMsg(t)
})
ndw.NDW_Initialize()
ndw.NDW_ConnectAll()
ndw.NDW_SubscribeAsyncAll()
```

### Sync subscription (poll) example

```go
// Subscribe synchronously to one topic
ndw.NDW_SubscribeSynchronous(topic)
for {
    var dropped int64
    rc := ndw.NDW_SynchronousPollForMsg(topic, 500_000 /*us*/, &dropped)
    if rc == 1 {
        msg := ndw.NDW_GetLastReceivedMsg(topic)
        ndw.NDW_PrintReceivedMsg(msg)
        ndw.NDW_CommitLastMsg(topic)
    }
}
```

- `NDW_PollAsyncQeueue(topic *NDW_TopicData, timeoutUs int64) int32` — poll the async queue explicitly
- `NDW_SynchronousPollForMsg(topic *NDW_TopicData, timeoutUs int64, dropped *int64) int32`
- `NDW_CommitLastMsg(topic *NDW_TopicData) int32`
- `NDW_GetLastReceivedMsg(topic *NDW_TopicData) *NDW_ReceivedMsg`
- `NDW_GetMsgCommitAckCounters(topic *NDW_TopicData, commit, ack, failedAck *int64)`

## Publishing

- `CreateOutMsgCxt(topic *NDW_TopicData, payload []byte, msgHeaderID int, encodingFormat int) *C.ndw_OutMsgCxt_T`
  - Prepares the outbound message in the underlying runtime. Must be called before `NDW_PublishMsg`.
- `NDW_PublishMsg(topic *NDW_TopicData) int32` — publishes the last prepared message; updates counters

### Publish example

```go
payload := []byte(`{"k":"v"}`)
ctx := ndw.CreateOutMsgCxt(topic, payload, ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
if ctx == nil { /* handle error */ }
if ndw.NDW_PublishMsg(topic) != 0 { /* handle error */ }
```

### Message helpers

- `NDW_BuildReceivedMsg(topicPtr unsafe.Pointer) *NDW_ReceivedMsg` — constructs a Go struct from the underlying C buffers
- `NDW_PrintReceivedMsg(msg *NDW_ReceivedMsg)` — pretty prints a received message
- `NDW_GetTopicDebugString(topic *NDW_TopicData) string`

## Topic utilities

- `CreateTopicList(num int)`
- `GetTopicData(index int) *NDW_TopicData`
- `InitializeTopicData(index int, domain, connection, topic string)`
- `PopulateTopicData() int` — resolves pointers for all entries in `TopicList`; called by `NDW_Initialize`

### Sequence numbers and keys

- `NDW_SetTopicSequenceNumber(topic *NDW_TopicData, seq int64)`
- `NDW_GetTopicSequenceNumber(topic *NDW_TopicData) int64`
- `NDW_IncrementTopicSequenceNumber(topic *NDW_TopicData) int64`
- `NDW_GetPubKey(topic *NDW_TopicData) string`
- `NDW_GetSubKey(topic *NDW_TopicData) string`

## Logging and time utilities

- Logging:
  - `NDW_LOG(format string, args ...any)`
  - `NDW_LOGX(format string, args ...any)` — includes file:line and function
  - `NDW_LOGERR(format string, args ...any)` — stderr with file:line and function
- Time helpers:
  - `PrintCurrentTime()`
  - `GetCurrentTimeString() string`
  - See `NDW_TimeDuration` above

## CLI and parsing utilities

- `SplitStringToTokens(input, sep string) ([]string, int)`
- `ParseMessageArgs(args []string) (count int, size int, waitSec int, topics []NDW_TopicData, err error)`
  - `-m` message count, `-b` message size, `-w` wait seconds, `-t` topics pipe-separated list of `Domain^Connection^Topic`

### Example: Parse topics

```go
count, size, wait, topics, err := ndw.ParseMessageArgs([]string{
    "-m", "10", "-b", "1024", "-w", "6",
    "-t", "DomainA^Conn1^TopicA|DomainA^Conn1^TopicB",
})
if err != nil { /* handle */ }
_ = count; _ = size; _ = wait; _ = topics
```

## End-to-end async example (publish + receive)

```go
ndw.NDW_RegisterGoAsyncMsgCallback(func(t *ndw.NDW_TopicData, m *ndw.NDW_ReceivedMsg) {
    ndw.NDW_PrintReceivedMsg(m)
    ndw.NDW_CommitLastMsg(t)
})

// Prepare topics
ndw.CreateTopicList(1)
ndw.InitializeTopicData(0, "DomainA", "NatsConn1", "TopicA")
if ndw.NDW_Initialize() != 0 { /* handle */ }
if ndw.NDW_ConnectAll() != 0 { /* handle */ }
if ndw.NDW_SubscribeAsyncAll() != 0 { /* handle */ }

payload := []byte(`{"msg":"hello"}`)
_ = ndw.CreateOutMsgCxt(&ndw.TopicList[0], payload, ndw.NDW_DEFAULT_MSG_HEADER, ndw.NDW_ENCODING_FORMAT_JSON)
ndw.NDW_PublishMsg(&ndw.TopicList[0])
ndw.NDW_SleepForMessages(2, 6)
```

## Notes

- Many functions will return errors if `TopicPtr` is nil; ensure `NDW_Initialize` (or `PopulateTopicData`) has resolved pointers.
- For async subscriptions, register the callback before `NDW_Initialize()` so the C-side handler is wired correctly.
- Building requires NDW C headers and shared libraries accessible to cgo.

## Additional utilities

- `CreateTestJSONMessageOfSize(topic *NDW_TopicData, message string, sequenceNumber int64, targetSize int) (string, int)`
- `PrettyPrintJSON(prefix, jsonStr, suffix string)`
- `NDW_SleepForMessages(unitSleepSec int, maxExpirySeconds int)`
- `IsNilOrEmpty(s *string) bool`