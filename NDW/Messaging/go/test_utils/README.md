# test_utils — NDW Go Example

Utility example showing argument parsing, logging, and callback registration using `ndw`.

## Build

```sh
make -C NDW/Messaging/go test_utils
# or
cd NDW/Messaging/go/test_utils && go build -o test_utils
```

## Run

```sh
./test_utils -m 10 -b 512 -w 6 -t "DomainA^Conn1^TopicA"
```

Demonstrates:
- `ParseMessageArgs` and `SplitStringToTokens`
- `NDW_RegisterGoAsyncMsgCallback`
- `PrintCurrentTime` and logging helpers