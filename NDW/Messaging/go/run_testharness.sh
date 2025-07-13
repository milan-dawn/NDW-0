
#!/bin/bash
#set -xe  # -x for debug output, -e to exit on error

source ./env.sh

#export NDW_VERBOSE="2"
export NDW_VERBOSE="3"

export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/GO_NATS_Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=778
export NDW_CAPTURE_LATENCY=1

export NDW_QUEUE_SIZE=1000000
export NUM_MSGS=12
export MSG_BYTES=1024

set +x

if [ $# -eq 0 ]; then
    echo "Usage: run_testharness [pubsub | syncsub | jspush | jspoll] -m number_of_messages -b bytes_size -w wait_time_in_seconds"
    exit 1
fi

test_type=$(echo "$1" | tr '[:upper:]' '[:lower:]')
case $test_type in
    pubsub)
        export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/GO_NATS_Registry.JSON"
        export NDW_APP_TOPIC_FILE="APP_pubsub.JSON"
        ;;
    syncsub)
        export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/GO_NATS_Registry.JSON"
        export NDW_APP_TOPIC_FILE="APP_syncsub.JSON"
        ;;
    jspush)
        export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/JetStream.JSON"
        export NDW_APP_TOPIC_FILE="APP_JS_PUSH.JSON"
        ;;
    jspoll)
        export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/JetStream.JSON"
        export NDW_APP_TOPIC_FILE="APP_JS_POLL.JSON"
        ;;
    *)
        echo "Unknown Test Harness Type: <$test_type>"
        exit 2
        ;;
esac

#export NDW_APP_TOPIC_FILE="APP_"$test_type".JSON"

echo
echo "Test Harness Type = <" $test_type "> ==> NDW_APP_TOPIC_FILE = <"$NDW_APP_TOPIC_FILE">"

if [ ! -f $APP_TOPIC_FILE ]; then
    echo "NDW_APP_TOPIC_FILE = <"$NDW_APP_TOPIC_FILE"> does NOT exists!"
    exit 3
fi

EXEC_NAME=./testharness/testharness

$EXEC_NAME "$@"
#gdb $EXEC_NAME "$@"
#gdb $EXEC_NAME

