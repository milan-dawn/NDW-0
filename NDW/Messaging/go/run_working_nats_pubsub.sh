
#!/bin/bash
set -xe  # -x for debug output, -e to exit on error

source ./env.sh

#export NDW_VERBOSE="2"
export NDW_VERBOSE="3"

export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="./GO_NATS_Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=777
export NDW_CAPTURE_LATENCY=1

export NDW_QUEUE_SIZE=1000000
export NUM_MSGS=12
export MSG_BYTES=1024

EXEC_NAME=./working_nats_pubsub

$EXEC_NAME

