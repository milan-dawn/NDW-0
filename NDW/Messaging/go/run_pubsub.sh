
#!/bin/bash
#set -xe  # -x for debug output, -e to exit on error

source ./env.sh

#export NDW_VERBOSE="2"
export NDW_VERBOSE="3"

export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/GO_NATS_Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=777
export NDW_CAPTURE_LATENCY=1

export NDW_QUEUE_SIZE=1000000
export NUM_MSGS=12
export MSG_BYTES=1024

Topic1="DomainA^NATSConn1^ACME.Orders"
Topic2="DomainA^NATSConn1^ACME.Invoices"
Topic3="DomainA^NATSConn2^ACME.Shipments"
ALLTOPICS="$Topic1|$Topic2|$Topic3"

#./your_go_program -m 1000 -b 512 -t "$ALLTOPICS"

EXEC_NAME=./pubsub/pubsub

$EXEC_NAME "$@" -t "$ALLTOPICS"
#gdb $EXEC_NAME "$@" -t "$ALLTOPICS"
#gdb $EXEC_NAME

