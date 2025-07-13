
#!/bin/bash
set -xe  # -x for debug output, -e to exit on error

export LD_LIBRARY_PATH=/home/subrata/AND/code/C/Messaging/core/build:$LD_LIBRARY_PATH
export PATH=/usr/local/go/bin:$PATH

export CGO_CFLAGS="-I/home/subrata/AND/code/C/Messaging/core/include"
export CGO_LDFLAGS="-L/home/subrata/AND/code/C/Messaging/core/build -lcoremessaging"

#export NDW_VERBOSE="2"
export NDW_VERBOSE="3"

export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="./GO_Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=777
export NDW_CAPTURE_LATENCY=1

export NDW_QUEUE_SIZE=1000000

#export NUM_MSGS=24
#export MSG_BYTES=1024

#export NUM_MSGS=100000
#export MSG_BYTES=1024

export NUM_MSGS=1000000
export MSG_BYTES=1024

./ndw_app >see 2>&1

echo
grep "^<<<" see | wc -l

echo
grep "^>>>" see | wc -l

set +xe
echo
grep "TOTAL MESSAGES SENT:" see

echo
grep "TOTAL MESSAGES RECEIVED:" see
echo



