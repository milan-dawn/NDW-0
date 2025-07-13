
set -x

source ./env.sh

if [ -z "$1" ]; then
  max_msgs=6
else
  max_msgs=$1
fi

if [ -z "$2" ]; then
  message_bytes_size=0
else
  message_bytes_size=$2
fi

export NDW_APP_TOPIC_FILE="./APP_PubSub.JSON"
if [ ! -f $NDW_APP_TOPIC_FILE ]; then
    echo "NDW_APP_TOPIC_FIL= file: " $NDW_APP_TOPIC_FILE " does not exists!"
    exit 3
fi

EXEC_FILE="./PubSub.out"
OUT_FILE="vsee_pubsub.txt"

valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes $EXEC_FILE $* >$OUT_FILE 2>&1

vi + $OUT_FILE

echo
set -x
grep "<<<" $OUT_FILE | wc -l
grep ">>>" $OUT_FILE | wc -l
set +x
grep "msg_published_count" $OUT_FILE
grep "HH:MM:SS" $OUT_FILE
grep "TOTALS: Published" $OUT_FILE
echo
grep -i "error" $OUT_FILE
grep -i "warning" $OUT_FILE
echo

