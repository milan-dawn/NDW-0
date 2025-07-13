
#set -x

source ./env.sh

export NDW_APP_CONFIG_FILE="./JetStream.JSON"

if [ -z "$NDW_APP_CONFIG_FILE" ]; then
    echo "NDW_APP_CONFIG_FILE not set"
    exit 1
fi

if [ ! -f $NDW_APP_CONFIG_FILE ]; then
    echo "NDW_APP_CONFIG_FILE file: " $NDW_APP_CONFIG_FILE " does not exists!"
    exit 2
fi

export NDW_APP_TOPIC_FILE="./APP_JS_PUSH.JSON"
if [ ! -f $NDW_APP_TOPIC_FILE ]; then
    echo "NDW_APP_TOPIC_FILE= file: " $NDW_APP_TOPIC_FILE " does not exists!"
    exit 3
fi

EXEC_FILE="./JSPush.out"
OUT_FILE=vsee_js_push.txt


valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes $EXEC_FILE $* >$OUT_FILE 2>&1


if true; then
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
fi

