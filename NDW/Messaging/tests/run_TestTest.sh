
#set -x

source ./env.sh

if [ -z "$NDW_APP_CONFIG_FILE" ]; then
    echo "NDW_APP_CONFIG_FILE not set"
    exit 1
fi

if [ ! -f $NDW_APP_CONFIG_FILE ]; then
    echo "NDW_APP_CONFIG_FILE file: " $NDW_APP_CONFIG_FILE " does not exists!"
    exit 2
fi

export NDW_APP_TOPIC_FILE="./APP_PubSub.JSON"
if [ ! -f $NDW_APP_TOPIC_FILE ]; then
    echo "NDW_APP_TOPIC_FIL= file: " $NDW_APP_TOPIC_FILE " does not exists!"
    exit 3
fi


#gdb ./TestTest.out $*
#valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes ./TestTest.out $* > see_testtest.txt 2>&1
./TestTest.out $* >see_testtest.txt 2>&1

vi + see_testtest.txt

