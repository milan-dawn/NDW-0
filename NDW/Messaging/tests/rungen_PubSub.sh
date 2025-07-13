
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


gdb ./PubSub.out $*
#./PubSub.out $* >see_pubsub.txt 2>&1

output=$(./validate_args.out "$@" | tail -n 1)

#echo "Output = ($output)"

#first_word=${output%% *}
#if [ "valgrind" = "$first_word" ]
#then
#    echo "Valgrind"
#    program_commands=${output#* }
#else
#    first_word=${output%% *}
#    program_commands=${output}
#fi

echo "Program Command = ($program_commands)"


#vi + see_pubsub.txt

#echo
#grep "main()" see_pubsub.txt
#grep "HH:" see_pubsub.txt
#echo "Total HandleAsyncMessage:"
#grep HandleAsyncMessage see_pubsub.txt | wc -l
#echo
#grep -i "error" see_pubsub.txt

