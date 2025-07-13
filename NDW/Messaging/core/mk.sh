
DEBUG="DEBUG=1"

#set -x
if [ -n "$1" ]; then
    case $1 in
        0) DEBUG="DEBUG=0" ;;
        1) DEBUG="DEBUG=1" ;;
        *) echo "Error: Invalid value for DEBUG. It should be 0 or 1."
            exit
    esac
fi

echo "Parameter: $DEBUG"

make -f Makefile clean
make -f Makefile $DEBUG >see 2>&1
vi + see

ls -ltr build/*.so
echo
echo

