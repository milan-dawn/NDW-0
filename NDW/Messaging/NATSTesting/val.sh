set -x

export LD_LIBRARY_PATH=../core:$LD_LIBRARY_PATH

#gdb ./NATSTest.out
#./NATSTest.out

valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes ./NATSTest.out > vsee.txt 2>&1

