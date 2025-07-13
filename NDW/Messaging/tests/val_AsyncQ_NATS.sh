
set -x

source ./asyncq_env.sh

PROGRAM="./AsyncQ_NATS.out"
OUTFILE="vsee_AsyncQ_NATS.txt"

valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes $PROGRAM $* >$OUTFILE 2>&1

vi + $OUTFILE

echo
grep "HH:" $OUTFILE
grep "TOTAL" $OUTFILE
echo
grep -i "error" $OUTFILE
grep -i "warning" $OUTFILE


