
set -x

source ./asyncq_env.sh

PROGRAM="./AsyncQ_NATS.out"
OUTFILE="see_ASYNCQ_NATS.txt"

echo "asyncq_env.sh: LD_LIBRARY_PATH = $LD_LIBRARY_PATH"

gdb $PROGRAM $*
#$PROGRAM $*
#$PROGRAM $* >$OUTFILE 2>&1

#vi + $OUTFILE

#echo
#grep "HH:" $OUTFILE
#grep "TOTAL" $OUTFILE
#grep -i "error" $OUTFILE
#grep -i "warning" $OUTFILE

