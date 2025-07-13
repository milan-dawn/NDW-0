set -x

source ./env.sh
#valgrind --leak-check=full --show-reachable=yes --track-origins=yes ./JSPullTest

valgrind -s --leak-check=full --show-reachable=yes --track-origins=yes ./JSPullTest > vsee_jspulltest.txt 2>&1

vi + vsee_jspulltest.txt

