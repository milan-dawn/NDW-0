
CHECK_FILE="see_js_poll.txt"
echo "======================== CHECKING: $CHECK_FILE ============================"
echo "PUBLISHED ==>"
grep ">>>" $CHECK_FILE
echo "--------------------------------------------------------------"
echo
echo "CONSUMED ==>"
echo "--------------------------------------------------------------"
grep "<<<" $CHECK_FILE | grep NEWS_Movies_Westerns
echo "--------------------------------------------------------------"
grep "<<<" $CHECK_FILE | grep NEWS_Movies_Romance
echo "--------------------------------------------------------------"
grep "<<<" $CHECK_FILE | grep NEWS_MOVIES_ALL
echo "--------------------------------------------------------------"

