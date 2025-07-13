
set -x
JSON_FILE="Registry.JSON"

if [ $# -ge 1 ]; then
    JSON_FILE=$1
fi

jq . $JSON_FILE

echo
echo "JSON_FILE = $JSON_FILE"
echo

