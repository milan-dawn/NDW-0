
#set -x

if [ $# -lt 1 ]; then
    echo "ERROR: Specific JSON file as an argument to this script!"
    exit 1
fi

jq . $*

