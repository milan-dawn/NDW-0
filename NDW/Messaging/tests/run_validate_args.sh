
. ./env.sh

#set -x

./validate_args.out $*

output=$(./validate_args.out "$@" | tail -n 1)

echo "Output = ($output)"

args=($output)

if [ "${args[0]} ${args[1]}" = "--v Y" ]; then
  echo "First argument starts with '--v Y'"
else
  echo "First argument does not start with '--v Y'"
fi

