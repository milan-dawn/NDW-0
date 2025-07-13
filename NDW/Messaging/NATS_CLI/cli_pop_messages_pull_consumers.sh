#!/bin/bash
set -e

# Pop atmost 10 messages.
# 1. RomanceMoviesArchiveConsumer (exact subject)
#nats consumer next NEWS RomanceMoviesArchiveConsumer --count 10 --ack
# 2. WesternsMoviesArchiveConsumer (exact subject)
#nats consumer next NEWS WesternsMoviesArchiveConsumer --count 10 --ack
# 3. MoviesArchiveConsumer (wildcard subject)
#nats consumer next NEWS MoviesArchiveConsumer --count 10 --ack
# ./cli_list_pull_consumer_info.sh

#!/bin/bash

# List of PULL-based consumers
declare -A consumers=(
  ["MoviesArchiveConsumer"]="NEWS"
  ["RomanceMoviesArchiveConsumer"]="NEWS"
  ["WesternsMoviesArchiveConsumer"]="NEWS"
)

echo
echo "🚀 Popping messages from PULL-based JetStream consumers..."
echo

for consumer in "${!consumers[@]}"; do
  stream="${consumers[$consumer]}"

  echo "🔍 Checking $consumer..."
  pending=$(nats consumer info "$stream" "$consumer" --json | jq '.num_pending')

  if [[ "$pending" -gt 0 ]]; then
    echo "📨 $pending pending messages for $consumer → Popping now..."
    nats consumer next "$stream" "$consumer" --count "$pending" --ack
  else
    echo "✅ No pending messages for $consumer."
  fi

  echo
done

echo "✅ All PULL consumers processed."

./cli_list_pull_consumer_info.sh

