#!/bin/bash
set -e

#nats sub JS_DLV.College --ack
#nats sub JS_DLV.Football --ack
#nats sub  JS_DLV.Sports --ack
# cli_list_push_consumer_info.sh


declare -A consumers=(
  [CollegeFootballConsumer]=JS_DLV.College
  [FootballConsumer]=JS_DLV.Football
  [SportsConsumer]=JS_DLV.Sports
)

for cname in "${!consumers[@]}"; do
  dsubject=${consumers[$cname]}

  echo
  echo "🔍 Checking $cname..."

  unprocessed=$(nats consumer info NEWS "$cname" --json | jq '.num_pending')

  if [[ "$unprocessed" -gt 0 ]]; then
    echo "📨 $unprocessed pending messages for $cname → Popping now..."
    nats sub "$dsubject" --ack --count="$unprocessed"
  else
    echo "✅ No pending messages for $cname."
  fi
done

echo
echo "✅ All consumers processed."


