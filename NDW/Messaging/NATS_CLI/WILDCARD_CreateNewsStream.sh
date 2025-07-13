#!/bin/bash
set -e

# Create the stream with multiple subjects
nats stream add --config NEWS.JSON

# push-based consumer
nats consumer add NEWS TechConsumer \
  --filter=News.Tech \
  --target=News.Tech.Consumer \
  --deliver=all \
  --ack=explicit \
  --max-deliver=100 \
  --flow-control \
  --heartbeat=30s \
  --defaults

# push-based consumer
nats consumer add NEWS SportsConsumer \
  --filter='News.Sports.>' \
  --target=News.Sports.Consumer \
  --deliver=all \
  --ack=explicit \
  --max-deliver=100 \
  --flow-control \
  --heartbeat=30s \
  --defaults

# push-based consumer
nats consumer add NEWS FootballConsumer \
  --filter=News.Sports.Football \
  --target=News.Football.Consumer \
  --deliver=all \
  --ack=explicit \
  --max-deliver=100 \
  --flow-control \
  --heartbeat=30s \
  --defaults

# TESTING: push-based testing for publication at the root level, which is in doubt per ChatGPT.
nats consumer add NEWS TestWildcardConsumer \
  --filter="News.Sports.>" \
  --target=News.Sports.Test \
  --deliver=all \
  --ack=explicit \
  --defaults


# push-based consumer
nats consumer add NEWS HeadlinesConsumer \
  --filter=News.Headlines \
  --target=News.Headlines.Consumer \
  --deliver=all \
  --ack=explicit \
  --max-deliver=100 \
  --flow-control \
  --heartbeat=30s \
  --defaults

# pull-based consumer (no --target, no --deliver)
nats consumer add NEWS MoviesArchiveConsumer \
  --filter='News.Archive.>' \
  --pull \
  --ack=explicit \
  --max-deliver=100 \
  --defaults

# pull-based consumer (no --target, no --deliver)
nats consumer add NEWS RomanceMoviesArchiveConsumer \
  --filter=News.Archive.Movies.Romance \
  --pull \
  --ack=explicit \
  --max-deliver=100 \
  --defaults

nats stream info NEWS
nats consumer list NEWS
nats stream info NEWS -j
nats consumer ls NEWS

