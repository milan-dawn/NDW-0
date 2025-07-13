#!/bin/bash
set -e

#!/usr/bin/env bash

# 1) Create the stream
echo "Creating Stream..."
nats stream add --config NEWS.JSON

# 2) Push‑based consumers:

nats consumer add NEWS TechConsumer \
  --filter News.Tech \
  --deliver all \
  --target JS_DLV.Tech \
  --ack explicit \
  --max-deliver 100 \
  --flow-control \
  --heartbeat 30s \
  --defaults

nats consumer add NEWS SportsConsumer \
  --filter 'News.Sports.>' \
  --deliver all \
  --target JS_DLV.Sports \
  --ack explicit \
  --max-deliver 100 \
  --flow-control \
  --heartbeat 30s \
  --defaults

nats consumer add NEWS FootballConsumer \
  --filter News.Sports.Football \
  --deliver all \
  --target JS_DLV.Football \
  --ack explicit \
  --max-deliver 100 \
  --flow-control \
  --heartbeat 30s \
  --defaults

nats consumer add NEWS CollegeFootballConsumer \
  --filter News.Sports.Football.College \
  --deliver all \
  --target JS_DLV.College \
  --ack explicit \
  --max-deliver 100 \
  --flow-control \
  --heartbeat 30s \
  --defaults

nats consumer add NEWS HeadlinesConsumer \
  --filter News.Headlines \
  --deliver all \
  --target JS_DLV.Headlines \
  --ack explicit \
  --max-deliver 100 \
  --flow-control \
  --heartbeat 30s \
  --defaults

# 3) Pull‑based consumers:

echo "Creating PULL MoviesArchiveConsumer..."
nats consumer add NEWS MoviesArchiveConsumer \
  --filter 'News.Archive.Movies.>' \
  --pull \
  --ack explicit \
  --max-deliver 100 \
  --max-pending 1000 \
  --defaults

echo "Creating PULL RomanceMoviesArchiveConsumer..."
nats consumer add NEWS RomanceMoviesArchiveConsumer \
  --filter News.Archive.Movies.Romance \
  --pull \
  --ack explicit \
  --max-deliver 100 \
  --max-pending 1000 \
  --defaults

echo "Creating PULL WesternsMoviesArchiveConsumer..."
nats consumer add NEWS WesternsMoviesArchiveConsumer \
  --filter News.Archive.Movies.Westerns \
  --pull \
  --ack explicit \
  --max-deliver 100 \
  --max-pending 1000 \
  --defaults


