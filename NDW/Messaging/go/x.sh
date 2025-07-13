#!/bin/bash
set -xe

source ./env.sh

EXEC_NAME=./nats_PubSub

go build -gcflags=all="-N -l" -o $EXEC_NAME

