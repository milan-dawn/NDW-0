#!/bin/bash

#set -x

source ./env.sh

echo "======"
echo "CGO_CFLAGS= $CGO_CFLAGS"
echo "CGO_LDFLAGS= $CGO_LDFLAGS"
echo "======"

make clean
make

