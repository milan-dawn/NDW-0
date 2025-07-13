#!/bin/bash

# Exit on any error
set -e

# File to store list of binaries to delete
BIN_LIST="to_delete.txt"

echo "Scanning for compiled ELF binaries..."

# Find all files, inspect type using 'file', and filter ELF binaries only
find . -type f -exec file {} + | grep 'ELF' | cut -d: -f1 > "$BIN_LIST"

NUM_FILES=$(wc -l < "$BIN_LIST")

if [ "$NUM_FILES" -eq 0 ]; then
    echo "No ELF binaries found."
    exit 0
fi

echo
echo "Found $NUM_FILES ELF binaries to delete:"
echo "----------------------------------------"
cat "$BIN_LIST"
echo "----------------------------------------"
echo

# Ask for confirmation before deletion
read -p "Delete all listed binaries? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 1
fi

# Delete the binaries
xargs -a "$BIN_LIST" rm -f

echo
echo "Deleted $NUM_FILES ELF binaries."

