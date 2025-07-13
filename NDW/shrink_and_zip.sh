#!/bin/bash

set -e

# Must be run from inside /tmp/NDW/Messaging
SCRIPT_DIR="$(pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
ZIP_NAME="Messaging.zip"
ZIP_PATH="$PARENT_DIR/$ZIP_NAME"

echo "🚿 Cleaning up large/unnecessary files..."

# Remove Git history
rm -rf NATS/nats.c/.git

# Remove compiled binaries and build output
rm -rf NATS/nats.c/build

# Remove Doxygen HTML (and optionally config)
rm -rf NATS/nats.c/doc/html
rm -f  NATS/nats.c/doc/DoxyFile.NATS.Client*

# Remove large test file and .deb package
rm -f NATS/nats.c/test/test.c
rm -f ../NATS_CLI/nats-0.2.3-amd64.deb

# Remove test result logs
rm -f ../tests/BestResults_*

echo "Cleanup complete."

# Remove existing zip if present
[ -f "$ZIP_PATH" ] && rm -f "$ZIP_PATH"

echo "Creating zip archive at: $ZIP_PATH"
(cd "$PARENT_DIR" && zip -r "$ZIP_NAME" "$(basename "$SCRIPT_DIR")")

echo "Done! Created zip file: $ZIP_PATH"
du -h "$ZIP_PATH"

