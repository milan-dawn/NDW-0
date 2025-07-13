#!/bin/bash

CURRENT_DIR="$(pwd)"
PARENT_DIR="$(dirname "$CURRENT_DIR")"
FOLDER_NAME="$(basename "$CURRENT_DIR")"
ZIP_PATH="$PARENT_DIR/$FOLDER_NAME.zip"

# Clean old zip
[ -f "$ZIP_PATH" ] && rm -f "$ZIP_PATH"

echo "Zipping folder '$FOLDER_NAME' to '$ZIP_PATH'..."

(cd "$PARENT_DIR" && zip -r "$ZIP_PATH" "$FOLDER_NAME")

echo "Done: Created $ZIP_PATH"


