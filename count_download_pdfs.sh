#!/usr/bin/env sh
set -eu

# Count PDF files under the download directory (case-insensitive).
find "download" -type f -iname "*.pdf" | wc -l
