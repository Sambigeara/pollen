#!/bin/sh
{
  find cmd pkg -type f | while IFS= read -r file; do
    echo '```'
    cat "$file"
    echo '```'
    echo
  done
} | pbcopy
