#!/usr/bin/env bash
# Compact view: role, log length, commit and apply progress per node
for i in 1 2 3 4 5; do
  s=$(curl -s --max-time 1 localhost:808$i/status)
  if [ -z "$s" ]; then echo "node$i: DOWN"; continue; fi
  echo "node$i: $(echo "$s" | grep -o '"role":"[A-Z]*"\|"term":[0-9]*\|"lastLogIndex":[0-9]*\|"commitIndex":[0-9]*\|"lastApplied":[0-9]*' | tr '\n' ' ')"
done
