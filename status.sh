#!/usr/bin/env bash
# Prints the /status of every node
for ((i=1; i<=5; i++)); do
  echo -n "node$i: "
  curl -s --max-time 1 localhost:808$i/status || echo -n "DOWN"
  echo
done