#!/usr/bin/env bash
# Prints the /status of every node
for i in 1 2 3 4 5; do
  echo -n "node$i: "
  curl -s --max-time 1 localhost:808$i/status || echo -n "DOWN"
  echo
done