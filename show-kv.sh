#!/usr/bin/env bash
# Prints each node's own copy of the config (local reads, no forwarding)
for i in 1 2 3 4 5; do
  echo -n "node$i: "
  curl -s --max-time 1 "localhost:808$i/config?consistency=local" || echo -n "DOWN"
  echo
done