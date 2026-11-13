#!/usr/bin/env bash
# Prints the Raft log of every node
for i in 1 2 3 4 5; do
  echo -n "node$i: "
  curl -s --max-time 1 localhost:808$i/admin/log || echo -n "DOWN"
  echo
done