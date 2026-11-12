#!/usr/bin/env bash
# Stops all 5 nodes
for ((i=1; i<=5; i++)); do
  bash kill-node.sh "$i"
done