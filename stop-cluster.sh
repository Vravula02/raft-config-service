#!/usr/bin/env bash
# Stops all 5 nodes
for i in 1 2 3 4 5; do
  bash kill-node.sh $i
done