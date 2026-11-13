#!/usr/bin/env bash
# Prints the current leader's number (e.g. 3). Exits 1 if there's no leader.
for i in 1 2 3 4 5; do
  if curl -s --max-time 2 localhost:808$i/status | grep -q '"role":"LEADER"'; then
    echo $i; exit 0
  fi
done
exit 1
