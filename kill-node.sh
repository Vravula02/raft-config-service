#!/usr/bin/env bash
# Kills one node by number, e.g.: bash kill-node.sh 3
#   Docker mode: if a running container named nodeN exists, `docker kill` it (SIGKILL = crash).
#   Local mode:  force-kill the java.exe listening on port 808N (never anything else).
n=$1
if [ -z "$n" ]; then echo "usage: bash kill-node.sh <1-5>"; exit 1; fi

if command -v docker > /dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "node$n"; then
  docker kill "node$n" > /dev/null && echo "killed container node$n"
  exit 0
fi

pid=$(netstat -ano | grep ":808$n " | grep LISTENING | awk '{print $5}' | head -1)
if [ -n "$pid" ] && tasklist //FI "PID eq $pid" | grep -qi "java.exe"; then
  taskkill //F //PID "$pid" > /dev/null && echo "killed node$n (pid $pid)"
else
  echo "node$n not running"
fi