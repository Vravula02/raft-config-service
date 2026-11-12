#!/usr/bin/env bash
# Kills one node by number, e.g.: bash kill-node.sh 3
# Finds the Windows PID listening on port 808N and force-kills it.
n=$1
if [ -z "$n" ]; then echo "usage: bash kill-node.sh <1-5>"; exit 1; fi
pid=$(netstat -ano | grep ":808$n " | grep LISTENING | awk '{print $5}' | head -1)
if [ -n "$pid" ]; then
  taskkill //F //PID "$pid" > /dev/null && echo "killed node$n (pid $pid)"
else
  echo "node$n not running"
fi