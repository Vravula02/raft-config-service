#!/usr/bin/env bash
# Kills one node by number (1-5), e.g.: bash kill-node.sh 3
#   Kubernetes:     delete pod raft-(N-1); the StatefulSet recreates it with the same disk.
#   Docker compose: `docker kill nodeN` (SIGKILL = crash).
#   Local:          force-kill the java.exe listening on port 808N (never anything else).
n=$1
if [ -z "$n" ]; then echo "usage: bash kill-node.sh <1-5>"; exit 1; fi
pod="raft-$((n - 1))"

if command -v kubectl > /dev/null 2>&1 && kubectl get pod "$pod" --request-timeout=10s > /dev/null 2>&1; then
  # No --force: the StatefulSet must not start a replacement before the old pod is really gone
  kubectl delete pod "$pod" --wait=false > /dev/null && echo "deleted pod $pod (StatefulSet will recreate it)"
  exit 0
fi

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