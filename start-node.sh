#!/usr/bin/env bash
# Starts one node by number (1-5), e.g.: bash start-node.sh 3
#   Kubernetes:     nothing to do, the StatefulSet recreates deleted pods by itself.
#   Docker compose: `docker start nodeN`.
#   Local:          launch the jar on port 808N (appends to logs/nodeN.log).
n=$1
if [ -z "$n" ]; then echo "usage: bash start-node.sh <1-5>"; exit 1; fi

if command -v kubectl > /dev/null 2>&1 && kubectl get statefulset raft --request-timeout=10s > /dev/null 2>&1; then
  echo "raft-$((n - 1)) is recreated automatically by the StatefulSet"
  exit 0
fi

if command -v docker > /dev/null 2>&1 && docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "node$n"; then
  docker start "node$n" > /dev/null && echo "started container node$n"
  exit 0
fi

JAR=target/raft-config-service-0.1.0-SNAPSHOT.jar
mkdir -p logs
RAFT_NODE_ID=node$n SERVER_PORT=808$n java -jar "$JAR" >> logs/node$n.log 2>&1 &
echo "started node$n on port 808$n"