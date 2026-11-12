#!/usr/bin/env bash
# Starts one node, e.g.: bash start-node.sh 3   (appends to its existing log)
JAR=target/raft-config-service-0.1.0-SNAPSHOT.jar
n=$1
if [ -z "$n" ]; then echo "usage: bash start-node.sh <1-5>"; exit 1; fi
mkdir -p logs
RAFT_NODE_ID=node$n SERVER_PORT=808$n java -jar "$JAR" >> logs/node$n.log 2>&1 &
echo "started node$n on port 808$n"