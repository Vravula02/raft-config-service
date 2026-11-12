#!/usr/bin/env bash
# Starts 5 nodes in the background. Logs go to logs/nodeN.log
JAR=target/raft-config-service-0.1.0-SNAPSHOT.jar

if [ ! -f "$JAR" ]; then
  echo "Jar not found. Run: mvn clean package -DskipTests"
  exit 1
fi

mkdir -p logs
for i in 1 2 3 4 5; do
  RAFT_NODE_ID=node$i SERVER_PORT=808$i java -jar "$JAR" > logs/node$i.log 2>&1 &
  echo $! > logs/node$i.pid
  echo "started node$i on port 808$i (pid $!)"
done