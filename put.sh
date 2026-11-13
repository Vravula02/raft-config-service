#!/usr/bin/env bash
# Writes a config value through the API, e.g.: bash put.sh color blue [node number]
n=${3:-$(bash find-leader.sh)}
curl -s -X PUT localhost:808$n/config/$1 -H "Content-Type: text/plain" -d "$2"; echo