#!/usr/bin/env bash
# Appends a command on the current leader, e.g.: bash append.sh "SET color blue"
L=$(bash find-leader.sh) || { echo "no leader found"; exit 1; }
curl -s -X POST localhost:808$L/debug/append -H "Content-Type: text/plain" -d "$1"; echo
