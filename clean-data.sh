#!/usr/bin/env bash
# Deletes all persisted Raft state for a fresh cluster. Stop the cluster first.
rm -rf data
echo "data/ removed"