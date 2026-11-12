package com.raftconfig.raft;

/** One entry in the replicated log. Used for real in Phase 2. */
public record LogEntry(long index, long term, String command) {}