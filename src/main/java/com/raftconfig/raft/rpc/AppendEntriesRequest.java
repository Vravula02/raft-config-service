package com.raftconfig.raft.rpc;

import com.raftconfig.raft.LogEntry;

import java.util.List;

/**
 * Sent by the leader. With no entries it's a heartbeat.
 * prevLogIndex/prevLogTerm/leaderCommit are used in Phase 2.
 */
public record AppendEntriesRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) {
    public AppendEntriesRequest {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }
}