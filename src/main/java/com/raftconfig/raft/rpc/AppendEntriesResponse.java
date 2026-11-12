package com.raftconfig.raft.rpc;

public record AppendEntriesResponse(
        long term,       // follower's term, so a stale leader can step down
        boolean success
) {}
