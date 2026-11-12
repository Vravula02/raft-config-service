package com.raftconfig.raft.rpc;

public record RequestVoteResponse(
        long term,          // responder's current term, so a stale candidate can step down
        boolean voteGranted
) {}