package com.raftconfig.raft;

/** This node isn't the leader, so the command was NOT appended. Safe to retry on the leader. */
public class NotLeaderException extends RuntimeException {

    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("not leader (leader=" + leaderId + ")");
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}