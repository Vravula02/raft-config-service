package com.raftconfig.raft;

/**
 * We appended the command, then lost leadership before it committed.
 * It may still commit under the new leader: the outcome is unknown.
 */
public class LeadershipLostException extends RuntimeException {

    private final String leaderId;

    public LeadershipLostException(String leaderId) {
        super("lost leadership before commit (new leader=" + leaderId + ")");
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}