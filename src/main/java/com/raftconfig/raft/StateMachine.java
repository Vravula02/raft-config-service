package com.raftconfig.raft;

/** Whatever committed log entries get applied to. Must be deterministic. */
public interface StateMachine {
    void apply(LogEntry entry);
}