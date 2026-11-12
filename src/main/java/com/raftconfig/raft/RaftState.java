package com.raftconfig.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;

/**
 * Core Raft state for one node (Figure 2 of the Raft paper).
 *
 * Plain Java, no Spring, so it's easy to unit test.
 * Every method is synchronized: one lock guards all state.
 * Simple and correct first; Raft bugs are almost always races.
 */
public class RaftState {

    private static final Logger log = LoggerFactory.getLogger(RaftState.class);

    private final String selfId;

    // Persistent state (we'll save these to disk in Phase 3)
    private long currentTerm = 0;
    private String votedFor = null;

    // Volatile state
    private Role role = Role.FOLLOWER;
    private String leaderId = null;

    public RaftState(String selfId) {
        this.selfId = selfId;
    }

    /**
     * Step down to follower. Called when we see a higher term,
     * or hear from a valid leader in our current term.
     */
    public synchronized void becomeFollower(long term, String newLeaderId) {
        if (term < currentTerm) {
            throw new IllegalStateException("Cannot go back in time: term " + term + " < " + currentTerm);
        }
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;   // new term, fresh vote
        }
        if (role != Role.FOLLOWER) {
            log.info("term={} {} -> FOLLOWER (leader={})", currentTerm, role, newLeaderId);
        }
        role = Role.FOLLOWER;
        leaderId = newLeaderId;
    }

    /**
     * Election timeout fired: start a new term and vote for ourselves.
     * Returns the new term so the caller can request votes for it.
     */
    public synchronized long becomeCandidate() {
        currentTerm++;
        votedFor = selfId;
        role = Role.CANDIDATE;
        leaderId = null;
        log.info("term={} -> CANDIDATE", currentTerm);
        return currentTerm;
    }

    /** Won a majority of votes in the given term. */
    public synchronized boolean becomeLeader(long electionTerm) {
        // Only promote if we're still a candidate in the term we ran in.
        // Votes can arrive late, after we've already moved on.
        if (role != Role.CANDIDATE || currentTerm != electionTerm) {
            return false;
        }
        role = Role.LEADER;
        leaderId = selfId;
        log.info("term={} -> LEADER", currentTerm);
        return true;
    }

    /**
     * Any RPC or response carrying a higher term means we're out of date.
     * Returns true if we stepped down.
     */
    public synchronized boolean stepDownIfNewerTerm(long term) {
        if (term > currentTerm) {
            becomeFollower(term, null);
            return true;
        }
        return false;
    }
    
    /** Handle an incoming RequestVote RPC from a candidate. */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        // Rule 1: reject candidates from an older term
        if (req.term() < currentTerm) {
            log.debug("term={} reject vote for {} (stale term {})", currentTerm, req.candidateId(), req.term());
            return new RequestVoteResponse(currentTerm, false);
        }

        // Rule 2: newer term -> adopt it and step down (this also clears votedFor)
        if (req.term() > currentTerm) {
            becomeFollower(req.term(), null);
        }

        // Rule 3: one vote per term, and only for an up-to-date log
        boolean canVote = votedFor == null || votedFor.equals(req.candidateId());
        boolean logOk = isCandidateLogUpToDate(req.lastLogIndex(), req.lastLogTerm());

        if (canVote && logOk) {
            votedFor = req.candidateId();
            log.info("term={} voted for {}", currentTerm, req.candidateId());
            return new RequestVoteResponse(currentTerm, true);
        }

        log.info("term={} reject vote for {} (votedFor={}, logOk={})",
                currentTerm, req.candidateId(), votedFor, logOk);
        return new RequestVoteResponse(currentTerm, false);
    }

    /**
     * Raft's "at least as up-to-date" check (section 5.4.1):
     * higher last term wins; if equal terms, longer log wins.
     */
    private boolean isCandidateLogUpToDate(long candLastIndex, long candLastTerm) {
        long myLastTerm = lastLogTerm();
        long myLastIndex = lastLogIndex();
        if (candLastTerm != myLastTerm) {
            return candLastTerm > myLastTerm;
        }
        return candLastIndex >= myLastIndex;
    }

    // No log yet, so our log is empty. Phase 2 replaces these with the real log.
    public synchronized long lastLogIndex() { return 0; }
    public synchronized long lastLogTerm()  { return 0; }

    public synchronized Snapshot snapshot() {
        return new Snapshot(selfId, currentTerm, role, votedFor, leaderId);
    }

    public String selfId() {
        return selfId;
    }

    /** Immutable copy of the state, safe to read outside the lock. */
    public record Snapshot(String nodeId, long term, Role role, String votedFor, String leaderId) {}
}