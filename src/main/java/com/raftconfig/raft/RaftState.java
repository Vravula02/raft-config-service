package com.raftconfig.raft;

import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Core Raft state for one node (Figure 2 of the Raft paper).
 *
 * Plain Java, no Spring, so it's easy to unit test.
 * Every method is synchronized: one lock guards all state, including the log.
 */
public class RaftState {

    private static final Logger log = LoggerFactory.getLogger(RaftState.class);

    private final String selfId;

    // Persistent state (saved to disk in Phase 3)
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog raftLog = new RaftLog();

    // Volatile state
    private Role role = Role.FOLLOWER;
    private String leaderId = null;

    public RaftState(String selfId) {
        this.selfId = selfId;
    }

    // ---------- Role transitions ----------

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
        } else if (newLeaderId != null && !newLeaderId.equals(leaderId)) {
            log.info("term={} following leader {}", currentTerm, newLeaderId);
        }
        role = Role.FOLLOWER;
        leaderId = newLeaderId;
    }

    public synchronized long becomeCandidate() {
        currentTerm++;
        votedFor = selfId;
        role = Role.CANDIDATE;
        leaderId = null;
        log.info("term={} -> CANDIDATE", currentTerm);
        return currentTerm;
    }

    public synchronized boolean becomeLeader(long electionTerm) {
        if (role != Role.CANDIDATE || currentTerm != electionTerm) {
            return false;
        }
        role = Role.LEADER;
        leaderId = selfId;
        log.info("term={} -> LEADER", currentTerm);
        return true;
    }

    public synchronized boolean stepDownIfNewerTerm(long term) {
        if (term > currentTerm) {
            becomeFollower(term, null);
            return true;
        }
        return false;
    }

    // ---------- RequestVote ----------

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term() < currentTerm) {
            log.debug("term={} reject vote for {} (stale term {})", currentTerm, req.candidateId(), req.term());
            return new RequestVoteResponse(currentTerm, false);
        }
        if (req.term() > currentTerm) {
            becomeFollower(req.term(), null);
        }

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
     * "At least as up-to-date" (section 5.4.1): higher last term wins;
     * equal terms -> longer log wins. Now uses the real log.
     */
    private boolean isCandidateLogUpToDate(long candLastIndex, long candLastTerm) {
        long myLastTerm = raftLog.lastTerm();
        long myLastIndex = raftLog.lastIndex();
        if (candLastTerm != myLastTerm) {
            return candLastTerm > myLastTerm;
        }
        return candLastIndex >= myLastIndex;
    }

    // ---------- AppendEntries ----------

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term() < currentTerm) {
            log.debug("term={} reject AppendEntries from stale leader {} (term {})",
                    currentTerm, req.leaderId(), req.term());
            return new AppendEntriesResponse(currentTerm, false);
        }
        becomeFollower(req.term(), req.leaderId());

        // Step 2: log consistency check + appending entries goes here
        return new AppendEntriesResponse(currentTerm, true);
    }

    // ---------- Log ----------

    /**
     * Leader only: append a new command to our own log in the current term.
     * Returns null if we're not the leader.
     */
    public synchronized LogEntry appendAsLeader(String command) {
        if (role != Role.LEADER) {
            return null;
        }
        LogEntry entry = raftLog.append(currentTerm, command);
        log.info("term={} appended index={} command={}", currentTerm, entry.index(), command);
        return entry;
    }

    public synchronized long lastLogIndex() { return raftLog.lastIndex(); }
    public synchronized long lastLogTerm()  { return raftLog.lastTerm(); }

    public synchronized List<LogEntry> logEntries() {
        return raftLog.all();
    }

    // ---------- Reading state ----------

    public synchronized Snapshot snapshot() {
        return new Snapshot(selfId, currentTerm, role, votedFor, leaderId,
                raftLog.lastIndex(), raftLog.lastTerm());
    }

    public String selfId() {
        return selfId;
    }

    public record Snapshot(String nodeId, long term, Role role, String votedFor, String leaderId,
                           long lastLogIndex, long lastLogTerm) {}
}