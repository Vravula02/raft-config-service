package com.raftconfig.raft;

import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Core Raft state for one node (Figure 2 of the Raft paper).
 *
 * Plain Java, no Spring, so it's easy to unit test.
 * Every method is synchronized: one lock guards all state, including the log.
 */
public class RaftState {

    private static final Logger log = LoggerFactory.getLogger(RaftState.class);
    private static final int MAX_ENTRIES_PER_RPC = 100;

    private final String selfId;

    // Persistent state (saved to disk in Phase 3)
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog raftLog = new RaftLog();

    // Volatile state
    private Role role = Role.FOLLOWER;
    private String leaderId = null;
    private long commitIndex = 0;

    // Leader-only state, reset on every election win
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

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
            votedFor = null;
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
        // Fresh leader state: assume followers are up to date until proven otherwise
        nextIndex.clear();
        matchIndex.clear();
        log.info("term={} -> LEADER (lastLogIndex={})", currentTerm, raftLog.lastIndex());
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

    private boolean isCandidateLogUpToDate(long candLastIndex, long candLastTerm) {
        long myLastTerm = raftLog.lastTerm();
        long myLastIndex = raftLog.lastIndex();
        if (candLastTerm != myLastTerm) {
            return candLastTerm > myLastTerm;
        }
        return candLastIndex >= myLastIndex;
    }

    // ---------- AppendEntries: follower side ----------

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term() < currentTerm) {
            log.debug("term={} reject AppendEntries from stale leader {} (term {})",
                    currentTerm, req.leaderId(), req.term());
            return new AppendEntriesResponse(currentTerm, false);
        }
        becomeFollower(req.term(), req.leaderId());

        if (raftLog.termAt(req.prevLogIndex()) != req.prevLogTerm()) {
            log.debug("term={} log mismatch at prevLogIndex={} (want term {}, have {})",
                    currentTerm, req.prevLogIndex(), req.prevLogTerm(), raftLog.termAt(req.prevLogIndex()));
            return new AppendEntriesResponse(currentTerm, false);
        }

        for (LogEntry entry : req.entries()) {
            long existingTerm = raftLog.termAt(entry.index());
            if (existingTerm == -1) {
                raftLog.appendReplicated(entry);
            } else if (existingTerm != entry.term()) {
                log.info("term={} conflict at index {} (have term {}, leader has {}), truncating",
                        currentTerm, entry.index(), existingTerm, entry.term());
                raftLog.truncateFrom(entry.index());
                raftLog.appendReplicated(entry);
            }
        }

        long lastNewIndex = req.prevLogIndex() + req.entries().size();
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), lastNewIndex);
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    // ---------- AppendEntries: leader side ----------

    /**
     * Build the next AppendEntries for a follower, based on its nextIndex.
     * Returns null if we're no longer leader.
     */
    public synchronized AppendEntriesRequest buildAppendEntries(String peer) {
        if (role != Role.LEADER) {
            return null;
        }
        long next = nextIndex.computeIfAbsent(peer, p -> raftLog.lastIndex() + 1);
        long prevIndex = next - 1;
        List<LogEntry> entries = raftLog.entriesFrom(next);
        if (entries.size() > MAX_ENTRIES_PER_RPC) {
            entries = entries.subList(0, MAX_ENTRIES_PER_RPC);
        }
        return new AppendEntriesRequest(currentTerm, selfId, prevIndex, raftLog.termAt(prevIndex),
                entries, commitIndex);
    }

    /** Process a follower's reply to an AppendEntries we sent. */
    public synchronized void handleAppendEntriesResponse(String peer, AppendEntriesRequest req,
                                                         AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) {
            becomeFollower(resp.term(), null);   // we're a stale leader
            return;
        }
        // Ignore replies from an older term or after we stopped being leader
        if (role != Role.LEADER || req.term() != currentTerm) {
            return;
        }

        if (resp.success()) {
            long match = req.prevLogIndex() + req.entries().size();
            if (match > matchIndex.getOrDefault(peer, 0L)) {
                matchIndex.put(peer, match);
                if (!req.entries().isEmpty()) {
                    log.info("term={} {} replicated up to index {}", currentTerm, peer, match);
                }
            }
            // max(): a late, older success must not move nextIndex backwards
            nextIndex.put(peer, Math.max(nextIndex.getOrDefault(peer, 1L), match + 1));
        } else {
            // Log mismatch: back up one entry and retry on the next tick.
            // min(): a late, older failure must not move nextIndex forwards.
            long backedUp = Math.max(1, Math.min(nextIndex.getOrDefault(peer, 1L), req.prevLogIndex()));
            log.info("term={} {} rejected prevLogIndex={}, backing up nextIndex to {}",
                    currentTerm, peer, req.prevLogIndex(), backedUp);
            nextIndex.put(peer, backedUp);
        }
    }

    /** Leader's view of each follower, for /status. Empty if not leader. */
    public synchronized Map<String, Map<String, Long>> replicationProgress() {
        if (role != Role.LEADER) {
            return Map.of();
        }
        Map<String, Map<String, Long>> out = new TreeMap<>();
        for (String peer : nextIndex.keySet()) {
            Map<String, Long> p = new LinkedHashMap<>();
            p.put("nextIndex", nextIndex.get(peer));
            p.put("matchIndex", matchIndex.getOrDefault(peer, 0L));
            out.put(peer, p);
        }
        return out;
    }

    // ---------- Log ----------

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
                raftLog.lastIndex(), raftLog.lastTerm(), commitIndex);
    }

    public String selfId() {
        return selfId;
    }

    public record Snapshot(String nodeId, long term, Role role, String votedFor, String leaderId,
                           long lastLogIndex, long lastLogTerm, long commitIndex) {}
}