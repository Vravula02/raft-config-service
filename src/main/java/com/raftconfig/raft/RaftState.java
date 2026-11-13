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
import java.util.concurrent.CompletableFuture;

/**
 * Core Raft state for one node (Figure 2 of the Raft paper).
 *
 * Plain Java, no Spring, so it's easy to unit test.
 * Every method is synchronized: one lock guards all state, including the log.
 */
public class RaftState {

    private static final Logger log = LoggerFactory.getLogger(RaftState.class);
    private static final int MAX_ENTRIES_PER_RPC = 100;
    public static final String NOOP = "NOOP";

    private final String selfId;
    private final int quorum;
    private final StateMachine stateMachine;
    private final MetadataStore metadata;

    // Persistent state: on disk before we reply to anyone
    private long currentTerm;
    private String votedFor;
    private final RaftLog raftLog;

    // Volatile state (rebuilt after a restart)
    private Role role = Role.FOLLOWER;
    private String leaderId = null;
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Leader-only state, reset on every election win
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // Leader-only: client writes waiting for their entry to be applied, by log index
    private record PendingWrite(long term, CompletableFuture<Long> future) {}
    private final Map<Long, PendingWrite> pending = new HashMap<>();

    public RaftState(String selfId, int quorum, StateMachine stateMachine,
                     MetadataStore metadata, RaftLog raftLog) {
        this.selfId = selfId;
        this.quorum = quorum;
        this.stateMachine = stateMachine;
        this.metadata = metadata;
        this.raftLog = raftLog;
        MetadataStore.Metadata saved = metadata.load();
        this.currentTerm = saved.currentTerm();
        this.votedFor = saved.votedFor();
        log.info("Loaded persisted state: term={} votedFor={} lastLogIndex={}",
                currentTerm, votedFor, raftLog.lastIndex());
    }

    // ---------- Role transitions ----------

    public synchronized void becomeFollower(long term, String newLeaderId) {
        if (term < currentTerm) {
            throw new IllegalStateException("Cannot go back in time: term " + term + " < " + currentTerm);
        }
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistMetadata();
        }
        if (role == Role.LEADER) {
            failPendingWrites(newLeaderId);
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
        persistMetadata();
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
        nextIndex.clear();
        matchIndex.clear();
        LogEntry noop = raftLog.append(currentTerm, NOOP);
        log.info("term={} -> LEADER (appended NOOP at index {})", currentTerm, noop.index());
        advanceLeaderCommit();
        return true;
    }

    public synchronized boolean stepDownIfNewerTerm(long term) {
        if (term > currentTerm) {
            becomeFollower(term, null);
            return true;
        }
        return false;
    }

    // ---------- Client writes ----------

    public synchronized CompletableFuture<Long> submit(String command) {
        if (role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
        }
        LogEntry entry = raftLog.append(currentTerm, command);
        log.info("term={} client write at index={} command={}", currentTerm, entry.index(), command);
        CompletableFuture<Long> future = new CompletableFuture<>();
        pending.put(entry.index(), new PendingWrite(entry.term(), future));
        advanceLeaderCommit();
        return future;
    }

    private void failPendingWrites(String newLeaderId) {
        if (pending.isEmpty()) return;
        log.info("term={} no longer leader, failing {} pending writes", currentTerm, pending.size());
        for (PendingWrite w : pending.values()) {
            w.future().completeExceptionally(new LeadershipLostException(newLeaderId));
        }
        pending.clear();
    }

    // ---------- Linearizable reads (ReadIndex, Raft thesis 6.4) ----------

    /**
     * Leader: the commit index a linearizable read must wait for.
     * Returns -1 if we haven't committed an entry in our own term yet (our NOOP),
     * because until then we don't know the latest commit index.
     * Throws NotLeaderException on followers.
     */
    public synchronized long readIndex() {
        if (role != Role.LEADER) {
            throw new NotLeaderException(leaderId);
        }
        if (raftLog.termAt(commitIndex) != currentTerm) {
            return -1;
        }
        return commitIndex;
    }

    /** Block until lastApplied >= index, or the timeout passes. Releases the lock while waiting. */
    public synchronized boolean awaitApplied(long index, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (lastApplied < index) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) {
                return false;
            }
            wait(left);
        }
        return true;
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
            if (!req.candidateId().equals(votedFor)) {
                votedFor = req.candidateId();
                persistMetadata();
            }
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

        List<LogEntry> incoming = req.entries();
        for (int i = 0; i < incoming.size(); i++) {
            LogEntry entry = incoming.get(i);
            long existingTerm = raftLog.termAt(entry.index());
            if (existingTerm == entry.term()) {
                continue;
            }
            if (existingTerm != -1) {
                log.info("term={} conflict at index {} (have term {}, leader has {}), truncating",
                        currentTerm, entry.index(), existingTerm, entry.term());
                raftLog.truncateFrom(entry.index());
            }
            raftLog.appendAll(incoming.subList(i, incoming.size()));
            break;
        }

        long lastNewIndex = req.prevLogIndex() + incoming.size();
        long newCommit = Math.min(req.leaderCommit(), lastNewIndex);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }

        return new AppendEntriesResponse(currentTerm, true);
    }

    // ---------- AppendEntries: leader side ----------

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

    public synchronized void handleAppendEntriesResponse(String peer, AppendEntriesRequest req,
                                                         AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) {
            becomeFollower(resp.term(), null);
            return;
        }
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
                advanceLeaderCommit();
            }
            nextIndex.put(peer, Math.max(nextIndex.getOrDefault(peer, 1L), match + 1));
        } else {
            long backedUp = Math.max(1, Math.min(nextIndex.getOrDefault(peer, 1L), req.prevLogIndex()));
            log.info("term={} {} rejected prevLogIndex={}, backing up nextIndex to {}",
                    currentTerm, peer, req.prevLogIndex(), backedUp);
            nextIndex.put(peer, backedUp);
        }
    }

    private void advanceLeaderCommit() {
        for (long n = raftLog.lastIndex(); n > commitIndex; n--) {
            if (raftLog.termAt(n) != currentTerm) {
                break;
            }
            int replicas = 1;
            for (long m : matchIndex.values()) {
                if (m >= n) replicas++;
            }
            if (replicas >= quorum) {
                log.info("term={} committed up to index {} ({} of {} replicas)",
                        currentTerm, n, replicas, quorum);
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    /** Apply committed entries in order, answer waiting writers, wake waiting readers. */
    private void applyCommitted() {
        boolean appliedAny = false;
        while (lastApplied < commitIndex) {
            lastApplied++;
            appliedAny = true;
            LogEntry entry = raftLog.get(lastApplied);
            stateMachine.apply(entry);
            PendingWrite w = pending.remove(lastApplied);
            if (w != null) {
                if (w.term() == entry.term()) {
                    w.future().complete(entry.index());
                } else {
                    w.future().completeExceptionally(new LeadershipLostException(leaderId));
                }
            }
        }
        if (appliedAny) {
            notifyAll();   // wake readers blocked in awaitApplied()
        }
    }

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

    public synchronized long lastLogIndex() { return raftLog.lastIndex(); }
    public synchronized long lastLogTerm()  { return raftLog.lastTerm(); }

    public synchronized List<LogEntry> logEntries() {
        return raftLog.all();
    }

    // ---------- Persistence ----------

    private void persistMetadata() {
        metadata.save(currentTerm, votedFor);
    }

    // ---------- Reading state ----------

    public synchronized Snapshot snapshot() {
        return new Snapshot(selfId, currentTerm, role, votedFor, leaderId,
                raftLog.lastIndex(), raftLog.lastTerm(), commitIndex, lastApplied);
    }

    public String selfId() {
        return selfId;
    }

    public record Snapshot(String nodeId, long term, Role role, String votedFor, String leaderId,
                           long lastLogIndex, long lastLogTerm, long commitIndex, long lastApplied) {}
}