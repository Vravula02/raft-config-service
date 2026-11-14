package com.raftconfig.raft;

import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import com.raftconfig.store.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Safety rules of Raft, tested directly on RaftState: no HTTP, no timers.
 * Every node is part of a 5-node cluster (quorum = 3).
 */
class RaftStateTest {

    @TempDir
    Path tmp;

    private final List<RaftLog> openLogs = new ArrayList<>();
    private int dirCounter = 0;

    @AfterEach
    void closeLogs() {
        openLogs.forEach(RaftLog::close);
        openLogs.clear();
    }

    // ---------- helpers ----------

    private Path newDir() {
        return tmp.resolve("node" + (++dirCounter));
    }

    private RaftState node(String id, Path dir, StateMachine stateMachine) {
        RaftLog raftLog = new RaftLog(dir);
        openLogs.add(raftLog);
        return new RaftState(id, 3, stateMachine, new MetadataStore(dir), raftLog);
    }

    private RaftState node(String id) {
        return node(id, newDir(), new KeyValueStore());
    }

    private static LogEntry e(long index, long term, String command) {
        return new LogEntry(index, term, command);
    }

    private static RequestVoteRequest vote(long term, String candidate, long lastLogIndex, long lastLogTerm) {
        return new RequestVoteRequest(term, candidate, lastLogIndex, lastLogTerm);
    }

    private static AppendEntriesRequest ae(long term, String leader, long prevIndex, long prevTerm,
                                           long leaderCommit, LogEntry... entries) {
        return new AppendEntriesRequest(term, leader, prevIndex, prevTerm, List.of(entries), leaderCommit);
    }

    private static void makeLeader(RaftState n) {
        long term = n.becomeCandidate();
        assertTrue(n.becomeLeader(term));
    }

    /** Simulate a follower acknowledging an AppendEntries from the leader. */
    private static void ack(RaftState leader, String peer, long prevIndex, long prevTerm, LogEntry... entries) {
        long term = leader.snapshot().term();
        AppendEntriesRequest req = new AppendEntriesRequest(
                term, leader.selfId(), prevIndex, prevTerm, List.of(entries), 0);
        leader.handleAppendEntriesResponse(peer, req, new AppendEntriesResponse(term, true));
    }

    // ---------- voting ----------

    @Test
    void grantsAtMostOneVotePerTerm() {
        RaftState n = node("node1");
        assertTrue(n.handleRequestVote(vote(1, "node2", 0, 0)).voteGranted());
        assertFalse(n.handleRequestVote(vote(1, "node3", 0, 0)).voteGranted());
        assertTrue(n.handleRequestVote(vote(1, "node2", 0, 0)).voteGranted(), "repeating the same vote is safe");
    }

    @Test
    void rejectsStaleTermAndReportsCurrentTerm() {
        RaftState n = node("node1");
        n.handleRequestVote(vote(2, "node2", 0, 0));
        RequestVoteResponse r = n.handleRequestVote(vote(1, "node3", 0, 0));
        assertFalse(r.voteGranted());
        assertEquals(2, r.term(), "the reply tells the stale candidate the current term");
    }

    @Test
    void newTermAllowsAFreshVote() {
        RaftState n = node("node1");
        n.handleRequestVote(vote(1, "node2", 0, 0));
        assertTrue(n.handleRequestVote(vote(2, "node3", 0, 0)).voteGranted());
        assertEquals(2, n.snapshot().term());
        assertEquals("node3", n.snapshot().votedFor());
    }

    @Test
    void votesOnlyForCandidatesWithUpToDateLogs() {
        RaftState n = node("node1");
        assertTrue(n.handleAppendEntries(ae(2, "node2", 0, 0, 0,
                e(1, 1, "SET a 1"), e(2, 2, "SET b 2"))).success());

        assertFalse(n.handleRequestVote(vote(3, "node3", 5, 1)).voteGranted(),
                "older last term loses, even with a longer log");
        assertFalse(n.handleRequestVote(vote(4, "node4", 1, 2)).voteGranted(),
                "same last term but shorter log loses");
        assertTrue(n.handleRequestVote(vote(5, "node5", 2, 2)).voteGranted(),
                "an equally up-to-date log wins");
    }

    // ---------- AppendEntries (follower) ----------

    @Test
    void rejectsEntriesWhenPrevLogIndexIsMissing() {
        RaftState n = node("node1");
        assertFalse(n.handleAppendEntries(ae(1, "node2", 5, 1, 0)).success());
    }

    @Test
    void rejectsEntriesWhenPrevLogTermDiffers() {
        RaftState n = node("node1");
        n.handleAppendEntries(ae(1, "node2", 0, 0, 0, e(1, 1, "SET a 1"), e(2, 1, "SET b 2")));
        assertFalse(n.handleAppendEntries(ae(1, "node2", 2, 9, 0)).success());
    }

    @Test
    void duplicateAppendEntriesIsHarmless() {
        RaftState n = node("node1");
        AppendEntriesRequest req = ae(1, "node2", 0, 0, 0, e(1, 1, "SET a 1"), e(2, 1, "SET b 2"));
        assertTrue(n.handleAppendEntries(req).success());
        assertTrue(n.handleAppendEntries(req).success());
        assertEquals(2, n.lastLogIndex());
    }

    @Test
    void conflictingEntriesAreReplacedByTheLeaders() {
        RaftState n = node("node1");
        n.handleAppendEntries(ae(1, "node2", 0, 0, 0,
                e(1, 1, "SET a 1"), e(2, 1, "SET b 2"), e(3, 1, "SET c 3")));

        // A new leader in term 2 has a different entry at index 2
        assertTrue(n.handleAppendEntries(ae(2, "node3", 1, 1, 0, e(2, 2, "SET b 20"))).success());

        assertEquals(List.of(e(1, 1, "SET a 1"), e(2, 2, "SET b 20")), n.logEntries(),
                "index 2 replaced, and the uncommitted index 3 after it removed");
    }

    @Test
    void delayedOldAppendEntriesDoesNotTruncateNewerEntries() {
        RaftState n = node("node1");
        n.handleAppendEntries(ae(1, "node2", 0, 0, 0,
                e(1, 1, "SET a 1"), e(2, 1, "SET b 2"), e(3, 1, "SET c 3")));

        // An older RPC from the same leader arrives late, carrying only entry 1
        assertTrue(n.handleAppendEntries(ae(1, "node2", 0, 0, 0, e(1, 1, "SET a 1"))).success());
        assertEquals(3, n.lastLogIndex());
    }

    @Test
    void commitIndexIsCappedAndNeverMovesBackwards() {
        KeyValueStore store = new KeyValueStore();
        RaftState n = node("node1", newDir(), store);

        n.handleAppendEntries(ae(1, "node2", 0, 0, 10, e(1, 1, "SET a 1"), e(2, 1, "SET b 2")));
        assertEquals(2, n.snapshot().commitIndex(), "capped at the last entry this RPC proved we have");
        assertEquals("1", store.get("a").orElseThrow(), "committed entries are applied");

        // A heartbeat that only proves index 1 must not lower commitIndex
        n.handleAppendEntries(ae(1, "node2", 1, 1, 2));
        assertEquals(2, n.snapshot().commitIndex());
    }

    // ---------- commit rule (leader) ----------

    @Test
    void leaderDoesNotCommitOldTermEntriesByCountingReplicas() {
        RaftState n = node("node1");
        LogEntry a = e(1, 1, "SET a 1");
        LogEntry b = e(2, 1, "SET b 2");
        n.handleAppendEntries(ae(1, "node2", 0, 0, 0, a, b));   // entries from a term-1 leader

        makeLeader(n);                                          // term 2, NOOP at index 3
        assertEquals(3, n.lastLogIndex());
        LogEntry noop = n.logEntries().get(2);

        ack(n, "node2", 0, 0, a, b);
        ack(n, "node3", 0, 0, a, b);
        assertEquals(0, n.snapshot().commitIndex(),
                "index 2 is on a majority, but it's from term 1: Figure 8 says don't commit it this way");

        ack(n, "node2", 2, 1, noop);
        assertEquals(0, n.snapshot().commitIndex(), "2 of 5 is not a majority");

        ack(n, "node3", 2, 1, noop);
        assertEquals(3, n.snapshot().commitIndex(),
                "committing the current-term NOOP commits everything before it");
    }

    // ---------- client writes ----------

    @Test
    void clientWriteCompletesOnlyAfterMajorityCommit() throws Exception {
        KeyValueStore store = new KeyValueStore();
        RaftState n = node("node1", newDir(), store);
        makeLeader(n);                                          // term 1, NOOP at index 1
        LogEntry noop = n.logEntries().get(0);

        CompletableFuture<Long> future = n.submit("SET color blue");
        LogEntry write = n.logEntries().get(1);
        assertFalse(future.isDone());

        ack(n, "node2", 0, 0, noop, write);
        assertFalse(future.isDone(), "2 of 5 is not a majority");

        ack(n, "node3", 0, 0, noop, write);
        assertEquals(Long.valueOf(2), future.get());
        assertEquals("blue", store.get("color").orElseThrow());
    }

    @Test
    void followerRejectsClientWrites() {
        RaftState n = node("node1");
        ExecutionException ex = assertThrows(ExecutionException.class, () -> n.submit("SET a 1").get());
        assertInstanceOf(NotLeaderException.class, ex.getCause());
    }

    @Test
    void pendingWritesFailWhenLeaderStepsDown() {
        RaftState n = node("node1");
        makeLeader(n);
        CompletableFuture<Long> future = n.submit("SET a 1");

        n.handleAppendEntries(ae(5, "node2", 0, 0, 0));          // a newer leader appears

        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(LeadershipLostException.class, ex.getCause(), "outcome unknown, not 'failed'");
        assertEquals(Role.FOLLOWER, n.snapshot().role());
    }

    // ---------- Pre-Vote ----------

    @Test
    void preVoteRejectedWhileLeaderIsAliveAndChangesNoState() {
        RaftState n = node("node1");
        n.handleAppendEntries(ae(3, "node2", 0, 0, 0));          // just heard from leader node2

        assertFalse(n.handlePreVote(vote(4, "node3", 0, 0), 10_000).voteGranted());

        RaftState.Snapshot s = n.snapshot();
        assertEquals(3, s.term(), "pre-vote must not bump our term");
        assertNull(s.votedFor(), "pre-vote must not record a vote");
        assertEquals("node2", s.leaderId());
    }

    @Test
    void preVoteGrantedWithNoLeaderAndStillChangesNoState() {
        RaftState n = node("node1");
        assertTrue(n.handlePreVote(vote(1, "node3", 0, 0), 10_000).voteGranted());
        assertEquals(0, n.snapshot().term());
        assertNull(n.snapshot().votedFor());
    }

    // ---------- persistence ----------

    @Test
    void voteAndLogSurviveRestart() {
        Path dir = newDir();
        RaftState before = node("node1", dir, new KeyValueStore());
        before.handleAppendEntries(ae(2, "node2", 0, 0, 0, e(1, 1, "SET a 1"), e(2, 2, "SET b 2")));
        assertTrue(before.handleRequestVote(vote(3, "node2", 2, 2)).voteGranted());
        List<LogEntry> logBefore = before.logEntries();

        closeLogs();                                              // "crash"
        RaftState after = node("node1", dir, new KeyValueStore());

        assertEquals(3, after.snapshot().term());
        assertEquals("node2", after.snapshot().votedFor());
        assertEquals(logBefore, after.logEntries());
        assertFalse(after.handleRequestVote(vote(3, "node3", 2, 2)).voteGranted(),
                "no second vote in term 3 after a restart");
    }
}