package com.raftconfig.raft;

import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives Raft: timers, elections, heartbeats (and soon replication).
 * RaftState holds the data; RaftNode decides when things happen.
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final RaftState state;
    private final RaftTransport transport;
    private final List<String> peerIds;
    private final int quorum;
    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;
    private final long heartbeatIntervalMs;

    // One thread for timers; virtual threads for RPCs (cheap to block on HTTP)
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcPool = Executors.newVirtualThreadPerTaskExecutor();

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTask;
    private long heartbeatTerm = -1;   // which term the running heartbeat task belongs to

    public RaftNode(RaftState state, RaftTransport transport, List<String> peerIds, int quorum,
                    long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs) {
        this.state = state;
        this.transport = transport;
        this.peerIds = List.copyOf(peerIds);
        this.quorum = quorum;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public void start() {
        log.info("Starting Raft node {} with peers {}", state.selfId(), peerIds);
        resetElectionTimer();
    }

    public void stop() {
        scheduler.shutdownNow();
        rpcPool.shutdownNow();
    }

    // ---------- Election timer ----------

    /** Cancel any pending timeout and start a new random one. */
    public synchronized void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        long timeout = ThreadLocalRandom.current().nextLong(electionTimeoutMinMs, electionTimeoutMaxMs + 1);
        electionTimer = scheduler.schedule(this::onElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        if (state.snapshot().role() == Role.LEADER) {
            // Leaders never start elections; keep the timer alive in case we step down
            resetElectionTimer();
            return;
        }
        startElection();
    }

    // ---------- Running an election ----------

    private void startElection() {
        long term = state.becomeCandidate();
        // If this election splits the vote, the next timeout starts a fresh one
        resetElectionTimer();

        RequestVoteRequest req = new RequestVoteRequest(
                term, state.selfId(), state.lastLogIndex(), state.lastLogTerm());

        AtomicInteger votes = new AtomicInteger(1); // we voted for ourselves
        if (votes.get() >= quorum) {                // single-node cluster
            if (state.becomeLeader(term)) onBecameLeader(term);
            return;
        }

        for (String peer : peerIds) {
            rpcPool.submit(() -> requestVoteFrom(peer, req, votes));
        }
    }

    private void requestVoteFrom(String peer, RequestVoteRequest req, AtomicInteger votes) {
        transport.requestVote(peer, req).ifPresent(resp -> {
            // A higher term in the reply means we're stale: step down, stop counting
            if (state.stepDownIfNewerTerm(resp.term())) {
                return;
            }
            // == (not >=) so only the vote that reaches quorum triggers the promotion
            if (resp.voteGranted() && votes.incrementAndGet() == quorum) {
                if (state.becomeLeader(req.term())) {
                    onBecameLeader(req.term());
                }
            }
        });
    }

    // ---------- Leader: heartbeats ----------

    private synchronized void onBecameLeader(long term) {
        log.info("Won election for term {}, sending heartbeats every {} ms", term, heartbeatIntervalMs);
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        heartbeatTerm = term;
        // Initial delay 0: announce leadership immediately so no one else times out
        heartbeatTask = scheduler.scheduleAtFixedRate(
                () -> heartbeatTick(term), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void heartbeatTick(long term) {
        RaftState.Snapshot s = state.snapshot();
        if (s.role() != Role.LEADER || s.term() != term) {
            stopHeartbeats(term);
            return;
        }
        AppendEntriesRequest req = new AppendEntriesRequest(
                term, state.selfId(), state.lastLogIndex(), state.lastLogTerm(), List.of(), 0);
        for (String peer : peerIds) {
            rpcPool.submit(() -> sendHeartbeat(peer, req));
        }
    }

    private void sendHeartbeat(String peer, AppendEntriesRequest req) {
        // If a follower is in a higher term, we're a stale leader: step down
        transport.appendEntries(peer, req).ifPresent(resp -> state.stepDownIfNewerTerm(resp.term()));
    }

    /** Only cancels the task if it still belongs to the given term (avoids killing a newer one). */
    private synchronized void stopHeartbeats(long term) {
        if (heartbeatTask != null && heartbeatTerm == term) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
            log.info("No longer leader of term {}, stopped heartbeats", term);
        }
    }

    // ---------- Incoming RPCs ----------

    public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        RequestVoteResponse resp = state.handleRequestVote(req);
        if (resp.voteGranted()) {
            // Granting a vote counts as hearing from a live candidate: don't start our own election
            resetElectionTimer();
        }
        return resp;
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        AppendEntriesResponse resp = state.handleAppendEntries(req);
        // If the sender's term matches ours, it's the legitimate current leader:
        // reset the timer. (In Phase 2 this holds even when the log check fails.)
        if (req.term() == resp.term()) {
            resetElectionTimer();
        }
        return resp;
    }
}