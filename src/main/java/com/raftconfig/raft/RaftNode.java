package com.raftconfig.raft;

import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives Raft: timers, pre-vote and elections, heartbeats, replication, client writes and reads.
 * RaftState holds the data; RaftNode decides when things happen.
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public enum ReadResult { OK, LEADER_NOT_READY, NO_QUORUM, TIMEOUT }

    private final RaftState state;
    private final RaftTransport transport;
    private final List<String> peerIds;
    private final int quorum;
    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;
    private final long heartbeatIntervalMs;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcPool = Executors.newVirtualThreadPerTaskExecutor();

    private final Map<String, AtomicBoolean> inFlight = new ConcurrentHashMap<>();

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTask;
    private long heartbeatTerm = -1;

    public RaftNode(RaftState state, RaftTransport transport, List<String> peerIds, int quorum,
                    long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs) {
        this.state = state;
        this.transport = transport;
        this.peerIds = List.copyOf(peerIds);
        this.quorum = quorum;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        for (String peer : this.peerIds) {
            inFlight.put(peer, new AtomicBoolean(false));
        }
    }

    public void start() {
        log.info("Starting Raft node {} with peers {}", state.selfId(), peerIds);
        resetElectionTimer();
    }

    public void stop() {
        scheduler.shutdownNow();
        rpcPool.shutdownNow();
    }

    // ---------- Client writes ----------

    public CompletableFuture<Long> submit(String command) {
        CompletableFuture<Long> future = state.submit(command);
        if (!future.isDone()) {
            replicateNow();
        }
        return future;
    }

    // ---------- Client reads (ReadIndex) ----------

    public ReadResult readBarrier(long timeoutMs) throws InterruptedException {
        long readIndex = state.readIndex();
        if (readIndex < 0) {
            return ReadResult.LEADER_NOT_READY;
        }
        if (!confirmLeadership(timeoutMs)) {
            return ReadResult.NO_QUORUM;
        }
        if (!state.awaitApplied(readIndex, timeoutMs)) {
            return ReadResult.TIMEOUT;
        }
        return ReadResult.OK;
    }

    private boolean confirmLeadership(long timeoutMs) throws InterruptedException {
        RaftState.Snapshot s = state.snapshot();
        if (s.role() != Role.LEADER) {
            return false;
        }
        long term = s.term();
        int needed = quorum - 1;
        if (needed <= 0) {
            return true;
        }
        CountDownLatch acks = new CountDownLatch(needed);
        for (String peer : peerIds) {
            rpcPool.submit(() -> {
                AppendEntriesRequest req = state.buildAppendEntries(peer);
                if (req == null || req.term() != term) {
                    return;
                }
                transport.appendEntries(peer, req).ifPresent(resp -> {
                    state.handleAppendEntriesResponse(peer, req, resp);
                    if (resp.term() == term) {
                        acks.countDown();
                    }
                });
            });
        }
        boolean gotMajority = acks.await(timeoutMs, TimeUnit.MILLISECONDS);
        RaftState.Snapshot after = state.snapshot();
        return gotMajority && after.role() == Role.LEADER && after.term() == term;
    }

    // ---------- Election timer ----------

    public synchronized void resetElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        long timeout = ThreadLocalRandom.current().nextLong(electionTimeoutMinMs, electionTimeoutMaxMs + 1);
        electionTimer = scheduler.schedule(this::onElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        RaftState.Snapshot s = state.snapshot();
        if (s.role() == Role.LEADER) {
            resetElectionTimer();
            return;
        }
        startPreVote(s.term());
    }

    // ---------- Pre-Vote (Raft thesis 9.6) ----------

    /** Ask "would you vote for me?" without touching any term. Only a majority yes starts a real election. */
    private void startPreVote(long term) {
        resetElectionTimer();   // if this round fails, we try again after another timeout
        long roundStartMs = System.currentTimeMillis();
        long proposedTerm = term + 1;
        log.info("Election timeout in term {}, starting pre-vote for term {}", term, proposedTerm);

        RequestVoteRequest req = new RequestVoteRequest(
                proposedTerm, state.selfId(), state.lastLogIndex(), state.lastLogTerm());

        AtomicInteger grants = new AtomicInteger(1);   // we'd vote for ourselves
        if (grants.get() >= quorum) {
            startElection();
            return;
        }
        for (String peer : peerIds) {
            rpcPool.submit(() -> preVoteFrom(peer, req, term, roundStartMs, grants));
        }
    }

    private void preVoteFrom(String peer, RequestVoteRequest req, long term, long roundStartMs,
                             AtomicInteger grants) {
        transport.preVote(peer, req).ifPresent(resp -> {
            if (state.stepDownIfNewerTerm(resp.term())) {
                return;
            }
            if (resp.voteGranted() && grants.incrementAndGet() == quorum) {
                RaftState.Snapshot s = state.snapshot();
                // Still in the same term, not leader, and no leader contacted us since the round began
                if (s.term() == term && s.role() != Role.LEADER
                        && state.lastLeaderContactMs() < roundStartMs) {
                    log.info("Pre-vote for term {} won, starting election", req.term());
                    startElection();
                }
            }
        });
    }

    // ---------- Running an election ----------

    private void startElection() {
        long term = state.becomeCandidate();
        resetElectionTimer();

        RequestVoteRequest req = new RequestVoteRequest(
                term, state.selfId(), state.lastLogIndex(), state.lastLogTerm());

        AtomicInteger votes = new AtomicInteger(1);
        if (votes.get() >= quorum) {
            if (state.becomeLeader(term)) onBecameLeader(term);
            return;
        }

        for (String peer : peerIds) {
            rpcPool.submit(() -> requestVoteFrom(peer, req, votes));
        }
    }

    private void requestVoteFrom(String peer, RequestVoteRequest req, AtomicInteger votes) {
        transport.requestVote(peer, req).ifPresent(resp -> {
            if (state.stepDownIfNewerTerm(resp.term())) {
                return;
            }
            if (resp.voteGranted() && votes.incrementAndGet() == quorum) {
                if (state.becomeLeader(req.term())) {
                    onBecameLeader(req.term());
                }
            }
        });
    }

    // ---------- Leader: heartbeats + replication ----------

    private synchronized void onBecameLeader(long term) {
        log.info("Won election for term {}, replicating every {} ms", term, heartbeatIntervalMs);
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        heartbeatTerm = term;
        heartbeatTask = scheduler.scheduleAtFixedRate(
                () -> heartbeatTick(term), 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void heartbeatTick(long term) {
        RaftState.Snapshot s = state.snapshot();
        if (s.role() != Role.LEADER || s.term() != term) {
            stopHeartbeats(term);
            return;
        }
        sendToAllPeers(term);
    }

    private void replicateNow() {
        RaftState.Snapshot s = state.snapshot();
        if (s.role() == Role.LEADER) {
            sendToAllPeers(s.term());
        }
    }

    private void sendToAllPeers(long term) {
        for (String peer : peerIds) {
            if (inFlight.get(peer).compareAndSet(false, true)) {
                rpcPool.submit(() -> replicateTo(peer, term));
            }
        }
    }

    private void replicateTo(String peer, long term) {
        try {
            AppendEntriesRequest req = state.buildAppendEntries(peer);
            if (req == null || req.term() != term) {
                return;
            }
            transport.appendEntries(peer, req)
                    .ifPresent(resp -> state.handleAppendEntriesResponse(peer, req, resp));
        } finally {
            inFlight.get(peer).set(false);
        }
    }

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
            resetElectionTimer();
        }
        return resp;
    }

    /** Pre-votes change nothing, so they don't reset our timer either. */
    public RequestVoteResponse handlePreVote(RequestVoteRequest req) {
        return state.handlePreVote(req, electionTimeoutMinMs);
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        AppendEntriesResponse resp = state.handleAppendEntries(req);
        if (req.term() == resp.term()) {
            resetElectionTimer();
        }
        return resp;
    }
}