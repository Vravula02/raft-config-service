package com.raftconfig.raft;

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
 * Drives Raft: timers, elections, and (soon) heartbeats and replication.
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

    // One thread for timers; virtual threads for RPCs (cheap to block on HTTP)
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService rpcPool = Executors.newVirtualThreadPerTaskExecutor();

    private ScheduledFuture<?> electionTimer;

    public RaftNode(RaftState state, RaftTransport transport, List<String> peerIds, int quorum,
                    long electionTimeoutMinMs, long electionTimeoutMaxMs) {
        this.state = state;
        this.transport = transport;
        this.peerIds = List.copyOf(peerIds);
        this.quorum = quorum;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
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
            state.becomeLeader(term);
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
                state.becomeLeader(req.term());
            }
        });
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
}