package com.raftconfig.raft;

import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;

import java.util.Optional;

/** How a node sends RPCs to peers. Empty = peer unreachable or timed out. */
public interface RaftTransport {
    Optional<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest req);
}