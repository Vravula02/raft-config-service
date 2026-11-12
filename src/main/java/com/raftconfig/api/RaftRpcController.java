package com.raftconfig.api;

import com.raftconfig.raft.RaftNode;
import com.raftconfig.raft.rpc.AppendEntriesRequest;
import com.raftconfig.raft.rpc.AppendEntriesResponse;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Internal node-to-node RPCs. Clients never call these. */
@RestController
@RequestMapping("/raft")
public class RaftRpcController {

    private final RaftNode node;

    public RaftRpcController(RaftNode node) {
        this.node = node;
    }

    @PostMapping("/request-vote")
    public RequestVoteResponse requestVote(@RequestBody RequestVoteRequest req) {
        return node.handleRequestVote(req);
    }

    @PostMapping("/append-entries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntriesRequest req) {
        return node.handleAppendEntries(req);
    }
}