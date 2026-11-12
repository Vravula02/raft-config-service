package com.raftconfig.api;

import com.raftconfig.config.NodeProperties;
import com.raftconfig.raft.RaftState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class StatusController {

    private final NodeProperties props;
    private final RaftState state;

    public StatusController(NodeProperties props, RaftState state) {
        this.props = props;
        this.state = state;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        RaftState.Snapshot s = state.snapshot();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", s.nodeId());
        out.put("role", s.role());
        out.put("term", s.term());
        out.put("leaderId", s.leaderId());
        out.put("votedFor", s.votedFor());
        out.put("lastLogIndex", s.lastLogIndex());
        out.put("lastLogTerm", s.lastLogTerm());
        out.put("commitIndex", s.commitIndex());
        out.put("clusterSize", props.clusterSize());
        out.put("quorum", props.quorum());
        return out;
    }
}