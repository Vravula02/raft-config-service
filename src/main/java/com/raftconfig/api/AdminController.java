package com.raftconfig.api;

import com.raftconfig.raft.LogEntry;
import com.raftconfig.raft.RaftState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only operational endpoints, used by the test scripts to compare nodes. */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final RaftState state;

    public AdminController(RaftState state) {
        this.state = state;
    }

    @GetMapping("/log")
    public List<LogEntry> log() {
        return state.logEntries();
    }

    /** Hash of the committed log: equal hashes mean identical committed logs. */
    @GetMapping("/log-hash")
    public Map<String, Object> logHash() {
        RaftState.LogDigest d = state.committedLogDigest();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", state.selfId());
        out.put("commitIndex", d.commitIndex());
        out.put("sha256", d.sha256());
        return out;
    }
}