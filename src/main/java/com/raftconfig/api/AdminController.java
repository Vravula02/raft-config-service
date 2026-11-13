package com.raftconfig.api;

import com.raftconfig.raft.LogEntry;
import com.raftconfig.raft.RaftState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only operational endpoints, used by the test scripts to compare logs across nodes. */
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
}