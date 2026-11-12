package com.raftconfig.api;

import com.raftconfig.raft.LogEntry;
import com.raftconfig.raft.RaftState;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Temporary endpoints for inspecting and poking the log. Replaced by the config API in Phase 4. */
@RestController
public class DebugController {

    private final RaftState state;

    public DebugController(RaftState state) {
        this.state = state;
    }

    @GetMapping("/log")
    public List<LogEntry> log() {
        return state.logEntries();
    }

    /** Append a raw command on the leader. Followers answer 409 with a hint. */
    @PostMapping("/debug/append")
    public ResponseEntity<?> append(@RequestBody String command) {
        LogEntry entry = state.appendAsLeader(command);
        if (entry == null) {
            String leader = String.valueOf(state.snapshot().leaderId());
            return ResponseEntity.status(409).body(Map.of("error", "not leader", "leaderId", leader));
        }
        return ResponseEntity.ok(entry);
    }
}