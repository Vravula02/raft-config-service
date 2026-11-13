package com.raftconfig.store;

import com.raftconfig.raft.LogEntry;
import com.raftconfig.raft.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The config store: an in-memory key-value map built by applying committed log entries.
 * Commands: "SET key value" (value may contain spaces), "DELETE key", "NOOP".
 *
 * apply() never throws: a bad command is skipped the same way on every node,
 * so all replicas stay identical.
 */
public class KeyValueStore implements StateMachine {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

    private final Map<String, String> data = new ConcurrentHashMap<>();

    @Override
    public void apply(LogEntry entry) {
        String cmd = entry.command().trim();
        String[] parts = cmd.split("\\s+", 3);
        switch (parts[0].toUpperCase()) {
            case "NOOP" -> { /* leader's no-op entry, nothing to do */ }
            case "SET" -> {
                if (parts.length == 3) {
                    data.put(parts[1], parts[2]);
                } else {
                    log.warn("index={} malformed SET, skipped: {}", entry.index(), cmd);
                    return;
                }
            }
            case "DELETE" -> {
                if (parts.length >= 2) {
                    data.remove(parts[1]);
                } else {
                    log.warn("index={} malformed DELETE, skipped: {}", entry.index(), cmd);
                    return;
                }
            }
            default -> {
                log.warn("index={} unknown command, skipped: {}", entry.index(), cmd);
                return;
            }
        }
        log.info("applied index={} {}", entry.index(), cmd);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    /** Sorted copy, for display. */
    public Map<String, String> all() {
        return new TreeMap<>(data);
    }
}
