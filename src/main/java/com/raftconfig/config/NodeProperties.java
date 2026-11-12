package com.raftconfig.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Identity of this node, its peers, and Raft timing settings.
 * peers format: node2@http://localhost:8082,node3@http://localhost:8083
 */
@ConfigurationProperties(prefix = "raft")
public record NodeProperties(
        String nodeId,
        String peers,
        long electionTimeoutMinMs,
        long electionTimeoutMaxMs,
        long heartbeatIntervalMs,
        long rpcTimeoutMs
) {

    public NodeProperties {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("raft.node-id must be set");
        }
        peers = peers == null ? "" : peers;
        if (electionTimeoutMinMs <= 0 || electionTimeoutMaxMs < electionTimeoutMinMs) {
            throw new IllegalArgumentException("Invalid election timeout range");
        }
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= electionTimeoutMinMs) {
            throw new IllegalArgumentException("heartbeat interval must be > 0 and < election timeout min");
        }
    }

    /** Peer id -> base URL, excluding this node. */
    public Map<String, String> peerMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (String entry : peers.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            String[] parts = trimmed.split("@", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Bad peer entry (expected id@url): " + trimmed);
            }
            if (!parts[0].equals(nodeId)) {
                map.put(parts[0], parts[1]);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    public int clusterSize() {
        return peerMap().size() + 1;
    }

    public int quorum() {
        return clusterSize() / 2 + 1;
    }
}