package com.raftconfig.api;

import com.raftconfig.raft.LeadershipLostException;
import com.raftconfig.raft.NotLeaderException;
import com.raftconfig.raft.RaftNode;
import com.raftconfig.store.KeyValueStore;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Client-facing configuration API. Any node accepts requests:
 * writes on a follower are forwarded to the leader, and every write
 * returns only after it's committed and applied.
 */
@RestController
@RequestMapping("/config")
public class ConfigController {

    private static final Pattern VALID_KEY = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    private static final long WRITE_TIMEOUT_MS = 3000;

    private final RaftNode node;
    private final KeyValueStore store;
    private final LeaderForwarder forwarder;

    public ConfigController(RaftNode node, KeyValueStore store, LeaderForwarder forwarder) {
        this.node = node;
        this.store = store;
        this.forwarder = forwarder;
    }

    // ---------- Reads (local, may be slightly stale on followers; see Step 3) ----------

    @GetMapping
    public Map<String, String> all() {
        return store.all();
    }

    @GetMapping("/{key}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String key) {
        return store.get(key)
                .map(v -> ResponseEntity.ok(body("key", key, "value", v)))
                .orElseGet(() -> ResponseEntity.status(404).body(body("error", "not found", "key", key)));
    }

    // ---------- Writes (through Raft, via the leader) ----------

    @PutMapping("/{key}")
    public ResponseEntity<?> put(@PathVariable String key,
                                 @RequestBody(required = false) String value,
                                 @RequestHeader(value = LeaderForwarder.FORWARDED_HEADER, required = false) String forwarded) {
        if (!VALID_KEY.matcher(key).matches()) {
            return ResponseEntity.badRequest().body(body("error", "invalid key: use letters, digits, . _ : - (max 128)"));
        }
        if (value == null || value.isBlank()) {
            return ResponseEntity.badRequest().body(body("error", "value must not be empty"));
        }
        String v = value.strip();
        return commit("SET " + key + " " + v, body("key", key, "value", v),
                HttpMethod.PUT, "/config/" + key, value, forwarded != null);
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<?> delete(@PathVariable String key,
                                    @RequestHeader(value = LeaderForwarder.FORWARDED_HEADER, required = false) String forwarded) {
        if (!VALID_KEY.matcher(key).matches()) {
            return ResponseEntity.badRequest().body(body("error", "invalid key"));
        }
        return commit("DELETE " + key, body("key", key, "deleted", true),
                HttpMethod.DELETE, "/config/" + key, null, forwarded != null);
    }

    /** Submit to Raft and wait for commit; if we're not the leader, route the request to it. */
    private ResponseEntity<?> commit(String command, Map<String, Object> okBody,
                                     HttpMethod method, String path, String rawBody, boolean alreadyForwarded) {
        try {
            long index = node.submit(command).get(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            okBody.put("index", index);
            return ResponseEntity.ok(okBody);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NotLeaderException nle) {
                return routeToLeader(nle.leaderId(), method, path, rawBody, alreadyForwarded);
            }
            if (e.getCause() instanceof LeadershipLostException lle) {
                return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1").body(body(
                        "error", "lost leadership before commit; the write may or may not be applied",
                        "leaderId", lle.leaderId()));
            }
            return ResponseEntity.status(500).body(body("error", String.valueOf(e.getCause())));
        } catch (TimeoutException e) {
            return ResponseEntity.status(504).body(body(
                    "error", "not committed within " + WRITE_TIMEOUT_MS + " ms (no quorum?); it may still be applied later"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(500).body(body("error", "interrupted"));
        }
    }

    private ResponseEntity<?> routeToLeader(String leaderId, HttpMethod method, String path,
                                            String rawBody, boolean alreadyForwarded) {
        if (leaderId == null) {
            // Election in progress: nobody to forward to yet
            return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1")
                    .body(body("error", "no leader elected yet, retry shortly"));
        }
        if (alreadyForwarded) {
            // One hop only: leadership moved while this request was in flight
            return ResponseEntity.status(409).body(body("error", "not leader", "leaderId", leaderId));
        }
        return forwarder.forward(leaderId, method, path, rawBody)
                .<ResponseEntity<?>>map(resp -> ResponseEntity.status(resp.getStatusCode())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header(LeaderForwarder.FORWARDED_TO_HEADER, leaderId)
                        .body(resp.getBody()))
                .orElseGet(() -> ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1")
                        .body(body("error", "leader " + leaderId + " unreachable, retry shortly",
                                "leaderId", leaderId)));
    }

    /** Mutable, ordered, null-tolerant map for JSON bodies. */
    private static Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }
}