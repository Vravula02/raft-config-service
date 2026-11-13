package com.raftconfig.api;

import com.raftconfig.config.NodeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Forwards a client write from a follower to the current leader. */
@Component
public class LeaderForwarder {

    private static final Logger log = LoggerFactory.getLogger(LeaderForwarder.class);

    /** Set on forwarded requests, so they are never forwarded a second time. */
    public static final String FORWARDED_HEADER = "X-Raft-Forwarded";
    /** Set on responses to tell the client which node actually handled the write. */
    public static final String FORWARDED_TO_HEADER = "X-Raft-Forwarded-To";

    private final Map<String, String> peerUrls;
    private final RestClient client;

    public LeaderForwarder(NodeProperties props) {
        this.peerUrls = props.peerMap();
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(500));
        // Longer than the leader's 3 s commit timeout, so the leader's own 504 reaches the client
        factory.setReadTimeout(Duration.ofMillis(5000));
        this.client = RestClient.builder().requestFactory(factory).build();
    }

    /**
     * Send the request to the leader and return its response (any status code).
     * Empty if the leader is unknown to us or unreachable.
     */
    public Optional<ResponseEntity<String>> forward(String leaderId, HttpMethod method, String path, String body) {
        String base = peerUrls.get(leaderId);
        if (base == null) {
            return Optional.empty();
        }
        try {
            RestClient.RequestBodySpec spec = client.method(method)
                    .uri(base + path)
                    .header(FORWARDED_HEADER, "true");
            if (body != null) {
                spec = spec.contentType(MediaType.TEXT_PLAIN).body(body);
            }
            ResponseEntity<String> resp = spec.retrieve()
                    .onStatus(status -> true, (req, res) -> { })   // pass every status through, don't throw
                    .toEntity(String.class);
            return Optional.of(resp);
        } catch (Exception e) {
            log.warn("Forwarding {} {} to leader {} failed: {}", method, path, leaderId, e.getMessage());
            return Optional.empty();
        }
    }
}