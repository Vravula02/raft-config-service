package com.raftconfig.transport;

import com.raftconfig.raft.RaftTransport;
import com.raftconfig.raft.rpc.RequestVoteRequest;
import com.raftconfig.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Sends Raft RPCs to peers as HTTP POST + JSON. */
public class HttpRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(HttpRaftTransport.class);

    private final Map<String, String> peerUrls;
    private final RestClient client;

    public HttpRaftTransport(Map<String, String> peerUrls, Duration timeout) {
        this.peerUrls = peerUrls;
        // Short timeouts: a slow or dead peer must not stall an election
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeout);
        factory.setReadTimeout(timeout);
        this.client = RestClient.builder().requestFactory(factory).build();
    }

    @Override
    public Optional<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest req) {
        return post(peerId, "/raft/request-vote", req, RequestVoteResponse.class);
    }

    private <T> Optional<T> post(String peerId, String path, Object body, Class<T> responseType) {
        String url = peerUrls.get(peerId) + path;
        try {
            T resp = client.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(responseType);
            return Optional.ofNullable(resp);
        } catch (Exception e) {
            // Down or slow peers are normal in Raft; don't spam the logs
            log.debug("RPC to {} failed: {}", peerId, e.getMessage());
            return Optional.empty();
        }
    }
}