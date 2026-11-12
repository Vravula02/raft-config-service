package com.raftconfig.config;

import com.raftconfig.raft.RaftNode;
import com.raftconfig.raft.RaftState;
import com.raftconfig.raft.RaftTransport;
import com.raftconfig.transport.HttpRaftTransport;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;

@Configuration
public class RaftConfiguration {

    @Bean
    public RaftState raftState(NodeProperties props) {
        return new RaftState(props.nodeId());
    }

    @Bean
    public RaftTransport raftTransport(NodeProperties props) {
        return new HttpRaftTransport(props.peerMap(), Duration.ofMillis(props.rpcTimeoutMs()));
    }

    // No initMethod: we must not start the election timer before we can receive heartbeats
    @Bean(destroyMethod = "stop")
    public RaftNode raftNode(RaftState state, RaftTransport transport, NodeProperties props) {
        return new RaftNode(
                state,
                transport,
                new ArrayList<>(props.peerMap().keySet()),
                props.quorum(),
                props.electionTimeoutMinMs(),
                props.electionTimeoutMaxMs(),
                props.heartbeatIntervalMs()
        );
    }

    /**
     * Start Raft only once the web server is up and accepting requests.
     * Otherwise a restarting node times out before it can hear the current
     * leader's heartbeats, and disrupts a healthy cluster.
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> startRaftWhenReady(RaftNode node) {
        return event -> node.start();
    }
}