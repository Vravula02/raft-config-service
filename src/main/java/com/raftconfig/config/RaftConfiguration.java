package com.raftconfig.config;

import com.raftconfig.raft.RaftNode;
import com.raftconfig.raft.RaftState;
import com.raftconfig.raft.RaftTransport;
import com.raftconfig.transport.HttpRaftTransport;
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

    @Bean(initMethod = "start", destroyMethod = "stop")
    public RaftNode raftNode(RaftState state, RaftTransport transport, NodeProperties props) {
        return new RaftNode(
                state,
                transport,
                new ArrayList<>(props.peerMap().keySet()),
                props.quorum(),
                props.electionTimeoutMinMs(),
                props.electionTimeoutMaxMs()
        );
    }
}