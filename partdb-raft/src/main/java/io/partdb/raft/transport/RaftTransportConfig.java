package io.partdb.raft.transport;

import io.partdb.raft.Peer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public record RaftTransportConfig(
    String bindHost,
    int bindPort,
    String advertisedHost,
    int advertisedPort,
    List<Peer> peers,
    Duration requestVoteTimeout,
    Duration appendEntriesTimeout,
    Duration installSnapshotTimeout,
    int snapshotChunkSize,
    boolean tlsEnabled,
    Optional<Path> certChainFile,
    Optional<Path> privateKeyFile,
    Optional<Path> trustCertFile
) {
    public RaftTransportConfig {
        Objects.requireNonNull(bindHost, "bindHost must not be null");
        if (bindPort <= 0 || bindPort > 65535) {
            throw new IllegalArgumentException("bindPort must be between 1 and 65535");
        }
        Objects.requireNonNull(advertisedHost, "advertisedHost must not be null");
        if (advertisedPort <= 0 || advertisedPort > 65535) {
            throw new IllegalArgumentException("advertisedPort must be between 1 and 65535");
        }
        Objects.requireNonNull(peers, "peers must not be null");
        Objects.requireNonNull(requestVoteTimeout, "requestVoteTimeout must not be null");
        Objects.requireNonNull(appendEntriesTimeout, "appendEntriesTimeout must not be null");
        Objects.requireNonNull(installSnapshotTimeout, "installSnapshotTimeout must not be null");
        if (snapshotChunkSize <= 0) {
            throw new IllegalArgumentException("snapshotChunkSize must be positive");
        }
        Objects.requireNonNull(certChainFile, "certChainFile must not be null");
        Objects.requireNonNull(privateKeyFile, "privateKeyFile must not be null");
        Objects.requireNonNull(trustCertFile, "trustCertFile must not be null");

        peers = List.copyOf(peers);
    }

    public Map<String, String> peerAddressMap() {
        return peers.stream()
            .collect(Collectors.toMap(Peer::nodeId, Peer::address));
    }

    public static RaftTransportConfig defaultConfig(String bindHost, int port, List<Peer> peers) {
        return new RaftTransportConfig(
            bindHost,
            port,
            bindHost,
            port,
            peers,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            Duration.ofSeconds(30),
            1024 * 1024,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }
}
