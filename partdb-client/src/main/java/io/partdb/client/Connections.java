package io.partdb.client;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;
import io.partdb.grpc.cluster.v1.GetStatusRequest;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class Connections implements AutoCloseable {

    private static final Duration KEEPALIVE = Duration.ofSeconds(30);
    private static final Duration KEEPALIVE_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(1);

    private final List<ServerEndpoint> endpoints;
    private final Duration connectTimeout;
    private final Map<ServerEndpoint, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, ServerEndpoint> nodes = new ConcurrentHashMap<>();
    private volatile ServerEndpoint preferred;
    private volatile boolean closed;

    Connections(List<ServerEndpoint> endpoints, Duration connectTimeout) {
        this.endpoints = List.copyOf(endpoints);
        this.connectTimeout = connectTimeout;
        this.preferred = this.endpoints.getFirst();
    }

    List<ServerEndpoint> endpoints() {
        return endpoints;
    }

    ServerEndpoint preferred() {
        return preferred;
    }

    ManagedChannel channel(ServerEndpoint endpoint) {
        if (closed) {
            throw new PartDbClientException.Closed();
        }
        return channels.computeIfAbsent(endpoint, this::open);
    }

    void rotate(ServerEndpoint failed) {
        if (!failed.equals(preferred)) {
            return;
        }
        int position = endpoints.indexOf(failed);
        preferred = endpoints.get((position + 1) % endpoints.size());
    }

    void redirect(ServerEndpoint current, Optional<String> leader, Deadline deadline) {
        if (leader.isEmpty()) {
            rotate(current);
            return;
        }
        var known = nodes.get(leader.get());
        if (known == null) {
            probe(deadline);
            known = nodes.get(leader.get());
        }
        if (known != null) {
            preferred = known;
        } else {
            rotate(current);
        }
    }

    void learn(String nodeId, ServerEndpoint endpoint) {
        nodes.put(nodeId, endpoint);
    }

    private void probe(Deadline deadline) {
        var probeDeadline = Deadline.after(PROBE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS).minimum(deadline);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (ServerEndpoint endpoint : endpoints) {
                executor.submit(() -> {
                    try {
                        var status = ClusterServiceGrpc.newBlockingStub(channel(endpoint))
                            .withDeadline(probeDeadline)
                            .getStatus(GetStatusRequest.getDefaultInstance());
                        learn(status.getNodeId(), endpoint);
                    } catch (StatusRuntimeException _) {
                    }
                });
            }
        }
    }

    private ManagedChannel open(ServerEndpoint endpoint) {
        return NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
            .usePlaintext()
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()))
            .keepAliveTime(KEEPALIVE.toMillis(), TimeUnit.MILLISECONDS)
            .keepAliveTimeout(KEEPALIVE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .build();
    }

    @Override
    public void close() {
        closed = true;
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
        }
        for (ManagedChannel channel : channels.values()) {
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }
}
