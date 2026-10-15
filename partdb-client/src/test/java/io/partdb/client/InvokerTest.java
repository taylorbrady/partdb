package io.partdb.client;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;
import io.partdb.grpc.cluster.v1.GetStatusRequest;
import io.partdb.grpc.cluster.v1.NodeStatus;
import io.partdb.grpc.cluster.v1.Role;
import io.partdb.grpc.kv.v1.GetRequest;
import io.partdb.grpc.kv.v1.GetResponse;
import io.partdb.grpc.kv.v1.KeyValueServiceGrpc;
import io.partdb.grpc.kv.v1.PutRequest;
import io.partdb.grpc.kv.v1.PutResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InvokerTest {

    private final List<Server> servers = new ArrayList<>();

    @AfterEach
    void stop() {
        servers.forEach(Server::shutdownNow);
    }

    private record FakeNode(String id, Function<PutRequest, Object> put, AtomicInteger puts) {}

    private String start(FakeNode node) throws IOException {
        var server = NettyServerBuilder.forPort(0)
            .addService(new KeyValueServiceGrpc.KeyValueServiceImplBase() {
                @Override
                public void put(PutRequest request, StreamObserver<PutResponse> responses) {
                    node.puts().incrementAndGet();
                    switch (node.put().apply(request)) {
                        case PutResponse response -> {
                            responses.onNext(response);
                            responses.onCompleted();
                        }
                        case RuntimeException error -> responses.onError(error);
                        default -> throw new IllegalStateException();
                    }
                }

                @Override
                public void get(GetRequest request, StreamObserver<GetResponse> responses) {
                    responses.onNext(GetResponse.newBuilder().setReadRevision(1).build());
                    responses.onCompleted();
                }
            })
            .addService(new ClusterServiceGrpc.ClusterServiceImplBase() {
                @Override
                public void getStatus(GetStatusRequest request, StreamObserver<NodeStatus> responses) {
                    responses.onNext(NodeStatus.newBuilder().setNodeId(node.id()).setRole(Role.ROLE_FOLLOWER).build());
                    responses.onCompleted();
                }
            })
            .build()
            .start();
        servers.add(server);
        return "localhost:" + server.getPort();
    }

    private static RuntimeException failure(Code code, String reason, Map<String, String> metadata) {
        var status = com.google.rpc.Status.newBuilder().setCode(code.getNumber()).setMessage(code.name());
        if (reason != null) {
            status.addDetails(Any.pack(ErrorInfo.newBuilder().setReason(reason).setDomain("partdb.io").putAllMetadata(metadata).build()));
        }
        return StatusProto.toStatusRuntimeException(status.build());
    }

    private static PartDbClient client(String... endpoints) {
        return PartDbClient.builder().endpoints(endpoints).requestTimeout(Duration.ofSeconds(3)).build();
    }

    @Test
    void followsTheLeaderHintToTheRightEndpoint() throws IOException {
        var leaderPuts = new AtomicInteger();
        var followerPuts = new AtomicInteger();
        String follower = start(new FakeNode("n1", _ -> failure(Code.UNAVAILABLE, "NOT_LEADER", Map.of("leader_id", "n2")), followerPuts));
        String leader = start(new FakeNode("n2", _ -> PutResponse.newBuilder().setRevision(7).build(), leaderPuts));

        try (var client = client(follower, leader)) {
            assertEquals(7, client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
            assertEquals(7, client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }

        assertEquals(1, followerPuts.get());
        assertEquals(2, leaderPuts.get());
    }

    @Test
    void retriesBusyWritesUntilTheyCommit() throws IOException {
        var attempts = new AtomicInteger();
        String node = start(new FakeNode("n1", _ -> attempts.get() < 3
            ? failure(Code.UNAVAILABLE, "BUSY", Map.of())
            : PutResponse.newBuilder().setRevision(3).build(), attempts));

        try (var client = client(node)) {
            assertEquals(3, client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }
        assertEquals(3, attempts.get());
    }

    @Test
    void skipsAnEndpointThatRefusesConnections() throws IOException {
        int closed;
        try (var socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        var puts = new AtomicInteger();
        String live = start(new FakeNode("n1", _ -> PutResponse.newBuilder().setRevision(1).build(), puts));

        try (var client = client("localhost:" + closed, live)) {
            assertEquals(1, client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }
    }

    @Test
    void doesNotRetryAWriteThatFailedInFlight() throws IOException {
        var puts = new AtomicInteger();
        String node = start(new FakeNode("n1", _ -> Status.UNAVAILABLE.withDescription("connection reset").asRuntimeException(), puts));

        try (var client = client(node)) {
            assertThrows(PartDbClientException.OutcomeUnknown.class, () -> client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }
        assertEquals(1, puts.get());
    }

    @Test
    void reportsServerOutcomeUnknownAndInvalidRequests() throws IOException {
        var puts = new AtomicInteger();
        String unknown = start(new FakeNode("n1", _ -> failure(Code.UNKNOWN, "OUTCOME_UNKNOWN", Map.of()), puts));
        String invalid = start(new FakeNode("n2", _ -> failure(Code.INVALID_ARGUMENT, null, Map.of()), puts));

        try (var first = client(unknown); var second = client(invalid)) {
            assertThrows(PartDbClientException.OutcomeUnknown.class, () -> first.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
            assertThrows(PartDbClientException.InvalidRequest.class, () -> second.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }
    }

    @Test
    void reportsUnavailableWhenNoLeaderAppearsBeforeTheDeadline() throws IOException {
        var puts = new AtomicInteger();
        String node = start(new FakeNode("n1", _ -> failure(Code.UNAVAILABLE, "NOT_LEADER", Map.of()), puts));

        try (var client = PartDbClient.builder().endpoints(node).requestTimeout(Duration.ofMillis(300)).build()) {
            assertThrows(PartDbClientException.Unavailable.class, () -> client.kv().put(Bytes.utf8("k"), Bytes.utf8("v")));
        }
    }

    @Test
    void readsAMissingKeyAsEmpty() throws IOException {
        String node = start(new FakeNode("n1", _ -> PutResponse.getDefaultInstance(), new AtomicInteger()));

        try (var client = client(node)) {
            assertEquals(java.util.Optional.empty(), client.kv().get(Bytes.utf8("missing")));
        }
    }
}
