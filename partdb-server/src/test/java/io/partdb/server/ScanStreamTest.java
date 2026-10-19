package io.partdb.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;
import io.partdb.grpc.cluster.v1.GetStatusRequest;
import io.partdb.grpc.cluster.v1.RaftRole;
import io.partdb.grpc.kv.v1.KeyValueServiceGrpc;
import io.partdb.grpc.kv.v1.Mutation;
import io.partdb.grpc.kv.v1.ReadConsistency;
import io.partdb.grpc.kv.v1.ScanRequest;
import io.partdb.grpc.kv.v1.ScanResponse;
import io.partdb.grpc.kv.v1.TxnRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScanStreamTest {

    private static final int BATCHES = 40;
    private static final int KEYS_PER_BATCH = 100;

    @TempDir
    Path directory;

    @Test
    void scanResumesAfterTheClientFallsBehind() throws Exception {
        var config = ServerConfig.builder("node1", directory).clientPort(freePort()).adminPort(freePort()).build();
        try (var _ = PartDbServer.start(config)) {
            ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", config.clientPort()).usePlaintext().build();
            try {
                awaitLeader(channel);
                var kv = KeyValueServiceGrpc.newBlockingStub(channel);
                var value = ByteString.copyFrom(new byte[1024]);
                for (int batch = 0; batch < BATCHES; batch++) {
                    var txn = TxnRequest.newBuilder();
                    for (int i = 0; i < KEYS_PER_BATCH; i++) {
                        var key = ByteString.copyFromUtf8("key-%02d-%03d".formatted(batch, i));
                        txn.addMutations(Mutation.newBuilder().setPut(Mutation.Put.newBuilder().setKey(key).setValue(value)));
                    }
                    kv.withDeadlineAfter(10, TimeUnit.SECONDS).txn(txn.build());
                }

                Iterator<ScanResponse> responses = kv.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .scan(ScanRequest.newBuilder().setConsistency(ReadConsistency.READ_CONSISTENCY_LOCAL).build());
                Thread.sleep(500);
                int entries = 0;
                while (responses.hasNext()) {
                    entries += responses.next().getEntriesCount();
                }

                assertEquals(BATCHES * KEYS_PER_BATCH, entries);
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitLeader(ManagedChannel channel) throws InterruptedException {
        var cluster = ClusterServiceGrpc.newBlockingStub(channel).withWaitForReady();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (cluster.withDeadlineAfter(5, TimeUnit.SECONDS).getStatus(GetStatusRequest.getDefaultInstance()).getRole() != RaftRole.RAFT_ROLE_LEADER) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("node did not become leader");
            }
            Thread.sleep(25);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
