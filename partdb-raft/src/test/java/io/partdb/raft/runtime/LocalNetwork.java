package io.partdb.raft.runtime;

import io.partdb.raft.Envelope;
import io.partdb.raft.NodeId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

final class LocalNetwork {

    private final Map<NodeId, RaftTransport.Receiver> receivers = new ConcurrentHashMap<>();
    private final Set<NodeId> isolated = ConcurrentHashMap.newKeySet();

    RaftTransport transport(NodeId self) {
        return new RaftTransport() {
            @Override
            public void start(Receiver receiver) {
                receivers.put(self, receiver);
            }

            @Override
            public void send(Envelope envelope) {
                var target = receivers.get(envelope.to());
                if (target == null || isolated.contains(self) || isolated.contains(envelope.to())) {
                    var sender = receivers.get(self);
                    if (sender != null) {
                        sender.unreachable(envelope.to());
                    }
                    return;
                }
                target.receive(envelope);
            }

            @Override
            public CompletionStage<Void> sendSnapshot(NodeId to, long term, SnapshotSource snapshot) {
                var target = receivers.get(to);
                if (target == null || isolated.contains(self) || isolated.contains(to)) {
                    return CompletableFuture.failedFuture(new IOException("unreachable: " + to));
                }
                return CompletableFuture.runAsync(() -> {
                    try {
                        var sink = target.receiveSnapshot(self, term, snapshot.info());
                        var chunk = ByteBuffer.allocate(1024);
                        long position = 0;
                        int read;
                        while ((read = snapshot.channel().read(chunk.clear(), position)) >= 0) {
                            position += read;
                            sink.write(chunk.flip());
                        }
                        sink.complete();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        };
    }

    void isolate(NodeId node) {
        isolated.add(node);
    }

    void heal(NodeId node) {
        isolated.remove(node);
    }
}
