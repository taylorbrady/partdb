package io.partdb.raft.runtime;

import io.partdb.raft.Envelope;
import io.partdb.raft.RaftMessage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

final class LocalNetwork {

    private final Map<String, RaftTransport.Receiver> receivers = new ConcurrentHashMap<>();
    private final Set<String> isolated = ConcurrentHashMap.newKeySet();

    RaftTransport transport(String self) {
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
            public CompletionStage<Void> sendSnapshot(String to, RaftMessage.InstallSnapshot request, Path payload) {
                var target = receivers.get(to);
                if (target == null || isolated.contains(self) || isolated.contains(to)) {
                    return CompletableFuture.failedFuture(new IOException("unreachable: " + to));
                }
                return CompletableFuture.runAsync(() -> {
                    try (FileChannel file = FileChannel.open(payload, StandardOpenOption.READ)) {
                        var receiver = target.receiveSnapshot(self, request);
                        var chunk = ByteBuffer.allocate(1024);
                        while (file.read(chunk.clear()) >= 0) {
                            receiver.write(chunk.flip());
                        }
                        receiver.complete();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }

            @Override
            public void close() {
                receivers.remove(self);
            }
        };
    }

    void isolate(String node) {
        isolated.add(node);
    }

    void heal(String node) {
        isolated.remove(node);
    }
}
