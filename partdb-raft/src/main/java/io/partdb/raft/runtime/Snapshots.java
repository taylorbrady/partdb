package io.partdb.raft.runtime;

import io.partdb.raft.NodeId;
import io.partdb.raft.Raft;
import io.partdb.raft.SnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.CheckedOutputStream;

final class Snapshots {

    private static final Logger log = LoggerFactory.getLogger(Snapshots.class);

    private record Staged(SnapshotInfo info, Path payload) {}

    private static final class Outgoing {
        private final Path payload;
        private final SnapshotInfo info;
        private int transfers;

        private Outgoing(Path payload, SnapshotInfo info) {
            this.payload = payload;
            this.info = info;
        }
    }

    private final NodeId self;
    private final LogStore storage;
    private final SnapshotFiles files;
    private final StateMachine<?> stateMachine;
    private final RaftTransport transport;
    private final Executor executor;
    private final Consumer<LoopEvent> events;
    private final Map<Long, Staged> staged = new HashMap<>();
    private final Map<NodeId, Long> waiting = new LinkedHashMap<>();
    private final Map<NodeId, Outgoing> sending = new HashMap<>();
    private Outgoing current;
    private CompletableFuture<Void> generation;

    Snapshots(
        NodeId self,
        LogStore storage,
        SnapshotFiles files,
        StateMachine<?> stateMachine,
        RaftTransport transport,
        Executor executor,
        Consumer<LoopEvent> events
    ) {
        this.self = self;
        this.storage = storage;
        this.files = files;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.executor = executor;
        this.events = events;
    }

    static void recover(StateMachine<?> stateMachine, LogStore storage, SnapshotFiles files) {
        long base = storage.firstIndex() - 1;
        long applied = stateMachine.appliedIndex();
        if (applied < base) {
            Path installing = files.installing(base).orElseThrow(() -> new RaftStorageException.Corruption(
                "state machine at " + applied + " is behind the raft log, which starts after " + base));
            restore(stateMachine, installing, base);
        }
        files.clearInstalls();
    }

    void send(NodeId peer, long term) {
        if (sending.containsKey(peer) || waiting.containsKey(peer)) {
            return;
        }
        if (current != null && current.info.metadata().index() >= storage.firstIndex() - 1) {
            transfer(peer, term, current);
            return;
        }
        waiting.put(peer, term);
        if (generation == null) {
            generate();
        }
    }

    void generated(LoopEvent.SnapshotGenerated generated, Raft raft) {
        generation = null;
        long index = generated.index();
        if (generated.failure() != null || index < storage.firstIndex() - 1 || index < 1 || index > storage.lastIndex()) {
            log.atWarn().addKeyValue("nodeId", self).addKeyValue("index", index).setCause(generated.failure())
                .log("Discarding a snapshot that cannot be sent");
            SnapshotFiles.deleteQuietly(generated.payload());
            waiting.keySet().forEach(peer -> events.accept(new LoopEvent.SnapshotDelivered(peer, false)));
            waiting.clear();
            return;
        }
        var metadata = new SnapshotMetadata(index, storage.term(index), raft.membershipAt(index));
        var fresh = new Outgoing(generated.payload(), new SnapshotInfo(metadata, generated.size(), generated.checksum()));
        current = fresh;
        waiting.forEach((peer, term) -> transfer(peer, term, fresh));
        waiting.clear();
        release(fresh);
    }

    void delivered(NodeId peer) {
        var outgoing = sending.remove(peer);
        if (outgoing != null) {
            outgoing.transfers--;
            release(outgoing);
        }
    }

    RaftTransport.SnapshotSink stage(NodeId from, long term, SnapshotInfo info) throws IOException {
        return new Sink(from, term, info, files.stagingFile("incoming"));
    }

    void received(LoopEvent.SnapshotReceived received) {
        var previous = staged.put(received.info().metadata().index(), new Staged(received.info(), received.payload()));
        if (previous != null) {
            SnapshotFiles.deleteQuietly(previous.payload());
        }
    }

    void install(SnapshotMetadata metadata) {
        var snapshot = staged.remove(metadata.index());
        if (snapshot == null) {
            throw new IllegalStateException("raft accepted snapshot " + metadata.index() + " without a payload");
        }
        if (generation != null) {
            generation.join();
        }
        Path installing = files.promote(snapshot.payload(), metadata.index());
        storage.reset(metadata);
        restore(stateMachine, installing, metadata.index());
        SnapshotFiles.deleteQuietly(installing);
    }

    void discardStaged() {
        staged.values().forEach(snapshot -> SnapshotFiles.deleteQuietly(snapshot.payload()));
        staged.clear();
    }

    void close() {
        discardStaged();
        sending.values().forEach(outgoing -> SnapshotFiles.deleteQuietly(outgoing.payload));
        if (current != null) {
            SnapshotFiles.deleteQuietly(current.payload);
        }
    }

    private void generate() {
        Path payload = files.stagingFile("outgoing");
        var done = new CompletableFuture<Void>();
        generation = done;
        executor.execute(() -> {
            try (FileChannel channel = FileChannel.open(payload, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                var crc = new CRC32C();
                OutputStream out = new BufferedOutputStream(new CheckedOutputStream(Channels.newOutputStream(channel), crc));
                long index = stateMachine.snapshot(out);
                out.flush();
                events.accept(new LoopEvent.SnapshotGenerated(payload, index, channel.size(), (int) crc.getValue(), null));
            } catch (IOException | RuntimeException e) {
                events.accept(new LoopEvent.SnapshotGenerated(payload, 0, 0, 0, e));
            } finally {
                done.complete(null);
            }
        });
    }

    private void transfer(NodeId peer, long term, Outgoing outgoing) {
        FileChannel channel;
        try {
            channel = FileChannel.open(outgoing.payload, StandardOpenOption.READ);
        } catch (IOException e) {
            log.atWarn().addKeyValue("nodeId", self).addKeyValue("peer", peer).setCause(e).log("Failed to open snapshot for transfer");
            events.accept(new LoopEvent.SnapshotDelivered(peer, false));
            return;
        }
        outgoing.transfers++;
        sending.put(peer, outgoing);
        var source = new Source(outgoing.info, channel);
        transport.sendSnapshot(peer, term, source).whenComplete((_, error) -> {
            source.close();
            if (error != null) {
                log.atDebug().addKeyValue("nodeId", self).addKeyValue("peer", peer).setCause(error).log("Snapshot transfer failed");
            }
            events.accept(new LoopEvent.SnapshotDelivered(peer, error == null));
        });
    }

    private void release(Outgoing outgoing) {
        if (outgoing.transfers > 0) {
            return;
        }
        SnapshotFiles.deleteQuietly(outgoing.payload);
        if (current == outgoing) {
            current = null;
        }
    }

    private static void restore(StateMachine<?> stateMachine, Path payload, long index) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(payload))) {
            stateMachine.restore(index, in);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to restore snapshot " + index, e);
        }
    }

    private static final class Source implements RaftTransport.SnapshotSource {
        private final SnapshotInfo info;
        private final FileChannel channel;

        private Source(SnapshotInfo info, FileChannel channel) {
            this.info = info;
            this.channel = channel;
        }

        @Override
        public SnapshotInfo info() {
            return info;
        }

        @Override
        public FileChannel channel() {
            return channel;
        }

        @Override
        public void close() {
            try {
                channel.close();
            } catch (IOException e) {
                log.atDebug().setCause(e).log("Failed to close snapshot source");
            }
        }
    }

    private final class Sink implements RaftTransport.SnapshotSink {
        private final NodeId from;
        private final long term;
        private final SnapshotInfo info;
        private final Path path;
        private final FileChannel channel;
        private final CRC32C crc = new CRC32C();
        private long written;

        private Sink(NodeId from, long term, SnapshotInfo info, Path path) throws IOException {
            this.from = from;
            this.term = term;
            this.info = info;
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        @Override
        public void write(ByteBuffer chunk) throws IOException {
            crc.update(chunk.duplicate());
            written += chunk.remaining();
            if (written > info.size()) {
                throw new IOException("snapshot " + info.metadata().index() + " is larger than its declared " + info.size() + " bytes");
            }
            while (chunk.hasRemaining()) {
                channel.write(chunk);
            }
        }

        @Override
        public void complete() throws IOException {
            channel.force(true);
            channel.close();
            if (written != info.size() || (int) crc.getValue() != info.checksum()) {
                SnapshotFiles.deleteQuietly(path);
                throw new IOException("snapshot " + info.metadata().index() + " arrived corrupt or incomplete");
            }
            events.accept(new LoopEvent.SnapshotReceived(from, term, info, path));
        }

        @Override
        public void abort() {
            try {
                channel.close();
            } catch (IOException e) {
                log.atDebug().setCause(e).log("Failed to close staged snapshot");
            }
            SnapshotFiles.deleteQuietly(path);
        }
    }
}
