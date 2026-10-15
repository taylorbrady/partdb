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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

final class Snapshots {

    private static final Logger log = LoggerFactory.getLogger(Snapshots.class);

    private record Task(Path payload, CompletableFuture<Void> done) {}

    private record Staged(SnapshotInfo info, Path payload) {}

    private final NodeId self;
    private final LogStore storage;
    private final StateMachine<?> stateMachine;
    private final RaftTransport transport;
    private final SnapshotPolicy policy;
    private final Executor executor;
    private final Consumer<LoopEvent> events;
    private final Map<Long, Staged> staged = new HashMap<>();
    private final Set<NodeId> sending = new HashSet<>();
    private long snapshotIndex;
    private Task task;

    Snapshots(
        NodeId self,
        LogStore storage,
        StateMachine<?> stateMachine,
        RaftTransport transport,
        SnapshotPolicy policy,
        Executor executor,
        Consumer<LoopEvent> events
    ) {
        this.self = self;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.policy = policy;
        this.executor = executor;
        this.events = events;
        this.snapshotIndex = storage.firstIndex() - 1;
    }

    long snapshotIndex() {
        return snapshotIndex;
    }

    void maybeCreate(long applied) {
        if (task != null || !policy.due(applied - snapshotIndex, storage.retainedBytes())) {
            return;
        }
        Path payload = storage.stagingFile();
        var done = new CompletableFuture<Void>();
        task = new Task(payload, done);
        executor.execute(() -> {
            try {
                var crc = new CRC32C();
                long index = write(payload, crc);
                events.accept(new LoopEvent.SnapshotCreated(payload, index, (int) crc.getValue(), null));
            } catch (RuntimeException e) {
                events.accept(new LoopEvent.SnapshotCreated(payload, 0, 0, e));
            } finally {
                done.complete(null);
            }
        });
    }

    void created(LoopEvent.SnapshotCreated created, Raft raft) {
        if (task != null && task.payload().equals(created.payload())) {
            task = null;
        }
        if (created.failure() != null) {
            log.atWarn().addKeyValue("nodeId", self).setCause(created.failure()).log("Snapshot failed");
            deleteQuietly(created.payload());
            return;
        }
        long index = created.index();
        if (index <= snapshotIndex) {
            deleteQuietly(created.payload());
            return;
        }
        var metadata = new SnapshotMetadata(index, storage.term(index), raft.membershipAt(index));
        storage.compact(metadata, created.payload(), created.checksum());
        snapshotIndex = index;
        log.atDebug().addKeyValue("nodeId", self).addKeyValue("index", index).log("Compacted raft log");
    }

    void send(NodeId peer, long term) {
        var snapshot = storage.snapshot().orElseThrow(() -> new IllegalStateException("raft requested a snapshot that does not exist"));
        if (!sending.add(peer)) {
            return;
        }
        var lease = storage.lease(snapshot);
        transport.sendSnapshot(peer, term, lease).whenComplete((_, error) -> {
            lease.close();
            if (error != null) {
                log.atDebug().addKeyValue("nodeId", self).addKeyValue("peer", peer).setCause(error).log("Snapshot transfer failed");
            }
            events.accept(new LoopEvent.SnapshotDelivered(peer, error == null));
        });
    }

    void delivered(NodeId peer) {
        sending.remove(peer);
    }

    RaftTransport.SnapshotSink stage(NodeId from, long term, SnapshotInfo info) throws IOException {
        return new Sink(from, term, info, storage.stagingFile());
    }

    void received(LoopEvent.SnapshotReceived received) {
        var previous = staged.put(received.info().metadata().index(), new Staged(received.info(), received.payload()));
        if (previous != null) {
            deleteQuietly(previous.payload());
        }
    }

    void install(SnapshotMetadata metadata) {
        var snapshot = staged.remove(metadata.index());
        if (snapshot == null) {
            throw new IllegalStateException("raft accepted snapshot " + metadata.index() + " without a payload");
        }
        if (task != null) {
            task.done().join();
        }
        storage.installSnapshot(metadata, snapshot.payload(), snapshot.info().checksum());
        restore(stateMachine, storage.snapshot().orElseThrow());
        snapshotIndex = metadata.index();
    }

    void discardStaged() {
        staged.values().forEach(snapshot -> deleteQuietly(snapshot.payload()));
        staged.clear();
    }

    static void restore(StateMachine<?> stateMachine, SnapshotStore.SnapshotFile snapshot) {
        var crc = new CRC32C();
        try (InputStream in = new CheckedInputStream(new BufferedInputStream(Files.newInputStream(snapshot.payload())), crc)) {
            stateMachine.restore(snapshot.metadata().index(), in);
            in.transferTo(OutputStream.nullOutputStream());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to restore snapshot " + snapshot.metadata().index(), e);
        }
        if ((int) crc.getValue() != snapshot.info().checksum()) {
            throw new RaftStorageException.Corruption("snapshot " + snapshot.payload() + " failed its checksum");
        }
    }

    static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.atWarn().addKeyValue("path", path).setCause(e).log("Failed to delete snapshot file");
        }
    }

    private long write(Path payload, CRC32C crc) {
        try (FileChannel channel = FileChannel.open(payload, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            OutputStream out = new BufferedOutputStream(new CheckedOutputStream(Channels.newOutputStream(channel), crc));
            long index = stateMachine.snapshot(out);
            out.flush();
            channel.force(true);
            return index;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write snapshot " + payload, e);
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
                deleteQuietly(path);
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
            deleteQuietly(path);
        }
    }
}
