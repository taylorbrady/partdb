package io.partdb.raft.runtime;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.SnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

final class WriteAheadLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final String SUFFIX = ".wal";

    record Replay(HardState hardState, Optional<SnapshotMetadata> base, List<LogEntry> entries) {}

    private record Segment(Path path, long sequence, long maxIndex) {}

    private final Path directory;
    private final long segmentBytes;
    private final List<Segment> sealed = new ArrayList<>();
    private final Replay replay;
    private WalSegment active;
    private long activeSequence;
    private long activeMaxIndex;
    private HardState hardState;

    private WriteAheadLog(Path directory, long segmentBytes) {
        this.directory = directory;
        this.segmentBytes = segmentBytes;
        this.replay = recover();
    }

    static WriteAheadLog open(Path directory, long segmentBytes) {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to create WAL directory " + directory, e);
        }
        Fsync.directory(directory.getParent());
        return new WriteAheadLog(directory, segmentBytes);
    }

    Replay replay() {
        return replay;
    }

    void append(Optional<HardState> state, List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            active.append(new WalRecord.Entry(entry));
            activeMaxIndex = Math.max(activeMaxIndex, entry.index());
        }
        state.ifPresent(next -> {
            active.append(new WalRecord.State(next));
            hardState = next;
        });
        if (active.size() >= segmentBytes) {
            roll();
        }
    }

    void sync() {
        active.sync();
    }

    void reset(SnapshotMetadata base) {
        var previous = new ArrayList<>(sealed);
        previous.add(new Segment(active.path(), activeSequence, activeMaxIndex));
        active.close();
        openSegment(activeSequence + 1);
        active.append(new WalRecord.Reset(base));
        active.append(new WalRecord.State(hardState));
        active.sync();
        sealed.clear();
        previous.forEach(segment -> delete(segment.path()));
        Fsync.directory(directory);
    }

    void compact(SnapshotMetadata base) {
        active.append(new WalRecord.Compacted(base));
        active.sync();
        boolean deleted = false;
        var iterator = sealed.iterator();
        while (iterator.hasNext()) {
            var segment = iterator.next();
            if (segment.maxIndex() > base.index()) {
                break;
            }
            delete(segment.path());
            iterator.remove();
            deleted = true;
        }
        if (deleted) {
            Fsync.directory(directory);
        }
    }

    @Override
    public void close() {
        active.close();
    }

    private Replay recover() {
        var paths = segmentPaths();
        hardState = HardState.INITIAL;
        var entries = new ArrayList<LogEntry>();
        Optional<SnapshotMetadata> base = Optional.empty();
        int firstLive = 0;

        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            var scan = WalSegment.scan(path);
            boolean last = i == paths.size() - 1;
            if (!scan.complete() && !last) {
                throw new RaftStorageException.Corruption("WAL segment " + path + " is corrupt at byte " + scan.validBytes());
            }
            long maxIndex = 0;
            for (WalRecord record : scan.records()) {
                switch (record) {
                    case WalRecord.Entry(var entry) -> {
                        truncateFrom(entries, entry.index());
                        if (!entries.isEmpty() && entry.index() != entries.getLast().index() + 1) {
                            throw new RaftStorageException.Corruption("WAL entry " + entry.index() + " leaves a gap in " + path);
                        }
                        entries.add(entry);
                        maxIndex = Math.max(maxIndex, entry.index());
                    }
                    case WalRecord.State(var state) -> hardState = state;
                    case WalRecord.Reset(var reset) -> {
                        entries.clear();
                        base = Optional.of(reset);
                        firstLive = i;
                        sealed.clear();
                    }
                    case WalRecord.Compacted(var compacted) -> base = Optional.of(compacted);
                }
            }
            long sequence = sequenceOf(path);
            if (last) {
                if (!scan.complete()) {
                    log.atWarn().addKeyValue("segment", path).addKeyValue("validBytes", scan.validBytes())
                        .log("Truncating torn WAL tail");
                }
                active = WalSegment.reopen(path, scan.validBytes());
                activeSequence = sequence;
                activeMaxIndex = maxIndex;
            } else if (i >= firstLive) {
                sealed.add(new Segment(path, sequence, maxIndex));
            }
        }
        for (int i = 0; i < firstLive; i++) {
            delete(paths.get(i));
        }
        if (active == null) {
            openSegment(0);
        } else if (firstLive > 0) {
            Fsync.directory(directory);
        }
        return new Replay(hardState, base, List.copyOf(entries));
    }

    private static void truncateFrom(List<LogEntry> entries, long index) {
        while (!entries.isEmpty() && entries.getLast().index() >= index) {
            entries.removeLast();
        }
    }

    private void roll() {
        sealed.add(new Segment(active.path(), activeSequence, activeMaxIndex));
        active.close();
        openSegment(activeSequence + 1);
        active.append(new WalRecord.State(hardState));
    }

    private void openSegment(long sequence) {
        active = WalSegment.create(directory.resolve("%016x%s".formatted(sequence, SUFFIX)));
        Fsync.directory(directory);
        activeSequence = sequence;
        activeMaxIndex = 0;
    }

    private List<Path> segmentPaths() {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(SUFFIX)).sorted().toList();
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to list WAL segments in " + directory, e);
        }
    }

    private static long sequenceOf(Path path) {
        String name = path.getFileName().toString();
        return Long.parseUnsignedLong(name.substring(0, name.length() - SUFFIX.length()), 16);
    }

    private static void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to delete WAL segment " + path, e);
        }
    }
}
