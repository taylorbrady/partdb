package io.partdb.server.raft;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftException;
import io.partdb.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class WriteAheadLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final long DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024;

    private final Path directory;
    private final long segmentSize;
    private final List<SealedSegment> sealed;
    private final Map<Long, EntryLocation> index;

    private ActiveSegment active;
    private HardState hardState;
    private long firstIndex;
    private long lastIndex;
    private int nextSequence;

    private WriteAheadLog(Path directory, long segmentSize) {
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.sealed = new ArrayList<>();
        this.index = new HashMap<>();
        this.hardState = HardState.INITIAL;
        this.firstIndex = 1;
        this.lastIndex = 0;
        this.nextSequence = 0;
    }

    public static WriteAheadLog create(Path directory) {
        return create(directory, DEFAULT_SEGMENT_SIZE);
    }

    public static WriteAheadLog create(Path directory, long segmentSize) {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to create WAL directory: " + directory, e);
        }

        WriteAheadLog wal = new WriteAheadLog(directory, segmentSize);
        wal.active = ActiveSegment.create(directory, 0, 1);
        wal.nextSequence = 1;
        return wal;
    }

    public static WriteAheadLog open(Path directory) {
        return open(directory, DEFAULT_SEGMENT_SIZE);
    }

    public static WriteAheadLog open(Path directory, long segmentSize) {
        WriteAheadLog wal = new WriteAheadLog(directory, segmentSize);
        wal.recover();
        return wal;
    }

    public void append(HardState newHardState, List<LogEntry> entries) {
        if (newHardState != null) {
            active.append(new LogRecord.State(newHardState));
            hardState = newHardState;
        }

        int activeSegmentIndex = sealed.size();

        for (LogEntry entry : entries) {
            maybeRollSegment();

            long offset = active.fileSize();
            active.append(new LogRecord.Entry(entry));
            index.put(entry.index(), new EntryLocation(activeSegmentIndex, offset));
            lastIndex = entry.index();

            if (firstIndex == 1 && lastIndex == 1) {
                firstIndex = 1;
            }
        }
    }

    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        if (fromIndex < firstIndex) {
            throw new RaftException.Compaction(fromIndex, firstIndex);
        }
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;

        for (long i = fromIndex; i < toIndex; i++) {
            EntryLocation loc = index.get(i);
            if (loc == null) {
                break;
            }

            LogEntry entry = readEntry(loc);
            int entrySize = LogCodec.entrySize(entry);

            if (!result.isEmpty() && bytes + entrySize > maxBytes) {
                break;
            }

            result.add(entry);
            bytes += entrySize;
        }

        return result;
    }

    public long term(long idx) {
        if (idx > 0 && idx < firstIndex) {
            throw new RaftException.Compaction(idx, firstIndex);
        }
        EntryLocation loc = index.get(idx);
        if (loc == null) {
            return 0;
        }
        LogEntry entry = readEntry(loc);
        return entry.term();
    }

    public HardState hardState() {
        return hardState;
    }

    public long firstIndex() {
        return firstIndex;
    }

    public long lastIndex() {
        return lastIndex;
    }

    public void sync() {
        active.sync();
    }

    public void recordSnapshotMarker(long idx, long term) {
        active.append(new LogRecord.SnapshotMarker(idx, term));
    }

    public void truncateBefore(long idx) {
        List<SealedSegment> toRemove = new ArrayList<>();

        for (SealedSegment segment : sealed) {
            if (segment.lastIndex() < idx) {
                toRemove.add(segment);
            }
        }

        for (SealedSegment segment : toRemove) {
            sealed.remove(segment);
            try {
                segment.close();
                Files.deleteIfExists(segment.path());
            } catch (IOException e) {
                throw new StorageException.IO("Failed to delete WAL segment: " + segment.path(), e);
            }

            for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
                index.remove(i);
            }
        }

        if (!sealed.isEmpty()) {
            firstIndex = sealed.getFirst().firstIndex();
        } else {
            firstIndex = idx;
        }
    }

    @Override
    public void close() {
        for (SealedSegment segment : sealed) {
            segment.close();
        }
        sealed.clear();

        if (active != null) {
            active.close();
            active = null;
        }
    }

    private void recover() {
        List<Path> segmentFiles = listSegmentFiles();

        if (segmentFiles.isEmpty()) {
            active = ActiveSegment.create(directory, 0, 1);
            nextSequence = 1;
            return;
        }

        segmentFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (int i = 0; i < segmentFiles.size(); i++) {
            Path path = segmentFiles.get(i);
            LogSegment.SegmentInfo info = LogSegment.parseFileName(path.getFileName().toString());
            boolean isLast = (i == segmentFiles.size() - 1);

            if (isLast) {
                recoverLastSegment(path, info);
            } else {
                recoverSealedSegment(path, info);
            }
        }

        if (active == null) {
            active = ActiveSegment.create(directory, nextSequence, lastIndex + 1);
            nextSequence++;
        }

        if (!sealed.isEmpty()) {
            firstIndex = sealed.getFirst().firstIndex();
        } else if (active.firstIndex() > 0) {
            firstIndex = active.firstIndex();
        }
    }

    private void recoverSealedSegment(Path path, LogSegment.SegmentInfo info) {
        int segmentIndex = sealed.size();
        long[] lastIdx = {info.firstIndex() - 1};

        SegmentScanner.ScanResult result = SegmentScanner.scan(path, scanned -> {
            switch (scanned.record()) {
                case LogRecord.Entry(LogEntry entry) -> {
                    index.put(entry.index(), new EntryLocation(segmentIndex, scanned.offset()));
                    lastIdx[0] = entry.index();
                    lastIndex = entry.index();
                }
                case LogRecord.State(HardState state) -> hardState = state;
                case LogRecord.SnapshotMarker _ -> {}
            }
        });

        switch (result) {
            case SegmentScanner.ScanResult.Incomplete incomplete ->
                    throw new StorageException.Corruption(
                            "Sealed segment " + path + " is corrupt: " + incomplete.reason() +
                            " at offset " + incomplete.problemOffset());
            case SegmentScanner.ScanResult.Complete _ -> {}
        }

        SealedSegment segment = SealedSegment.open(path, info.sequence(), info.firstIndex(), lastIdx[0]);
        sealed.add(segment);
        nextSequence = Math.max(nextSequence, info.sequence() + 1);
    }

    private void recoverLastSegment(Path path, LogSegment.SegmentInfo info) {
        int segmentIndex = sealed.size();
        long[] lastIdx = {info.firstIndex() - 1};
        HardState[] lastState = {hardState};

        SegmentScanner.ScanResult result = SegmentScanner.scan(path, scanned -> {
            switch (scanned.record()) {
                case LogRecord.Entry(LogEntry entry) -> {
                    index.put(entry.index(), new EntryLocation(segmentIndex, scanned.offset()));
                    lastIdx[0] = entry.index();
                    lastIndex = entry.index();
                }
                case LogRecord.State(HardState state) -> {
                    hardState = state;
                    lastState[0] = state;
                }
                case LogRecord.SnapshotMarker _ -> {}
            }
        });

        switch (result) {
            case SegmentScanner.ScanResult.Incomplete incomplete -> {
                log.warn("Repairing WAL segment {}: {} at offset {}. Truncating from {} to {} bytes.",
                        path, incomplete.reason(), incomplete.problemOffset(),
                        incomplete.originalFileSize(), incomplete.validEndOffset());
                SegmentScanner.repair(path, incomplete.validEndOffset());
            }
            case SegmentScanner.ScanResult.Complete _ -> {}
        }

        lastIndex = lastIdx[0];
        hardState = lastState[0];
        active = ActiveSegment.openForAppend(path, info.sequence(), info.firstIndex(), lastIndex);
        nextSequence = info.sequence() + 1;
    }

    private List<Path> listSegmentFiles() {
        try (Stream<Path> files = Files.list(directory)) {
            return files
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .toList();
        } catch (IOException e) {
            throw new StorageException.IO("Failed to list WAL segment files in: " + directory, e);
        }
    }

    private void maybeRollSegment() {
        if (active.fileSize() >= segmentSize) {
            rollSegment();
        }
    }

    private void rollSegment() {
        SealedSegment sealedSegment = active.seal();
        sealed.add(sealedSegment);

        active = ActiveSegment.create(directory, nextSequence, lastIndex + 1);
        nextSequence++;
    }

    private LogEntry readEntry(EntryLocation loc) {
        if (loc.segmentIndex() < sealed.size()) {
            return sealed.get(loc.segmentIndex()).readEntry(loc.offset());
        } else {
            return readFromActive(loc.offset());
        }
    }

    private LogEntry readFromActive(long offset) {
        active.flush();
        try (SealedSegment temp = SealedSegment.open(active.path(), active.sequence(),
                active.firstIndex(), active.lastIndex())) {
            return temp.readEntry(offset);
        }
    }

    public record EntryLocation(int segmentIndex, long offset) {}
}
