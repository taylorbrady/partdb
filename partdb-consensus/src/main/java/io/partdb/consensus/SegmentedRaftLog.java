package io.partdb.consensus;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class SegmentedRaftLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SegmentedRaftLog.class);
    private static final long DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024;

    private final Path directory;
    private final long segmentSize;
    private final List<SealedSegment> sealed;
    private final Map<Long, EntryLocation> index;

    private ActiveSegment active;
    private RaftHardState hardState;
    private long firstIndex;
    private long lastIndex;
    private int nextSequence;

    private SegmentedRaftLog(Path directory, long segmentSize) {
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.sealed = new ArrayList<>();
        this.index = new HashMap<>();
        this.hardState = RaftHardState.INITIAL;
        this.firstIndex = 1;
        this.lastIndex = 0;
        this.nextSequence = 0;
    }

    public static SegmentedRaftLog create(Path directory) {
        return create(directory, DEFAULT_SEGMENT_SIZE);
    }

    public static SegmentedRaftLog create(Path directory, long segmentSize) {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new ConsensusStorageException.IO("Failed to create WAL directory: " + directory, e);
        }

        SegmentedRaftLog wal = new SegmentedRaftLog(directory, segmentSize);
        wal.active = ActiveSegment.create(directory, 0, 1);
        wal.nextSequence = 1;
        return wal;
    }

    public static SegmentedRaftLog open(Path directory) {
        return open(directory, DEFAULT_SEGMENT_SIZE);
    }

    public static SegmentedRaftLog open(Path directory, long segmentSize) {
        SegmentedRaftLog wal = new SegmentedRaftLog(directory, segmentSize);
        wal.recover();
        return wal;
    }

    public void append(RaftHardState newHardState, List<RaftLogEntry> entries) {
        if (!entries.isEmpty()) {
            truncateSuffixFrom(entries.getFirst().index());
        }

        if (newHardState != null) {
            active.append(new LogRecord.State(newHardState));
            hardState = newHardState;
        }

        for (RaftLogEntry entry : entries) {
            maybeRollSegment();

            long offset = active.fileSize();
            active.append(new LogRecord.Entry(entry));
            index.put(entry.index(), new EntryLocation(sealed.size(), offset));
            lastIndex = entry.index();

            if (firstIndex == 1 && lastIndex == 1) {
                firstIndex = 1;
            }
        }
    }

    public List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        if (fromIndex < firstIndex) {
            throw new ConsensusException.Compaction(fromIndex, firstIndex);
        }
        List<RaftLogEntry> result = new ArrayList<>();
        long bytes = 0;

        for (long i = fromIndex; i < toIndex; i++) {
            EntryLocation loc = index.get(i);
            if (loc == null) {
                break;
            }

            RaftLogEntry entry = readEntry(loc);
            int entrySize = LogRecordCodec.entrySize(entry);

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
            throw new ConsensusException.Compaction(idx, firstIndex);
        }
        EntryLocation loc = index.get(idx);
        if (loc == null) {
            return 0;
        }
        RaftLogEntry entry = readEntry(loc);
        return entry.term();
    }

    public RaftHardState hardState() {
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
                throw new ConsensusStorageException.IO("Failed to delete WAL segment: " + segment.path(), e);
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
                case LogRecord.Entry(RaftLogEntry entry) -> {
                    index.put(entry.index(), new EntryLocation(segmentIndex, scanned.offset()));
                    lastIdx[0] = entry.index();
                    lastIndex = entry.index();
                }
                case LogRecord.State(RaftHardState state) -> hardState = state;
                case LogRecord.SnapshotMarker _ -> {}
            }
        });

        switch (result) {
            case SegmentScanner.ScanResult.Incomplete incomplete ->
                    throw new ConsensusStorageException.Corruption(
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
        RaftHardState[] lastState = {hardState};

        SegmentScanner.ScanResult result = SegmentScanner.scan(path, scanned -> {
            switch (scanned.record()) {
                case LogRecord.Entry(RaftLogEntry entry) -> {
                    index.put(entry.index(), new EntryLocation(segmentIndex, scanned.offset()));
                    lastIdx[0] = entry.index();
                    lastIndex = entry.index();
                }
                case LogRecord.State(RaftHardState state) -> {
                    hardState = state;
                    lastState[0] = state;
                }
                case LogRecord.SnapshotMarker _ -> {}
            }
        });

        switch (result) {
            case SegmentScanner.ScanResult.Incomplete incomplete -> {
                log.atWarn()
                    .addKeyValue("path", path.toString())
                    .addKeyValue("reason", incomplete.reason())
                    .addKeyValue("problemOffset", incomplete.problemOffset())
                    .addKeyValue("originalSize", incomplete.originalFileSize())
                    .addKeyValue("validEndOffset", incomplete.validEndOffset())
                    .log("Repairing WAL segment");
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
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        } catch (IOException e) {
            throw new ConsensusStorageException.IO("Failed to list WAL segment files in: " + directory, e);
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

    private void truncateSuffixFrom(long firstReplacementIndex) {
        if (firstReplacementIndex > lastIndex) {
            return;
        }
        if (firstReplacementIndex < firstIndex) {
            throw new ConsensusException.Compaction(firstReplacementIndex, firstIndex);
        }

        EntryLocation location = index.get(firstReplacementIndex);
        if (location == null) {
            return;
        }

        removeIndexEntriesFrom(firstReplacementIndex);

        if (location.segmentIndex() < sealed.size()) {
            truncateSealedSuffix(location);
        } else {
            active.truncateTo(location.offset(), firstReplacementIndex - 1);
        }

        lastIndex = firstReplacementIndex - 1;
        if (lastIndex < firstIndex) {
            firstIndex = firstReplacementIndex;
        }
    }

    private void truncateSealedSuffix(EntryLocation location) {
        SealedSegment target = sealed.get(location.segmentIndex());
        for (int i = sealed.size() - 1; i > location.segmentIndex(); i--) {
            deleteSealedSegment(i);
        }

        active.close();
        deleteFile(active.path());

        sealed.remove(location.segmentIndex());
        target.close();

        truncateFile(target.path(), location.offset());
        active = ActiveSegment.openForAppend(
            target.path(),
            target.sequence(),
            target.firstIndex(),
            lastIndexAfterTruncatingSegment(target.firstIndex(), location.offset())
        );
    }

    private long lastIndexAfterTruncatingSegment(long segmentFirstIndex, long truncateOffset) {
        return truncateOffset == 0 ? segmentFirstIndex - 1 : lastIndexBeforeOffset(truncateOffset);
    }

    private long lastIndexBeforeOffset(long truncateOffset) {
        long result = firstIndex - 1;
        for (Map.Entry<Long, EntryLocation> entry : index.entrySet()) {
            EntryLocation location = entry.getValue();
            if (location.segmentIndex() <= sealed.size() && location.offset() < truncateOffset) {
                result = Math.max(result, entry.getKey());
            }
        }
        return result;
    }

    private void removeIndexEntriesFrom(long firstReplacementIndex) {
        index.keySet().removeIf(i -> i >= firstReplacementIndex);
    }

    private void deleteSealedSegment(int segmentIndex) {
        SealedSegment segment = sealed.remove(segmentIndex);
        try {
            segment.close();
            Files.deleteIfExists(segment.path());
        } catch (IOException e) {
            throw new ConsensusStorageException.IO("Failed to delete WAL segment: " + segment.path(), e);
        }
    }

    private void deleteFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new ConsensusStorageException.IO("Failed to delete WAL segment: " + path, e);
        }
    }

    private void truncateFile(Path path, long offset) {
        try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.truncate(offset);
            channel.force(true);
        } catch (IOException e) {
            throw new ConsensusStorageException.IO("Failed to truncate WAL segment: " + path, e);
        }
    }

    private RaftLogEntry readEntry(EntryLocation loc) {
        if (loc.segmentIndex() < sealed.size()) {
            return sealed.get(loc.segmentIndex()).readEntry(loc.offset());
        } else {
            return readFromActive(loc.offset());
        }
    }

    private RaftLogEntry readFromActive(long offset) {
        active.flush();
        try (SealedSegment temp = SealedSegment.open(active.path(), active.sequence(),
                active.firstIndex(), active.lastIndex())) {
            return temp.readEntry(offset);
        }
    }

    public record EntryLocation(int segmentIndex, long offset) {}
}
