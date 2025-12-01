package io.partdb.raft;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedCollection;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class SegmentedRaftLog implements RaftLog {

    private static final Pattern SEGMENT_PATTERN = Pattern.compile("(\\d{16})\\.raftlog");
    private static final String SEGMENT_FORMAT = "%016d.raftlog";

    private final RaftLogConfig config;
    private final ReentrantLock lock = new ReentrantLock();
    private final ConcurrentSkipListMap<Long, SealedSegment> sealedSegments;

    private ActiveSegment activeSegment;
    private long nextSegmentId;

    public static SegmentedRaftLog open(RaftLogConfig config) {
        try {
            Files.createDirectories(config.logDirectory());

            ConcurrentSkipListMap<Long, SealedSegment> sealedSegments = new ConcurrentSkipListMap<>();
            long maxSegmentId = 0;

            List<Path> segmentPaths = List.of();
            if (Files.exists(config.logDirectory())) {
                try (Stream<Path> paths = Files.list(config.logDirectory())) {
                    segmentPaths = paths
                        .filter(path -> SEGMENT_PATTERN.matcher(path.getFileName().toString()).matches())
                        .sorted(Comparator.comparingLong(SegmentedRaftLog::extractSegmentId))
                        .toList();
                }
            }

            ActiveSegment activeSegment;
            long nextSegmentId;

            if (segmentPaths.isEmpty()) {
                nextSegmentId = 1;
                long firstIndex = 1;
                Path segmentPath = config.logDirectory().resolve(String.format(SEGMENT_FORMAT, nextSegmentId));
                activeSegment = ActiveSegment.create(segmentPath, nextSegmentId, firstIndex);
                nextSegmentId++;
            } else {
                for (int i = 0; i < segmentPaths.size() - 1; i++) {
                    Path path = segmentPaths.get(i);
                    long segmentId = extractSegmentId(path);
                    maxSegmentId = Math.max(maxSegmentId, segmentId);

                    SealedSegment segment = SealedSegment.open(path, segmentId);
                    if (segment.firstIndex() >= 0) {
                        sealedSegments.put(segment.firstIndex(), segment);
                    }
                }

                Path lastPath = segmentPaths.getLast();
                long lastSegmentId = extractSegmentId(lastPath);
                maxSegmentId = Math.max(maxSegmentId, lastSegmentId);

                activeSegment = ActiveSegment.open(lastPath, lastSegmentId);
                nextSegmentId = maxSegmentId + 1;
            }

            return new SegmentedRaftLog(config, sealedSegments, activeSegment, nextSegmentId);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to open Raft log", e);
        }
    }

    private SegmentedRaftLog(
        RaftLogConfig config,
        ConcurrentSkipListMap<Long, SealedSegment> sealedSegments,
        ActiveSegment activeSegment,
        long nextSegmentId
    ) {
        this.config = config;
        this.sealedSegments = sealedSegments;
        this.activeSegment = activeSegment;
        this.nextSegmentId = nextSegmentId;
    }

    @Override
    public long append(LogEntry entry) {
        lock.lock();
        try {
            activeSegment.append(entry);

            if (activeSegment.size() >= config.segmentSize()) {
                rotateSegment();
            }

            return entry.index();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<LogEntry> get(long index) {
        if (activeSegment != null &&
            index >= activeSegment.firstIndex() &&
            index <= activeSegment.lastIndex()) {
            return activeSegment.get(index);
        }

        Map.Entry<Long, SealedSegment> entry = sealedSegments.floorEntry(index);
        if (entry == null) {
            return Optional.empty();
        }

        SealedSegment segment = entry.getValue();
        if (index > segment.lastIndex()) {
            return Optional.empty();
        }

        return segment.get(index);
    }

    @Override
    public SequencedCollection<LogEntry> getRange(long startIndex, long endIndex) {
        LinkedHashMap<Long, LogEntry> result = new LinkedHashMap<>();

        for (SealedSegment segment : sealedSegments.values()) {
            if (segment.firstIndex() > endIndex) {
                break;
            }

            if (segment.lastIndex() < startIndex) {
                continue;
            }

            segment.getRange(startIndex, endIndex)
                .forEach(entry -> result.put(entry.index(), entry));
        }

        if (activeSegment != null &&
            activeSegment.lastIndex() >= startIndex &&
            activeSegment.firstIndex() <= endIndex) {
            activeSegment.getRange(startIndex, endIndex)
                .forEach(entry -> result.put(entry.index(), entry));
        }

        return result.sequencedValues();
    }

    @Override
    public long firstIndex() {
        if (!sealedSegments.isEmpty()) {
            return sealedSegments.firstKey();
        }
        if (activeSegment != null && activeSegment.firstIndex() >= 0) {
            return activeSegment.firstIndex();
        }
        return 1;
    }

    @Override
    public long lastIndex() {
        if (activeSegment == null) {
            return 0;
        }
        return activeSegment.lastIndex();
    }

    @Override
    public long lastTerm() {
        long index = lastIndex();
        if (index == 0) {
            return 0;
        }

        return get(index).map(LogEntry::term).orElse(0L);
    }

    @Override
    public long sizeInBytes() {
        long size = sealedSegments.values().stream()
            .mapToLong(SealedSegment::size)
            .sum();

        if (activeSegment != null) {
            size += activeSegment.size();
        }

        return size;
    }

    @Override
    public void truncateAfter(long index) {
        lock.lock();
        try {
            List<Long> toRemove = new ArrayList<>();

            for (Map.Entry<Long, SealedSegment> segmentEntry : sealedSegments.entrySet()) {
                SealedSegment segment = segmentEntry.getValue();

                if (segment.firstIndex() > index) {
                    toRemove.add(segmentEntry.getKey());
                    segment.close();
                    try {
                        Files.deleteIfExists(segment.path());
                    } catch (IOException e) {
                        throw new RaftException.LogException("Failed to delete segment", e);
                    }
                } else if (segment.lastIndex() > index) {
                    toRemove.add(segmentEntry.getKey());
                    SealedSegment newSegment = rebuildSegment(segment, index);
                    sealedSegments.put(newSegment.firstIndex(), newSegment);
                }
            }

            toRemove.forEach(sealedSegments::remove);

            if (activeSegment != null && activeSegment.firstIndex() > index) {
                activeSegment.close();
                try {
                    Files.deleteIfExists(activeSegment.path());
                } catch (IOException e) {
                    throw new RaftException.LogException("Failed to delete segment", e);
                }

                if (sealedSegments.isEmpty()) {
                    Path segmentPath = config.logDirectory().resolve(String.format(SEGMENT_FORMAT, nextSegmentId));
                    activeSegment = ActiveSegment.create(segmentPath, nextSegmentId, 1);
                    nextSegmentId++;
                } else {
                    SealedSegment lastSealed = sealedSegments.lastEntry().getValue();
                    sealedSegments.remove(lastSealed.firstIndex());
                    activeSegment = reopenAsActive(lastSealed);
                }
            } else if (activeSegment != null && activeSegment.lastIndex() > index) {
                activeSegment = rebuildActiveSegment(activeSegment, index);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteBefore(long index) {
        lock.lock();
        try {
            List<Long> toRemove = new ArrayList<>();

            for (Map.Entry<Long, SealedSegment> segmentEntry : sealedSegments.entrySet()) {
                SealedSegment segment = segmentEntry.getValue();

                if (segment.lastIndex() < index) {
                    toRemove.add(segmentEntry.getKey());
                    segment.close();
                    try {
                        Files.deleteIfExists(segment.path());
                    } catch (IOException e) {
                        throw new RaftException.LogException("Failed to delete segment", e);
                    }
                }
            }

            toRemove.forEach(sealedSegments::remove);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void sync() {
        lock.lock();
        try {
            if (activeSegment != null) {
                activeSegment.sync();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            for (SealedSegment segment : sealedSegments.values()) {
                segment.close();
            }
            sealedSegments.clear();

            if (activeSegment != null) {
                activeSegment.close();
                activeSegment = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private void rotateSegment() {
        SealedSegment sealed = activeSegment.seal();
        sealedSegments.put(sealed.firstIndex(), sealed);

        long firstIndex = sealed.lastIndex() + 1;
        Path segmentPath = config.logDirectory().resolve(String.format(SEGMENT_FORMAT, nextSegmentId));

        activeSegment = ActiveSegment.create(segmentPath, nextSegmentId, firstIndex);
        nextSegmentId++;
    }

    private SealedSegment rebuildSegment(SealedSegment old, long truncateAfterIndex) {
        long firstIdx = old.firstIndex();
        long segmentId = old.segmentId();
        Path path = old.path();

        List<LogEntry> entries = old.readAll();
        old.close();

        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to delete segment", e);
        }

        ActiveSegment temp = ActiveSegment.create(path, segmentId, firstIdx);
        for (LogEntry entry : entries) {
            if (entry.index() <= truncateAfterIndex) {
                temp.append(entry);
            }
        }

        return temp.seal();
    }

    private ActiveSegment rebuildActiveSegment(ActiveSegment old, long truncateAfterIndex) {
        long firstIdx = old.firstIndex();
        long segmentId = old.segmentId();
        Path path = old.path();

        List<LogEntry> entries = old.readAll();
        old.close();

        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to delete segment", e);
        }

        ActiveSegment newSegment = ActiveSegment.create(path, segmentId, firstIdx);
        for (LogEntry entry : entries) {
            if (entry.index() <= truncateAfterIndex) {
                newSegment.append(entry);
            }
        }

        return newSegment;
    }

    private ActiveSegment reopenAsActive(SealedSegment sealed) {
        Path path = sealed.path();
        long segmentId = sealed.segmentId();
        sealed.close();
        return ActiveSegment.open(path, segmentId);
    }

    private static long extractSegmentId(Path path) {
        Matcher matcher = SEGMENT_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid segment filename: " + path);
    }
}
