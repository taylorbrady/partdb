package io.partdb.raft;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class SegmentedRaftLog implements RaftLog {
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("(\\d{16})\\.raftlog");
    private static final String SEGMENT_FORMAT = "%016d.raftlog";

    private final RaftLogConfig config;
    private final Object lock = new Object();
    private final ConcurrentSkipListMap<Long, RaftLogSegment> segments;

    private RaftLogSegment activeSegment;
    private long nextSegmentId;

    private SegmentedRaftLog(
        RaftLogConfig config,
        ConcurrentSkipListMap<Long, RaftLogSegment> segments,
        RaftLogSegment activeSegment,
        long nextSegmentId
    ) {
        this.config = config;
        this.segments = segments;
        this.activeSegment = activeSegment;
        this.nextSegmentId = nextSegmentId;
    }

    public static SegmentedRaftLog open(RaftLogConfig config) {
        try {
            Files.createDirectories(config.logDirectory());

            ConcurrentSkipListMap<Long, RaftLogSegment> segments = new ConcurrentSkipListMap<>();
            long maxSegmentId = 0;

            if (Files.exists(config.logDirectory())) {
                try (Stream<Path> paths = Files.list(config.logDirectory())) {
                    List<Path> segmentPaths = paths
                        .filter(path -> SEGMENT_PATTERN.matcher(path.getFileName().toString()).matches())
                        .sorted(Comparator.comparingLong(SegmentedRaftLog::extractSegmentId))
                        .toList();

                    for (Path path : segmentPaths) {
                        long segmentId = extractSegmentId(path);
                        maxSegmentId = Math.max(maxSegmentId, segmentId);

                        RaftLogSegment segment = RaftLogSegment.open(path, segmentId);
                        if (segment.firstIndex() >= 0) {
                            segments.put(segment.firstIndex(), segment);
                        }
                    }
                }
            }

            RaftLogSegment activeSegment;
            long nextSegmentId = maxSegmentId + 1;

            if (segments.isEmpty()) {
                long firstIndex = 1;
                Path segmentPath = config.logDirectory().resolve(String.format(SEGMENT_FORMAT, nextSegmentId));
                activeSegment = RaftLogSegment.create(segmentPath, nextSegmentId, firstIndex);
                nextSegmentId++;
            } else {
                activeSegment = segments.lastEntry().getValue();
            }

            return new SegmentedRaftLog(config, segments, activeSegment, nextSegmentId);
        } catch (IOException e) {
            throw new RaftException.LogException("Failed to open Raft log", e);
        }
    }

    @Override
    public long append(LogEntry entry) {
        synchronized (lock) {
            activeSegment.append(entry);

            if (activeSegment.size() >= config.segmentSize()) {
                rotateSegment();
            }

            return entry.index();
        }
    }

    @Override
    public Optional<LogEntry> get(long index) {
        Map.Entry<Long, RaftLogSegment> entry = segments.floorEntry(index);

        if (entry == null) {
            return Optional.empty();
        }

        RaftLogSegment segment = entry.getValue();
        if (index > segment.lastIndex()) {
            return Optional.empty();
        }

        return segment.get(index);
    }

    @Override
    public SequencedCollection<LogEntry> getRange(long startIndex, long endIndex) {
        LinkedHashMap<Long, LogEntry> result = new LinkedHashMap<>();

        for (RaftLogSegment segment : segments.values()) {
            if (segment.firstIndex() > endIndex) {
                break;
            }

            if (segment.lastIndex() < startIndex) {
                continue;
            }

            segment.getRange(startIndex, endIndex)
                   .forEach(entry -> result.put(entry.index(), entry));
        }

        return result.sequencedValues();
    }

    @Override
    public long firstIndex() {
        return segments.isEmpty() ? 1 : segments.firstKey();
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
        return segments.values().stream()
            .mapToLong(RaftLogSegment::size)
            .sum();
    }

    @Override
    public void truncateAfter(long index) {
        synchronized (lock) {
            List<Long> toRemove = new ArrayList<>();

            for (Map.Entry<Long, RaftLogSegment> segmentEntry : segments.entrySet()) {
                RaftLogSegment segment = segmentEntry.getValue();

                if (segment.firstIndex() > index) {
                    toRemove.add(segmentEntry.getKey());
                    segment.close();
                    try {
                        Files.deleteIfExists(segment.path());
                    } catch (IOException e) {
                        throw new RaftException.LogException("Failed to delete segment", e);
                    }
                } else if (segment.lastIndex() > index) {
                    long firstIndex = segment.firstIndex();
                    long segmentId = segment.segmentId();
                    Path path = segment.path();

                    segment.close();

                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RaftException.LogException("Failed to delete segment", e);
                    }

                    RaftLogSegment newSegment = RaftLogSegment.create(path, segmentId, firstIndex);

                    List<LogEntry> entries = segment.readAll();
                    for (LogEntry entry : entries) {
                        if (entry.index() <= index) {
                            newSegment.append(entry);
                        }
                    }

                    segments.put(firstIndex, newSegment);

                    if (segment == activeSegment) {
                        activeSegment = newSegment;
                    }
                }
            }

            toRemove.forEach(segments::remove);
        }
    }

    @Override
    public void deleteBefore(long index) {
        synchronized (lock) {
            List<Long> toRemove = new ArrayList<>();

            for (Map.Entry<Long, RaftLogSegment> segmentEntry : segments.entrySet()) {
                RaftLogSegment segment = segmentEntry.getValue();

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

            toRemove.forEach(segments::remove);
        }
    }

    @Override
    public void sync() {
        synchronized (lock) {
            if (activeSegment != null) {
                activeSegment.sync();
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            for (RaftLogSegment segment : segments.values()) {
                segment.close();
            }
            segments.clear();
            activeSegment = null;
        }
    }

    private void rotateSegment() {
        activeSegment.seal();

        long firstIndex = activeSegment.lastIndex() + 1;
        Path segmentPath = config.logDirectory().resolve(String.format(SEGMENT_FORMAT, nextSegmentId));

        activeSegment = RaftLogSegment.create(segmentPath, nextSegmentId, firstIndex);
        segments.put(firstIndex, activeSegment);
        nextSegmentId++;
    }

    private static long extractSegmentId(Path path) {
        Matcher matcher = SEGMENT_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid segment filename: " + path);
    }
}
