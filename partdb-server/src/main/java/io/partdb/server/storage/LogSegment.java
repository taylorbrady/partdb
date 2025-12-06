package io.partdb.server.storage;

import java.nio.file.Path;

public sealed interface LogSegment extends AutoCloseable permits ActiveSegment, SealedSegment {

    int RECORD_HEADER_SIZE = 9;

    Path path();

    int sequence();

    long firstIndex();

    long lastIndex();

    long fileSize();

    @Override
    void close();

    static String formatFileName(int sequence, long firstIndex) {
        return String.format("%016x-%016x.log", sequence, firstIndex);
    }

    static SegmentInfo parseFileName(String fileName) {
        if (!fileName.endsWith(".log") || fileName.length() != 37) {
            throw new IllegalArgumentException("Invalid segment file name: " + fileName);
        }
        int sequence = Integer.parseUnsignedInt(fileName.substring(0, 16), 16);
        long firstIndex = Long.parseUnsignedLong(fileName.substring(17, 33), 16);
        return new SegmentInfo(sequence, firstIndex);
    }

    record SegmentInfo(int sequence, long firstIndex) {}
}
