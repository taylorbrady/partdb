package io.partdb.storage.sstable;

public record SSTableHeader(int magic, int version) {

    public static final int MAGIC_NUMBER = 0x53535442;
    public static final int CURRENT_VERSION = 1;
    public static final int HEADER_SIZE = 12;

    public SSTableHeader {
        if (magic != MAGIC_NUMBER) {
            throw new SSTableException("Invalid SSTable magic number: 0x%08X".formatted(magic));
        }
        if (version != CURRENT_VERSION) {
            throw new SSTableException("Unsupported SSTable version: " + version);
        }
    }

}
