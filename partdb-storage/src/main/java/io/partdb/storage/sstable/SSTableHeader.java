package io.partdb.storage.sstable;

record SSTableHeader(int magic, int version) {

    static final int MAGIC_NUMBER = 0x53535442;
    static final int CURRENT_VERSION = 2;
    static final int HEADER_SIZE = 12;

    SSTableHeader {
        if (magic != MAGIC_NUMBER) {
            throw new SSTableException("Invalid SSTable magic number: 0x%08X".formatted(magic));
        }
        if (version != CURRENT_VERSION) {
            throw new SSTableException("Unsupported SSTable version: " + version);
        }
    }
}
