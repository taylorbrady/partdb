package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

record SSTableHeader(int magic, int version, byte codecId) {

    static final int MAGIC_NUMBER = 0x53535442;
    static final int CURRENT_VERSION = 1;
    static final int HEADER_SIZE = 13;

    SSTableHeader {
        if (magic != MAGIC_NUMBER) {
            throw new StorageException.Corruption("Invalid SSTable magic number: 0x%08X".formatted(magic));
        }
        if (version != CURRENT_VERSION) {
            throw new StorageException.Corruption("Unsupported SSTable version: " + version);
        }
    }
}
