package io.partdb.storage.sstable;

import io.partdb.common.exception.StorageException;

public final class SSTableException extends StorageException {

    public SSTableException(String message) {
        super(message);
    }

    public SSTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
