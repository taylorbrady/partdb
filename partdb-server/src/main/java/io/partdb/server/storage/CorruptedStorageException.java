package io.partdb.server.storage;

public class CorruptedStorageException extends RuntimeException {

    public CorruptedStorageException(String message) {
        super(message);
    }
}
