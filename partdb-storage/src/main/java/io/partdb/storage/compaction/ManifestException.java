package io.partdb.storage.compaction;

public final class ManifestException extends RuntimeException {

    public ManifestException(String message) {
        super(message);
    }

    public ManifestException(String message, Throwable cause) {
        super(message, cause);
    }
}
