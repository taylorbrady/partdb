package io.partdb.storage;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.partdb.StorageCheckpoint")
@Label("PartDB Storage Checkpoint")
@Category({"PartDB", "Storage"})
final class StorageCheckpointEvent extends Event {
    @Label("Phase")
    String phase;

    @Label("Bytes")
    long bytes;

    @Label("Success")
    boolean success;

    @Label("Error")
    String error;
}
