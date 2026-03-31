package io.partdb.storage;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.partdb.StorageCompaction")
@Label("PartDB Storage Compaction")
@Category({"PartDB", "Storage"})
final class StorageCompactionEvent extends Event {
    @Label("Input Level")
    int inputLevel;

    @Label("Output Level")
    int outputLevel;

    @Label("Input Table Count")
    int inputTableCount;

    @Label("Output Table Count")
    int outputTableCount;

    @Label("Input Bytes")
    long inputBytes;

    @Label("Output Bytes")
    long outputBytes;

    @Label("Success")
    boolean success;

    @Label("Error")
    String error;
}
