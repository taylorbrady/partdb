package io.partdb.raft.runtime;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class Fsync {

    private static final boolean SUPPORTED = !System.getProperty("os.name", "").startsWith("Windows");

    private Fsync() {
    }

    static void directory(Path directory) {
        if (!SUPPORTED) {
            return;
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to sync directory " + directory, e);
        }
    }
}
