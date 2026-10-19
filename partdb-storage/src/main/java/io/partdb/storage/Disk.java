package io.partdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

final class Disk {

    static final String TEMPORARY_SUFFIX = ".tmp";

    private Disk() {
    }

    static void syncDirectory(Path directory) {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to sync directory " + directory, e);
        }
    }

    static void writeAtomically(Path target, byte[] contents) {
        Path temporary = target.resolveSibling(target.getFileName() + TEMPORARY_SUFFIX);
        try (var channel = FileChannel.open(
            temporary,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )) {
            var buffer = ByteBuffer.wrap(contents);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to write " + temporary, e);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to rename " + temporary + " to " + target, e);
        }
        syncDirectory(target.getParent());
    }

    static List<Path> list(Path directory) {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.toList();
        } catch (IOException e) {
            throw new StoreException.IO("Failed to list " + directory, e);
        }
    }

    static void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to delete " + path, e);
        }
    }
}
