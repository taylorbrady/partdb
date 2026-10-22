package io.partdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.stream.Stream;

final class Disk {

    static final String TEMPORARY_SUFFIX = ".tmp";

    private static final String LOCK_FILE = "LOCK";
    private static final boolean DIRECTORY_SYNC = !System.getProperty("os.name", "").startsWith("Windows");
    private static final boolean POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private Disk() {
    }

    static void createPrivateDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
            if (POSIX) {
                Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
            }
        } catch (IOException e) {
            throw new StoreException.IO("Failed to create " + directory, e);
        }
    }

    static FileLock lock(Path directory) {
        Path path = directory.resolve(LOCK_FILE);
        FileChannel channel;
        FileLock lock;
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to open " + path, e);
        }
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException _) {
            lock = null;
        } catch (IOException e) {
            close(channel);
            throw new StoreException.IO("Failed to lock " + path, e);
        }
        if (lock == null) {
            close(channel);
            throw new IllegalStateException("Store directory " + directory + " is already in use");
        }
        return lock;
    }

    static void unlock(FileLock lock) {
        close(lock.acquiredBy());
    }

    static void syncDirectory(Path directory) {
        if (!DIRECTORY_SYNC) {
            return;
        }
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

    private static void close(Channel channel) {
        try {
            channel.close();
        } catch (IOException _) {
        }
    }
}
