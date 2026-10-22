package io.partdb.raft.runtime;

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
            throw new RaftStorageException.IO("failed to create directory " + directory, e);
        }
    }

    static FileLock lock(Path directory) {
        Path path = directory.resolve(LOCK_FILE);
        FileChannel channel;
        FileLock lock;
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open " + path, e);
        }
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException _) {
            lock = null;
        } catch (IOException e) {
            close(channel);
            throw new RaftStorageException.IO("failed to lock " + path, e);
        }
        if (lock == null) {
            close(channel);
            throw new IllegalStateException("raft directory " + directory + " is already in use");
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
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to sync directory " + directory, e);
        }
    }

    static void writeAtomically(Path target, ByteBuffer contents) {
        Path temporary = target.resolveSibling(target.getFileName() + TEMPORARY_SUFFIX);
        try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (contents.hasRemaining()) {
                channel.write(contents);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to write " + temporary, e);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to rename " + temporary + " to " + target, e);
        }
        syncDirectory(target.getParent());
    }

    private static void close(Channel channel) {
        try {
            channel.close();
        } catch (IOException _) {
        }
    }
}
