package io.partdb.server.storage;

import io.partdb.raft.Membership;
import io.partdb.raft.Snapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.partdb.server.storage.LogCodec.BYTE_ORDER;

public final class SnapshotStore implements AutoCloseable {

    private static final int MAGIC = 0x534E4150;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 4 + 4 + 8 + 8;

    private final Path directory;

    private SnapshotStore(Path directory) {
        this.directory = directory;
    }

    public static SnapshotStore open(Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SnapshotStore(directory);
    }

    public void save(Snapshot snapshot) {
        String fileName = formatFileName(snapshot.term(), snapshot.index());
        Path tempPath = directory.resolve(fileName + ".tmp");
        Path finalPath = directory.resolve(fileName);

        try (FileChannel channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            byte[] membershipBytes = encodeMembership(snapshot.membership());
            int membershipLen = membershipBytes.length;
            byte[] data = snapshot.data() != null ? snapshot.data() : new byte[0];

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE + 4 + membershipLen).order(BYTE_ORDER);
            header.putInt(MAGIC);
            header.putInt(VERSION);
            header.putLong(snapshot.term());
            header.putLong(snapshot.index());
            header.putInt(membershipLen);
            header.put(membershipBytes);
            header.flip();

            channel.write(header);
            channel.write(ByteBuffer.wrap(data));

            CRC32C crc = new CRC32C();
            header.rewind();
            crc.update(header);
            crc.update(data);

            ByteBuffer crcBuf = ByteBuffer.allocate(4).order(BYTE_ORDER);
            crcBuf.putInt((int) crc.getValue());
            crcBuf.flip();
            channel.write(crcBuf);

            channel.force(true);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Optional<Snapshot> latest() {
        List<SnapshotInfo> snapshots = list();
        if (snapshots.isEmpty()) {
            return Optional.empty();
        }

        SnapshotInfo latest = snapshots.getLast();
        return Optional.of(load(latest.path()));
    }

    public List<SnapshotInfo> list() {
        try (Stream<Path> files = Files.list(directory)) {
            return files
                    .filter(p -> p.getFileName().toString().endsWith(".snap"))
                    .map(p -> {
                        String name = p.getFileName().toString();
                        long term = Long.parseUnsignedLong(name.substring(0, 16), 16);
                        long index = Long.parseUnsignedLong(name.substring(17, 33), 16);
                        return new SnapshotInfo(term, index, p);
                    })
                    .sorted(Comparator.comparingLong(SnapshotInfo::index))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteOlderThan(long index) {
        for (SnapshotInfo info : list()) {
            if (info.index() < index) {
                try {
                    Files.deleteIfExists(info.path());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    @Override
    public void close() {
    }

    private Snapshot load(Path path) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE + 4).order(BYTE_ORDER);
            channel.read(headerBuf);
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != MAGIC) {
                throw new CorruptedStorageException("Invalid snapshot magic: " + Integer.toHexString(magic));
            }

            int version = headerBuf.getInt();
            if (version != VERSION) {
                throw new CorruptedStorageException("Unsupported snapshot version: " + version);
            }

            long term = headerBuf.getLong();
            long index = headerBuf.getLong();
            int membershipLen = headerBuf.getInt();

            ByteBuffer membershipBuf = ByteBuffer.allocate(membershipLen).order(BYTE_ORDER);
            channel.read(membershipBuf);
            membershipBuf.flip();
            Membership membership = LogCodec.readMembership(membershipBuf);

            int dataLen = (int) (fileSize - HEADER_SIZE - 4 - membershipLen - 4);
            byte[] data = new byte[dataLen];
            ByteBuffer dataBuf = ByteBuffer.wrap(data);
            channel.read(dataBuf);

            ByteBuffer crcBuf = ByteBuffer.allocate(4).order(BYTE_ORDER);
            channel.read(crcBuf);
            crcBuf.flip();
            int storedCrc = crcBuf.getInt();

            CRC32C crc = new CRC32C();
            headerBuf.rewind();
            crc.update(headerBuf);
            membershipBuf.rewind();
            crc.update(membershipBuf);
            crc.update(data);

            if ((int) crc.getValue() != storedCrc) {
                throw new CorruptedStorageException("Snapshot CRC mismatch");
            }

            return new Snapshot(index, term, membership, data);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] encodeMembership(Membership membership) {
        int size = LogCodec.membershipSize(membership);
        ByteBuffer buf = ByteBuffer.allocate(size).order(BYTE_ORDER);
        LogCodec.writeMembership(buf, membership);
        return buf.array();
    }

    private static String formatFileName(long term, long index) {
        return String.format("%016x-%016x.snap", term, index);
    }

    public record SnapshotInfo(long term, long index, Path path) {}
}
