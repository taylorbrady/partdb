package io.partdb.storage.internal;

import io.partdb.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

final class VersionCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(VersionCheckpoint.class);
    private static final int MAGIC = 0x53544350;
    private static final int VERSION = 2;
    private static final int COPY_BUFFER_SIZE = 64 * 1024;
    private static final String STAGED_SUFFIX = ".restore";

    private final SSTableManifest manifest;

    private VersionCheckpoint(SSTableManifest manifest) {
        this.manifest = manifest;
    }

    static long write(VersionLease view, OutputStream target) throws IOException {
        SSTableManifest manifest = view.manifest();
        var readersById = view.all().stream()
            .collect(Collectors.toMap(SSTableReader::id, Function.identity()));
        byte[] manifestBytes = manifest.toBytes();

        var out = new DataOutputStream(target);
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeInt(manifestBytes.length);
        out.write(manifestBytes);
        long written = 3L * Integer.BYTES + manifestBytes.length;

        WritableByteChannel channel = Channels.newChannel(out);
        for (SSTableMetadata metadata : manifest.sstables()) {
            SSTableReader reader = readersById.get(metadata.id());
            if (reader == null) {
                throw new StorageException.Corruption("Missing SSTable reader for checkpoint: " + metadata.id());
            }
            ByteBuffer contents = reader.contents();
            out.writeLong(metadata.id());
            out.writeLong(contents.remaining());
            written += 2L * Long.BYTES + contents.remaining();
            while (contents.hasRemaining()) {
                channel.write(contents);
            }
        }
        out.flush();
        return written;
    }

    static VersionCheckpoint stage(InputStream source, Path directory) throws IOException {
        var in = new DataInputStream(source);
        List<Path> staged = new ArrayList<>();
        try {
            if (in.readInt() != MAGIC) {
                throw new StorageException.Corruption("Invalid checkpoint magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new StorageException.Corruption("Unsupported checkpoint version: " + version);
            }
            int manifestLength = in.readInt();
            if (manifestLength < 0) {
                throw new StorageException.Corruption("Negative checkpoint manifest length");
            }
            SSTableManifest manifest = SSTableManifest.fromBytes(in.readNBytes(manifestLength));

            Files.createDirectories(directory);
            byte[] buffer = new byte[COPY_BUFFER_SIZE];
            for (SSTableMetadata metadata : manifest.sstables()) {
                long id = in.readLong();
                if (id != metadata.id()) {
                    throw new StorageException.Corruption("Checkpoint SSTable id does not match manifest: " + id);
                }
                long length = in.readLong();
                if (length < 0) {
                    throw new StorageException.Corruption("Negative checkpoint SSTable length");
                }
                Path path = stagedPath(directory, id);
                staged.add(path);
                copy(in, path, length, buffer);
            }

            var checkpoint = new VersionCheckpoint(manifest);
            checkpoint.validate(directory);
            return checkpoint;
        } catch (EOFException e) {
            deleteQuietly(staged);
            throw new StorageException.Corruption("Truncated checkpoint", e);
        } catch (IOException | RuntimeException e) {
            deleteQuietly(staged);
            if (e instanceof IllegalArgumentException) {
                throw new StorageException.Corruption("Malformed checkpoint", e);
            }
            throw e;
        }
    }

    SSTableManifest manifest() {
        return manifest;
    }

    LoadedStoreVersion activate(Path directory, ManifestStore manifestStore, SSTableStore sstableStore) throws IOException {
        for (SSTableMetadata metadata : manifest.sstables()) {
            Files.move(
                stagedPath(directory, metadata.id()),
                directory.resolve("%06d.sst".formatted(metadata.id())),
                StandardCopyOption.REPLACE_EXISTING
            );
        }
        manifestStore.write(manifest);
        return sstableStore.loadState(manifest);
    }

    void cleanup(Path directory) {
        deleteQuietly(manifest.sstables().stream().map(metadata -> stagedPath(directory, metadata.id())).toList());
    }

    private void validate(Path directory) {
        List<SSTableReader> readers = new ArrayList<>(manifest.sstables().size());
        try {
            for (SSTableMetadata metadata : manifest.sstables()) {
                SSTableReader reader = SSTableReader.open(
                    metadata.id(),
                    metadata.level(),
                    stagedPath(directory, metadata.id()),
                    NoOpBlockCache.INSTANCE
                );
                readers.add(reader);
                if (!reader.metadata().equals(metadata)) {
                    throw new StorageException.Corruption(
                        "Checkpoint SSTable metadata does not match file contents: " + metadata.id()
                    );
                }
            }
        } catch (RuntimeException e) {
            throw new StorageException.Corruption("Checkpoint validation failed", e);
        } finally {
            readers.forEach(SSTableReader::close);
        }
    }

    private static void copy(InputStream in, Path target, long length, byte[] buffer) throws IOException {
        try (FileChannel channel = FileChannel.open(
            target,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )) {
            long remaining = length;
            while (remaining > 0) {
                int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    throw new EOFException("Checkpoint ended inside SSTable " + target.getFileName());
                }
                ByteBuffer chunk = ByteBuffer.wrap(buffer, 0, read);
                while (chunk.hasRemaining()) {
                    channel.write(chunk);
                }
                remaining -= read;
            }
            channel.force(true);
        }
    }

    private static void deleteQuietly(List<Path> paths) {
        for (Path path : paths) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.atWarn().setCause(e).addKeyValue("path", path).log("Failed to clean staged checkpoint SSTable");
            }
        }
    }

    private static Path stagedPath(Path directory, long id) {
        return directory.resolve("%06d.sst%s".formatted(id, STAGED_SUFFIX));
    }
}
