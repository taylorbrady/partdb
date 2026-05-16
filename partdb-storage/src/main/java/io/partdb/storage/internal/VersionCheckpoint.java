package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class VersionCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(VersionCheckpoint.class);
    private static final int CHECKPOINT_MAGIC = 0x53544350;
    private static final int CHECKPOINT_VERSION = 1;
    private static final String RESTORE_SUFFIX = ".restore";

    private final SSTableManifest manifest;
    private final List<CheckpointSSTable> sstables;

    private VersionCheckpoint(SSTableManifest manifest, List<CheckpointSSTable> sstables) {
        this.manifest = manifest;
        this.sstables = List.copyOf(sstables);
    }

    static VersionCheckpoint capture(VersionLease view) {
        SSTableManifest manifest = view.manifest();
        var readersById = view.all().stream()
            .collect(Collectors.toMap(SSTableReader::id, reader -> reader));

        List<CheckpointSSTable> sstables = new ArrayList<>(manifest.sstables().size());
        for (SSTableMetadata metadata : manifest.sstables()) {
            SSTableReader reader = readersById.get(metadata.id());
            if (reader == null) {
                throw new StorageException.Corruption(
                    "Missing SSTable reader for checkpoint: " + metadata.id()
                );
            }
            sstables.add(new CheckpointSSTable(metadata.id(), reader.fileBytes()));
        }

        return new VersionCheckpoint(manifest, sstables);
    }

    static VersionCheckpoint fromBytes(byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            if (buffer.remaining() < Integer.BYTES * 4) {
                throw new StorageException.Corruption("Checkpoint too small");
            }
            int magic = buffer.getInt();
            if (magic != CHECKPOINT_MAGIC) {
                throw new StorageException.Corruption(
                    "Invalid checkpoint magic: " + Integer.toHexString(magic)
                );
            }

            int version = buffer.getInt();
            if (version != CHECKPOINT_VERSION) {
                throw new StorageException.Corruption("Unsupported checkpoint version: " + version);
            }

            int manifestLength = buffer.getInt();
            if (manifestLength < 0) {
                throw new StorageException.Corruption("Negative checkpoint manifest length");
            }

            int sstableCount = buffer.getInt();
            if (sstableCount < 0) {
                throw new StorageException.Corruption("Negative checkpoint SSTable count");
            }
            if (buffer.remaining() < manifestLength) {
                throw new StorageException.Corruption("Truncated checkpoint manifest");
            }

            byte[] manifestBytes = new byte[manifestLength];
            buffer.get(manifestBytes);
            SSTableManifest manifest = SSTableManifest.fromBytes(manifestBytes);
            if (manifest.sstables().size() != sstableCount) {
                throw new StorageException.Corruption("Checkpoint SSTable count does not match manifest");
            }

            List<CheckpointSSTable> sstables = new ArrayList<>(sstableCount);
            for (SSTableMetadata metadata : manifest.sstables()) {
                if (buffer.remaining() < Long.BYTES + Integer.BYTES) {
                    throw new StorageException.Corruption("Truncated checkpoint SSTable header");
                }
                long id = buffer.getLong();
                if (id != metadata.id()) {
                    throw new StorageException.Corruption(
                        "Checkpoint SSTable id does not match manifest: " + id
                    );
                }

                int fileLength = buffer.getInt();
                if (fileLength < 0) {
                    throw new StorageException.Corruption("Negative checkpoint SSTable length");
                }
                if (buffer.remaining() < fileLength) {
                    throw new StorageException.Corruption("Truncated checkpoint SSTable payload");
                }

                byte[] fileBytes = new byte[fileLength];
                buffer.get(fileBytes);
                sstables.add(new CheckpointSSTable(id, fileBytes));
            }

            if (buffer.hasRemaining()) {
                throw new StorageException.Corruption("Trailing checkpoint data");
            }

            return new VersionCheckpoint(manifest, sstables);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new StorageException.Corruption("Malformed checkpoint", e);
        }
    }

    SSTableManifest manifest() {
        return manifest;
    }

    byte[] toBytes() {
        byte[] manifestBytes = manifest.toBytes();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 4);
        header.putInt(CHECKPOINT_MAGIC);
        header.putInt(CHECKPOINT_VERSION);
        header.putInt(manifestBytes.length);
        header.putInt(sstables.size());
        output.writeBytes(header.array());
        output.writeBytes(manifestBytes);

        for (CheckpointSSTable sstable : sstables) {
            ByteBuffer sstableHeader = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
            sstableHeader.putLong(sstable.id());
            sstableHeader.putInt(sstable.data().length);
            output.writeBytes(sstableHeader.array());
            output.writeBytes(sstable.data());
        }

        return output.toByteArray();
    }

    void stage(Path directory) throws IOException {
        Files.createDirectories(directory);
        for (CheckpointSSTable sstable : sstables) {
            Files.write(
                stagedPath(directory, sstable.id()),
                sstable.data(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
        }
    }

    void validate(Path directory) {
        List<SSTableReader> readers = new ArrayList<>(sstables.size());
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
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }
    }

    LoadedStoreVersion activate(Path directory, ManifestStore manifestStore, SSTableStore sstableStore) throws IOException {
        commitTo(directory);
        manifestStore.write(manifest);
        return sstableStore.loadState(manifest);
    }

    void cleanup(Path directory) {
        for (CheckpointSSTable sstable : sstables) {
            try {
                Files.deleteIfExists(stagedPath(directory, sstable.id()));
            } catch (IOException e) {
                log.atWarn()
                    .setCause(e)
                    .addKeyValue("sstableId", sstable.id())
                    .log("Failed to clean staged checkpoint SSTable");
            }
        }
    }

    private void commitTo(Path directory) throws IOException {
        for (CheckpointSSTable sstable : sstables) {
            Files.move(
                stagedPath(directory, sstable.id()),
                directory.resolve("%06d.sst".formatted(sstable.id())),
                StandardCopyOption.REPLACE_EXISTING
            );
        }
    }

    private static Path stagedPath(Path directory, long id) {
        return directory.resolve("%06d.sst%s".formatted(id, RESTORE_SUFFIX));
    }

    private record CheckpointSSTable(long id, byte[] data) {}
}
