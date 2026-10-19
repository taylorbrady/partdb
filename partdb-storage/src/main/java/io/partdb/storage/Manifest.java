package io.partdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

record Manifest(long durableRevision, long nextTableId, List<Placement> tables) {

    static final String FILE_NAME = "MANIFEST";

    private static final int MAGIC = 0x4d424450;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 4;
    private static final int PLACEMENT_SIZE = 8 + 4;

    record Placement(long id, int level) {}

    Manifest {
        tables = List.copyOf(tables);
    }

    static Manifest read(Path directory) {
        Path path = directory.resolve(FILE_NAME);
        try {
            return decode(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new StoreException.IO("Failed to read manifest " + path, e);
        }
    }

    void write(Path directory) {
        Disk.writeAtomically(directory.resolve(FILE_NAME), encode());
    }

    byte[] encode() {
        ByteBuffer buffer = Encoding.allocate(HEADER_SIZE + tables.size() * PLACEMENT_SIZE + Integer.BYTES);
        buffer.putInt(MAGIC).putInt(VERSION).putLong(durableRevision).putLong(nextTableId).putInt(tables.size());
        for (Placement table : tables) {
            buffer.putLong(table.id()).putInt(table.level());
        }
        buffer.putInt(Encoding.crc(buffer.array(), 0, buffer.position()));
        return buffer.array();
    }

    static Manifest decode(byte[] bytes) {
        Encoding.check(bytes.length >= HEADER_SIZE + Integer.BYTES, "Manifest too small");
        ByteBuffer buffer = Encoding.wrap(bytes);
        Encoding.check(buffer.getInt() == MAGIC, "Invalid manifest magic");
        int version = buffer.getInt();
        Encoding.check(version == VERSION, "Unsupported manifest version: " + version);
        int checksumOffset = bytes.length - Integer.BYTES;
        Encoding.check(buffer.getInt(checksumOffset) == Encoding.crc(bytes, 0, checksumOffset), "Manifest checksum mismatch");

        long durableRevision = buffer.getLong();
        long nextTableId = buffer.getLong();
        int count = buffer.getInt();
        Encoding.check(count >= 0 && HEADER_SIZE + (long) count * PLACEMENT_SIZE == checksumOffset, "Invalid manifest table count");
        List<Placement> tables = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long id = buffer.getLong();
            int level = buffer.getInt();
            Encoding.check(id > 0 && id < nextTableId && level >= 0 && level < Version.LEVELS, "Invalid manifest table entry");
            tables.add(new Placement(id, level));
        }
        Encoding.check(durableRevision >= 0, "Negative manifest durable revision");
        return new Manifest(durableRevision, nextTableId, tables);
    }
}
