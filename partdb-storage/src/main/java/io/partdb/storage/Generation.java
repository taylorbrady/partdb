package io.partdb.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Generation {

    static final String CURRENT = "CURRENT";

    private static final Pattern GENERATION = Pattern.compile("gen-(\\d+)");
    private static final Pattern TABLE = Pattern.compile("(\\d+)\\.sst");

    private final Path root;
    private final long number;
    private final AtomicLong nextTableId;

    record Recovered(Generation generation, Manifest manifest) {}

    private Generation(Path root, long number, long nextTableId) {
        this.root = root;
        this.number = number;
        this.nextTableId = new AtomicLong(nextTableId);
    }

    static Recovered recover(Path root) {
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to create " + root, e);
        }
        boolean changed = false;
        for (Path path : Disk.list(root)) {
            if (path.getFileName().toString().endsWith(Disk.TEMPORARY_SUFFIX)) {
                Disk.deleteRecursively(path);
                changed = true;
            }
        }

        long number = Files.exists(root.resolve(CURRENT)) ? readCurrent(root) : 0;
        for (Path path : Disk.list(root)) {
            Matcher matcher = GENERATION.matcher(path.getFileName().toString());
            if (matcher.matches() && Long.parseLong(matcher.group(1)) != number) {
                Disk.deleteRecursively(path);
                changed = true;
            }
        }
        if (changed) {
            Disk.syncDirectory(root);
        }
        if (number == 0) {
            Generation initial = stage(root);
            Manifest empty = new Manifest(0, 1, List.of());
            initial.commit(empty);
            initial.activate();
            return new Recovered(initial, empty);
        }

        Generation active = new Generation(root, number, 1);
        Path directory = active.directory();
        Encoding.check(Files.isDirectory(directory), "Missing generation directory " + directory);
        Manifest manifest = Manifest.read(directory);
        Set<Long> referenced = new HashSet<>();
        manifest.tables().forEach(table -> referenced.add(table.id()));

        long maxId = 0;
        boolean cleaned = false;
        Set<Long> present = new HashSet<>();
        for (Path path : Disk.list(directory)) {
            String name = path.getFileName().toString();
            Matcher matcher = TABLE.matcher(name);
            if (name.endsWith(Disk.TEMPORARY_SUFFIX)) {
                Disk.delete(path);
                cleaned = true;
            } else if (matcher.matches()) {
                long id = Long.parseLong(matcher.group(1));
                maxId = Math.max(maxId, id);
                if (referenced.contains(id)) {
                    present.add(id);
                } else {
                    Disk.delete(path);
                    cleaned = true;
                }
            }
        }
        if (cleaned) {
            Disk.syncDirectory(directory);
        }
        referenced.removeAll(present);
        Encoding.check(referenced.isEmpty(), "Manifest references missing tables " + referenced + " in " + directory);
        active.nextTableId.set(Math.max(manifest.nextTableId(), maxId + 1));
        return new Recovered(active, manifest);
    }

    static Generation stage(Path root) {
        long highest = 0;
        for (Path path : Disk.list(root)) {
            Matcher matcher = GENERATION.matcher(path.getFileName().toString());
            if (matcher.matches()) {
                highest = Math.max(highest, Long.parseLong(matcher.group(1)));
            }
        }
        Generation staged = new Generation(root, highest + 1, 1);
        try {
            Files.createDirectory(staged.directory());
        } catch (IOException e) {
            throw new StoreException.IO("Failed to create " + staged.directory(), e);
        }
        Disk.syncDirectory(root);
        return staged;
    }

    void activate() {
        Disk.writeAtomically(root.resolve(CURRENT), name().getBytes(StandardCharsets.US_ASCII));
    }

    void commit(Manifest manifest) {
        manifest.write(directory());
    }

    void delete() {
        Disk.deleteRecursively(directory());
    }

    Path directory() {
        return root.resolve(name());
    }

    Path tablePath(long id) {
        return directory().resolve("%06d.sst".formatted(id));
    }

    long allocateTableId() {
        return nextTableId.getAndIncrement();
    }

    long nextTableId() {
        return nextTableId.get();
    }

    private String name() {
        return "gen-" + number;
    }

    private static long readCurrent(Path root) {
        Path current = root.resolve(CURRENT);
        try {
            Matcher matcher = GENERATION.matcher(Files.readString(current, StandardCharsets.US_ASCII).strip());
            Encoding.check(matcher.matches(), "Malformed " + current);
            long number = Long.parseLong(matcher.group(1));
            Encoding.check(number > 0, "Malformed " + current);
            return number;
        } catch (IOException e) {
            throw new StoreException.IO("Failed to read " + current, e);
        }
    }
}
