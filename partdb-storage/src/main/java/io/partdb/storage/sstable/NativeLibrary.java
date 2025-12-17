package io.partdb.storage.sstable;

import io.partdb.storage.StorageException;

import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class NativeLibrary {

    private static final List<Path> SEARCH_PATHS = List.of(
        Path.of("/opt/homebrew/lib"),
        Path.of("/usr/local/lib"),
        Path.of("/usr/lib"),
        Path.of("/usr/lib/x86_64-linux-gnu"),
        Path.of("/usr/lib/aarch64-linux-gnu")
    );

    static SymbolLookup load(String name) {
        try {
            return SymbolLookup.libraryLookup(name, Arena.global());
        } catch (IllegalArgumentException _) {
        }

        String libFile = System.mapLibraryName(name);
        for (Path dir : SEARCH_PATHS) {
            Path lib = dir.resolve(libFile);
            if (Files.exists(lib)) {
                return SymbolLookup.libraryLookup(lib, Arena.global());
            }
        }

        throw new StorageException.IO(
            "Native library not found: " + name + ". Install with: brew install " + name
        );
    }
}
