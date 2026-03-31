package io.partdb.app;

import java.util.Objects;

final class Args {
    private final String[] values;
    private int index;

    Args(String[] values) {
        this.values = Objects.requireNonNull(values, "values must not be null").clone();
    }

    boolean hasNext() {
        return index < values.length;
    }

    String next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more arguments available");
        }
        return values[index++];
    }

    String requireValue(String optionName) {
        if (!hasNext()) {
            throw new IllegalArgumentException(optionName + " requires a value");
        }
        return next();
    }

    int requireIntValue(String optionName) {
        String value = requireValue(optionName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be a valid integer, got: " + value);
        }
    }
}
