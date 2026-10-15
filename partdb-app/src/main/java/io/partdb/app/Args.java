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
            throw new IllegalStateException("no more arguments");
        }
        return values[index++];
    }

    String value(String option) {
        if (!hasNext()) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return next();
    }

    int intValue(String option) {
        String value = value(option);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " must be an integer, got: " + value);
        }
    }

    long longValue(String option) {
        String value = value(option);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " must be an integer, got: " + value);
        }
    }
}
