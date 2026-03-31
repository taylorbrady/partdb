package io.partdb.app;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class JsonWriter {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private JsonWriter() {}

    static String quote(String value) {
        Objects.requireNonNull(value, "value must not be null");
        StringBuilder builder = new StringBuilder(value.length() + 2);
        appendQuoted(builder, value);
        return builder.toString();
    }

    static String object(Consumer<ObjectWriter> consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        StringBuilder builder = new StringBuilder();
        ObjectWriter writer = new ObjectWriter(builder);
        writer.begin();
        consumer.accept(writer);
        writer.end();
        return builder.toString();
    }

    private static void appendQuoted(StringBuilder builder, String value) {
        builder.append('"');
        appendEscaped(builder, value);
        builder.append('"');
    }

    private static void appendEscaped(StringBuilder builder, String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"' -> builder.append("\\\"");
                case '\\' -> builder.append("\\\\");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (ch <= 0x1f) {
                        builder.append("\\u");
                        builder.append(HEX[(ch >>> 12) & 0xf]);
                        builder.append(HEX[(ch >>> 8) & 0xf]);
                        builder.append(HEX[(ch >>> 4) & 0xf]);
                        builder.append(HEX[ch & 0xf]);
                    } else {
                        builder.append(ch);
                    }
                }
            }
        }
    }

    static final class ObjectWriter {
        private final StringBuilder builder;
        private boolean first = true;

        private ObjectWriter(StringBuilder builder) {
            this.builder = builder;
        }

        private void begin() {
            builder.append('{');
        }

        private void end() {
            builder.append('}');
        }

        void field(String name, String value) {
            Objects.requireNonNull(value, "value must not be null");
            writeName(name);
            appendQuoted(builder, value);
        }

        void field(String name, long value) {
            writeName(name);
            builder.append(value);
        }

        void field(String name, boolean value) {
            writeName(name);
            builder.append(value);
        }

        void field(String name, Optional<String> value) {
            Objects.requireNonNull(value, "value must not be null");
            if (value.isPresent()) {
                field(name, value.get());
            } else {
                nullField(name);
            }
        }

        void nullField(String name) {
            writeName(name);
            builder.append("null");
        }

        <T> void array(String name, Iterable<T> values, BiConsumer<ArrayWriter, T> writeElement) {
            Objects.requireNonNull(values, "values must not be null");
            Objects.requireNonNull(writeElement, "writeElement must not be null");
            writeName(name);
            ArrayWriter arrayWriter = new ArrayWriter(builder);
            arrayWriter.begin();
            for (T value : values) {
                writeElement.accept(arrayWriter, value);
            }
            arrayWriter.end();
        }

        void object(String name, Consumer<ObjectWriter> consumer) {
            Objects.requireNonNull(consumer, "consumer must not be null");
            writeName(name);
            ObjectWriter objectWriter = new ObjectWriter(builder);
            objectWriter.begin();
            consumer.accept(objectWriter);
            objectWriter.end();
        }

        private void writeName(String name) {
            Objects.requireNonNull(name, "name must not be null");
            separator();
            appendQuoted(builder, name);
            builder.append(':');
        }

        private void separator() {
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
        }
    }

    static final class ArrayWriter {
        private final StringBuilder builder;
        private boolean first = true;

        private ArrayWriter(StringBuilder builder) {
            this.builder = builder;
        }

        private void begin() {
            builder.append('[');
        }

        private void end() {
            builder.append(']');
        }

        void object(Consumer<ObjectWriter> consumer) {
            Objects.requireNonNull(consumer, "consumer must not be null");
            separator();
            ObjectWriter objectWriter = new ObjectWriter(builder);
            objectWriter.begin();
            consumer.accept(objectWriter);
            objectWriter.end();
        }

        private void separator() {
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
        }
    }
}
