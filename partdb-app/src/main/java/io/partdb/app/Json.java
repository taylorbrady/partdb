package io.partdb.app;

import java.lang.reflect.RecordComponent;
import java.util.List;
import java.util.Optional;

final class Json {

    private Json() {
    }

    static String write(Object value) {
        var out = new StringBuilder();
        write(out, value);
        return out.toString();
    }

    private static void write(StringBuilder out, Object value) {
        switch (value) {
            case null -> out.append("null");
            case String string -> string(out, string);
            case Number number -> out.append(number);
            case Boolean bool -> out.append(bool);
            case Enum<?> constant -> string(out, constant.name());
            case Optional<?> optional -> write(out, optional.orElse(null));
            case List<?> list -> {
                out.append('[');
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        out.append(',');
                    }
                    write(out, list.get(i));
                }
                out.append(']');
            }
            case Record record -> {
                out.append('{');
                var components = record.getClass().getRecordComponents();
                for (int i = 0; i < components.length; i++) {
                    if (i > 0) {
                        out.append(',');
                    }
                    string(out, components[i].getName());
                    out.append(':');
                    write(out, value(components[i], record));
                }
                out.append('}');
            }
            default -> throw new IllegalArgumentException("cannot write " + value.getClass() + " as JSON");
        }
    }

    private static Object value(RecordComponent component, Record record) {
        try {
            return component.getAccessor().invoke(record);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot read " + component.getName() + " of " + record.getClass(), e);
        }
    }

    private static void string(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append("\\u%04x".formatted((int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
