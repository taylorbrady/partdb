package io.partdb.app;

import java.util.List;
import java.util.Map;

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
            case Map<?, ?> map -> {
                out.append('{');
                boolean first = true;
                for (var entry : map.entrySet()) {
                    if (!first) {
                        out.append(',');
                    }
                    first = false;
                    string(out, String.valueOf(entry.getKey()));
                    out.append(':');
                    write(out, entry.getValue());
                }
                out.append('}');
            }
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
            default -> throw new IllegalArgumentException("cannot write " + value.getClass() + " as JSON");
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
