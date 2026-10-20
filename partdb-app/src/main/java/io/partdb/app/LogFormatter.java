package io.partdb.app;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

final class LogFormatter extends Formatter {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

    private final OutputFormat format;
    private final String nodeId;

    LogFormatter(OutputFormat format, String nodeId) {
        this.format = Objects.requireNonNull(format, "format must not be null");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public String format(LogRecord record) {
        var fields = new LinkedHashMap<String, Object>();
        var error = Optional.ofNullable(record.getThrown());
        if (record.getParameters() != null) {
            for (Object parameter : record.getParameters()) {
                var thrown = collect(parameter, fields);
                if (error.isEmpty()) {
                    error = thrown;
                }
            }
        }
        return switch (format) {
            case TEXT -> text(record, fields, error);
            case JSON -> json(record, fields, error);
        };
    }

    private String text(LogRecord record, Map<String, Object> fields, Optional<Throwable> error) {
        var out = new StringBuilder(160)
            .append(TIME.format(record.getInstant())).append(' ')
            .append("%-5s".formatted(level(record.getLevel()))).append(' ')
            .append(simpleName(record.getLoggerName())).append(": ")
            .append(formatMessage(record));
        appendText(out, "nodeId", nodeId);
        fields.forEach((key, value) -> appendText(out, key, value instanceof Duration duration ? human(duration) : String.valueOf(value)));
        out.append('\n');
        error.ifPresent(thrown -> out.append(stackTrace(thrown)));
        return out.toString();
    }

    private String json(LogRecord record, Map<String, Object> fields, Optional<Throwable> error) {
        var object = new LinkedHashMap<String, Object>();
        object.put("time", TIME.format(record.getInstant()));
        object.put("level", level(record.getLevel()));
        object.put("logger", Objects.requireNonNullElse(record.getLoggerName(), ""));
        object.put("thread", Thread.currentThread().getName());
        object.put("nodeId", nodeId);
        object.put("msg", formatMessage(record));
        fields.forEach((key, value) -> object.putIfAbsent(key, switch (value) {
            case Duration duration -> duration.toNanos() / 1e9;
            case Number number -> number;
            case Boolean bool -> bool;
            default -> String.valueOf(value);
        }));
        error.ifPresent(thrown -> {
            object.put("error", thrown.toString());
            object.put("stack", stackTrace(thrown));
        });
        return Json.write(object) + "\n";
    }

    private static Optional<Throwable> collect(Object parameter, Map<String, Object> fields) {
        return switch (parameter) {
            case Optional<?> optional -> optional.flatMap(value -> collect(value, fields));
            case Map.Entry<?, ?> field -> {
                var value = field.getValue() instanceof Optional<?> optional ? optional.orElse(null) : field.getValue();
                if (value != null) {
                    fields.putIfAbsent(String.valueOf(field.getKey()), value);
                }
                yield Optional.empty();
            }
            case Throwable thrown -> Optional.of(thrown);
            case null, default -> Optional.empty();
        };
    }

    private static void appendText(StringBuilder out, String key, String value) {
        out.append(' ').append(key).append('=');
        if (value.isEmpty() || value.chars().anyMatch(c -> c <= ' ' || c == '"' || c == '=')) {
            out.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
        } else {
            out.append(value);
        }
    }

    private static String level(Level level) {
        int value = level.intValue();
        if (value >= Level.SEVERE.intValue()) {
            return "ERROR";
        }
        if (value >= Level.WARNING.intValue()) {
            return "WARN";
        }
        if (value >= Level.INFO.intValue()) {
            return "INFO";
        }
        return value >= Level.FINE.intValue() ? "DEBUG" : "TRACE";
    }

    private static String human(Duration duration) {
        long millis = duration.toMillis();
        return millis < 1000 ? millis + "ms" : String.format(Locale.ROOT, "%.3fs", duration.toNanos() / 1e9);
    }

    private static String simpleName(String logger) {
        return logger == null ? "" : logger.substring(logger.lastIndexOf('.') + 1);
    }

    private static String stackTrace(Throwable thrown) {
        var out = new StringWriter();
        thrown.printStackTrace(new PrintWriter(out));
        return out.toString();
    }
}
