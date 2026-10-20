package io.partdb.app;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.encoder.EncoderBase;
import org.slf4j.event.KeyValuePair;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;

public final class LogEncoder extends EncoderBase<ILoggingEvent> {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

    private OutputFormat format = OutputFormat.TEXT;

    public void setFormat(String format) {
        this.format = OutputFormat.valueOf(format.toUpperCase(Locale.ROOT));
    }

    @Override
    public byte[] headerBytes() {
        return null;
    }

    @Override
    public byte[] encode(ILoggingEvent event) {
        String line = switch (format) {
            case TEXT -> text(event);
            case JSON -> json(event);
        };
        return line.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] footerBytes() {
        return null;
    }

    private static String text(ILoggingEvent event) {
        var out = new StringBuilder(160)
            .append(TIME.format(event.getInstant())).append(' ')
            .append("%-5s".formatted(event.getLevel())).append(' ')
            .append(simpleName(event.getLoggerName())).append(": ")
            .append(event.getFormattedMessage());
        for (KeyValuePair pair : pairs(event)) {
            out.append(' ').append(pair.key).append('=');
            String value = String.valueOf(pair.value);
            if (value.isEmpty() || value.chars().anyMatch(c -> c <= ' ' || c == '"' || c == '=')) {
                out.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            } else {
                out.append(value);
            }
        }
        out.append('\n');
        IThrowableProxy error = event.getThrowableProxy();
        if (error != null) {
            out.append(ThrowableProxyUtil.asString(error));
        }
        return out.toString();
    }

    private static String json(ILoggingEvent event) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("time", TIME.format(event.getInstant()));
        fields.put("level", event.getLevel().toString());
        fields.put("logger", event.getLoggerName());
        fields.put("thread", event.getThreadName());
        fields.put("msg", event.getFormattedMessage());
        for (KeyValuePair pair : pairs(event)) {
            fields.putIfAbsent(pair.key, switch (pair.value) {
                case null -> null;
                case Number number -> number;
                case Boolean bool -> bool;
                default -> String.valueOf(pair.value);
            });
        }
        IThrowableProxy error = event.getThrowableProxy();
        if (error != null) {
            fields.put("error", error.getClassName() + ": " + error.getMessage());
            fields.put("stack", ThrowableProxyUtil.asString(error));
        }
        return Json.write(fields) + "\n";
    }

    private static List<KeyValuePair> pairs(ILoggingEvent event) {
        var pairs = event.getKeyValuePairs();
        return pairs == null ? List.of() : pairs;
    }

    private static String simpleName(String logger) {
        return logger.substring(logger.lastIndexOf('.') + 1);
    }
}
