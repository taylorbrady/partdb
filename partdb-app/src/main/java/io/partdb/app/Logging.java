package io.partdb.app;

import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

final class Logging {

    private static final Logger ROOT = Logger.getLogger("");
    private static final List<Logger> LIBRARIES = List.of(Logger.getLogger("io.grpc"), Logger.getLogger("io.netty"));

    private Logging() {
    }

    static void configure(OutputFormat format, System.Logger.Level level, String nodeId) {
        for (Handler handler : ROOT.getHandlers()) {
            ROOT.removeHandler(handler);
        }
        Level threshold = julLevel(level);
        ROOT.setLevel(threshold);
        ROOT.addHandler(new StdoutHandler(new LogFormatter(format, nodeId)));
        Level libraries = threshold.intValue() > Level.WARNING.intValue() ? threshold : Level.WARNING;
        LIBRARIES.forEach(logger -> logger.setLevel(libraries));
    }

    static Level julLevel(System.Logger.Level level) {
        return switch (level) {
            case ALL -> Level.ALL;
            case TRACE -> Level.FINER;
            case DEBUG -> Level.FINE;
            case INFO -> Level.INFO;
            case WARNING -> Level.WARNING;
            case ERROR -> Level.SEVERE;
            case OFF -> Level.OFF;
        };
    }
}
