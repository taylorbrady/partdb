package io.partdb.app;

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

final class StdoutHandler extends StreamHandler {

    StdoutHandler(Formatter formatter) {
        super(System.out, formatter);
        setLevel(Level.ALL);
    }

    @Override
    public synchronized void publish(LogRecord record) {
        super.publish(record);
        flush();
    }

    @Override
    public synchronized void close() {
        flush();
    }
}
