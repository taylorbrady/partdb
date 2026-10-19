package io.partdb.server.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.partdb.grpc.kv.v1.KeyValue;
import io.partdb.grpc.kv.v1.ScanResponse;

import java.util.Iterator;
import java.util.stream.Stream;

final class ScanStream {

    private static final int BATCH_ENTRIES = 256;
    private static final int BATCH_BYTES = 1 << 20;

    private final ServerCallStreamObserver<ScanResponse> call;
    private final long revision;
    private final Stream<KeyValue> entries;
    private final Iterator<KeyValue> cursor;
    private int remaining;
    private boolean sent;
    private boolean closed;

    private ScanStream(ServerCallStreamObserver<ScanResponse> call, long revision, Stream<KeyValue> entries, int limit) {
        this.call = call;
        this.revision = revision;
        this.entries = entries;
        this.cursor = entries.iterator();
        this.remaining = limit;
    }

    static void start(ServerCallStreamObserver<ScanResponse> call, long revision, Stream<KeyValue> entries, int limit) {
        var stream = new ScanStream(call, revision, entries, limit);
        call.setOnCancelHandler(stream::close);
        call.setOnReadyHandler(stream::drain);
        stream.drain();
    }

    private void drain() {
        try {
            while (!closed && call.isReady()) {
                var batch = ScanResponse.newBuilder().setReadRevision(revision);
                int bytes = 0;
                while (remaining > 0 && cursor.hasNext() && batch.getEntriesCount() < BATCH_ENTRIES && bytes < BATCH_BYTES) {
                    var entry = cursor.next();
                    bytes += entry.getSerializedSize();
                    batch.addEntries(entry);
                    remaining--;
                }
                boolean done = remaining == 0 || !cursor.hasNext();
                if (batch.getEntriesCount() > 0 || !sent) {
                    call.onNext(batch.build());
                    sent = true;
                }
                if (done) {
                    close();
                    call.onCompleted();
                }
            }
        } catch (RuntimeException e) {
            close();
            call.onError(Calls.toStatus(e));
        }
    }

    private void close() {
        if (!closed) {
            closed = true;
            entries.close();
        }
    }
}
