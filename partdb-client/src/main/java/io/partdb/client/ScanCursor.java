package io.partdb.client;

import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import io.partdb.grpc.kv.v1.KvProto;
import io.partdb.grpc.kv.v1.KvProto.ScanResponse;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class ScanCursor implements Iterator<KeyValue>, AutoCloseable {

    private final Context.CancellableContext context;
    private final Iterator<ScanResponse> responses;
    private final long readRevision;
    private Iterator<KvProto.KeyValue> batch;
    private boolean closed;

    ScanCursor(Context.CancellableContext context, Iterator<ScanResponse> responses, ScanResponse first) {
        this.context = context;
        this.responses = responses;
        this.readRevision = first.getReadRevision();
        this.batch = first.getEntriesList().iterator();
    }

    public long readRevision() {
        return readRevision;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        try {
            while (!batch.hasNext()) {
                if (!responses.hasNext()) {
                    batch = Collections.emptyIterator();
                    close();
                    return false;
                }
                batch = responses.next().getEntriesList().iterator();
            }
            return true;
        } catch (StatusRuntimeException error) {
            close();
            throw new PartDbException.Unavailable("the scan stream failed after it started", error);
        }
    }

    @Override
    public KeyValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return Protos.keyValue(batch.next());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            context.cancel(null);
        }
    }
}
