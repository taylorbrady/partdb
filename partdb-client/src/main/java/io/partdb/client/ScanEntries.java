package io.partdb.client;

import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import io.partdb.grpc.kv.v1.ScanResponse;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class ScanEntries implements Iterator<KeyValue> {

    private final Context.CancellableContext context;
    private final Iterator<ScanResponse> responses;
    private Iterator<io.partdb.grpc.kv.v1.KeyValue> batch;

    ScanEntries(Context.CancellableContext context, Iterator<ScanResponse> responses, ScanResponse first) {
        this.context = context;
        this.responses = responses;
        this.batch = first.getEntriesList().iterator();
    }

    @Override
    public boolean hasNext() {
        try {
            while (!batch.hasNext()) {
                if (!responses.hasNext()) {
                    batch = Collections.emptyIterator();
                    context.cancel(null);
                    return false;
                }
                batch = responses.next().getEntriesList().iterator();
            }
            return true;
        } catch (StatusRuntimeException error) {
            context.cancel(null);
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
}
