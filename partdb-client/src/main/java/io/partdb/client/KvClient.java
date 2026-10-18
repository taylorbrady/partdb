package io.partdb.client;

import io.grpc.Context;
import io.grpc.StatusRuntimeException;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.kv.v1.DeleteRequest;
import io.partdb.grpc.kv.v1.GetRequest;
import io.partdb.grpc.kv.v1.KeyValueServiceGrpc;
import io.partdb.grpc.kv.v1.PutRequest;
import io.partdb.grpc.kv.v1.ScanRequest;
import io.partdb.grpc.kv.v1.WriteRequest;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class KvClient {

    private final Invoker invoker;

    KvClient(Invoker invoker) {
        this.invoker = invoker;
    }

    public Read<Optional<KeyValue>> get(Bytes key) {
        return get(key, ReadConsistency.LINEARIZABLE);
    }

    public Read<Optional<KeyValue>> get(Bytes key, ReadConsistency consistency) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(consistency, "consistency must not be null");
        var request = GetRequest.newBuilder().setKey(Protos.bytes(key)).setConsistency(Protos.consistency(consistency)).build();
        var response = invoker.call(Invoker.Kind.READ, (channel, deadline) ->
            KeyValueServiceGrpc.newBlockingStub(channel).withDeadline(deadline).get(request));
        var entry = response.hasEntry() ? Optional.of(Protos.keyValue(response.getEntry())) : Optional.<KeyValue>empty();
        return new Read<>(entry, response.getReadRevision());
    }

    public long put(Bytes key, Bytes value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        var request = PutRequest.newBuilder().setKey(Protos.bytes(key)).setValue(Protos.bytes(value)).build();
        return invoker.call(Invoker.Kind.WRITE, (channel, deadline) ->
            KeyValueServiceGrpc.newBlockingStub(channel).withDeadline(deadline).put(request)).getRevision();
    }

    public long delete(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        var request = DeleteRequest.newBuilder().setKey(Protos.bytes(key)).build();
        return invoker.call(Invoker.Kind.WRITE, (channel, deadline) ->
            KeyValueServiceGrpc.newBlockingStub(channel).withDeadline(deadline).delete(request)).getRevision();
    }

    public long write(List<Mutation> mutations) {
        Objects.requireNonNull(mutations, "mutations must not be null");
        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("mutations must not be empty");
        }
        var request = WriteRequest.newBuilder().addAllMutations(mutations.stream().map(Protos::mutation).toList()).build();
        return invoker.call(Invoker.Kind.WRITE, (channel, deadline) ->
            KeyValueServiceGrpc.newBlockingStub(channel).withDeadline(deadline).write(request)).getRevision();
    }

    public ScanCursor scan(KeyRange range) {
        return scan(range, ScanOptions.DEFAULT);
    }

    public ScanCursor scan(KeyRange range, ScanOptions options) {
        Objects.requireNonNull(range, "range must not be null");
        Objects.requireNonNull(options, "options must not be null");
        var request = ScanRequest.newBuilder()
            .setStartKey(Protos.bytes(range.start()))
            .setLimit(options.limit().orElse(0))
            .setConsistency(Protos.consistency(options.consistency()));
        range.end().ifPresent(end -> request.setEndKey(Protos.bytes(end)));
        return invoker.call(Invoker.Kind.READ, (channel, deadline) -> {
            var context = Context.current().withCancellation();
            try {
                var responses = context.call(() -> KeyValueServiceGrpc.newBlockingStub(channel).withDeadline(deadline).scan(request.build()));
                if (!responses.hasNext()) {
                    throw new PartDbException.Internal("the server ended the scan without a response", null);
                }
                return new ScanCursor(context, responses, responses.next());
            } catch (StatusRuntimeException | PartDbException e) {
                context.cancel(null);
                throw e;
            } catch (Exception e) {
                context.cancel(null);
                throw new PartDbException.Internal("failed to start the scan", e);
            }
        });
    }
}
