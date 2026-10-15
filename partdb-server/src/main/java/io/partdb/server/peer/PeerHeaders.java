package io.partdb.server.peer;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.ClusterId;

final class PeerHeaders {

    static final Context.Key<NodeId> SENDER = Context.key("partdb-peer-sender");

    private static final Metadata.Key<String> CLUSTER = Metadata.Key.of("partdb-cluster-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> FROM = Metadata.Key.of("partdb-sender", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> TO = Metadata.Key.of("partdb-recipient", Metadata.ASCII_STRING_MARSHALLER);

    private PeerHeaders() {
    }

    static ClientInterceptor outgoing(ClusterId cluster, NodeId from, NodeId to) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel next
            ) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(CLUSTER, cluster.value());
                        headers.put(FROM, from.value());
                        headers.put(TO, to.value());
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    static ServerInterceptor incoming(ClusterId cluster, NodeId self) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next
            ) {
                if (!cluster.value().equals(headers.get(CLUSTER))) {
                    return reject(call, Status.FAILED_PRECONDITION.withDescription(
                        "peer belongs to cluster " + headers.get(CLUSTER) + ", not " + cluster));
                }
                if (!self.value().equals(headers.get(TO))) {
                    return reject(call, Status.FAILED_PRECONDITION.withDescription(
                        "message for " + headers.get(TO) + " reached " + self));
                }
                NodeId sender;
                try {
                    sender = new NodeId(String.valueOf(headers.get(FROM)));
                } catch (IllegalArgumentException e) {
                    return reject(call, Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                }
                return Contexts.interceptCall(Context.current().withValue(SENDER, sender), call, headers, next);
            }
        };
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> reject(ServerCall<ReqT, RespT> call, Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
