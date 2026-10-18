package io.partdb.client;

import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;
import io.partdb.grpc.cluster.v1.GetStatusRequest;
import io.partdb.grpc.cluster.v1.ListMembersRequest;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class ClusterClient {

    private final Connections connections;
    private final Invoker invoker;
    private final Duration timeout;

    ClusterClient(Connections connections, Invoker invoker, Duration timeout) {
        this.connections = connections;
        this.invoker = invoker;
        this.timeout = timeout;
    }

    public NodeStatus status() {
        return invoker.call(Invoker.Kind.READ, (channel, deadline) ->
            Protos.status(ClusterServiceGrpc.newBlockingStub(channel).withDeadline(deadline).getStatus(GetStatusRequest.getDefaultInstance())));
    }

    public NodeStatus status(ServerEndpoint endpoint) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        try {
            var status = Protos.status(ClusterServiceGrpc.newBlockingStub(connections.channel(endpoint))
                .withDeadline(Deadline.after(timeout.toNanos(), TimeUnit.NANOSECONDS))
                .getStatus(GetStatusRequest.getDefaultInstance()));
            connections.learn(status.nodeId(), endpoint);
            return status;
        } catch (StatusRuntimeException error) {
            var failure = Failure.of(error);
            throw switch (failure) {
                case Failure.DeadlineExceeded _ -> new PartDbException.Timeout(failure.message(), error);
                case Failure.Unreachable _, Failure.NotLeader _, Failure.Busy _ -> new PartDbException.Unavailable(failure.message(), error);
                case Failure.Invalid _ -> new PartDbException.InvalidRequest(failure.message(), error);
                case Failure.Cancelled _ -> new PartDbException.Cancelled(failure.message(), error);
                case Failure.OutcomeUnknown _, Failure.Internal _ -> new PartDbException.Internal(failure.message(), error);
            };
        }
    }

    public Members members() {
        return invoker.call(Invoker.Kind.READ, (channel, deadline) ->
            Protos.members(ClusterServiceGrpc.newBlockingStub(channel).withDeadline(deadline).listMembers(ListMembersRequest.getDefaultInstance())));
    }
}
