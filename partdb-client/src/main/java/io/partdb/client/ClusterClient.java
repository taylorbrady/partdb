package io.partdb.client;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.partdb.grpc.cluster.v1.ClusterProto.GetStatusRequest;
import io.partdb.grpc.cluster.v1.ClusterProto.ListMembersRequest;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;

import java.util.Objects;

public final class ClusterClient {

    private final Connections connections;
    private final Invoker invoker;

    ClusterClient(Connections connections, Invoker invoker) {
        this.connections = connections;
        this.invoker = invoker;
    }

    public NodeStatus status(ServerEndpoint endpoint) {
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        var status = invoker.read(endpoint, (channel, deadline) ->
            Protos.status(stub(channel, deadline).getStatus(GetStatusRequest.getDefaultInstance())));
        connections.learn(status.nodeId(), endpoint);
        return status;
    }

    public Membership membership() {
        return invoker.call(Invoker.Kind.READ, (channel, deadline) ->
            Protos.membership(stub(channel, deadline).listMembers(ListMembersRequest.getDefaultInstance())));
    }

    private static ClusterServiceGrpc.ClusterServiceBlockingStub stub(ManagedChannel channel, Deadline deadline) {
        return ClusterServiceGrpc.newBlockingStub(channel).withDeadline(deadline);
    }
}
