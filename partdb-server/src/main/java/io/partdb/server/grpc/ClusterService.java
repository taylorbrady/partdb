package io.partdb.server.grpc;

import io.grpc.stub.StreamObserver;
import io.partdb.grpc.cluster.v1.ClusterServiceGrpc;
import io.partdb.grpc.cluster.v1.GetStatusRequest;
import io.partdb.grpc.cluster.v1.ListMembersRequest;
import io.partdb.grpc.cluster.v1.ListMembersResponse;
import io.partdb.grpc.cluster.v1.Member;
import io.partdb.grpc.cluster.v1.MemberRole;
import io.partdb.grpc.cluster.v1.NodeStatus;
import io.partdb.grpc.cluster.v1.RaftRole;
import io.partdb.node.Node;
import io.partdb.raft.NodeId;
import io.partdb.raft.Role;

final class ClusterService extends ClusterServiceGrpc.ClusterServiceImplBase {

    private final Node node;

    ClusterService(Node node) {
        this.node = node;
    }

    @Override
    public void getStatus(GetStatusRequest request, StreamObserver<NodeStatus> responses) {
        Calls.unary(responses, _ -> {
            var status = node.status();
            var response = NodeStatus.newBuilder()
                .setNodeId(status.nodeId().value())
                .setRole(role(status.role()))
                .setTerm(status.term())
                .setCommitIndex(status.commitIndex())
                .setAppliedIndex(status.appliedIndex())
                .setSnapshotIndex(status.snapshotIndex());
            status.leader().map(NodeId::value).ifPresent(response::setLeaderId);
            return response.build();
        });
    }

    @Override
    public void listMembers(ListMembersRequest request, StreamObserver<ListMembersResponse> responses) {
        Calls.unary(responses, _ -> {
            var membership = node.membership();
            var response = ListMembersResponse.newBuilder();
            membership.voters().forEach(voter -> response.addMembers(member(voter, MemberRole.MEMBER_ROLE_VOTER)));
            membership.learners().forEach(learner -> response.addMembers(member(learner, MemberRole.MEMBER_ROLE_LEARNER)));
            node.status().leader().map(NodeId::value).ifPresent(response::setLeaderId);
            return response.build();
        });
    }

    private static Member member(NodeId node, MemberRole role) {
        return Member.newBuilder().setNodeId(node.value()).setRole(role).build();
    }

    private static RaftRole role(Role role) {
        return switch (role) {
            case FOLLOWER -> RaftRole.RAFT_ROLE_FOLLOWER;
            case PRE_CANDIDATE -> RaftRole.RAFT_ROLE_PRE_CANDIDATE;
            case CANDIDATE -> RaftRole.RAFT_ROLE_CANDIDATE;
            case LEADER -> RaftRole.RAFT_ROLE_LEADER;
        };
    }
}
