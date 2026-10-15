package io.partdb.server.grpc;

import io.grpc.stub.StreamObserver;
import io.partdb.grpc.cluster.proto.ClusterProto.Error;
import io.partdb.grpc.cluster.proto.ClusterProto.ErrorCode;
import io.partdb.grpc.cluster.proto.ClusterProto.Member;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListResponse;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberRole;
import io.partdb.grpc.cluster.proto.ClusterProto.NodeRole;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusResponse;
import io.partdb.grpc.cluster.proto.ClusterServiceGrpc;
import io.partdb.node.PartDbNode;
import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.Role;
import io.partdb.raft.runtime.RaftStatus;

final class ClusterService extends ClusterServiceGrpc.ClusterServiceImplBase {

    private final PartDbNode node;

    ClusterService(PartDbNode node) {
        this.node = node;
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusResponse> responseObserver) {
        RaftStatus status = node.cluster().status();
        var response = StatusResponse.newBuilder()
            .setError(okError())
            .setNodeId(status.nodeId().value())
            .setRole(toProtoRole(status.role()))
            .setTerm(status.term())
            .setLeaderId(status.leader().map(NodeId::value).orElse(""))
            .setCommitIndex(status.commitIndex())
            .setLastAppliedIndex(status.appliedIndex())
            .setIsRunning(status.running())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void memberList(MemberListRequest request, StreamObserver<MemberListResponse> responseObserver) {
        Membership membership = node.cluster().membership();
        RaftStatus status = node.cluster().status();
        String leaderId = status.leader().map(NodeId::value).orElse("");
        String selfId = status.nodeId().value();

        var builder = MemberListResponse.newBuilder()
            .setError(okError())
            .setLeaderId(leaderId);

        for (NodeId voter : membership.voters()) {
            builder.addMembers(buildMember(voter.value(), MemberRole.VOTER, leaderId, selfId));
        }
        for (NodeId learner : membership.learners()) {
            builder.addMembers(buildMember(learner.value(), MemberRole.LEARNER, leaderId, selfId));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private Member buildMember(String nodeId, MemberRole role, String leaderId, String selfId) {
        return Member.newBuilder()
            .setNodeId(nodeId)
            .setRole(role)
            .setIsLeader(nodeId.equals(leaderId))
            .setIsSelf(nodeId.equals(selfId))
            .build();
    }

    private static NodeRole toProtoRole(Role role) {
        return switch (role) {
            case FOLLOWER -> NodeRole.FOLLOWER;
            case PRE_CANDIDATE -> NodeRole.PRE_CANDIDATE;
            case CANDIDATE -> NodeRole.CANDIDATE;
            case LEADER -> NodeRole.LEADER;
        };
    }

    private static Error okError() {
        return Error.newBuilder().setCode(ErrorCode.OK).build();
    }
}
