package io.partdb.transport.grpc;

import io.grpc.stub.StreamObserver;
import io.partdb.grpc.cluster.proto.ClusterProto.Error;
import io.partdb.grpc.cluster.proto.ClusterProto.ErrorCode;
import io.partdb.grpc.cluster.proto.ClusterProto.Member;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberListResponse;
import io.partdb.grpc.cluster.proto.ClusterProto.MemberRole;
import io.partdb.grpc.cluster.proto.ClusterProto.NodeRole;
import io.partdb.node.NodeMembership;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusRequest;
import io.partdb.grpc.cluster.proto.ClusterProto.StatusResponse;
import io.partdb.grpc.cluster.proto.ClusterServiceGrpc;
import io.partdb.node.NodeStatus;
import io.partdb.node.PartDbNode;

import java.util.Map;

final class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {

    private final PartDbNode node;
    private final Map<String, String> raftPeerAddresses;
    private final String selfRaftAddress;

    ClusterServiceImpl(PartDbNode node, Map<String, String> raftPeerAddresses, String selfRaftAddress) {
        this.node = node;
        this.raftPeerAddresses = Map.copyOf(raftPeerAddresses);
        this.selfRaftAddress = selfRaftAddress;
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusResponse> responseObserver) {
        NodeStatus status = node.status();
        var response = StatusResponse.newBuilder()
            .setError(okError())
            .setNodeId(status.nodeId())
            .setRole(toProtoRole(status.role()))
            .setTerm(status.term())
            .setLeaderId(status.leaderId().orElse(""))
            .setCommitIndex(status.commitIndex())
            .setLastAppliedIndex(status.lastAppliedIndex())
            .setIsRunning(status.running())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void memberList(MemberListRequest request, StreamObserver<MemberListResponse> responseObserver) {
        NodeMembership membership = node.membership();
        String leaderId = node.leaderId().orElse("");
        String selfId = node.nodeId();

        var builder = MemberListResponse.newBuilder()
            .setError(okError())
            .setLeaderId(leaderId);

        for (String voterId : membership.voters()) {
            builder.addMembers(buildMember(voterId, MemberRole.VOTER, leaderId, selfId));
        }
        for (String learnerId : membership.learners()) {
            builder.addMembers(buildMember(learnerId, MemberRole.LEARNER, leaderId, selfId));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private Member buildMember(String nodeId, MemberRole role, String leaderId, String selfId) {
        String raftAddress = nodeId.equals(selfId)
            ? selfRaftAddress
            : raftPeerAddresses.getOrDefault(nodeId, "");
        return Member.newBuilder()
            .setNodeId(nodeId)
            .setRaftAddress(raftAddress)
            .setRole(role)
            .setIsLeader(nodeId.equals(leaderId))
            .setIsSelf(nodeId.equals(selfId))
            .build();
    }

    private static NodeRole toProtoRole(io.partdb.node.NodeRole role) {
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
