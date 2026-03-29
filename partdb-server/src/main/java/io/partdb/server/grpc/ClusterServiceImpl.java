package io.partdb.server.grpc;

import io.grpc.stub.StreamObserver;
import io.partdb.node.raft.RaftNode;
import io.partdb.protocol.cluster.proto.ClusterProto.Error;
import io.partdb.protocol.cluster.proto.ClusterProto.ErrorCode;
import io.partdb.protocol.cluster.proto.ClusterProto.Member;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberListRequest;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberListResponse;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberRole;
import io.partdb.protocol.cluster.proto.ClusterProto.NodeRole;
import io.partdb.protocol.cluster.proto.ClusterProto.StatusRequest;
import io.partdb.protocol.cluster.proto.ClusterProto.StatusResponse;
import io.partdb.protocol.cluster.proto.ClusterServiceGrpc;
import io.partdb.raft.Membership;
import io.partdb.raft.Role;

import java.util.Map;

public final class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {

    private final RaftNode raftNode;
    private final Map<String, String> peerAddresses;
    private final String selfAddress;

    public ClusterServiceImpl(RaftNode raftNode, Map<String, String> peerAddresses, String selfAddress) {
        this.raftNode = raftNode;
        this.peerAddresses = Map.copyOf(peerAddresses);
        this.selfAddress = selfAddress;
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusResponse> responseObserver) {
        var response = StatusResponse.newBuilder()
            .setError(okError())
            .setNodeId(raftNode.nodeId())
            .setRole(toProtoRole(raftNode.role()))
            .setTerm(raftNode.currentTerm())
            .setLeaderId(raftNode.leaderId().orElse(""))
            .setCommitIndex(raftNode.commitIndex())
            .setLastAppliedIndex(raftNode.lastAppliedIndex())
            .setIsRunning(raftNode.isRunning())
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void memberList(MemberListRequest request, StreamObserver<MemberListResponse> responseObserver) {
        Membership membership = raftNode.membership();
        String leaderId = raftNode.leaderId().orElse("");
        String selfId = raftNode.nodeId();

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
        String address = nodeId.equals(selfId) ? selfAddress : peerAddresses.getOrDefault(nodeId, "");
        return Member.newBuilder()
            .setNodeId(nodeId)
            .setAddress(address)
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
