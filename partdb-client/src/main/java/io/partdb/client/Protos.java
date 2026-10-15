package io.partdb.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.cluster.v1.ListMembersResponse;

final class Protos {

    private Protos() {
    }

    static ByteString bytes(Bytes value) {
        return UnsafeByteOperations.unsafeWrap(value.asReadOnlyByteBuffer());
    }

    static Bytes bytes(ByteString value) {
        return Bytes.copyOf(value.asReadOnlyByteBuffer());
    }

    static KeyValue keyValue(io.partdb.grpc.kv.v1.KeyValue proto) {
        return new KeyValue(bytes(proto.getKey()), bytes(proto.getValue()), proto.getRevision());
    }

    static io.partdb.grpc.kv.v1.ReadConsistency consistency(ReadConsistency consistency) {
        return switch (consistency) {
            case LINEARIZABLE -> io.partdb.grpc.kv.v1.ReadConsistency.READ_CONSISTENCY_LINEARIZABLE;
            case LOCAL -> io.partdb.grpc.kv.v1.ReadConsistency.READ_CONSISTENCY_LOCAL;
        };
    }

    static io.partdb.grpc.kv.v1.Mutation mutation(Mutation mutation) {
        var builder = io.partdb.grpc.kv.v1.Mutation.newBuilder();
        switch (mutation) {
            case Mutation.Put(var key, var value) -> builder.setPut(io.partdb.grpc.kv.v1.Mutation.Put.newBuilder()
                .setKey(bytes(key)).setValue(bytes(value)));
            case Mutation.Delete(var key) -> builder.setDelete(io.partdb.grpc.kv.v1.Mutation.Delete.newBuilder().setKey(bytes(key)));
        }
        return builder.build();
    }

    static NodeStatus status(io.partdb.grpc.cluster.v1.NodeStatus proto) {
        return new NodeStatus(
            proto.getNodeId(),
            role(proto.getRole()),
            proto.getTerm(),
            proto.hasLeaderId() ? java.util.Optional.of(proto.getLeaderId()) : java.util.Optional.empty(),
            proto.getCommitIndex(),
            proto.getAppliedIndex(),
            proto.getSnapshotIndex()
        );
    }

    static Members members(ListMembersResponse proto) {
        return new Members(
            proto.getMembersList().stream().map(member -> new Member(member.getNodeId(), memberRole(member.getRole()))).toList(),
            proto.hasLeaderId() ? java.util.Optional.of(proto.getLeaderId()) : java.util.Optional.empty()
        );
    }

    private static Role role(io.partdb.grpc.cluster.v1.Role role) {
        return switch (role) {
            case ROLE_FOLLOWER -> Role.FOLLOWER;
            case ROLE_PRE_CANDIDATE -> Role.PRE_CANDIDATE;
            case ROLE_CANDIDATE -> Role.CANDIDATE;
            case ROLE_LEADER -> Role.LEADER;
            case ROLE_UNSPECIFIED, UNRECOGNIZED -> throw new PartDbClientException.Internal("the server reported an unknown role: " + role, null);
        };
    }

    private static MemberRole memberRole(io.partdb.grpc.cluster.v1.MemberRole role) {
        return switch (role) {
            case MEMBER_ROLE_VOTER -> MemberRole.VOTER;
            case MEMBER_ROLE_LEARNER -> MemberRole.LEARNER;
            case MEMBER_ROLE_UNSPECIFIED, UNRECOGNIZED ->
                throw new PartDbClientException.Internal("the server reported an unknown member role: " + role, null);
        };
    }
}
