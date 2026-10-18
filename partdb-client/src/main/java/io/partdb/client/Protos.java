package io.partdb.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.partdb.bytes.Bytes;
import io.partdb.grpc.cluster.v1.ListMembersResponse;
import io.partdb.grpc.kv.v1.TxnRequest;
import io.partdb.grpc.kv.v1.TxnResponse;

import java.util.Optional;

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

    private static io.partdb.grpc.kv.v1.Condition condition(Condition condition) {
        var builder = io.partdb.grpc.kv.v1.Condition.newBuilder();
        switch (condition) {
            case Condition.Exists(var key) -> builder.setExists(io.partdb.grpc.kv.v1.Condition.Exists.newBuilder().setKey(bytes(key)));
            case Condition.Missing(var key) -> builder.setMissing(io.partdb.grpc.kv.v1.Condition.Missing.newBuilder().setKey(bytes(key)));
            case Condition.ValueEquals(var key, var value) -> builder.setValueEquals(io.partdb.grpc.kv.v1.Condition.ValueEquals.newBuilder()
                .setKey(bytes(key)).setValue(bytes(value)));
            case Condition.RevisionEquals(var key, long revision) -> builder.setRevisionEquals(io.partdb.grpc.kv.v1.Condition.RevisionEquals.newBuilder()
                .setKey(bytes(key)).setRevision(revision));
        }
        return builder.build();
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

    static TxnRequest txn(Txn txn) {
        return TxnRequest.newBuilder()
            .addAllConditions(txn.conditions().stream().map(Protos::condition).toList())
            .addAllMutations(txn.mutations().stream().map(Protos::mutation).toList())
            .build();
    }

    static TxnResult result(TxnResponse response) {
        return switch (response.getResultCase()) {
            case COMMITTED -> new TxnResult.Committed(response.getCommitted().getRevision());
            case CONDITION_FAILED -> new TxnResult.ConditionFailed();
            case RESULT_NOT_SET -> throw new PartDbException.Internal("the server sent a transaction response without a result", null);
        };
    }

    static NodeStatus status(io.partdb.grpc.cluster.v1.NodeStatus proto) {
        return new NodeStatus(
            proto.getNodeId(),
            role(proto.getRole()),
            proto.getTerm(),
            proto.hasLeaderId() ? Optional.of(proto.getLeaderId()) : Optional.empty(),
            proto.getCommitIndex(),
            proto.getAppliedIndex(),
            proto.getSnapshotIndex()
        );
    }

    static Membership membership(ListMembersResponse proto) {
        return new Membership(
            proto.getMembersList().stream().map(member -> new Member(member.getNodeId(), memberRole(member.getRole()))).toList(),
            proto.hasLeaderId() ? Optional.of(proto.getLeaderId()) : Optional.empty()
        );
    }

    private static RaftRole role(io.partdb.grpc.cluster.v1.RaftRole role) {
        return switch (role) {
            case RAFT_ROLE_FOLLOWER -> RaftRole.FOLLOWER;
            case RAFT_ROLE_PRE_CANDIDATE -> RaftRole.PRE_CANDIDATE;
            case RAFT_ROLE_CANDIDATE -> RaftRole.CANDIDATE;
            case RAFT_ROLE_LEADER -> RaftRole.LEADER;
            case RAFT_ROLE_UNSPECIFIED, UNRECOGNIZED -> throw new PartDbException.Internal("the server reported an unknown role: " + role, null);
        };
    }

    private static MemberRole memberRole(io.partdb.grpc.cluster.v1.MemberRole role) {
        return switch (role) {
            case MEMBER_ROLE_VOTER -> MemberRole.VOTER;
            case MEMBER_ROLE_LEARNER -> MemberRole.LEARNER;
            case MEMBER_ROLE_UNSPECIFIED, UNRECOGNIZED ->
                throw new PartDbException.Internal("the server reported an unknown member role: " + role, null);
        };
    }
}
