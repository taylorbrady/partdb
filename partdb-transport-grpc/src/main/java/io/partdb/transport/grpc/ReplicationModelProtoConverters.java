package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;
import io.partdb.node.replication.ReplicationLogEntry;
import io.partdb.transport.grpc.raft.proto.RaftProto;

import java.util.Set;

final class ReplicationModelProtoConverters {

    static ReplicationLogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new ReplicationLogEntry.Data(
                proto.getIndex(),
                proto.getTerm(),
                Bytes.copyOf(proto.getData().toByteArray())
            );
            case NO_OP -> new ReplicationLogEntry.NoOp(
                proto.getIndex(),
                proto.getTerm()
            );
            case CONFIG -> new ReplicationLogEntry.Config(
                proto.getIndex(),
                proto.getTerm(),
                fromProto(proto.getConfig())
            );
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("LogEntry type not set");
        };
    }

    static RaftProto.LogEntry toProto(ReplicationLogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());

        switch (entry) {
            case ReplicationLogEntry.Data data -> builder.setData(ByteString.copyFrom(data.data().asReadOnlyByteBuffer()));
            case ReplicationLogEntry.NoOp _ -> builder.setNoOp(true);
            case ReplicationLogEntry.Config config -> builder.setConfig(toProto(config.membership()));
        }

        return builder.build();
    }

    static ClusterMembership fromProto(RaftProto.Membership proto) {
        return new ClusterMembership(
            Set.copyOf(proto.getVotersList()),
            Set.copyOf(proto.getLearnersList())
        );
    }

    static RaftProto.Membership toProto(ClusterMembership membership) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(membership.voters())
            .addAllLearners(membership.learners())
            .build();
    }
}
