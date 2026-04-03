package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.consensus.ClusterMembership;
import io.partdb.consensus.transport.ConsensusLogEntry;
import io.partdb.transport.grpc.raft.proto.RaftProto;

import java.util.Set;

final class ConsensusModelProtoConverters {

    static ConsensusLogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new ConsensusLogEntry.Data(
                proto.getIndex(),
                proto.getTerm(),
                Bytes.copyOf(proto.getData().toByteArray())
            );
            case NO_OP -> new ConsensusLogEntry.NoOp(
                proto.getIndex(),
                proto.getTerm()
            );
            case CONFIG -> new ConsensusLogEntry.Config(
                proto.getIndex(),
                proto.getTerm(),
                fromProto(proto.getConfig())
            );
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("LogEntry type not set");
        };
    }

    static RaftProto.LogEntry toProto(ConsensusLogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());

        switch (entry) {
            case ConsensusLogEntry.Data data -> builder.setData(ByteString.copyFrom(data.data().asReadOnlyByteBuffer()));
            case ConsensusLogEntry.NoOp _ -> builder.setNoOp(true);
            case ConsensusLogEntry.Config config -> builder.setConfig(toProto(config.membership()));
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
