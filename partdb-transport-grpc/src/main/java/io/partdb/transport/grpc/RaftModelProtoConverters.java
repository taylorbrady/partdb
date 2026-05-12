package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.transport.grpc.raft.proto.RaftProto;

import java.util.Set;

final class RaftModelProtoConverters {

    static RaftLogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new RaftLogEntry.Data(
                proto.getIndex(),
                proto.getTerm(),
                Bytes.copyOf(proto.getData().toByteArray())
            );
            case NO_OP -> new RaftLogEntry.NoOp(
                proto.getIndex(),
                proto.getTerm()
            );
            case CONFIG -> new RaftLogEntry.Config(
                proto.getIndex(),
                proto.getTerm(),
                fromProto(proto.getConfig())
            );
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("Raft log entry type not set");
        };
    }

    static RaftProto.LogEntry toProto(RaftLogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());

        switch (entry) {
            case RaftLogEntry.Data data -> builder.setData(ByteString.copyFrom(data.data().asReadOnlyByteBuffer()));
            case RaftLogEntry.NoOp _ -> builder.setNoOp(true);
            case RaftLogEntry.Config config -> builder.setConfig(toProto(config.membership()));
        }

        return builder.build();
    }

    static RaftMembership fromProto(RaftProto.Membership proto) {
        return new RaftMembership(
            Set.copyOf(proto.getVotersList()),
            Set.copyOf(proto.getLearnersList())
        );
    }

    static RaftProto.Membership toProto(RaftMembership configuration) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(configuration.voters())
            .addAllLearners(configuration.learners())
            .build();
    }
}
