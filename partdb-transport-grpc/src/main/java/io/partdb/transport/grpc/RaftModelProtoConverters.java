package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftConfiguration;
import io.partdb.transport.grpc.raft.proto.RaftProto;

import java.util.Set;

final class RaftModelProtoConverters {

    static LogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new LogEntry.Data(
                proto.getIndex(),
                proto.getTerm(),
                Bytes.copyOf(proto.getData().toByteArray())
            );
            case NO_OP -> new LogEntry.NoOp(
                proto.getIndex(),
                proto.getTerm()
            );
            case CONFIG -> new LogEntry.Config(
                proto.getIndex(),
                proto.getTerm(),
                fromProto(proto.getConfig())
            );
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("LogEntry type not set");
        };
    }

    static RaftProto.LogEntry toProto(LogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());

        switch (entry) {
            case LogEntry.Data data -> builder.setData(ByteString.copyFrom(data.data().asReadOnlyByteBuffer()));
            case LogEntry.NoOp _ -> builder.setNoOp(true);
            case LogEntry.Config config -> builder.setConfig(toProto(config.configuration()));
        }

        return builder.build();
    }

    static RaftConfiguration fromProto(RaftProto.Membership proto) {
        return new RaftConfiguration(
            Set.copyOf(proto.getVotersList()),
            Set.copyOf(proto.getLearnersList())
        );
    }

    static RaftProto.Membership toProto(RaftConfiguration configuration) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(configuration.voters())
            .addAllLearners(configuration.learners())
            .build();
    }
}
