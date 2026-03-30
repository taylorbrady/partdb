package io.partdb.app;

import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMemberRole;
import io.partdb.client.ClusterMembership;
import io.partdb.client.ServerEndpoint;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemberCommandTest {

    @Test
    void toJsonEscapesStringValues() {
        var membership = new ClusterMembership(
            Optional.of("leader\"1"),
            List.of(new ClusterMember(
                "node\n1",
                Optional.of(new ServerEndpoint("::1", 8100)),
                ClusterMemberRole.VOTER,
                true,
                false
            ))
        );

        assertEquals(
            "{\"leaderId\":\"leader\\\"1\",\"members\":[{\"nodeId\":\"node\\n1\","
                + "\"raftAddress\":\"[::1]:8100\",\"role\":\"voter\","
                + "\"isLeader\":true,\"isSelf\":false}]}",
            MemberCommand.toJson(membership)
        );
    }
}
