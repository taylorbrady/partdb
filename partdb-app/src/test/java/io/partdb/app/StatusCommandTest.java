package io.partdb.app;

import io.partdb.client.ClusterNodeRole;
import io.partdb.client.ClusterStatus;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatusCommandTest {

    @Test
    void toJsonEscapesStringValues() {
        var status = new ClusterStatus(
            "node\"1",
            ClusterNodeRole.LEADER,
            7,
            Optional.of("leader\n1"),
            42,
            40,
            true
        );

        assertEquals(
            "{\"nodeId\":\"node\\\"1\",\"role\":\"LEADER\",\"term\":7,"
                + "\"leaderId\":\"leader\\n1\",\"commitIndex\":42,"
                + "\"lastAppliedIndex\":40,\"isRunning\":true}",
            StatusCommand.toJson(status)
        );
    }
}
