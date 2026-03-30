package io.partdb.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerEndpointTest {

    @Test
    void parseBuildsTypedEndpoint() {
        var endpoint = ServerEndpoint.parse("localhost:8101");

        assertEquals("localhost", endpoint.host());
        assertEquals(8101, endpoint.port());
        assertEquals("localhost:8101", endpoint.toString());
    }

    @Test
    void parseSupportsBracketedIpv6() {
        var endpoint = ServerEndpoint.parse("[::1]:8101");

        assertEquals("::1", endpoint.host());
        assertEquals(8101, endpoint.port());
        assertEquals("[::1]:8101", endpoint.toString());
    }

    @Test
    void parseRejectsMissingPort() {
        assertThrows(IllegalArgumentException.class, () -> ServerEndpoint.parse("localhost"));
    }

    @Test
    void tryParseReturnsEmptyForInvalidHint() {
        assertTrue(ServerEndpoint.tryParse("leader-1").isEmpty());
        assertFalse(ServerEndpoint.tryParse("localhost:8101").isEmpty());
    }

    @Test
    void clientConfigsUseTypedEndpoints() {
        var clusterConfig = ClusterClientConfig.defaultConfig("localhost:8101");
        var kvConfig = KvClientConfig.defaultConfig("localhost:8101", "localhost:8102");

        assertEquals(new ServerEndpoint("localhost", 8101), clusterConfig.endpoint());
        assertEquals(2, kvConfig.endpoints().size());
        assertEquals(new ServerEndpoint("localhost", 8102), kvConfig.endpoints().get(1));
    }
}
