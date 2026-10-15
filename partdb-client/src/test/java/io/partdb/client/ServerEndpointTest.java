package io.partdb.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void builderParsesAndDeduplicatesEndpoints() {
        try (var client = PartDbClient.builder().endpoints("localhost:8101", "localhost:8102", "localhost:8101").build()) {
            assertEquals(List.of(new ServerEndpoint("localhost", 8101), new ServerEndpoint("localhost", 8102)), client.endpoints());
        }
    }

    @Test
    void builderRequiresAnEndpoint() {
        assertThrows(IllegalArgumentException.class, () -> PartDbClient.builder().build());
    }
}
