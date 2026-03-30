package io.partdb.transport.grpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PeerEndpointTest {

    @Test
    void parseBuildsHostAndPort() {
        var endpoint = PeerEndpoint.parse("127.0.0.1:8100");

        assertEquals("127.0.0.1", endpoint.host());
        assertEquals(8100, endpoint.port());
        assertEquals("127.0.0.1:8100", endpoint.toString());
    }

    @Test
    void parseNormalizesBracketedIpv6() {
        var endpoint = PeerEndpoint.parse("[::1]:8100");

        assertEquals("::1", endpoint.host());
        assertEquals(8100, endpoint.port());
        assertEquals("[::1]:8100", endpoint.toString());
    }

    @Test
    void parseRejectsMissingPort() {
        assertThrows(IllegalArgumentException.class, () -> PeerEndpoint.parse("127.0.0.1"));
    }
}
