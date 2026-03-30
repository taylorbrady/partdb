package io.partdb.app;

import io.partdb.client.ServerEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CliSupportTest {

    @Test
    void parseOutputFormatAcceptsKnownFormats() {
        assertEquals(OutputFormat.TEXT, CliSupport.parseOutputFormat("text"));
        assertEquals(OutputFormat.JSON, CliSupport.parseOutputFormat("json"));
    }

    @Test
    void parseOutputFormatRejectsUnknownFormats() {
        assertThrows(IllegalArgumentException.class, () -> CliSupport.parseOutputFormat("yaml"));
    }

    @Test
    void parseNodeEndpointSpecAcceptsBracketedIpv6() {
        var peer = CliSupport.parseNodeEndpointSpec("node1=[::1]:8100", "--raft-peer");

        assertEquals("node1", peer.nodeId());
        assertEquals("[::1]:8100", peer.endpoint());
    }

    @Test
    void defaultClusterClientConfigUsesTypedEndpointParsing() {
        var config = CliSupport.defaultClusterClientConfig("[::1]:8101");

        assertEquals(new ServerEndpoint("::1", 8101), config.endpoint());
    }
}
