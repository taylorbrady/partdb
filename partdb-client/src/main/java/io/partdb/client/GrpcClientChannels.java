package io.partdb.client;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class GrpcClientChannels {
    private static final Duration DEFAULT_KEEPALIVE_TIME = Duration.ofSeconds(30);
    private static final Duration DEFAULT_KEEPALIVE_TIMEOUT = Duration.ofSeconds(10);

    private GrpcClientChannels() {}

    static ManagedChannel createChannel(ServerEndpoint endpoint, Duration connectTimeout) {
        int connectTimeoutMillis = Math.toIntExact(connectTimeout.toMillis());
        return NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
            .usePlaintext()
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
            .keepAliveTime(DEFAULT_KEEPALIVE_TIME.toMillis(), TimeUnit.MILLISECONDS)
            .keepAliveTimeout(DEFAULT_KEEPALIVE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .build();
    }
}
