package io.partdb.client;

public record KeyValue(byte[] key, byte[] value, long revision) {}
