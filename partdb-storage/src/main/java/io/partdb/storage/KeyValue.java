package io.partdb.storage;

import io.partdb.common.Timestamp;

public record KeyValue(byte[] key, byte[] value, Timestamp version) {}
