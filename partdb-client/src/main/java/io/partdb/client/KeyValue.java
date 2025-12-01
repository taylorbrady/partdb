package io.partdb.client;

import io.partdb.common.ByteArray;

public record KeyValue(ByteArray key, ByteArray value, long revision) {}
