package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;

public record KeyValue(ByteArray key, ByteArray value, Timestamp version) {}
