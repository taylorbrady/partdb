package io.partdb.common.statemachine;

import io.partdb.common.ByteArray;

public sealed interface Operation permits Put, Delete {
    ByteArray key();
}
