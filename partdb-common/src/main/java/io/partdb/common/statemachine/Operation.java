package io.partdb.common.statemachine;

public sealed interface Operation permits Put, Delete, GrantLease, RevokeLease, KeepAliveLease {
}
