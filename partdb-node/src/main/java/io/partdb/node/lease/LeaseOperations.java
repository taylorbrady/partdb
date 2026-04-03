package io.partdb.node.lease;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public interface LeaseOperations {
    CompletionStage<LeaseGrant> grant(Duration ttl);

    CompletionStage<LeaseKeepAliveResult> keepAlive(LeaseId leaseId);

    CompletionStage<LeaseRevokeResult> revoke(LeaseId leaseId);
}
