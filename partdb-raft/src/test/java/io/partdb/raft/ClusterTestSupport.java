package io.partdb.raft;

abstract class ClusterTestSupport {

    void electLeader(ClusterHarness cluster) {
        cluster.runUntil(harness -> harness.leader().isPresent(), 50);
    }
}
