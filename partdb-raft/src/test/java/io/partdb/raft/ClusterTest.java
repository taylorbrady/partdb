package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterTest {

    private static String follower(ClusterHarness cluster, String leader) {
        return cluster.nodes().stream().map(raft -> raft.id().value()).filter(id -> !id.equals(leader)).findFirst().orElseThrow();
    }

    @Nested
    class Elections {
        @Test
        void electsExactlyOneLeader() {
            for (int size : new int[]{1, 3, 5}) {
                var cluster = ClusterHarness.create(size);
                String leader = cluster.electLeader();
                long term = cluster.node(leader).term();

                cluster.run(50);

                assertEquals(1, cluster.leaderCount(term));
                assertTrue(cluster.nodes().stream().allMatch(raft -> raft.leaderId().orElseThrow().equals(Ids.id(leader))));
            }
        }

        @Test
        void partitionedNodeDoesNotInflateTerm() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String isolated = follower(cluster, leader);
            long term = cluster.node(leader).term();

            cluster.isolate(isolated);
            cluster.run(100);
            cluster.heal(isolated);
            cluster.run(20);

            assertEquals(term, cluster.node(isolated).term());
            assertEquals(leader, cluster.leader().orElseThrow());
        }

        @Test
        void majorityElectsNewLeaderWhenLeaderIsIsolated() {
            var cluster = ClusterHarness.create(3);
            String oldLeader = cluster.electLeader();

            cluster.isolate(oldLeader);
            cluster.runUntil(c -> c.leader().filter(id -> !id.equals(oldLeader)).isPresent(), 100);
            String newLeader = cluster.leader().orElseThrow();
            assertNotEquals(oldLeader, newLeader);

            cluster.heal(oldLeader);
            cluster.run(20);
            assertEquals(Role.FOLLOWER, cluster.node(oldLeader).role());
            assertEquals(Ids.id(newLeader), cluster.node(oldLeader).leaderId().orElseThrow());
        }

        @Test
        void minorityCannotElectLeader() {
            var cluster = ClusterHarness.create(5);
            String leader = cluster.electLeader();
            var minority = cluster.nodes().stream().map(raft -> raft.id().value()).filter(id -> !id.equals(leader)).limit(2).toList();
            cluster.isolate(leader);
            cluster.nodes().stream().map(raft -> raft.id().value()).filter(id -> !minority.contains(id) && !id.equals(leader)).forEach(cluster::isolate);

            cluster.run(100);

            assertTrue(minority.stream().noneMatch(id -> cluster.node(id).role() == Role.LEADER));
        }

        @Test
        void leadershipTransferMovesLeadership() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String target = follower(cluster, leader);
            cluster.propose(leader, "a");
            cluster.run(5);

            cluster.transferLeadership(leader, target);
            cluster.runUntil(c -> c.leader().filter(target::equals).isPresent(), 20);

            assertEquals(Role.FOLLOWER, cluster.node(leader).role());
        }
    }

    @Nested
    class Replication {
        @Test
        void committedEntriesReachEveryNodeInOrder() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();

            for (int i = 0; i < 20; i++) {
                cluster.propose(leader, "v" + i);
            }
            cluster.run(10);

            var expected = cluster.appliedCommands(leader);
            assertEquals(20, expected.size());
            for (Raft raft : cluster.nodes()) {
                assertEquals(expected, cluster.appliedCommands(raft.id().value()));
            }
        }

        @Test
        void commitsWithMajorityWhileOneFollowerIsDown() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String down = follower(cluster, leader);
            cluster.isolate(down);

            cluster.propose(leader, "a");
            cluster.run(5);

            assertEquals(1, cluster.appliedCommands(leader).size());
            assertTrue(cluster.appliedCommands(down).isEmpty());
        }

        @Test
        void laggingFollowerCatchesUpAfterHealing() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String lagging = follower(cluster, leader);
            cluster.isolate(lagging);
            for (int i = 0; i < 50; i++) {
                cluster.propose(leader, "v" + i);
            }
            cluster.run(5);

            cluster.heal(lagging);
            cluster.run(10);

            assertEquals(cluster.appliedCommands(leader), cluster.appliedCommands(lagging));
        }

        @Test
        void laggingFollowerInstallsSnapshotAfterCompaction() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String lagging = follower(cluster, leader);
            cluster.isolate(lagging);
            for (int i = 0; i < 5; i++) {
                cluster.propose(leader, "v" + i);
            }
            cluster.run(5);
            long compacted = cluster.node(leader).appliedIndex();
            cluster.compact(leader);

            cluster.heal(lagging);
            cluster.run(20);

            assertEquals(compacted, cluster.log(lagging).snapshot().orElseThrow().index());
            assertEquals(cluster.node(leader).commitIndex(), cluster.node(lagging).commitIndex());
        }

        @Test
        void learnerReplicatesWithoutAffectingQuorum() {
            var cluster = ClusterHarness.create(2, 1);
            String leader = cluster.electLeader();
            String voter = cluster.nodes().stream().map(raft -> raft.id().value())
                .filter(id -> !id.equals(leader) && cluster.node(id).membership().isVoter(Ids.id(id))).findFirst().orElseThrow();

            cluster.propose(leader, "a");
            cluster.run(5);
            assertEquals(1, cluster.appliedCommands("n2").size());

            cluster.isolate("n2");
            cluster.propose(leader, "b");
            cluster.run(5);
            assertEquals(2, cluster.appliedCommands(voter).size());
            assertEquals(Role.FOLLOWER, cluster.node("n2").role());
        }
    }

    @Nested
    class Reads {
        @Test
        void leaderAndFollowersConfirmReads() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            cluster.run(5);

            for (Raft raft : cluster.nodes()) {
                long read = cluster.readIndex(raft.id().value());
                cluster.run(5);
                var results = cluster.drainOutcomes(raft.id().value());
                assertEquals(1, results.size());
                var confirmed = assertInstanceOf(Outcome.ReadReady.class, results.getFirst());
                assertEquals(read, confirmed.id());
                assertTrue(confirmed.index() >= 1);
            }
            assertEquals(leader, cluster.leader().orElseThrow());
        }

        @Test
        void isolatedFollowerEventuallyRejectsRead() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            String isolated = follower(cluster, leader);
            cluster.isolate(isolated);

            cluster.readIndex(isolated);
            cluster.run(30);

            assertEquals(List.of(Outcome.Rejected.class),
                cluster.drainOutcomes(isolated).stream().map(Object::getClass).toList());
        }
    }

    @Nested
    class Membership {
        @Test
        void addsLearnerAndPromotesIt() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            cluster.run(5);
            cluster.add("n3", Ids.voters("n0", "n1", "n2"), 0);

            cluster.changeMembership(leader, new MembershipChange.AddLearner(Ids.id("n3")));
            cluster.run(10);
            assertTrue(cluster.node(leader).membership().isLearner(Ids.id("n3")));

            cluster.changeMembership(leader, new MembershipChange.PromoteToVoter(Ids.id("n3")));
            cluster.run(10);

            for (Raft raft : cluster.nodes()) {
                assertTrue(raft.membership().isVoter(Ids.id("n3")), raft.id().value());
            }
            cluster.propose(leader, "a");
            cluster.run(5);
            assertEquals(1, cluster.appliedCommands("n3").size());
        }

        @Test
        void leaderRemovesItselfAndHandsOffLeadership() {
            var cluster = ClusterHarness.create(3);
            String leader = cluster.electLeader();
            cluster.run(5);

            long change = cluster.changeMembership(leader, new MembershipChange.RemoveNode(Ids.id(leader)));
            cluster.runUntil(c -> c.leader().filter(id -> !id.equals(leader)).isPresent(), 50);

            assertEquals(List.of(), cluster.drainOutcomes(leader).stream().filter(o -> o instanceof Outcome.Rejected).toList());
            String successor = cluster.leader().orElseThrow();
            for (Raft raft : cluster.nodes()) {
                if (!raft.id().value().equals(leader)) {
                    assertFalse(raft.membership().isMember(Ids.id(leader)), raft.id().value());
                }
            }
            assertTrue(change > 0);
            cluster.run(100);
            assertEquals(Optional.of(Ids.id(successor)), cluster.node(successor).leaderId());
            cluster.propose(successor, "after");
            cluster.run(5);
            assertEquals(1, cluster.appliedCommands(successor).size());
        }
    }
}
