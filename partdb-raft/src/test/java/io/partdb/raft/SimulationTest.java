package io.partdb.raft;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulationTest {

    private static final long SEEDS = Long.getLong("partdb.raft.simulation.seeds", 200);
    private static final long FIRST_SEED = Long.getLong("partdb.raft.simulation.firstSeed", 0);

    @TestFactory
    Stream<DynamicTest> randomizedClusterHistoriesAreLinearizable() {
        return LongStream.range(FIRST_SEED, FIRST_SEED + SEEDS).mapToObj(seed -> DynamicTest.dynamicTest("seed " + seed, () -> {
            var report = new Simulation(seed, Simulation.Settings.forSeed(seed)).run();
            assertTrue(report.completedWrites() > 0, "no writes completed: " + report);
            assertTrue(report.completedReads() > 0, "no reads completed: " + report);
        }));
    }
}
