import java.time.Duration

plugins {
    id("partdb.java-conventions")
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
}

val simulationTest by tasks.registering(Test::class) {
    description = "Runs the deterministic Raft simulation across many seeds."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    useJUnitPlatform()
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
        includeTestsMatching("io.partdb.raft.SimulationTest")
    }
    systemProperty("partdb.raft.simulation.seeds", providers.gradleProperty("seeds").getOrElse("5000"))
    systemProperty("partdb.raft.simulation.firstSeed", providers.gradleProperty("firstSeed").getOrElse("0"))
    timeout.set(Duration.ofHours(1))
}
