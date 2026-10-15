plugins {
    alias(libs.plugins.protobuf) apply false
}

tasks.register("integrationTest") {
    group = "verification"
    description = "Runs in-process cross-module integration tests."
    dependsOn(":partdb-app:integrationTest")
}

tasks.register("packagedIntegrationTest") {
    group = "verification"
    description = "Runs packaged application integration tests."
    dependsOn(":partdb-app:packagedIntegrationTest")
}

tasks.register("ci") {
    group = "verification"
    description = "Runs all checks required before merge."
    dependsOn(subprojects.map { "${it.path}:check" }, "integrationTest", "packagedIntegrationTest")
}

tasks.register("jmhBaseline") {
    group = "benchmark"
    description = "Runs the curated PartDB JMH performance baseline."
    dependsOn(":partdb-storage:jmhBaseline", ":partdb-benchmarks:jmhBaseline")
}
