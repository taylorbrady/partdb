import me.champeau.jmh.JmhParameters
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.jvm.tasks.Jar

plugins {
    id("me.champeau.jmh")
}

val partdbJmhVersion = "1.37"
val jmhJvmArgs = listOf(
    "--sun-misc-unsafe-memory-access=allow",
    "-Xms2g",
    "-Xmx2g"
)

extensions.configure<JmhParameters>("jmh") {
    jmhVersion.set(partdbJmhVersion)
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(1)
    timeOnIteration.set("5s")
    warmup.set("3s")
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
    humanOutputFile.set(layout.buildDirectory.file("reports/jmh/results.txt"))
    duplicateClassesStrategy.set(DuplicatesStrategy.EXCLUDE)
    jvmArgsAppend.set(jmhJvmArgs)
}

val jmhJar = tasks.named<Jar>("jmhJar")

tasks.register<JavaExec>("benchmarkList") {
    group = "benchmark"
    description = "List available JMH benchmarks."
    dependsOn(jmhJar)
    classpath = files(jmhJar.flatMap { it.archiveFile })
    mainClass.set("org.openjdk.jmh.Main")
    args = listOf("-l")
}

tasks.register<JavaExec>("jmhQuick") {
    group = "benchmark"
    description = "Run a short JMH smoke benchmark for this module."
    dependsOn(jmhJar)
    classpath = files(jmhJar.flatMap { it.archiveFile })
    mainClass.set("org.openjdk.jmh.Main")
    jvmArgs(jmhJvmArgs)
    doFirst {
        layout.buildDirectory.dir("reports/jmh").get().asFile.mkdirs()
    }
    args = listOf(
        "-wi", "1",
        "-i", "1",
        "-w", "1s",
        "-r", "1s",
        "-f", "1",
        "-foe", "true",
        "-rf", "JSON",
        "-rff", layout.buildDirectory.file("reports/jmh/quick-results.json").get().asFile.absolutePath,
        "-o", layout.buildDirectory.file("reports/jmh/quick-results.txt").get().asFile.absolutePath
    ) + when (project.name) {
        "partdb-storage" -> listOf(".*BlockCodecBenchmark.compress.*")
        "partdb-benchmarks" -> listOf(".*StoragePointReadBenchmark.hotHit.*")
        else -> emptyList()
    }
}
