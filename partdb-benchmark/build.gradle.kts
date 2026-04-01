plugins {
    id("me.champeau.jmh") version "0.7.2"
}

dependencies {
    jmh(project(":partdb-storage"))

    val logbackVersion = "1.5.32"
    val logstashEncoderVersion = "9.0"

    jmhRuntimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    jmhRuntimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}

jmh {
    jmhVersion = "1.37"
    warmupIterations = 3
    iterations = 5
    fork = 1
    timeOnIteration = "5s"
    warmup = "3s"
    resultFormat = "JSON"
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
    humanOutputFile.set(layout.buildDirectory.file("reports/jmh/results.txt"))
    duplicateClassesStrategy = DuplicatesStrategy.EXCLUDE
    jvmArgsAppend = listOf(
        "--sun-misc-unsafe-memory-access=allow",
        "-Xms2g",
        "-Xmx2g"
    )
}

val jmhJar = tasks.named<org.gradle.jvm.tasks.Jar>("jmhJar")

tasks.register<JavaExec>("benchmarkList") {
    group = "benchmark"
    description = "List available JMH benchmarks"
    dependsOn(jmhJar)
    classpath = files(jmhJar.flatMap { it.archiveFile })
    mainClass.set("org.openjdk.jmh.Main")
    args = listOf("-l")
}
