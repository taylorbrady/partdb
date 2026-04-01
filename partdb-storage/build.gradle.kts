plugins {
    `java-library`
    id("me.champeau.jmh") version "0.7.3"
}

dependencies {
    api(project(":partdb-bytes"))
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
val jmhSourceSet = the<org.gradle.api.tasks.SourceSetContainer>().named("jmh")

tasks.register<JavaExec>("benchmarkList") {
    group = "benchmark"
    description = "List available storage JMH benchmarks"
    dependsOn(jmhJar)
    classpath = files(jmhJar.flatMap { it.archiveFile })
    mainClass.set("org.openjdk.jmh.Main")
    args = listOf("-l")
}

tasks.register<JavaExec>("codecReport") {
    group = "benchmark"
    description = "Print block codec compression ratios for benchmark payloads"
    dependsOn(tasks.named("jmhClasses"))
    classpath = jmhSourceSet.get().runtimeClasspath
    mainClass.set("io.partdb.storage.CodecCompressionReport")
}
