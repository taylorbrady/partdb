plugins {
    java
    application
}

val jmhVersion = "1.37"

dependencies {
    implementation(project(":partdb-storage"))
    implementation("org.openjdk.jmh:jmh-core:$jmhVersion")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")

    val logbackVersion = "1.5.23"
    val logstashEncoderVersion = "8.1"

    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}

application {
    mainClass.set("org.openjdk.jmh.Main")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.openjdk.jmh.Main"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    archiveClassifier.set("all")
}

tasks.register<JavaExec>("benchmarkList") {
    group = "benchmark"
    description = "List available JMH benchmarks"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    args = listOf("-l")
}

tasks.register<JavaExec>("benchmark") {
    group = "benchmark"
    description = "Run JMH benchmarks"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    jvmArgs = listOf("-Xms2g", "-Xmx2g")

    val benchArgs = mutableListOf("-rf", "json", "-rff", "build/benchmark-results.json")

    if (project.hasProperty("bench")) {
        benchArgs += project.property("bench").toString()
    }

    args = benchArgs
}
