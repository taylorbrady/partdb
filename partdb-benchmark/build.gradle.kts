plugins {
    java
    application
}

val jmhVersion = "1.37"

dependencies {
    implementation(project(":partdb-common"))
    implementation(project(":partdb-storage"))
    implementation("org.openjdk.jmh:jmh-core:$jmhVersion")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")
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

tasks.register<JavaExec>("benchmark") {
    group = "benchmark"
    description = "Run JMH benchmarks"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    args = listOf("-l")
}
