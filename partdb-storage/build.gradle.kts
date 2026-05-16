plugins {
    `java-library`
    id("partdb.jmh-conventions")
}

dependencies {
    api(project(":partdb-bytes"))

    implementation("org.slf4j:slf4j-api:2.0.17")
}

val jmhSourceSet = the<org.gradle.api.tasks.SourceSetContainer>().named("jmh")

tasks.register<JavaExec>("codecReport") {
    group = "benchmark"
    description = "Print block codec compression ratios for benchmark payloads"
    dependsOn(tasks.named("jmhClasses"))
    classpath = jmhSourceSet.get().runtimeClasspath
    mainClass.set("io.partdb.storage.internal.CodecCompressionReport")
}
