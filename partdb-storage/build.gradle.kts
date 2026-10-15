plugins {
    id("partdb.java-conventions")
    `java-library`
    id("partdb.jmh-conventions")
}

dependencies {
    api(project(":partdb-bytes"))

    implementation(libs.slf4j.api)
}

val jmhSourceSet = the<org.gradle.api.tasks.SourceSetContainer>().named("jmh")

tasks.register<JavaExec>("codecReport") {
    group = "benchmark"
    description = "Print block codec compression ratios for benchmark payloads"
    dependsOn(tasks.named("jmhClasses"))
    classpath = jmhSourceSet.get().runtimeClasspath
    mainClass.set("io.partdb.storage.CodecCompressionReport")
}
