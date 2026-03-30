plugins {
    application
}

dependencies {
    implementation(project(":partdb-transport-grpc"))
    implementation(project(":partdb-client"))

    val logbackVersion = "1.5.32"
    val logstashEncoderVersion = "9.0"

    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}

application {
    mainClass.set("io.partdb.app.PartDbApp")
    applicationDefaultJvmArgs = listOf("--sun-misc-unsafe-memory-access=allow")
}
