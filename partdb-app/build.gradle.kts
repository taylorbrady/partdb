plugins {
    application
}

dependencies {
    implementation(project(":partdb-transport-grpc"))
    implementation(project(":partdb-client"))

    val logbackVersion = "1.5.23"
    val logstashEncoderVersion = "8.1"

    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}

application {
    mainClass.set("io.partdb.app.PartDbApp")
}
