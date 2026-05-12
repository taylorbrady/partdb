plugins {
    id("partdb.jmh-conventions")
}

dependencies {
    jmh(project(":partdb-storage"))

    val logbackVersion = "1.5.32"
    val logstashEncoderVersion = "9.0"

    jmhRuntimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    jmhRuntimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}
