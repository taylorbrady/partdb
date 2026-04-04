plugins {
    application
}

val logbackVersion = "1.5.32"
val logstashEncoderVersion = "9.0"

dependencies {
    implementation(project(":partdb-server"))
    implementation(project(":partdb-client"))

    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")
}

application {
    mainClass.set("io.partdb.app.PartDbApp")
    applicationDefaultJvmArgs = listOf("--sun-misc-unsafe-memory-access=allow")
}

testing {
    suites {
        register<JvmTestSuite>("packagedIntegrationTest") {
            useJUnitJupiter()

            dependencies {
                implementation(project(":partdb-client"))
                runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
            }

            targets {
                all {
                    testTask.configure {
                        dependsOn(tasks.named("installDist"))
                        shouldRunAfter(tasks.named("test"))
                        maxParallelForks = 1
                        jvmArgs("--sun-misc-unsafe-memory-access=allow")
                        systemProperty(
                            "partdb.app.installDir",
                            layout.buildDirectory.dir("install/partdb-app").get().asFile.absolutePath
                        )
                    }
                }
            }
        }
    }
}
