plugins {
    id("partdb.java-conventions")
    application
    `java-test-fixtures`
}

version = "0.1.0-SNAPSHOT"

dependencies {
    implementation(project(":partdb-server"))
    implementation(project(":partdb-client"))

    testFixturesApi(project(":partdb-client"))

    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
}

tasks.jar {
    manifest {
        attributes("Implementation-Version" to project.version)
    }
}

application {
    mainClass.set("io.partdb.app.PartDbApp")
    applicationDefaultJvmArgs = listOf("--sun-misc-unsafe-memory-access=allow")
}

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            dependencies {
                implementation(project())
                implementation(project(":partdb-server"))
                implementation(testFixtures(project()))
            }

            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(tasks.named("test"))
                        maxParallelForks = 1
                    }
                }
            }
        }

        register<JvmTestSuite>("packagedIntegrationTest") {
            dependencies {
                implementation(testFixtures(project()))
            }

            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(tasks.named("integrationTest"))
                        maxParallelForks = 1

                        val installDir = tasks.installDist.map { it.destinationDir }
                        inputs.dir(installDir).withPropertyName("installDir")
                        jvmArgumentProviders.add(CommandLineArgumentProvider {
                            listOf("-Dpartdb.app.installDir=" + installDir.get().absolutePath)
                        })
                    }
                }
            }
        }
    }
}
