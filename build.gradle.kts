plugins {
    java
    id("com.google.protobuf") version "0.9.6" apply false
}

allprojects {
    group = "io.partdb"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    dependencies {
        val junitVersion = "6.0.3"

        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
        jvmArgs("--sun-misc-unsafe-memory-access=allow")
    }
}

tasks.register("integrationTest") {
    group = "verification"
    description = "Runs in-process cross-module integration tests."
    dependsOn(":partdb-app:integrationTest")
}

tasks.register("packagedIntegrationTest") {
    group = "verification"
    description = "Runs packaged application integration tests."
    dependsOn(":partdb-app:packagedIntegrationTest")
}

tasks.register("ci") {
    group = "verification"
    description = "Runs all checks required before merge."
    dependsOn("check", "integrationTest", "packagedIntegrationTest")
}
