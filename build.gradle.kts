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
        val slf4jVersion = "2.0.17"
        val junitVersion = "6.0.3"

        implementation("org.slf4j:slf4j-api:$slf4jVersion")

        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
        jvmArgs("--sun-misc-unsafe-memory-access=allow")
    }
}
