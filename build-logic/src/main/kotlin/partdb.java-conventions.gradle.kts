import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import java.time.Duration

plugins {
    java
}

val libs = the<VersionCatalogsExtension>().named("libs")

group = "io.partdb"
version = "0.1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

testing {
    suites {
        withType<JvmTestSuite>().configureEach {
            useJUnitJupiter(libs.findVersion("junit").get().requiredVersion)
        }
    }
}

tasks.withType<Test>().configureEach {
    jvmArgs("--sun-misc-unsafe-memory-access=allow")
    timeout.set(Duration.ofMinutes(10))
    testLogging {
        events("failed")
        exceptionFormat = TestExceptionFormat.FULL
    }
}
