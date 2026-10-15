plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(libs.jmh.gradle.plugin)
}

java {
    sourceCompatibility = JavaVersion.toVersion(25)
    targetCompatibility = JavaVersion.toVersion(25)
}
