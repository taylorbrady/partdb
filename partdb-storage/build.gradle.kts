plugins {
    id("partdb.java-conventions")
    `java-library`
    id("partdb.jmh-conventions")
}

dependencies {
    api(project(":partdb-bytes"))

    implementation(libs.slf4j.api)
}
