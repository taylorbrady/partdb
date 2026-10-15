plugins {
    id("partdb.java-conventions")
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))

    implementation(libs.slf4j.api)
}
