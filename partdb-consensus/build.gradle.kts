plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-raft"))

    implementation("org.slf4j:slf4j-api:2.0.17")
}
