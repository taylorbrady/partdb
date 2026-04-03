plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    implementation(project(":partdb-raft"))
}
