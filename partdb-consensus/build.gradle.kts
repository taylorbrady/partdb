plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-cluster"))
    implementation(project(":partdb-raft"))
}
