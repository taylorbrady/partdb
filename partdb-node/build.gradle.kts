plugins {
    id("partdb.java-conventions")
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-raft"))
    implementation(project(":partdb-storage"))
}
