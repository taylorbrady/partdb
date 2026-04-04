plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-cluster"))
    api(project(":partdb-consensus"))
    implementation(project(":partdb-storage"))
}
