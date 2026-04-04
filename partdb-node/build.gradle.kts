plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-cluster"))
    implementation(project(":partdb-consensus"))
    implementation(project(":partdb-storage"))
}
