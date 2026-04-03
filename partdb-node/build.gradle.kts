plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    implementation(project(":partdb-consensus"))
    implementation(project(":partdb-storage"))
}
