plugins {
    id("partdb.java-conventions")
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    api(project(":partdb-consensus"))
    implementation(project(":partdb-storage"))
}
