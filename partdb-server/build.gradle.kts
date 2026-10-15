plugins {
    id("partdb.java-conventions")
    `java-library`
}

dependencies {
    implementation(project(":partdb-consensus"))
    implementation(project(":partdb-node"))
    implementation(project(":partdb-transport-grpc"))

    implementation(libs.slf4j.api)
}
