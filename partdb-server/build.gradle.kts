plugins {
    `java-library`
}

dependencies {
    implementation(project(":partdb-consensus"))
    implementation(project(":partdb-node"))
    implementation(project(":partdb-transport-grpc"))
}
