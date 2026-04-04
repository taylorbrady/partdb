plugins {
    `java-library`
}

dependencies {
    implementation(project(":partdb-node"))
    implementation(project(":partdb-transport-grpc"))
}
