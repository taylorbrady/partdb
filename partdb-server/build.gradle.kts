plugins {
    `java-library`
}

dependencies {
    implementation(project(":partdb-consensus"))
    implementation(project(":partdb-node"))
    implementation(project(":partdb-transport-grpc"))

    implementation("org.slf4j:slf4j-api:2.0.17")
}
