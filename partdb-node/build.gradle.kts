plugins {
    id("com.google.protobuf")
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    implementation(project(":partdb-storage"))
    implementation(project(":partdb-raft"))

    val protobufVersion = "4.34.0"

    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.34.0"
    }
}
