plugins {
    id("com.google.protobuf")
    `java-library`
}

dependencies {
    implementation(project(":partdb-storage"))
    api(project(":partdb-raft"))

    val protobufVersion = "4.33.1"

    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.33.1"
    }
}
