plugins {
    id("com.google.protobuf")
    `java-library`
}

dependencies {
    implementation(project(":partdb-node"))
    api(project(":partdb-raft"))
    implementation(project(":partdb-grpc"))

    val grpcVersion = "1.80.0"
    val protobufVersion = "4.34.0"

    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("org.slf4j:slf4j-api:2.0.17")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.34.0"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.80.0"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}
