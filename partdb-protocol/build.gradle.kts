plugins {
    id("com.google.protobuf")
}

dependencies {
    val grpcVersion = "1.68.1"
    val protobufVersion = "4.29.1"

    implementation(project(":partdb-common"))

    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    runtimeOnly("io.grpc:grpc-netty-shaded:$grpcVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.29.1"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.68.1"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
            }
        }
    }
}
