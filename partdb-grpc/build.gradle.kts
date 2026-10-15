plugins {
    id("partdb.java-conventions")
    `java-library`
    alias(libs.plugins.protobuf)
}

dependencies {
    api(libs.grpc.stub)
    api(libs.grpc.protobuf)
    api(libs.protobuf.java)
}

val protocArtifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
val grpcPluginArtifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"

protobuf {
    protoc {
        artifact = protocArtifact
    }
    plugins {
        create("grpc") {
            artifact = grpcPluginArtifact
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
