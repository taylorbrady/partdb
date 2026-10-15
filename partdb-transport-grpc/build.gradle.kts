plugins {
    id("partdb.java-conventions")
    `java-library`
    alias(libs.plugins.protobuf)
}

dependencies {
    implementation(project(":partdb-node"))
    implementation(project(":partdb-consensus"))
    api(project(":partdb-raft"))
    implementation(project(":partdb-grpc"))

    implementation(libs.grpc.netty)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.protobuf.java)
    implementation(libs.slf4j.api)
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
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}
