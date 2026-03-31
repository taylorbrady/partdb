plugins {
    `java-library`
}

dependencies {
    api(project(":partdb-bytes"))
    implementation(project(":partdb-grpc"))

    val grpcVersion = "1.80.0"
    val protobufVersion = "4.34.0"

    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}
