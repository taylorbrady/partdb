dependencies {
    implementation(project(":partdb-common"))
    implementation(project(":partdb-protocol"))

    val grpcVersion = "1.75.0"
    val protobufVersion = "4.33.1"

    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}
