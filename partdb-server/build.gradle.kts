dependencies {
    implementation(project(":partdb-common"))
    implementation(project(":partdb-storage"))
    implementation(project(":partdb-raft"))
    implementation(project(":partdb-protocol"))

    val grpcVersion = "1.75.0"
    val protobufVersion = "4.33.1"

    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}
