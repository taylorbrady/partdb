plugins {
    id("partdb.java-conventions")
    `java-library`
    `java-test-fixtures`
}

dependencies {
    api(project(":partdb-bytes"))
    implementation(project(":partdb-grpc"))

    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.netty)
    implementation(libs.protobuf.java)
}
