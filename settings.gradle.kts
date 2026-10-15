pluginManagement {
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "partdb"

include(
    "partdb-bytes",
    "partdb-consensus",
    "partdb-node",
    "partdb-storage",
    "partdb-raft",
    "partdb-grpc",
    "partdb-transport-grpc",
    "partdb-server",
    "partdb-client",
    "partdb-app",
    "partdb-benchmarks"
)
