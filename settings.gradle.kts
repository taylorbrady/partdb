pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "partdb"

include(
    "partdb-bytes",
    "partdb-cluster",
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
