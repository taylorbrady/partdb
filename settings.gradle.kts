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
    "partdb-node",
    "partdb-storage",
    "partdb-raft",
    "partdb-grpc",
    "partdb-server",
    "partdb-client",
    "partdb-app",
    "partdb-benchmarks"
)
