plugins {
    application
}

dependencies {
    implementation(project(":partdb-common"))
    implementation(project(":partdb-server"))
}

application {
    mainClass.set("io.partdb.cli.PartDbCli")
}
