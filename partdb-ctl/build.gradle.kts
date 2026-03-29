plugins {
    application
}

dependencies {
    implementation(project(":partdb-client"))
    implementation(project(":partdb-protocol"))
}

application {
    mainClass.set("io.partdb.ctl.PartDbCtl")
}
