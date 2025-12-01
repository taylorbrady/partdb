plugins {
    application
}

dependencies {
    implementation(project(":partdb-common"))
    implementation(project(":partdb-client"))
}

application {
    mainClass.set("io.partdb.ctl.PartDbCtl")
}
