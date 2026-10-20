plugins {
    id("partdb.java-conventions")
    id("partdb.jmh-conventions")
}

dependencies {
    jmh(project(":partdb-storage"))
}
