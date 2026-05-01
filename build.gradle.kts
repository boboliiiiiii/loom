plugins {
    kotlin("jvm") version "2.2.0" apply false
}

allprojects {
    group = "llc.ethereal"
    version = "0.1.0"

    repositories {
        mavenCentral()
        maven("https://repo.papermc.io/repository/maven-public/")
    }
}
