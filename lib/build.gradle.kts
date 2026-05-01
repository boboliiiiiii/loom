plugins {
    kotlin("jvm")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(24) }
}
kotlin { jvmToolchain(24) }

dependencies {
    compileOnly(kotlin("stdlib"))
    compileOnly("org.msgpack:msgpack-core:0.9.10")
}

tasks.jar {
    archiveBaseName.set("loom")
    archiveVersion.set("0.1.0")
}
