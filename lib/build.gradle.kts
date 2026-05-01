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

    testImplementation(kotlin("stdlib"))
    testImplementation("org.msgpack:msgpack-core:0.9.10")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging { events("passed", "failed", "skipped") }
}

tasks.jar {
    archiveBaseName.set("loom")
    archiveVersion.set("0.1.0")
}
