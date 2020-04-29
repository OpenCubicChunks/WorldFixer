plugins {
    java
    application
    id("com.github.johnrengelman.shadow").version("4.0.3")
}

group = "io.github.opencubicchunks"
version = "1.2-SNAPSHOT"

application {
    mainClassName = "io.github.opencubicchunks.worldfixer.Main"
}

repositories {
    mavenCentral()
    maven {
        setUrl("https://oss.sonatype.org/content/repositories/public/")
    }
}
dependencies {
    testCompile("junit", "junit", "4.12")
    compile("io.github.opencubicchunks:regionlib:0.61.0-SNAPSHOT")
    compile("net.kyori:nbt:1.12-1.0.0-SNAPSHOT")
    compile("org.fusesource.jansi:jansi:1.18")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks["build"].dependsOn(tasks["shadowJar"])