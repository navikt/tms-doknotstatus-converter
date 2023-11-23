import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    kotlin("jvm").version(Kotlin.version)
    id(Shadow.pluginId) version (Shadow.version)
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
    maven("https://maven.pkg.github.com/navikt/*") {
        credentials {
            username = System.getenv("GITHUB_ACTOR")?: "x-access-token"
            password = System.getenv("GITHUB_TOKEN")?: project.findProperty("githubPassword") as String
        }
    }
    mavenLocal()
}

dependencies {
    implementation(Doknotifikasjon.schemas)
    implementation(TmsCommonLib.utils)
    implementation(Avro.avroSerializer)
    implementation(Ktor.Server.netty)
    implementation(Logstash.logbackEncoder)

    testImplementation(kotlin("test"))
    testImplementation(Kotest.assertionsCore)
    testImplementation(Kotest.runnerJunit5)
}

application {
    mainClass.set("no.nav.tms.doknotstatus.converter.ApplicationKt")
}

tasks {
    test {
        useJUnitPlatform()
        testLogging {
            showExceptions = true
            showStackTraces = true
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
    }
}

apply(plugin = Shadow.pluginId)
