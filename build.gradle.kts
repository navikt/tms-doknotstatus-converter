import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    kotlin("jvm").version(managed.versions.kotlin)

    id("no.nav.tms.jarBundling") version "0.1.0"
    application
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
    maven {
        url = uri("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }
    mavenLocal()
}

dependencies {

    implementation(managed.tms.commonLib.utils)
    implementation(managed.tms.commonLib.observability)
    implementation(managed.tms.commonLib.teamLogger)
    implementation(managed.ktor.server.netty)
    implementation(managed.kotlinLogging)
    implementation(managed.prometheus.metric.core)
    implementation(managed.prometheus.exporter)
    implementation(managed.tms.kafka.producerUtils)
    implementation(managed.logback.encoder)
    implementation(libs.avro.serializer)
    implementation(libs.nav.doknotifikasjon)

    testImplementation(kotlin("test"))
    testImplementation(managed.bundles.kotest)
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
