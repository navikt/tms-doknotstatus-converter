import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    kotlin("jvm").version(Kotlin.version)

    id(TmsJarBundling.plugin)

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
    implementation(Doknotifikasjon.schemas)
    implementation(TmsCommonLib.utils)
    implementation(Avro.avroSerializer)
    implementation(Ktor.Server.netty)
    implementation(KotlinLogging.logging)
    implementation(Prometheus.metricsCore)
    implementation(Prometheus.exporterCommon)
    implementation(TmsCommonLib.observability)
    implementation(TmsCommonLib.teamLogger)
    implementation(Logstash.logbackEncoder)
    implementation(TmsKafkaTools.kafkaProducerUtils)


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
