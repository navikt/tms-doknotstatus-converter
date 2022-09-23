package no.nav.tms.dokstatus.mottak

import no.nav.personbruker.dittnav.common.util.config.StringEnvVar.getEnvVar

data class Environment(
        val applicationName: String = "tms-doknotstatus-converter",
        val aivenBrokers: String = getEnvVar("KAFKA_BROKERS"),
        val aivenSchemaRegistry: String = getEnvVar("KAFKA_SCHEMA_REGISTRY"),
        val doknotifikasjonStatusGroupId: String = getEnvVar("GROUP_ID_DOKNOTIFIKASJON_STATUS"),
        val doknotifikasjonStatusTopicName: String = getEnvVar("DOKNOTIFIKASJON_STATUS_TOPIC"),
)


data class SecurityVars(
        val aivenTruststorePath: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
        val aivenKeystorePath: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
        val aivenCredstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
        val aivenSchemaRegistryUser: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_USER"),
        val aivenSchemaRegistryPassword: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_PASSWORD")
)
